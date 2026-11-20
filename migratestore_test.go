package scs

import (
	"context"
	"errors"

	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"

	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// migrationStore is a controllable in-memory store used by the migration
// tests. It can be made to fail or block writes so that the failure and
// concurrency paths can be exercised deterministically.
type migrationStore struct {
	mu      sync.Mutex
	items   map[string]migrationItem
	commits int
	deletes int
	finds   int

	commitErr   error
	deleteErr   error
	findErr     error
	blockCommit chan struct{}

	// failFindAt makes the FindCtx call with the given (1-indexed) ordinal
	// return findErr, to simulate a verification read failing.
	failFindAt int

	// dropCommits makes CommitCtx report success without storing anything, to
	// simulate a store whose writes silently fail to become visible.
	dropCommits bool
}

type migrationItem struct {
	b      []byte
	expiry time.Time
}

func newMigrationStore() *migrationStore {
	return &migrationStore{items: make(map[string]migrationItem)}
}

func (m *migrationStore) gate(ctx context.Context) error {
	m.mu.Lock()
	block := m.blockCommit
	m.mu.Unlock()
	if block == nil {
		return nil
	}
	select {
	case <-block:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *migrationStore) block() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blockCommit == nil {
		m.blockCommit = make(chan struct{})
	}
}

func (m *migrationStore) unblock() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blockCommit != nil {
		close(m.blockCommit)
		m.blockCommit = nil
	}
}

func (m *migrationStore) FindCtx(ctx context.Context, token string) ([]byte, bool, error) {
	if err := m.gate(ctx); err != nil {
		return nil, false, err
	}

	m.mu.Lock()
	m.finds++
	findErr := m.findErr
	failFindAt := m.failFindAt
	m.mu.Unlock()

	if findErr != nil {
		return nil, false, findErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if failFindAt > 0 && m.finds == failFindAt {
		return nil, false, errors.New("find failed")
	}
	it, ok := m.items[token]
	if !ok || !it.expiry.After(time.Now()) {
		return nil, false, nil
	}
	return it.b, true, nil
}

func (m *migrationStore) CommitCtx(ctx context.Context, token string, b []byte, expiry time.Time) error {
	if err := m.gate(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	m.commits++
	err := m.commitErr
	drop := m.dropCommits
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if drop {
		return nil
	}

	m.mu.Lock()
	m.items[token] = migrationItem{b: append([]byte(nil), b...), expiry: expiry}
	m.mu.Unlock()
	return nil
}

func (m *migrationStore) DeleteCtx(ctx context.Context, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletes++
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.items, token)
	return nil
}

func (m *migrationStore) setCommitErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitErr = err
}

func (m *migrationStore) setDropCommits() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropCommits = true
}

func (m *migrationStore) setFailFindAt(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failFindAt = n
}

func (m *migrationStore) setDeleteErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteErr = err
}

// The plain Store methods are unused by these tests, but implementing them
// keeps migrationStore usable as a plain Store as well as a CtxStore.
func (m *migrationStore) Find(token string) ([]byte, bool, error) {
	return m.FindCtx(context.Background(), token)
}

func (m *migrationStore) Commit(token string, b []byte, expiry time.Time) error {
	return m.CommitCtx(context.Background(), token, b, expiry)
}

func (m *migrationStore) Delete(token string) error {
	return m.DeleteCtx(context.Background(), token)
}

func (m *migrationStore) get(token string) (migrationItem, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	it, ok := m.items[token]
	return it, ok
}

func (m *migrationStore) AllCtx(ctx context.Context) (map[string][]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	all := make(map[string][]byte, len(m.items))
	for token, item := range m.items {
		all[token] = item.b
	}
	return all, nil
}

func (m *migrationStore) All() (map[string][]byte, error) {
	return m.AllCtx(context.Background())
}

func (m *migrationStore) set(token string, b []byte, expiry time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[token] = migrationItem{b: b, expiry: expiry}
}

func (m *migrationStore) counts() (commits, finds, deletes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commits, m.finds, m.deletes
}

func encodeTestSession(t *testing.T, s *SessionManager, deadline time.Time, values map[string]interface{}) []byte {
	t.Helper()
	b, err := s.Codec.Encode(deadline, values)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}
	return b
}

func sessionValues(t *testing.T, s *SessionManager, b []byte) map[string]interface{} {
	t.Helper()
	_, values, err := s.Codec.Decode(b)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	return values
}

func TestMigratingStoreMigratesLegacySession(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	expiry := deadline
	values := map[string]interface{}{"foo": "bar"}
	source.set("legacy-token", encodeTestSession(t, s, deadline, values), expiry)

	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}

	if got := s.GetString(ctx, "foo"); got != "bar" {
		t.Errorf("want %q; got %q", "bar", got)
	}

	migrated, ok := target.get("legacy-token")
	if !ok {
		t.Fatal("session was not copied to the target store")
	}
	if got := sessionValues(t, s, migrated.b)["foo"]; got != "bar" {
		t.Errorf("want %q in target store; got %v", "bar", got)
	}

	// The legacy copy must be kept so that the migration can be rolled back.
	if _, ok := source.get("legacy-token"); !ok {
		t.Error("legacy session was removed from the source store")
	}

	// The session is migrated, not modified: a read-only request must not
	// write to the target store a second time.
	if commits, _, _ := target.counts(); commits != 1 {
		t.Errorf("want exactly one commit to the target store; got %d", commits)
	}

	if s.Status(ctx) != Unmodified {
		t.Errorf("want status Unmodified; got %v", s.Status(ctx))
	}
}

func TestMigratingStoreSendsCookieAfterMigration(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	var handlerErr error
	var handlerToken string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerToken = s.Token(r.Context())
		if got := s.GetString(r.Context(), "foo"); got != "bar" {
			handlerErr = errors.New("session value not loaded")
		}
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: "legacy-token"})
	s.LoadAndSave(handler).ServeHTTP(rec, req)

	if handlerErr != nil {
		t.Fatal(handlerErr)
	}
	if handlerToken != "legacy-token" {
		t.Errorf("want token %q; got %q", "legacy-token", handlerToken)
	}

	cookies := rec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("want one session cookie after migration; got %d", len(cookies))
	}
	if cookies[0].Name != s.Cookie.Name || cookies[0].Value != "legacy-token" {
		t.Errorf("want cookie %s=legacy-token; got %s=%s", s.Cookie.Name, cookies[0].Name, cookies[0].Value)
	}
	if _, ok := target.get("legacy-token"); !ok {
		t.Error("session was not migrated to the target store")
	}
}

func TestMigratingStoreFailuresKeepLegacySession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(*migrationStore)
	}{
		{
			name:  "commit error",
			setup: func(m *migrationStore) { m.setCommitErr(errors.New("commit failed")) },
		},
		{
			name:  "find error while verifying",
			setup: func(m *migrationStore) { m.setFailFindAt(2) },
		},
		{
			name:  "silently dropped write",
			setup: func(m *migrationStore) { m.setDropCommits() },
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := newMigrationStore()
			target := newMigrationStore()

			s := New()
			s.Store = NewMigratingStore(source, target)

			deadline := time.Now().Add(time.Hour).UTC()
			source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

			tt.setup(target)

			var handlerSawValue bool
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlerSawValue = s.GetString(r.Context(), "foo") == "bar"
				// Modifying the session must be persisted to the legacy store
				// rather than failing just because the target is unavailable.
				s.Put(r.Context(), "baz", "qux")
			})

			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/", nil)
			req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: "legacy-token"})
			s.LoadAndSave(handler).ServeHTTP(rec, req)

			if !handlerSawValue {
				t.Error("handler did not receive the legacy session data")
			}

			sourceItem := source.getMust(t, "legacy-token")
			if got := sessionValues(t, s, sourceItem.b)["baz"]; got != "qux" {
				t.Errorf("session modification was not written to the source store; got %v", got)
			}

			// No session for the failed migration may be left in the target.
			if _, ok := target.get("legacy-token"); ok {
				if tt.name == "silently dropped write" {
					// The write was dropped by the store itself; nothing to assert.
					return
				}
				t.Error("failed migration left a copy in the target store")
			}
		})
	}
}

func (m *migrationStore) getMust(t *testing.T, token string) migrationItem {
	t.Helper()
	it, ok := m.get(token)
	if !ok {
		t.Fatalf("token %q not found in store", token)
	}
	return it
}

func TestMigratingStoreConcurrentMigrationWritesOnce(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	const requests = 25
	var wg sync.WaitGroup
	var failures int32

	start := make(chan struct{})
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			ctx, err := s.Load(context.Background(), "legacy-token")
			if err != nil {
				atomic.AddInt32(&failures, 1)
				return
			}
			if s.GetString(ctx, "foo") != "bar" {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if failures != 0 {
		t.Errorf("want no failures; got %d", failures)
	}

	if commits, _, _ := target.counts(); commits != 1 {
		t.Errorf("want exactly one commit to the target store; got %d", commits)
	}
}

func TestMigratingStoreConcurrentRequestReusesCommittedCopy(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)
	s := New()
	s.Store = ms

	deadline := time.Now().Add(time.Hour).UTC()

	// Another process has already migrated a *different* copy of the session.
	// A slow request which read the source store before that must not overwrite
	// the newer copy.
	newer := encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "new"})
	target.set("legacy-token", newer, deadline)

	older := encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "old"})
	committed, err := ms.MigrateCtx(context.Background(), "legacy-token", older, deadline)
	if err != nil {
		t.Fatalf("unexpected migration error: %v", err)
	}
	if got := sessionValues(t, s, committed)["foo"]; got != "new" {
		t.Errorf("want the committed copy to be reused; got %v", got)
	}

	it, ok := target.get("legacy-token")
	if !ok {
		t.Fatal("target session is missing")
	}
	if got := sessionValues(t, s, it.b)["foo"]; got != "new" {
		t.Errorf("target store was overwritten with older data; got %v", got)
	}
}

func TestMigratingStoreRenewTokenAndDestroyBothStores(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)
	s := New()
	s.Store = ms

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}

	if err := s.RenewToken(ctx); err != nil {
		t.Fatalf("unexpected renew error: %v", err)
	}
	newToken, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatalf("unexpected commit error: %v", err)
	}

	if _, found, _ := ms.Find(newToken); !found {
		t.Fatal("renewed token was not committed to the target store")
	}
	if _, found, _ := source.Find(newToken); !found {
		t.Fatal("renewed token was not committed to the source store")
	}

	// The old token must be gone from both stores.
	if _, found, _ := target.Find("legacy-token"); found {
		t.Error("old token was not deleted from the target store")
	}
	if _, found, _ := source.Find("legacy-token"); found {
		t.Error("old token was not deleted from the source store")
	}

	// Destroy must also remove the session from both stores so that a rollback
	// cannot resurrect it.
	destroyCtx, err := s.Load(context.Background(), newToken)
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if err := s.Destroy(destroyCtx); err != nil {
		t.Fatalf("unexpected destroy error: %v", err)
	}
	if _, found, _ := target.Find(newToken); found {
		t.Error("destroyed session is still present in the target store")
	}
	if _, found, _ := source.Find(newToken); found {
		t.Error("destroyed session is still present in the source store")
	}
}

func TestMigratingStoreRollbackShadowsPartialData(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)
	s := New()
	s.Store = ms

	deadline := time.Now().Add(time.Hour).UTC()
	legacy := encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "legacy"})
	source.set("legacy-token", legacy, deadline)

	// Simulate a partial migration: the target store has an incomplete copy of
	// the session.
	target.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "partial"}), deadline)

	ms.Rollback()

	if _, err := ms.MigrateCtx(context.Background(), "legacy-token", legacy, deadline); !errors.Is(err, ErrMigrationRolledBack) {
		t.Errorf("want ErrMigrationRolledBack; got %v", err)
	}

	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if got := s.GetString(ctx, "foo"); got != "legacy" {
		t.Errorf("want the legacy value %q after rollback; got %q", "legacy", got)
	}

	// Writes after a rollback must go to the source store, so that data is not
	// stranded in an abandoned target store.
	s.Put(ctx, "baz", "qux")
	if _, _, err := s.Commit(ctx); err != nil {
		t.Fatalf("unexpected commit error: %v", err)
	}

	if got := sessionValues(t, s, source.getMust(t, "legacy-token").b)["baz"]; got != "qux" {
		t.Errorf("commit after rollback did not reach the source store; got %v", got)
	}
}

func TestMigratingStoreCloseReleasesWaiters(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	b := []byte("session")

	source.set("token", b, deadline)

	// Hold the migration open so that a second caller has to wait for it.
	target.block()

	firstDone := make(chan error, 1)
	go func() {
		_, err := ms.MigrateCtx(context.Background(), "token", b, deadline)
		firstDone <- err
	}()

	// Wait until the first caller has registered its migration.
	for {
		ms.mu.Lock()
		_, inFlight := ms.migrations["token"]
		ms.mu.Unlock()
		if inFlight {
			break
		}
		time.Sleep(time.Millisecond)
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := ms.MigrateCtx(context.Background(), "token", b, deadline)
		secondDone <- err
	}()

	// Wait until the second caller is waiting for the first.
	time.Sleep(10 * time.Millisecond)

	if err := ms.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	select {
	case err := <-secondDone:
		if !errors.Is(err, ErrMigratingStoreClosed) {
			t.Errorf("want ErrMigratingStoreClosed; got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting caller was not released by Close")
	}

	target.unblock()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Errorf("unexpected error from in-flight migration: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight migration did not finish")
	}
}

func TestMigratingStoreContextCancellation(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	b := []byte("session")

	source.set("token", b, deadline)

	// Hold the migration open, then cancel the waiting caller's context.
	target.block()

	firstDone := make(chan struct{})
	go func() {
		_, _ = ms.MigrateCtx(context.Background(), "token", b, deadline)
		close(firstDone)
	}()

	for {
		ms.mu.Lock()
		_, inFlight := ms.migrations["token"]
		ms.mu.Unlock()
		if inFlight {
			break
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := ms.MigrateCtx(ctx, "token", b, deadline)
		secondDone <- err
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("want context.Canceled; got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting caller did not observe context cancellation")
	}

	target.unblock()
	<-firstDone
}

func TestMigratingStoreCancelledMigrationCanBeRetaken(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	b := []byte("session")

	source.set("token", b, deadline)

	// The first caller is blocked and then cancelled while it owns the
	// migration. A second caller with a live context must still be able to
	// migrate the token rather than inheriting the cancellation.
	target.block()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		_, _ = ms.MigrateCtx(firstCtx, "token", b, deadline)
		close(firstDone)
	}()

	for {
		ms.mu.Lock()
		_, inFlight := ms.migrations["token"]
		ms.mu.Unlock()
		if inFlight {
			break
		}
		time.Sleep(time.Millisecond)
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := ms.MigrateCtx(context.Background(), "token", b, deadline)
		secondDone <- err
	}()

	time.Sleep(10 * time.Millisecond)
	cancelFirst()
	<-firstDone

	// The second caller takes over the migration once the first has released
	// it, so unblock the target store.
	target.unblock()

	select {
	case err := <-secondDone:
		if err != nil {
			t.Errorf("unexpected error from the second caller: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second caller did not complete the migration")
	}

	if _, ok := target.get("token"); !ok {
		t.Error("session was not migrated after the first caller was cancelled")
	}
}

func TestMigratingStoreHashTokenInStore(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.HashTokenInStore = true
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	hashed := hashToken("legacy-token")
	source.set(hashed, encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if got := s.GetString(ctx, "foo"); got != "bar" {
		t.Errorf("want %q; got %q", "bar", got)
	}
	if _, ok := target.get(hashed); !ok {
		t.Fatal("session was not migrated under the hashed token")
	}
	if _, ok := target.get("legacy-token"); ok {
		t.Error("session was stored under the raw token")
	}
}

func TestMigratingStoreExpiryConsistentAcrossStores(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}

	s.SetDeadline(ctx, time.Now().Add(2*time.Hour))
	want := s.Expiry(ctx)

	if _, _, err := s.Commit(ctx); err != nil {
		t.Fatalf("unexpected commit error: %v", err)
	}

	sourceItem := source.getMust(t, "legacy-token")
	targetItem := target.getMust(t, "legacy-token")
	if !sourceItem.expiry.Equal(targetItem.expiry) {
		t.Errorf("expiry differs between stores: source %v, target %v", sourceItem.expiry, targetItem.expiry)
	}
	if !targetItem.expiry.Equal(want) {
		t.Errorf("want target expiry %v; got %v", want, targetItem.expiry)
	}
}

func TestMigratingStoreIterateMergesBothStores(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()

	source.set("both", encodeTestSession(t, s, deadline, map[string]interface{}{"v": "source"}), deadline)
	source.set("legacy", encodeTestSession(t, s, deadline, map[string]interface{}{"v": "legacy"}), deadline)
	// A partial migration in the target store must not hide the source copy in
	// forward mode, because the target copy is the newer one there.
	target.set("both", encodeTestSession(t, s, deadline, map[string]interface{}{"v": "target"}), deadline)

	var seen = map[string]string{}
	err := s.Iterate(context.Background(), func(ctx context.Context) error {
		seen[s.Token(ctx)] = s.GetString(ctx, "v")
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected iterate error: %v", err)
	}

	if len(seen) != 2 {
		t.Fatalf("want 2 sessions; got %d (%v)", len(seen), seen)
	}
	if seen["both"] != "target" {
		t.Errorf("forward mode should prefer the target copy; got %q", seen["both"])
	}
	if seen["legacy"] != "legacy" {
		t.Errorf("want legacy session to be visible; got %q", seen["legacy"])
	}

	ms := s.Store.(*MigratingStore)
	ms.Rollback()

	seen = map[string]string{}
	if err := s.Iterate(context.Background(), func(ctx context.Context) error {
		seen[s.Token(ctx)] = s.GetString(ctx, "v")
		return nil
	}); err != nil {
		t.Fatalf("unexpected iterate error: %v", err)
	}
	if seen["both"] != "source" {
		t.Errorf("after rollback the source copy should win; got %q", seen["both"])
	}
}

func TestMigratingStoreCommittedCookieOnlyOnce(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	reads := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads++
	})

	// The first request to the legacy token migrates it and returns a cookie.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: "legacy-token"})
	s.LoadAndSave(handler).ServeHTTP(rec, req)

	if got := rec.Result().Header.Get("Set-Cookie"); got == "" {
		t.Fatal("expected a session cookie for the migrated session")
	}

	// Once the session lives in the target store, later requests are ordinary
	// read-only requests and must not return another cookie.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: "legacy-token"})
	s.LoadAndSave(handler).ServeHTTP(rec, req)

	if got := rec.Result().Header.Get("Set-Cookie"); got != "" {
		t.Errorf("want no cookie for an already-migrated session; got %q", got)
	}

	if reads != 2 {
		t.Errorf("want handler called twice; got %d", reads)
	}
	if commits, _, _ := target.counts(); commits != 1 {
		t.Errorf("want the session to be written to the target store once; got %d commits", commits)
	}
}

func TestMigratingStoreDoesNotCommitMigratedSessionAgain(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Modify the session after the migration. This must overwrite both
		// stores with the updated data, not just leave the migrated copy.
		s.Put(r.Context(), "foo", "baz")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: "legacy-token"})
	s.LoadAndSave(handler).ServeHTTP(rec, req)

	targetItem := target.getMust(t, "legacy-token")
	if got := sessionValues(t, s, targetItem.b)["foo"]; got != "baz" {
		t.Errorf("want updated value in target store; got %v", got)
	}
	sourceItem := source.getMust(t, "legacy-token")
	if got := sessionValues(t, s, sourceItem.b)["foo"]; got != "baz" {
		t.Errorf("want updated value in source store; got %v", got)
	}
}

func TestMigratingStoreDestroyedSessionNotResurrectedByRollback(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)
	s := New()
	s.Store = ms

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if err := s.Destroy(ctx); err != nil {
		t.Fatalf("unexpected destroy error: %v", err)
	}

	ms.Rollback()

	if _, found, _ := source.Find("legacy-token"); found {
		t.Error("destroyed session is still present in the source store after rollback")
	}

	newCtx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if v := s.Get(newCtx, "foo"); v != nil {
		t.Errorf("destroyed session was resurrected; got %v", v)
	}
}

func TestNewMigrating(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := NewMigrating(source, target)
	ms, ok := s.Store.(*MigratingStore)
	if !ok {
		t.Fatal("Store is not a MigratingStore")
	}
	if ms.Source() != source {
		t.Error("unexpected source store")
	}
	if ms.Target() != target {
		t.Error("unexpected target store")
	}
}

func TestMigratingStoreSourceOnlyAfterFailure(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	target.setCommitErr(errors.New("target unavailable"))

	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}

	// The request must continue with the old session, and its modifications
	// must be persisted to the source store without touching the failing target.
	s.Put(ctx, "baz", "qux")
	if _, _, err := s.Commit(ctx); err != nil {
		t.Fatalf("unexpected commit error: %v", err)
	}

	if got := sessionValues(t, s, source.getMust(t, "legacy-token").b)["baz"]; got != "qux" {
		t.Errorf("modification was not persisted to the source store; got %v", got)
	}
}

func TestMigratingStoreIdleTimeoutRefreshMirroredBothStores(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.IdleTimeout = time.Hour
	s.Store = NewMigratingStore(source, target)

	// The legacy session is close to its idle expiry. Loading it refreshes the
	// idle expiry, and the refreshed expiry must be present in both stores.
	deadline := time.Now().Add(24 * time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), time.Now().Add(time.Minute))

	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}

	sourceItem := source.getMust(t, "legacy-token")
	targetItem := target.getMust(t, "legacy-token")
	if !sourceItem.expiry.Equal(targetItem.expiry) {
		t.Errorf("expiry differs between stores: source %v, target %v", sourceItem.expiry, targetItem.expiry)
	}
	if want := s.Expiry(ctx); !targetItem.expiry.Equal(want) {
		t.Errorf("want migrated expiry %v; got %v", want, targetItem.expiry)
	}
	if !targetItem.expiry.After(time.Now().Add(30 * time.Minute)) {
		t.Errorf("idle expiry was not refreshed during migration; got %v", targetItem.expiry)
	}
}

func TestMigratingStoreRollbackIgnoresTargetOnlySession(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)
	s := New()
	s.Store = ms

	deadline := time.Now().Add(time.Hour).UTC()

	// A session which exists only in the target store was created during the
	// switch. After a rollback it must not be used as a substitute for a legacy
	// session the source store no longer has (for example one which expired
	// there), otherwise a stale shadow could outlive the real session.
	target.set("token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "target"}), deadline)

	ms.Rollback()

	ctx, err := s.Load(context.Background(), "token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if v := s.Get(ctx, "foo"); v != nil {
		t.Errorf("target-only session was used after rollback; got %v", v)
	}
}

func TestMigratingStoreWritesDuringMigrationWindow(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)
	s := New()
	s.Store = ms

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	// Migrate the session, then update it in the same "window" while the source
	// copy is still present. Both stores must end up with the new data and the
	// new expiry.
	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	s.Put(ctx, "foo", "baz")

	newExpiry := time.Now().Add(30 * time.Minute)
	s.SetDeadline(ctx, newExpiry)
	if _, _, err := s.Commit(ctx); err != nil {
		t.Fatalf("unexpected commit error: %v", err)
	}

	for name, store := range map[string]*migrationStore{"source": source, "target": target} {
		item := store.getMust(t, "legacy-token")
		if got := sessionValues(t, s, item.b)["foo"]; got != "baz" {
			t.Errorf("%s store has stale data: %v", name, got)
		}
		if !item.expiry.Equal(newExpiry) && item.expiry.Before(newExpiry) {
			t.Errorf("%s store has older expiry %v than %v", name, item.expiry, newExpiry)
		}
	}
}

func TestMigratingStoreDeleteErrorsLeaveTokenInBothStores(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("token", []byte("source"), deadline)
	target.set("token", []byte("target"), deadline)

	// The target store cannot delete. The source copy must still be removed,
	// otherwise the old token would stay valid in the store which is still
	// serving legacy sessions.
	target.setDeleteErr(errors.New("target unavailable"))

	if err := ms.Delete("token"); err == nil {
		t.Fatal("expected an error from Delete")
	}
	if _, found, _ := source.Find("token"); found {
		t.Error("source copy was left behind while the target delete failed")
	}

	// Once the store can delete, both copies are removed.
	target.setDeleteErr(nil)
	if err := ms.Delete("token"); err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}
	if _, found, _ := source.Find("token"); found {
		t.Error("source copy was not deleted")
	}
	if _, found, _ := target.Find("token"); found {
		t.Error("target copy was not deleted")
	}
}

func TestMigratingStoreRollbackIsIdempotentAndCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)
	ms.Rollback()
	ms.Rollback()
	if !ms.RolledBack() {
		t.Error("RolledBack should report true after Rollback")
	}

	if err := ms.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}
	if err := ms.Close(); err != nil {
		t.Fatalf("unexpected second close error: %v", err)
	}
	if _, _, err := ms.Find("token"); !errors.Is(err, ErrMigratingStoreClosed) {
		t.Errorf("want ErrMigratingStoreClosed after Close; got %v", err)
	}
}

type closableStore struct {
	migrationStore
	closed    int32
	stopCalls int32
}

func (c *closableStore) Close() error {
	atomic.AddInt32(&c.closed, 1)
	return nil
}

func (c *closableStore) StopCleanup() {
	atomic.AddInt32(&c.stopCalls, 1)
}

func TestMigratingStoreCloseReleasesBothStores(t *testing.T) {
	t.Parallel()

	source := &closableStore{migrationStore: *newMigrationStore()}
	target := &closableStore{migrationStore: *newMigrationStore()}

	ms := NewMigratingStore(source, target)
	if err := ms.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	if got := atomic.LoadInt32(&source.closed); got != 1 {
		t.Errorf("want source Close called once; got %d", got)
	}
	if got := atomic.LoadInt32(&target.closed); got != 1 {
		t.Errorf("want target Close called once; got %d", got)
	}
	if got := atomic.LoadInt32(&source.stopCalls); got != 0 {
		t.Errorf("StopCleanup should not be called when Close is available; got %d", got)
	}
}

func TestMigratingStoreDoesNotStartGoroutines(t *testing.T) {
	source := newMigrationStore()
	target := newMigrationStore()

	before := runtime.NumGoroutine()
	ms := NewMigratingStore(source, target)
	defer ms.Close()

	time.Sleep(20 * time.Millisecond)

	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("MigratingStore started %d goroutine(s)", after-before)
	}
}

func TestMigratingStoreConcurrentMutationsDuringMigration(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	const workers = 20
	var wg sync.WaitGroup
	var migrated int32
	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			ctx, err := s.Load(context.Background(), "legacy-token")
			if err != nil {
				t.Errorf("unexpected load error: %v", err)
				return
			}
			if s.GetString(ctx, "foo") != "bar" {
				t.Errorf("worker %d did not see the migrated session", i)
				return
			}
			if s.Token(ctx) == "" {
				t.Errorf("worker %d has an empty token", i)
				return
			}
			atomic.AddInt32(&migrated, 1)
		}()
	}

	close(start)
	wg.Wait()

	if migrated != workers {
		t.Errorf("want %d successful migrations; got %d", workers, migrated)
	}
	if commits, _, _ := target.counts(); commits != 1 {
		t.Errorf("want exactly one target commit; got %d", commits)
	}

	it := target.getMust(t, "legacy-token")
	if got := sessionValues(t, s, it.b)["foo"]; got != "bar" {
		t.Errorf("want %q in target; got %v", "bar", got)
	}
}

func TestMigratingStoreFailedMigrationSendsNoCookie(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	target.setCommitErr(errors.New("target unavailable"))

	var sawValue bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawValue = s.GetString(r.Context(), "foo") == "bar"
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: "legacy-token"})
	s.LoadAndSave(handler).ServeHTTP(rec, req)

	if !sawValue {
		t.Error("handler did not receive the old session")
	}
	if got := rec.Result().Header.Get("Set-Cookie"); got != "" {
		t.Errorf("want no cookie when the migration fails; got %q", got)
	}
	if _, ok := target.get("legacy-token"); ok {
		t.Error("failed migration left data in the target store")
	}
	if _, ok := source.get("legacy-token"); !ok {
		t.Error("old session was removed from the source store")
	}
}

func TestMigratingStoreTargetReadFailureFallsBackToSource(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), deadline)

	// The target store cannot be read at all. Requests must still be served
	// from the source store rather than failing.
	target.setFailFindAt(1)

	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if got := s.GetString(ctx, "foo"); got != "bar" {
		t.Errorf("want %q; got %q", "bar", got)
	}
}

func TestMigratingStoreHashTokenAfterRollback(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	ms := NewMigratingStore(source, target)
	s := New()
	s.HashTokenInStore = true
	s.Store = ms

	deadline := time.Now().Add(time.Hour).UTC()
	hashed := hashToken("legacy-token")
	legacy := encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "legacy"})
	source.set(hashed, legacy, deadline)
	// A partial migration in the target store must be ignored after rollback.
	target.set(hashed, encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "partial"}), deadline)

	ms.Rollback()

	ctx, err := s.Load(context.Background(), "legacy-token")
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if got := s.GetString(ctx, "foo"); got != "legacy" {
		t.Errorf("want %q after rollback; got %q", "legacy", got)
	}
}

func TestMigratingStoreFailedMigrationStaysLegacyAcrossRequests(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.IdleTimeout = time.Hour
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(24 * time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), time.Now().Add(time.Minute))

	target.setCommitErr(errors.New("target unavailable"))

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := s.GetString(r.Context(), "foo"); got != "bar" {
			t.Errorf("want %q; got %q", "bar", got)
		}
	})

	// Two read-only requests. Neither may issue a cookie, and the old session
	// must remain untouched in the source store so that the session is not lost
	// if the target stays unavailable.
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: "legacy-token"})
		s.LoadAndSave(handler).ServeHTTP(rec, req)

		if got := rec.Result().Header.Get("Set-Cookie"); got != "" {
			t.Errorf("request %d: want no cookie; got %q", i+1, got)
		}
	}

	if _, ok := target.get("legacy-token"); ok {
		t.Error("failed migration left data in the target store")
	}
	if _, ok := source.get("legacy-token"); !ok {
		t.Error("legacy session was removed from the source store")
	}

	// Once the target recovers, the session migrates and a cookie is issued.
	target.setCommitErr(nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: "legacy-token"})
	s.LoadAndSave(handler).ServeHTTP(rec, req)

	if got := rec.Result().Header.Get("Set-Cookie"); got == "" {
		t.Error("want a cookie once the migration succeeds")
	}
	if _, ok := target.get("legacy-token"); !ok {
		t.Error("session was not migrated once the target recovered")
	}
}

func TestMigratingStoreMigratedWithIdleTimeoutRefreshesExpiry(t *testing.T) {
	t.Parallel()

	source := newMigrationStore()
	target := newMigrationStore()

	s := New()
	s.IdleTimeout = time.Hour
	s.Store = NewMigratingStore(source, target)

	deadline := time.Now().Add(24 * time.Hour).UTC()
	source.set("legacy-token", encodeTestSession(t, s, deadline, map[string]interface{}{"foo": "bar"}), time.Now().Add(time.Minute))

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: "legacy-token"})
	s.LoadAndSave(handler).ServeHTTP(rec, req)

	cookie := rec.Result().Header.Get("Set-Cookie")
	if cookie == "" {
		t.Fatal("want a session cookie after migration")
	}
	// With an idle timeout the cookie should be persistent and carry the
	// refreshed expiry rather than the original deadline.
	if strings.Contains(cookie, "Max-Age=86400") {
		t.Errorf("cookie appears to carry the original deadline: %q", cookie)
	}
	if !strings.Contains(cookie, "Max-Age=") {
		t.Errorf("want a Max-Age on the cookie; got %q", cookie)
	}

	targetItem := target.getMust(t, "legacy-token")
	if !targetItem.expiry.Before(deadline) {
		t.Errorf("want the migrated expiry (%v) to be refreshed before the deadline (%v)", targetItem.expiry, deadline)
	}
	sourceItem := source.getMust(t, "legacy-token")
	if !sourceItem.expiry.Equal(targetItem.expiry) {
		t.Errorf("expiry differs between stores: source %v, target %v", sourceItem.expiry, targetItem.expiry)
	}
}
