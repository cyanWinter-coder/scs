package scs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// controllableStore is an in-memory Store whose Commit and Delete calls can be
// made to fail on demand, and which records how often each method is called.
type controllableStore struct {
	mu          sync.Mutex
	items       map[string][]byte
	commitErr   error
	deleteErr   error
	commitCalls int32
	deleteCalls int32
	committed   []string
	deleted     []string
}

func newControllableStore() *controllableStore {
	return &controllableStore{items: make(map[string][]byte)}
}

func (c *controllableStore) setCommitErr(err error) {
	c.mu.Lock()
	c.commitErr = err
	c.mu.Unlock()
}

func (c *controllableStore) setDeleteErr(err error) {
	c.mu.Lock()
	c.deleteErr = err
	c.mu.Unlock()
}

func (c *controllableStore) Commit(token string, b []byte, expiry time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	atomic.AddInt32(&c.commitCalls, 1)
	if c.commitErr != nil {
		return c.commitErr
	}
	c.items[token] = b
	c.committed = append(c.committed, token)
	return nil
}

func (c *controllableStore) Delete(token string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	atomic.AddInt32(&c.deleteCalls, 1)
	if c.deleteErr != nil {
		return c.deleteErr
	}
	delete(c.items, token)
	c.deleted = append(c.deleted, token)
	return nil
}

func (c *controllableStore) Find(token string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.items[token]
	return b, ok, nil
}

func (c *controllableStore) All() (map[string][]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	all := make(map[string][]byte, len(c.items))
	for token, b := range c.items {
		all[token] = b
	}
	return all, nil
}

func (c *controllableStore) has(token string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[token]
	return ok
}

func (c *controllableStore) commits() int { return int(atomic.LoadInt32(&c.commitCalls)) }
func (c *controllableStore) deletes() int { return int(atomic.LoadInt32(&c.deleteCalls)) }

var errStoreUnavailable = errors.New("store unavailable")

// Requirement 1 & 2: a failed commit of a new session must not swap the
// request-scope token, must leave the status readable as Modified, and the same
// change can be committed successfully afterwards.
func TestCommitFailureKeepsRequestScopeState(t *testing.T) {
	t.Parallel()

	s := New()
	store := newControllableStore()
	s.Store = store

	ctx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(ctx, "foo", "bar")

	store.setCommitErr(errStoreUnavailable)
	token, _, err := s.Commit(ctx)
	if !errors.Is(err, errStoreUnavailable) {
		t.Fatalf("expected store error, got %v", err)
	}
	if token != "" {
		t.Errorf("expected empty token on failed commit, got %q", token)
	}
	if got := s.Token(ctx); got != "" {
		t.Errorf("request-scope token was mutated by failed commit: got %q", got)
	}
	if got := s.Status(ctx); got != Modified {
		t.Errorf("expected readable Modified status after failure, got %v", got)
	}
	if got := s.GetString(ctx, "foo"); got != "bar" {
		t.Errorf("in-memory change lost after failed commit: got %q", got)
	}
	if store.commits() != 1 {
		t.Errorf("expected 1 store commit attempt, got %d", store.commits())
	}

	// The failure must not masquerade as persisted state: a retry works.
	store.setCommitErr(nil)
	token2, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatalf("retry commit failed: %v", err)
	}
	if token2 == "" {
		t.Fatal("expected a token after successful retry commit")
	}
	if !store.has(token2) {
		t.Error("successfully committed token is not in the store")
	}
	if got := s.Status(ctx); got != Committed {
		t.Errorf("expected Committed status after retry, got %v", got)
	}
	if got := s.Token(ctx); got != token2 {
		t.Errorf("expected request token %q, got %q", token2, got)
	}
}

// Requirement 2: when the middleware's commit fails, no new Set-Cookie header is
// written and the response carries a readable failure status instead.
func TestLoadAndSaveCommitFailureKeepsOldCookie(t *testing.T) {
	t.Parallel()

	s := New()
	store := newControllableStore()
	s.Store = store

	// Establish a persisted session with an existing token.
	setupCtx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(setupCtx, "existing", "data")
	existingToken, _, err := s.Commit(setupCtx)
	if err != nil {
		t.Fatal(err)
	}

	var commitErr error
	mux := http.NewServeMux()
	mux.HandleFunc("/modify", func(w http.ResponseWriter, r *http.Request) {
		s.Put(r.Context(), "foo", "bar")
		_, _, commitErr = s.Commit(r.Context())
	})

	// First request: make the store commit fail.
	store.setCommitErr(errStoreUnavailable)
	req := httptest.NewRequest(http.MethodGet, "/modify", nil)
	req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: existingToken})
	rec := httptest.NewRecorder()
	s.LoadAndSave(mux).ServeHTTP(rec, req)

	if !errors.Is(commitErr, errStoreUnavailable) {
		t.Fatalf("expected handler to see the commit error, got %v", commitErr)
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on commit failure, got %d", rec.Code)
	}
	if cookie := rec.Header().Get("Set-Cookie"); cookie != "" {
		t.Errorf("failed commit must not issue a new cookie, got %q", cookie)
	}
	// The original session must still be readable under the old token.
	if !store.has(existingToken) {
		t.Error("original session row was lost after failed commit")
	}
}

// Requirement 3: repeated Commit calls in one request write to the store only
// once, and a failed commit is never reported as Committed.
func TestCommitIsIdempotentWithinRequest(t *testing.T) {
	t.Parallel()

	s := New()
	store := newControllableStore()
	s.Store = store

	ctx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(ctx, "foo", "bar")

	first, expiry, err := s.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, expiry2, err := s.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	third, expiry3, err := s.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if first != second || second != third {
		t.Errorf("repeated Commit returned different tokens: %q, %q, %q", first, second, third)
	}
	if !expiry.Equal(expiry2) || !expiry.Equal(expiry3) {
		t.Error("repeated Commit returned inconsistent expiry times")
	}
	if store.commits() != 1 {
		t.Errorf("expected exactly 1 store commit, got %d", store.commits())
	}

	// A modification after commit makes the next Commit persist once more.
	s.Put(ctx, "baz", "qux")
	if _, _, err := s.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if store.commits() != 2 {
		t.Errorf("expected 2 store commits after further modification, got %d", store.commits())
	}
}

// Requirement 4: RenewToken must not touch the store before commit, and on
// commit failure the original token and store row survive while the new token
// is never persisted.
func TestRenewTokenCommitFailurePreservesOldSession(t *testing.T) {
	t.Parallel()

	s := New()
	store := newControllableStore()
	s.Store = store

	ctx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(ctx, "foo", "bar")
	oldToken, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Reset the per-request state as a new request loading the old token.
	ctx, err = s.Load(context.Background(), oldToken)
	if err != nil {
		t.Fatal(err)
	}
	if store.deletes() != 0 {
		t.Fatalf("Load must not delete anything, saw %d deletes", store.deletes())
	}

	if err := s.RenewToken(ctx); err != nil {
		t.Fatal(err)
	}
	// Rotation is staged: the store is untouched and the old row still exists.
	if store.deletes() != 0 {
		t.Errorf("RenewToken eagerly deleted from store, saw %d deletes", store.deletes())
	}
	if !store.has(oldToken) {
		t.Error("old session row was removed before successful commit")
	}
	staged := s.Token(ctx)
	if staged == "" || staged == oldToken {
		t.Fatalf("expected a staged new token, got %q", staged)
	}

	store.setCommitErr(errStoreUnavailable)
	if _, _, err := s.Commit(ctx); !errors.Is(err, errStoreUnavailable) {
		t.Fatalf("expected commit error, got %v", err)
	}
	if !store.has(oldToken) {
		t.Error("old session lost after failed rotation commit")
	}
	if store.has(staged) {
		t.Error("staged token was persisted despite commit failure")
	}
	if store.deletes() != 0 {
		t.Errorf("old token deleted despite failed commit, saw %d deletes", store.deletes())
	}
	if got := s.Status(ctx); got != Modified {
		t.Errorf("expected Modified after failed rotation commit, got %v", got)
	}

	// A successful retry persists the new token and only then removes the old
	// one.
	store.setCommitErr(nil)
	newToken, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if newToken != staged {
		t.Errorf("retry changed the staged token: got %q want %q", newToken, staged)
	}
	if !store.has(newToken) {
		t.Error("new token not persisted after successful retry")
	}
	if store.has(oldToken) {
		t.Error("old token was not removed after successful rotation")
	}
	if store.deletes() != 1 {
		t.Errorf("expected exactly 1 deletion of the old token, got %d", store.deletes())
	}
}

// Requirement 4: a failure while deleting the old token during rotation must
// fail the whole commit and leave the client on the old token.
func TestRenewTokenOldTokenDeleteFailure(t *testing.T) {
	t.Parallel()

	s := New()
	store := newControllableStore()
	s.Store = store

	ctx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(ctx, "foo", "bar")
	oldToken, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ctx, err = s.Load(context.Background(), oldToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RenewToken(ctx); err != nil {
		t.Fatal(err)
	}
	staged := s.Token(ctx)

	// The new token commits fine, but removing the old token fails.
	store.setDeleteErr(errStoreUnavailable)
	if _, _, err := s.Commit(ctx); !errors.Is(err, errStoreUnavailable) {
		t.Fatalf("expected delete error to fail the commit, got %v", err)
	}
	if !store.has(oldToken) {
		t.Error("old token should remain when its deletion fails")
	}
	if got := s.Status(ctx); got != Modified {
		t.Errorf("expected commit to stay uncommitted on delete failure, got %v", got)
	}
	if s.Token(ctx) != staged {
		t.Error("staged token changed after failed commit")
	}
}

// Requirement 4: Destroy stages the deletion; a failing staged deletion in the
// middleware keeps the old cookie and the store row.
func TestDestroyFailureKeepsOldSessionAndCookie(t *testing.T) {
	t.Parallel()

	s := New()
	store := newControllableStore()
	s.Store = store

	ctx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(ctx, "foo", "bar")
	oldToken, _, err := s.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ctx, err = s.Load(context.Background(), oldToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Destroy(ctx); err != nil {
		t.Fatalf("Destroy should stage without touching the store: %v", err)
	}
	if store.deletes() != 0 {
		t.Errorf("Destroy eagerly deleted from store, saw %d deletes", store.deletes())
	}
	if !store.has(oldToken) {
		t.Error("store row removed by staged Destroy")
	}
	if got := s.Status(ctx); got != Destroyed {
		t.Errorf("expected Destroyed status, got %v", got)
	}

	// Drive the staged deletion through the middleware with a failing store.
	store.setDeleteErr(errStoreUnavailable)
	mux := http.NewServeMux()
	mux.HandleFunc("/destroy", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Destroy(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	req := httptest.NewRequest(http.MethodGet, "/destroy", nil)
	req.AddCookie(&http.Cookie{Name: s.Cookie.Name, Value: oldToken})
	rec := httptest.NewRecorder()
	s.LoadAndSave(mux).ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 when staged destroy fails, got %d", rec.Code)
	}
	if cookie := rec.Header().Get("Set-Cookie"); cookie != "" {
		t.Errorf("failed destroy must not clear the cookie, got %q", cookie)
	}
	if !store.has(oldToken) {
		t.Error("store row lost after failed destroy")
	}

	// A subsequent successful request destroys the session and clears the
	// cookie.
	store.setDeleteErr(nil)
	rec = httptest.NewRecorder()
	s.LoadAndSave(mux).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 on successful destroy, got %d", rec.Code)
	}
	cookie := rec.Header().Get("Set-Cookie")
	if !strings.HasPrefix(cookie, s.Cookie.Name+"=;") {
		t.Errorf("expected cookie clearing, got %q", cookie)
	}
	if store.has(oldToken) {
		t.Error("store row remains after successful destroy")
	}
}

// Requirement 4: MergeSession must not delete the merged-from token until the
// merge has been committed successfully.
func TestMergeSessionStagesDeletionUntilCommit(t *testing.T) {
	t.Parallel()

	s := New()
	store := newControllableStore()
	s.Store = store

	// Persist the session to be merged from.
	fromCtx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(fromCtx, "merged", "value")
	fromToken, _, err := s.Commit(fromCtx)
	if err != nil {
		t.Fatal(err)
	}

	// A fresh request-scoped session merges the other one in.
	ctx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	if err := s.MergeSession(ctx, fromToken); err != nil {
		t.Fatal(err)
	}
	if !store.has(fromToken) {
		t.Error("merged-from token deleted before commit")
	}
	if got := s.GetString(ctx, "merged"); got != "value" {
		t.Errorf("merged value missing from request scope: got %q", got)
	}

	store.setCommitErr(errStoreUnavailable)
	if _, _, err := s.Commit(ctx); !errors.Is(err, errStoreUnavailable) {
		t.Fatalf("expected commit error, got %v", err)
	}
	if !store.has(fromToken) {
		t.Error("merged-from token lost after failed commit")
	}

	store.setCommitErr(nil)
	if _, _, err := s.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if store.has(fromToken) {
		t.Error("merged-from token not removed after successful commit")
	}
}

// Requirement 4: if a session is merged and then destroyed within the same
// request, both the original and merged-from tokens must be removed by the
// staged destroy, and the failure of any deletion must keep the whole request
// uncommitted.
func TestMergeThenDestroyRemovesAllStagedTokens(t *testing.T) {
	t.Parallel()

	s := New()
	store := newControllableStore()
	s.Store = store

	fromCtx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(fromCtx, "x", "y")
	fromToken, _, err := s.Commit(fromCtx)
	if err != nil {
		t.Fatal(err)
	}

	curCtx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(curCtx, "k", "v")
	currentToken, _, err := s.Commit(curCtx)
	if err != nil {
		t.Fatal(err)
	}

	ctx, err := s.Load(context.Background(), currentToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.MergeSession(ctx, fromToken); err != nil {
		t.Fatal(err)
	}
	if err := s.Destroy(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitDestroy(ctx); err != nil {
		t.Fatal(err)
	}

	if store.has(currentToken) || store.has(fromToken) {
		t.Error("both tokens should be removed after merge followed by destroy")
	}
	if got := s.Status(ctx); got != Destroyed {
		t.Errorf("expected Destroyed status, got %v", got)
	}
}

// Requirement 4: the documented Iterate + Destroy revocation pattern must keep
// working outside of an HTTP request: Destroy stages the deletion and
// CommitDestroy flushes it to the store.
func TestIterateDestroyCommitsStagedDeletion(t *testing.T) {
	t.Parallel()

	s := New()
	store := newControllableStore()
	s.Store = store

	var revokeToken, keepToken string
	for i := 0; i < 2; i++ {
		ctx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
		if i == 0 {
			s.Put(ctx, "userID", 4)
		} else {
			s.Put(ctx, "userID", 7)
		}
		token, _, err := s.Commit(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			revokeToken = token
		} else {
			keepToken = token
		}
	}

	err := s.Iterate(context.Background(), func(ctx context.Context) error {
		if s.GetInt(ctx, "userID") != 4 {
			return nil
		}
		if err := s.Destroy(ctx); err != nil {
			return err
		}
		return s.CommitDestroy(ctx)
	})
	if err != nil {
		t.Fatalf("Iterate returned error: %v", err)
	}

	if store.has(revokeToken) {
		t.Error("revoked session was not removed from the store")
	}
	if !store.has(keepToken) {
		t.Error("unrelated session was incorrectly removed")
	}
}

// Requirement 5: concurrent requests committing the same token preserve the
// store's existing last-write-wins Commit semantics. Every successful request
// lands its own write and none is silently dropped by the commit boundary
// machinery.
func TestConcurrentCommitsPreserveStoreSemantics(t *testing.T) {
	t.Parallel()

	s := New()
	store := newControllableStore()
	s.Store = store

	// Persist a shared session that all requests load.
	rootCtx := s.addSessionDataToContext(context.Background(), newSessionData(s.Lifetime, 0))
	s.Put(rootCtx, "k", "v0")
	token, _, err := s.Commit(rootCtx)
	if err != nil {
		t.Fatal(err)
	}

	const n = 25
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, lerr := s.Load(context.Background(), token)
			if lerr != nil {
				errs <- lerr
				return
			}
			s.Put(ctx, "k", i)
			if _, _, cerr := s.Commit(ctx); cerr != nil {
				errs <- cerr
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	// All n updates were committed to the store (plus the initial commit), and
	// the surviving value is one of the written values — last writer wins, as
	// the Store contract specifies.
	if got := store.commits(); got != n+1 {
		t.Errorf("expected %d total commits, got %d", n+1, got)
	}
	loaded, err := s.Load(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.GetInt(loaded, "k"); got < 0 || got >= n {
		t.Errorf("stored value %d is not one of the concurrently written values", got)
	}
}
