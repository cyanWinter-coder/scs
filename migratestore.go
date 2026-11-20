package scs

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrMigratingStoreClosed is returned when an operation is attempted on a
	// MigratingStore after it has been closed.
	ErrMigratingStoreClosed = errors.New("scs: the migrating session store is closed")

	// ErrMigrationRolledBack is returned by MigrateCtx after the store has been
	// rolled back to the source store.
	ErrMigrationRolledBack = errors.New("scs: session store migration has been rolled back")

	// ErrMigrationNotSynchronised is returned when a migrated session cannot
	// be read back from the target store, or the data read back is not the
	// data that was written.
	ErrMigrationNotSynchronised = errors.New("scs: migrated session could not be verified in the target store")

	// ErrIterationNotSupported is returned by MigratingStore.AllCtx if neither
	// the source nor the target store supports iteration.
	ErrIterationNotSupported = errors.New("scs: neither the source nor the target session store supports iteration")

	// ErrMigrationSourceMissing is returned by MigrateCtx if the session has
	// disappeared from the source store (for example because it was destroyed
	// or expired) before it could be copied. Migrating the caller's copy would
	// resurrect the session, so it is not attempted.
	ErrMigrationSourceMissing = errors.New("scs: the session is no longer present in the source store")
)

// cleanupTimeout bounds the best-effort cleanup of a partially written target
// copy after the request context has already been cancelled.
const cleanupTimeout = 5 * time.Second

// MigrationFinder is implemented by session stores which can report whether a
// session was read from the legacy source store. SessionManager uses this to
// decide whether an existing session still needs to be migrated.
type MigrationFinder interface {
	// FindForMigration behaves like CtxStore.FindCtx, except that the
	// fromSource return value reports whether the session data was read from
	// the source store.
	FindForMigration(ctx context.Context, token string) (b []byte, found, fromSource bool, err error)
}

// MigrationCommitter is implemented by stores which can copy a session which
// was read from the source store into the target store.
type MigrationCommitter interface {
	// MigrateCtx copies the session data for token to the target store,
	// verifies that it can be read back, and returns the data which the target
	// store now holds. The token and expiry are used unchanged, so the
	// client's existing session token stays valid throughout the migration.
	//
	// If the target store already holds a session for the token, that copy is
	// returned as-is and is not overwritten. The source copy of the session is
	// retained, which makes the migration reversible.
	MigrateCtx(ctx context.Context, token string, b []byte, expiry time.Time) ([]byte, error)
}

// MigratingStore moves sessions from an existing source store to a new target
// store without logging users out.
//
// It implements the Store, CtxStore, IterableStore and IterableCtxStore
// interfaces. All operations which receive a context.Context pass that context
// on to stores which implement the corresponding context-aware interface.
//
// In the default (forward) mode sessions are read from the target store first,
// falling back to the source store (including when the target store cannot be
// read). Commit, Delete and All operations are applied to both stores with the
// same token, data and expiry, so that the stores remain consistent while the
// switch is in progress and a rollback is always possible.
//
// Migrating a session copies it to the target store but deliberately leaves
// the source copy in place. If the target store is unavailable, or the write
// cannot be verified, the old session continues to be served from the source
// store.
//
// Rollback switches the read order so that the source store takes precedence
// over the target store. Partially migrated data in the target store can
// therefore no longer shadow the old session. After a rollback the target store
// is not read at all, and the source store is the only store written to; the
// target store is abandoned until a new migration is started.
//
// A MigratingStore does not start any background goroutines. Close releases
// the source and target stores (using Close or StopCleanup where available)
// and wakes any requests which are waiting for another request's migration.
//
// Note: SessionManager.HashTokenInStore must have the same value for the whole
// duration of a migration. Both stores receive the same (optionally hashed)
// token, but a session written under one value cannot be read back using the
// other.
type MigratingStore struct {
	source Store
	target Store

	mu         sync.Mutex
	rolledBack bool
	closed     bool
	closedCh   chan struct{}
	migrations map[string]*migrationCall
}

type migrationCall struct {
	done chan struct{}
	b    []byte
	err  error
}

var (
	_ Store              = (*MigratingStore)(nil)
	_ CtxStore           = (*MigratingStore)(nil)
	_ IterableStore      = (*MigratingStore)(nil)
	_ IterableCtxStore   = (*MigratingStore)(nil)
	_ MigrationFinder    = (*MigratingStore)(nil)
	_ MigrationCommitter = (*MigratingStore)(nil)
)

// NewMigratingStore returns a MigratingStore which reads existing sessions
// from source and migrates them to target.
func NewMigratingStore(source, target Store) *MigratingStore {
	if source == nil {
		panic("scs: source store must not be nil")
	}
	if target == nil {
		panic("scs: target store must not be nil")
	}

	return &MigratingStore{
		source:     source,
		target:     target,
		closedCh:   make(chan struct{}),
		migrations: make(map[string]*migrationCall),
	}
}

// NewMigrating returns a new session manager which migrates sessions from the
// source store to the target store. It is equivalent to calling New and then
// setting the Store field to NewMigratingStore(source, target).
func NewMigrating(source, target Store) *SessionManager {
	s := New()
	s.Store = NewMigratingStore(source, target)
	return s
}

// Source returns the store which sessions are migrated from.
func (m *MigratingStore) Source() Store {
	return m.source
}

// Target returns the store which sessions are migrated to.
func (m *MigratingStore) Target() Store {
	return m.target
}

// RolledBack reports whether the store has been rolled back to the source
// store.
func (m *MigratingStore) RolledBack() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rolledBack
}

// Rollback makes the source store authoritative again. Sessions which exist in
// both stores are read from the source store, so partially migrated data in
// the target store cannot shadow the old session, and all subsequent writes go
// to the source store only. The source copies of migrated sessions were
// retained, so no session data is lost.
//
// Rollback does not wait for in-flight requests. A migration which is already
// in progress may still complete, but its target copy is ignored from then on.
// Rollback is idempotent and can be called at any time.
func (m *MigratingStore) Rollback() {
	m.mu.Lock()
	m.rolledBack = true
	m.mu.Unlock()
}

// Close releases the source and target stores. Stores which implement
// io.Closer are closed; otherwise stores which implement StopCleanup have
// their cleanup goroutine stopped. Requests which are waiting for another
// request to finish a migration are released and told that the store is
// closed, so they do not leak. Close is idempotent.
func (m *MigratingStore) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	close(m.closedCh)
	m.mu.Unlock()

	var firstErr error
	if err := releaseSessionStore(m.source); err != nil {
		firstErr = err
	}
	if err := releaseSessionStore(m.target); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// FindCtx returns the session data for token. In forward mode the target store
// is consulted first; after a rollback the source store takes precedence.
func (m *MigratingStore) FindCtx(ctx context.Context, token string) ([]byte, bool, error) {
	b, found, _, err := m.find(ctx, token)
	return b, found, err
}

// Find implements the Store interface.
func (m *MigratingStore) Find(token string) ([]byte, bool, error) {
	return m.FindCtx(context.Background(), token)
}

// FindForMigration implements the MigrationFinder interface.
func (m *MigratingStore) FindForMigration(ctx context.Context, token string) ([]byte, bool, bool, error) {
	return m.find(ctx, token)
}

func (m *MigratingStore) find(ctx context.Context, token string) (b []byte, found, fromSource bool, err error) {
	rolledBack, closed := m.state()
	if closed {
		return nil, false, false, ErrMigratingStoreClosed
	}

	// After a rollback the source store is the system of record again. Reads do
	// not fall back to the target store: its contents may be partial, or may
	// have been written while the source store was unavailable, and serving
	// that data could resurrect a session which the source store has already
	// expired or destroyed. There is nothing left to migrate either.
	if rolledBack {
		b, found, err = findFromSessionStore(ctx, m.source, token)
		return b, found, false, err
	}

	// A request whose migration failed keeps using the session from the source
	// store, so that it is not handed a half-written target copy. It must not
	// retry the migration in the same request cycle.
	if isSourceOnly(ctx) {
		b, found, err = findFromSessionStore(ctx, m.source, token)
		return b, found, false, err
	}

	b, found, err = findFromSessionStore(ctx, m.target, token)
	if err != nil {
		// The target store cannot be read. Fall back to the old session so that
		// the transition does not take down requests which can still be served
		// from the source store. If the source store does not have the session
		// either, report the target error rather than silently treating the
		// session as missing (which would log the user out during an outage).
		sb, sfound, serr := findFromSessionStore(ctx, m.source, token)
		if serr != nil || !sfound {
			return nil, false, false, err
		}
		return sb, true, true, nil
	}
	if found {
		return b, true, false, nil
	}
	b, found, err = findFromSessionStore(ctx, m.source, token)
	return b, found, found, err
}

// CommitCtx writes the session to both stores, using the same token, data and
// expiry for each. After a rollback, or for a request whose session could not
// be migrated, only the source store is written.
func (m *MigratingStore) CommitCtx(ctx context.Context, token string, b []byte, expiry time.Time) error {
	rolledBack, closed := m.state()
	if closed {
		return ErrMigratingStoreClosed
	}

	if rolledBack || isSourceOnly(ctx) {
		return commitToSessionStore(ctx, m.source, token, b, expiry)
	}

	if err := commitToSessionStore(ctx, m.target, token, b, expiry); err != nil {
		return err
	}
	return commitToSessionStore(ctx, m.source, token, b, expiry)
}

// Commit implements the Store interface.
func (m *MigratingStore) Commit(token string, b []byte, expiry time.Time) error {
	return m.CommitCtx(context.Background(), token, b, expiry)
}

// DeleteCtx deletes the session from both stores. The store which currently
// takes precedence is deleted first, but both stores are always attempted: a
// copy left behind would let a destroyed session be resurrected by a rollback,
// and would also let a session token which has been renewed stay valid in the
// old store. If both deletes fail, the first error is returned.
func (m *MigratingStore) DeleteCtx(ctx context.Context, token string) error {
	rolledBack, closed := m.state()
	if closed {
		return ErrMigratingStoreClosed
	}

	stores := [2]Store{m.target, m.source}
	if rolledBack || isSourceOnly(ctx) {
		stores = [2]Store{m.source, m.target}
	}

	var firstErr error
	for _, st := range stores {
		if err := deleteFromSessionStore(ctx, st, token); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Delete implements the Store interface.
func (m *MigratingStore) Delete(token string) error {
	return m.DeleteCtx(context.Background(), token)
}

// MigrateCtx copies a complete session from the source store to the target
// store. The session keeps the same token, so the client's existing cookie
// remains valid.
//
// The write is only considered successful once the session can be read back
// from the target store with exactly the data that was written. If the write
// or the verification fails, the target copy is removed and the caller should
// continue to serve the session from the source store.
//
// Concurrent calls for the same token produce a single target copy. Callers
// for a token whose migration is already in flight wait for it to finish and
// reuse the committed data instead of writing again. A caller which finds a
// copy already present in the target store also reuses it rather than
// overwriting it, so a slow request with stale data cannot clobber a session
// which another request has already updated.
func (m *MigratingStore) MigrateCtx(ctx context.Context, token string, b []byte, expiry time.Time) ([]byte, error) {
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, ErrMigratingStoreClosed
		}
		if m.rolledBack {
			m.mu.Unlock()
			return nil, ErrMigrationRolledBack
		}
		if call, ok := m.migrations[token]; ok {
			m.mu.Unlock()
			committedB, err := m.waitForMigration(ctx, call)
			// The request which was performing the migration may have been
			// cancelled. If this caller still has a live context, it takes over
			// rather than leaving the token unmigrated.
			if err != nil && ctx.Err() == nil && isCancellationError(err) {
				continue
			}
			return committedB, err
		}

		call := &migrationCall{done: make(chan struct{})}
		m.migrations[token] = call
		m.mu.Unlock()

		committedB, err := m.migrate(ctx, token, b, expiry)

		m.mu.Lock()
		call.b = committedB
		call.err = err
		delete(m.migrations, token)
		close(call.done)
		m.mu.Unlock()

		return committedB, err
	}
}

// isCancellationError reports whether err came from the cancellation or
// expiry of a request context.
func isCancellationError(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

func (m *MigratingStore) waitForMigration(ctx context.Context, call *migrationCall) ([]byte, error) {
	select {
	case <-call.done:
		if call.err != nil {
			return nil, call.err
		}
		return call.b, nil
	case <-m.closedCh:
		return nil, ErrMigratingStoreClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *MigratingStore) migrate(ctx context.Context, token string, b []byte, expiry time.Time) ([]byte, error) {
	// Another process may have migrated this session already. Reuse the
	// committed copy instead of overwriting it with data from the caller,
	// which was read before that migration completed.
	if existing, found, err := findFromSessionStore(ctx, m.target, token); err != nil {
		return nil, err
	} else if found {
		return existing, nil
	}

	// Read the session from the source store again immediately before copying
	// it. The caller read the session earlier, and another request may have
	// updated it since then. Copying the freshest complete copy means a slow
	// request cannot migrate stale data into the target store.
	current, found, err := findFromSessionStore(ctx, m.source, token)
	if err != nil {
		return nil, err
	}
	if !found {
		// The session has been destroyed or has expired in the source store
		// since the caller read it. Writing the data the caller still holds
		// would resurrect it in the target store, so abort instead.
		return nil, ErrMigrationSourceMissing
	}
	b = current

	// The target copy must be written completely before the client is told
	// that the session has been migrated.
	if err := commitToSessionStore(ctx, m.target, token, b, expiry); err != nil {
		m.removeTargetCopy(ctx, token)
		return nil, err
	}

	// Only report success once the target store can serve the session back
	// with exactly the data that was written.
	got, found, err := findFromSessionStore(ctx, m.target, token)
	if err != nil {
		m.removeTargetCopy(ctx, token)
		return nil, err
	}
	if !found || !bytes.Equal(got, b) {
		m.removeTargetCopy(ctx, token)
		return nil, ErrMigrationNotSynchronised
	}

	// Mirror the same data and expiry onto the source copy so that both stores
	// agree on the session. The source copy itself is intentionally kept: it is
	// what makes the migration reversible.
	//
	// The mirror write is skipped if the source copy has changed or disappeared
	// in the meantime, so that it cannot overwrite a newer update or resurrect
	// a session which another request has destroyed. If the mirror write fails
	// the migration still succeeds, because the target store is authoritative
	// from now on and later commits will update the source copy again.
	if current, found, err := findFromSessionStore(ctx, m.source, token); err == nil && found && bytes.Equal(current, b) {
		_ = commitToSessionStore(ctx, m.source, token, b, expiry)
	}

	return b, nil
}

func (m *MigratingStore) removeTargetCopy(ctx context.Context, token string) {
	// If the request context has been cancelled, best effort with the request
	// context would fail immediately for context-aware stores and leave a
	// partial copy behind. Use a short bounded context instead, so cleanup
	// cannot hang and no background goroutine outlives the call.
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
	}

	// Best effort: if this fails, a rollback still protects the old session
	// because reads then consult the source store first.
	_ = deleteFromSessionStore(ctx, m.target, token)
}

// AllCtx returns all active sessions from both stores. A session which exists
// in both stores is returned once, with the copy from the store which
// currently takes precedence winning.
//
// If only one of the stores supports iteration, that store is used. If the
// target store cannot be read, the sessions from the source store are returned
// instead of failing: the source store is the complete legacy system of record
// for the duration of a migration.
func (m *MigratingStore) AllCtx(ctx context.Context) (map[string][]byte, error) {
	rolledBack, closed := m.state()
	if closed {
		return nil, ErrMigratingStoreClosed
	}

	// After a rollback, or for a request whose session could not be migrated,
	// the target store is not consulted at all: a session which only exists
	// there must not reappear, and an unavailable target must not break
	// iteration over the authoritative source store.
	if rolledBack || isSourceOnly(ctx) {
		return allFromSessionStore(ctx, m.source)
	}

	source, sourceErr := allFromSessionStore(ctx, m.source)
	target, targetErr := allFromSessionStore(ctx, m.target)

	switch {
	case sourceErr == nil && targetErr == nil:
		// Both stores can be read; merge them below.
	case sourceErr == nil:
		// The source store holds the complete legacy sessions, so it is a
		// complete view on its own.
		return source, nil
	case sourceErr == ErrIterationNotSupported && targetErr == nil:
		// Only the target store supports iteration.
		return target, nil
	default:
		// Prefer the source store's error: the legacy sessions live there, so
		// failing loudly is better than returning a partial view.
		return nil, sourceErr
	}

	merged := make(map[string][]byte, len(target)+len(source))
	for token, b := range source {
		merged[token] = b
	}
	for token, b := range target {
		merged[token] = b
	}

	return merged, nil
}

// All implements the IterableStore interface.
func (m *MigratingStore) All() (map[string][]byte, error) {
	return m.AllCtx(context.Background())
}

func (m *MigratingStore) state() (rolledBack, closed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rolledBack, m.closed
}

type migrationContextKey struct{}

// withSourceOnly marks a request whose session could not be migrated. Store
// operations made with the returned context are routed to the source store
// only, so that the request can continue with the old session even when the
// target store is unavailable. Deletes still reach both stores.
func withSourceOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, migrationContextKey{}, true)
}

func isSourceOnly(ctx context.Context) bool {
	v, _ := ctx.Value(migrationContextKey{}).(bool)
	return v
}

func findFromSessionStore(ctx context.Context, st Store, token string) ([]byte, bool, error) {
	if cs, ok := st.(interface {
		FindCtx(context.Context, string) ([]byte, bool, error)
	}); ok {
		return cs.FindCtx(ctx, token)
	}
	return st.Find(token)
}

func commitToSessionStore(ctx context.Context, st Store, token string, b []byte, expiry time.Time) error {
	if cs, ok := st.(interface {
		CommitCtx(context.Context, string, []byte, time.Time) error
	}); ok {
		return cs.CommitCtx(ctx, token, b, expiry)
	}
	return st.Commit(token, b, expiry)
}

func deleteFromSessionStore(ctx context.Context, st Store, token string) error {
	if cs, ok := st.(interface {
		DeleteCtx(context.Context, string) error
	}); ok {
		return cs.DeleteCtx(ctx, token)
	}
	return st.Delete(token)
}

func allFromSessionStore(ctx context.Context, st Store) (map[string][]byte, error) {
	if cs, ok := st.(IterableCtxStore); ok {
		return cs.AllCtx(ctx)
	}
	if is, ok := st.(IterableStore); ok {
		return is.All()
	}
	return nil, ErrIterationNotSupported
}

func releaseSessionStore(st Store) error {
	if c, ok := st.(interface{ Close() error }); ok {
		return c.Close()
	}
	if c, ok := st.(interface{ StopCleanup() }); ok {
		c.StopCleanup()
	}
	return nil
}
