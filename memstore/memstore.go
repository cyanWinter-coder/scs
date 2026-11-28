package memstore

import (
	"sync"
	"time"
)

type item struct {
	object     []byte
	expiration int64
	// version is the store-wide lifecycle version of this entry. It changes on
	// every Commit (both inserts and renewals), allowing an in-flight cleanup
	// round to tell a renewed entry apart from the snapshot it captured when
	// the round began.
	version uint64
}

// expiredEntry is a snapshot of a session token that a cleanup round found to
// be expired when the round started. The version recorded here must still
// match the live entry when the round comes to delete it.
type expiredEntry struct {
	token   string
	version uint64
}

// MemStore represents the session store.
type MemStore struct {
	items       map[string]item
	mu          sync.RWMutex
	stopCleanup chan bool
	stopOnce    sync.Once
	// nextVersion is a store-wide monotonic counter. Every Commit assigns the
	// next value to the entry, so a re-committed token always gets a version
	// that no previous cleanup round could have captured.
	nextVersion uint64
	// stopped is set to true when StopCleanup has terminated the background
	// cleanup goroutine. Once true, no cleanup round may mutate the items map
	// anymore.
	stopped bool
}

// New returns a new MemStore instance, with a background cleanup goroutine that
// runs every minute to remove expired session data.
func New() *MemStore {
	return NewWithCleanupInterval(time.Minute)
}

// NewWithCleanupInterval returns a new MemStore instance. The cleanupInterval
// parameter controls how frequently expired session data is removed by the
// background cleanup goroutine. Setting it to 0 prevents the cleanup goroutine
// from running (i.e. expired sessions will not be removed).
func NewWithCleanupInterval(cleanupInterval time.Duration) *MemStore {
	m := &MemStore{
		items: make(map[string]item),
	}

	if cleanupInterval > 0 {
		m.stopCleanup = make(chan bool)
		go m.startCleanup(cleanupInterval)
	}

	return m
}

// Find returns the data for a given session token from the MemStore instance.
// If the session token is not found or is expired, the returned exists flag will
// be set to false.
func (m *MemStore) Find(token string) ([]byte, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item, found := m.items[token]
	if !found {
		return nil, false, nil
	}

	if time.Now().UnixNano() > item.expiration {
		return nil, false, nil
	}

	return item.object, true, nil
}

// Commit adds a session token and data to the MemStore instance with the given
// expiry time. If the session token already exists, then the data and expiry
// time are updated.
func (m *MemStore) Commit(token string, b []byte, expiry time.Time) error {
	m.mu.Lock()
	m.nextVersion++
	m.items[token] = item{
		object:     b,
		expiration: expiry.UnixNano(),
		version:    m.nextVersion,
	}
	m.mu.Unlock()

	return nil
}

// Delete removes a session token and corresponding data from the MemStore
// instance.
func (m *MemStore) Delete(token string) error {
	m.mu.Lock()
	delete(m.items, token)
	m.mu.Unlock()

	return nil
}

// All returns a map containing the token and data for all active (i.e.
// not expired) sessions.
func (m *MemStore) All() (map[string][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var mm = make(map[string][]byte)

	for token, item := range m.items {
		if item.expiration > time.Now().UnixNano() {
			mm[token] = item.object
		}
	}

	return mm, nil
}

func (m *MemStore) startCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.deleteExpired()
		case <-m.stopCleanup:
			return
		}
	}
}

// StopCleanup terminates the background cleanup goroutine for the MemStore
// instance. It's rare to terminate this; generally MemStore instances and
// their cleanup goroutines are intended to be long-lived and run for the lifetime
// of your application.
//
// There may be occasions though when your use of the MemStore is transient.
// An example is creating a new MemStore instance in a test function. In this
// scenario, the cleanup goroutine (which will run forever) will prevent the
// MemStore object from being garbage collected even after the test function
// has finished. You can prevent this by manually calling StopCleanup.
//
// StopCleanup is safe to call multiple times and on instances without a
// cleanup goroutine; in both cases it is a no-op. After it returns, no further
// cleanup rounds will run and a late round cannot write back to or delete any
// entries.
func (m *MemStore) StopCleanup() {
	if m.stopCleanup == nil {
		return
	}

	m.stopOnce.Do(func() {
		m.mu.Lock()
		m.stopped = true
		m.mu.Unlock()

		close(m.stopCleanup)
	})
}

// snapshotExpired captures the tokens (and their current lifecycle versions)
// which are already expired at the instant the cleanup round begins. Entries
// that are renewed or deleted after this snapshot is taken are not touched by
// the round, because their versions no longer match.
func (m *MemStore) snapshotExpired(now int64) []expiredEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	expired := make([]expiredEntry, 0)
	for token, item := range m.items {
		if now > item.expiration {
			expired = append(expired, expiredEntry{
				token:   token,
				version: item.version,
			})
		}
	}

	return expired
}

// deleteCandidates removes the entries captured at the start of a cleanup
// round, but only while the store is still running and the live entry still
// holds the same version and expiration that the snapshot recorded. It
// returns the number of entries actually deleted.
func (m *MemStore) deleteCandidates(candidates []expiredEntry, now int64) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.stopped {
		return 0
	}

	deleted := 0
	for _, candidate := range candidates {
		item, found := m.items[candidate.token]
		if !found {
			// The token was already deleted (or recreated under a new
			// version); the round has no terminal state to produce here.
			continue
		}

		if item.version != candidate.version {
			// The entry was renewed (or deleted and re-committed) after the
			// round started. The stale round must not touch it.
			continue
		}

		// The round only ever removes entries that were already expired when
		// it began; a version match implies the entry has not been renewed,
		// and this re-check makes that explicit.
		if now <= item.expiration {
			continue
		}

		delete(m.items, candidate.token)
		deleted++
	}

	return deleted
}

// deleteExpired runs a single cleanup round and returns the number of expired
// entries that it removed.
func (m *MemStore) deleteExpired() int {
	now := time.Now().UnixNano()

	m.mu.RLock()
	if m.stopped {
		m.mu.RUnlock()
		return 0
	}
	m.mu.RUnlock()

	candidates := m.snapshotExpired(now)
	if len(candidates) == 0 {
		return 0
	}

	return m.deleteCandidates(candidates, now)
}
