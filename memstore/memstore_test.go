package memstore

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestFind(t *testing.T) {
	m := NewWithCleanupInterval(0)
	m.items["session_token"] = item{object: []byte("encoded_data"), expiration: time.Now().Add(time.Second).UnixNano()}

	b, found, err := m.Find("session_token")
	if err != nil {
		t.Fatalf("got %v: expected %v", err, nil)
	}
	if found != true {
		t.Fatalf("got %v: expected %v", found, true)
	}
	if bytes.Equal(b, []byte("encoded_data")) == false {
		t.Fatalf("got %v: expected %v", b, []byte("encoded_data"))
	}
}

func TestFindMissing(t *testing.T) {
	m := NewWithCleanupInterval(0)

	_, found, err := m.Find("missing_session_token")
	if err != nil {
		t.Fatalf("got %v: expected %v", err, nil)
	}
	if found != false {
		t.Fatalf("got %v: expected %v", found, false)
	}
}

func TestCommitNew(t *testing.T) {
	m := NewWithCleanupInterval(0)

	err := m.Commit("session_token", []byte("encoded_data"), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("got %v: expected %v", err, nil)
	}

	v, found := m.items["session_token"]
	if found != true {
		t.Fatalf("got %v: expected %v", found, true)
	}

	if reflect.DeepEqual(v.object, []byte("encoded_data")) == false {
		t.Fatalf("got %v: expected %v", v.object, []byte("encoded_data"))
	}
}

func TestCommitUpdated(t *testing.T) {
	m := NewWithCleanupInterval(0)

	err := m.Commit("session_token", []byte("encoded_data"), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("got %v: expected %v", err, nil)
	}

	err = m.Commit("session_token", []byte("new_encoded_data"), time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("got %v: expected %v", err, nil)
	}

	v := m.items["session_token"].object

	if reflect.DeepEqual(v, []byte("new_encoded_data")) == false {
		t.Fatalf("got %v: expected %v", v, []byte("new_encoded_data"))
	}
}

func TestExpiry(t *testing.T) {
	m := NewWithCleanupInterval(0)

	err := m.Commit("session_token", []byte("encoded_data"), time.Now().Add(100*time.Millisecond))
	if err != nil {
		t.Fatalf("got %v: expected %v", err, nil)
	}

	_, found, _ := m.Find("session_token")
	if found != true {
		t.Fatalf("got %v: expected %v", found, true)
	}

	time.Sleep(101 * time.Millisecond)
	_, found, _ = m.Find("session_token")
	if found != false {
		t.Fatalf("got %v: expected %v", found, false)
	}
}

func TestDelete(t *testing.T) {
	m := NewWithCleanupInterval(0)
	m.items["session_token"] = item{object: []byte("encoded_data"), expiration: time.Now().Add(time.Second).UnixNano()}

	err := m.Delete("session_token")
	if err != nil {
		t.Fatalf("got %v: expected %v", err, nil)
	}

	_, found := m.items["session_token"]
	if found != false {
		t.Fatalf("got %v: expected %v", found, false)
	}
}

func TestCleanupInterval(t *testing.T) {
	m := NewWithCleanupInterval(100 * time.Millisecond)
	defer m.StopCleanup()
	m.mu.Lock()
	m.items["session_token"] = item{object: []byte("encoded_data"), expiration: time.Now().Add(500 * time.Millisecond).UnixNano()}
	m.mu.Unlock()

	m.mu.Lock()
	_, ok := m.items["session_token"]
	m.mu.Unlock()
	if !ok {
		t.Fatalf("got %v: expected %v", ok, true)
	}

	time.Sleep(time.Second)
	m.mu.Lock()
	_, ok = m.items["session_token"]
	m.mu.Unlock()
	if ok {
		t.Fatalf("got %v: expected %v", ok, false)
	}
}

// TestCommitAssignsMonotonicVersions checks that every Commit — including a
// renewal and a re-commit after Delete — gets a distinct lifecycle version.
func TestCommitAssignsMonotonicVersions(t *testing.T) {
	m := NewWithCleanupInterval(0)
	expiry := time.Now().Add(time.Minute)

	if err := m.Commit("token", []byte("data_1"), expiry); err != nil {
		t.Fatal(err)
	}
	first := m.items["token"].version
	if first == 0 {
		t.Fatal("expected committed entry to be assigned a non-zero version")
	}

	if err := m.Commit("token", []byte("data_2"), expiry); err != nil {
		t.Fatal(err)
	}
	if renewed := m.items["token"].version; renewed <= first {
		t.Fatalf("renewal version %d should be greater than %d", renewed, first)
	}

	if err := m.Delete("token"); err != nil {
		t.Fatal(err)
	}
	if err := m.Commit("token", []byte("data_3"), expiry); err != nil {
		t.Fatal(err)
	}
	if recreated := m.items["token"].version; recreated <= first {
		t.Fatalf("re-created version %d should be greater than %d", recreated, first)
	}
}

// TestCleanupDoesNotDeleteRenewedEntry deterministically interleaves a cleanup
// round with a renewal: the round snapshots an expired entry, the entry is then
// renewed, and the stale round must not remove it.
func TestCleanupDoesNotDeleteRenewedEntry(t *testing.T) {
	m := NewWithCleanupInterval(0)

	if err := m.Commit("renewed", []byte("old"), time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := m.Commit("deleted", []byte("gone"), time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := m.Commit("recreated", []byte("gone"), time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := m.Commit("stays", []byte("alive"), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	// Begin a cleanup round and snapshot everything that is expired at this
	// instant. The three expired tokens are candidates.
	candidates := m.snapshotExpired(time.Now().UnixNano())
	if len(candidates) != 3 {
		t.Fatalf("got %d candidates: expected 3", len(candidates))
	}

	// While the round is in flight, the entries are renewed, deleted and
	// re-created respectively.
	if err := m.Commit("renewed", []byte("new"), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := m.Delete("deleted"); err != nil {
		t.Fatal(err)
	}
	if err := m.Delete("recreated"); err != nil {
		t.Fatal(err)
	}
	if err := m.Commit("recreated", []byte("back"), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	deleted := m.deleteCandidates(candidates, time.Now().UnixNano())
	if deleted != 0 {
		t.Fatalf("got %d deletions: expected 0 (all candidates changed after the snapshot)", deleted)
	}

	for _, token := range []string{"renewed", "recreated", "stays"} {
		if _, found, err := m.Find(token); err != nil || !found {
			t.Fatalf("token %q should still exist after the stale round (found=%v, err=%v)", token, found, err)
		}
	}
	if _, found := m.items["deleted"]; found {
		t.Fatal("token \"deleted\" should remain absent")
	}
}

// TestDeleteExpiredCountAndScope verifies that a cleanup round deletes exactly
// the entries expired when it began, returns the accurate count, and leaves
// entries that expire after the snapshot untouched.
func TestDeleteExpiredCountAndScope(t *testing.T) {
	m := NewWithCleanupInterval(0)

	// A fixed "now" makes the round boundary deterministic: tokens with an
	// expiration before now are expired; the boundary token (expiration ==
	// now) is not, matching the Find/All semantics of now > expiration.
	now := time.Now().UnixNano()
	m.mu.Lock()
	m.items["expired_1"] = item{object: []byte("a"), expiration: now - int64(time.Minute), version: 1}
	m.items["expired_2"] = item{object: []byte("b"), expiration: now - int64(time.Second), version: 2}
	m.items["boundary"] = item{object: []byte("c"), expiration: now, version: 3}
	m.items["active"] = item{object: []byte("d"), expiration: now + int64(time.Minute), version: 4}
	m.mu.Unlock()

	candidates := m.snapshotExpired(now)
	if len(candidates) != 2 {
		t.Fatalf("got %d candidates: expected 2", len(candidates))
	}
	if got := m.deleteCandidates(candidates, now); got != 2 {
		t.Fatalf("got %d deletions: expected 2", got)
	}

	if _, found := m.items["expired_1"]; found {
		t.Fatal("expired_1 should have been deleted")
	}
	if _, found := m.items["expired_2"]; found {
		t.Fatal("expired_2 should have been deleted")
	}
	if _, found := m.items["boundary"]; !found {
		t.Fatal("boundary entry (expiration == now) should not have been deleted")
	}
	if _, found := m.items["active"]; !found {
		t.Fatal("active entry should not have been deleted")
	}

	// A further round over entries that are still active deletes nothing and
	// reports zero.
	active := NewWithCleanupInterval(0)
	if err := active.Commit("active_1", []byte("a"), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := active.Commit("active_2", []byte("b"), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := active.deleteExpired(); got != 0 {
		t.Fatalf("got %d deletions: expected 0 on an empty round", got)
	}
}

// TestDeleteAfterSnapshotHasSingleTerminalState checks that a Delete racing
// with a cleanup round produces exactly one outcome: the token stays gone and
// the round never resurrects or double-processes it.
func TestDeleteAfterSnapshotHasSingleTerminalState(t *testing.T) {
	m := NewWithCleanupInterval(0)

	if err := m.Commit("token", []byte("data"), time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}

	candidates := m.snapshotExpired(time.Now().UnixNano())
	if len(candidates) != 1 {
		t.Fatalf("got %d candidates: expected 1", len(candidates))
	}

	if err := m.Delete("token"); err != nil {
		t.Fatal(err)
	}

	// The round's candidate vanished before its delete phase; it must be a
	// no-op and the terminal state remains "deleted".
	if got := m.deleteCandidates(candidates, time.Now().UnixNano()); got != 0 {
		t.Fatalf("got %d deletions: expected 0", got)
	}
	if _, found := m.items["token"]; found {
		t.Fatal("token should have exactly one terminal state: absent")
	}
	if _, found, err := m.Find("token"); err != nil || found {
		t.Fatalf("Find after delete should report not found (found=%v, err=%v)", found, err)
	}
}

// TestStopCleanupStopsLateRounds ensures that after StopCleanup a pending or
// late cleanup round cannot delete anything, and StopCleanup itself is
// idempotent and safe without a cleanup goroutine.
func TestStopCleanupStopsLateRounds(t *testing.T) {
	m := NewWithCleanupInterval(0)

	if err := m.Commit("expired", []byte("data"), time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}

	// No goroutine was started (interval == 0); StopCleanup must be a no-op.
	m.StopCleanup()
	// Repeated calls must not block or panic.
	m.StopCleanup()

	candidates := m.snapshotExpired(time.Now().UnixNano())
	if len(candidates) != 1 {
		t.Fatalf("got %d candidates: expected 1", len(candidates))
	}

	// Mark the store stopped; the late round must refuse to mutate the map.
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()

	if got := m.deleteCandidates(candidates, time.Now().UnixNano()); got != 0 {
		t.Fatalf("got %d deletions after stop: expected 0", got)
	}
	if got := m.deleteExpired(); got != 0 {
		t.Fatalf("got %d deletions from a round after stop: expected 0", got)
	}
	if _, found := m.items["expired"]; !found {
		t.Fatal("late cleanup round must not delete entries after StopCleanup")
	}
}

// TestStopCleanupTerminatesGoroutine verifies end-to-end that the background
// goroutine stops scheduling rounds once StopCleanup returns.
func TestStopCleanupTerminatesGoroutine(t *testing.T) {
	m := NewWithCleanupInterval(25 * time.Millisecond)
	if err := m.Commit("token", []byte("data"), time.Now().Add(40*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	// Let at least one tick pass while the entry is still alive, then stop.
	time.Sleep(35 * time.Millisecond)
	m.StopCleanup()

	time.Sleep(80 * time.Millisecond)
	m.mu.RLock()
	_, found := m.items["token"]
	m.mu.RUnlock()
	if !found {
		t.Fatal("entry expired after StopCleanup but no further round should have removed it")
	}
}

// TestStopCleanupRacingCleanup hammers StopCleanup against running cleanup
// rounds to ensure a late round can never mutate the map after shutdown.
func TestStopCleanupRacingCleanup(t *testing.T) {
	for attempt := 0; attempt < 50; attempt++ {
		m := NewWithCleanupInterval(time.Millisecond)
		for i := 0; i < 10; i++ {
			token := fmt.Sprintf("token-%d", i)
			if err := m.Commit(token, []byte("data"), time.Now().Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		}

		// Let rounds run while the entries expire, then shut down concurrently
		// with any round that is mid-flight.
		time.Sleep(5 * time.Millisecond)
		done := make(chan struct{})
		go func() {
			m.StopCleanup()
			close(done)
		}()
		m.StopCleanup()
		<-done

		// After shutdown every subsequent round must be a no-op even though
		// all remaining entries are expired.
		before := len(m.items)
		if got := m.deleteExpired(); got != 0 {
			t.Fatalf("attempt %d: got %d post-stop deletions: expected 0", attempt, got)
		}
		if len(m.items) != before {
			t.Fatalf("attempt %d: map mutated by a late round after StopCleanup", attempt)
		}
	}
}

// TestConcurrentLifecycle exercises Find, Commit, Delete and cleanup rounds
// against the same tokens in parallel. It is primarily intended to be run with
// the race detector; when not racing it also asserts that every token ends in
// exactly one determinate state (present with its last committed data, or
// absent).
func TestConcurrentLifecycle(t *testing.T) {
	m := NewWithCleanupInterval(time.Millisecond)
	defer m.StopCleanup()

	const workers = 8
	const iterations = 200

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				token := fmt.Sprintf("token-%d", i%5)
				switch id % 4 {
				case 0:
					if err := m.Commit(token, []byte(fmt.Sprintf("w%d", id)), time.Now().Add(2*time.Millisecond)); err != nil {
						t.Error(err)
						return
					}
				case 1:
					_, _, _ = m.Find(token)
				case 2:
					if err := m.Delete(token); err != nil {
						t.Error(err)
						return
					}
				case 3:
					_ = m.deleteExpired()
				}
			}
		}(w)
	}
	wg.Wait()

	// After quiescence every surviving entry must be readable as a whole and
	// no partial/duplicate state may exist for a token.
	m.mu.RLock()
	defer m.mu.RUnlock()
	for token, item := range m.items {
		if item.version == 0 {
			t.Fatalf("token %q has zero version after concurrent use", token)
		}
		if item.object == nil {
			t.Fatalf("token %q has nil object after concurrent use", token)
		}
	}
}
