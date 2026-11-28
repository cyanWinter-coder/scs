package boltstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

// newTestStore opens a BoltStore backed by a temporary database file with the
// cleanup goroutine disabled.
func newTestStore(t *testing.T) (*BoltStore, *bbolt.DB) {
	t.Helper()
	db, err := bbolt.Open(t.TempDir()+"/testing.db", 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return NewWithCleanupInterval(db, 0), db
}

func expiryPrefix(t time.Time) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(t.UnixNano()))
	return buf
}

// putRawValue writes a value record directly, bypassing Commit, so tests can
// simulate one-sided or old-format records.
func putRawValue(t *testing.T, boltDB *bbolt.DB, token string, val []byte) {
	t.Helper()
	err := boltDB.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(token), val)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// putRawIndex writes an expiry index entry directly, bypassing Commit.
func putRawIndex(t *testing.T, boltDB *bbolt.DB, token string, prefix []byte) {
	t.Helper()
	err := boltDB.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(expiryBucketName).Put(expiryIndexKey(prefix, token), []byte(token))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func deleteRawIndex(t *testing.T, boltDB *bbolt.DB, token string, prefix []byte) {
	t.Helper()
	err := boltDB.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(expiryBucketName).Delete(expiryIndexKey(prefix, token))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func deleteRawValue(t *testing.T, boltDB *bbolt.DB, token string) {
	t.Helper()
	err := boltDB.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).Delete([]byte(token))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func bucketCount(t *testing.T, boltDB *bbolt.DB, name []byte) int {
	t.Helper()
	var n int
	err := boltDB.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(name)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			n++
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestFindRequiresBothSides verifies that a session is only reported as found
// when both the value record and the expiry index entry exist and agree.
func TestFindRequiresBothSides(t *testing.T) {
	bs, db := newTestStore(t)
	expiry := time.Now().Add(time.Minute)

	if err := bs.Commit("key1", []byte("value1"), expiry); err != nil {
		t.Fatal(err)
	}
	if _, found, err := bs.Find("key1"); err != nil || !found {
		t.Fatalf("healthy session: found=%v err=%v, want found=true", found, err)
	}

	// Ghost: expiry index entry survives but the value record is gone.
	deleteRawValue(t, db, "key1")
	if _, found, err := bs.Find("key1"); err != nil || found {
		t.Fatalf("orphaned index: found=%v err=%v, want found=false", found, err)
	}

	// Re-create the value, then remove the index instead.
	if err := bs.Commit("key1", []byte("value1"), expiry); err != nil {
		t.Fatal(err)
	}
	deleteRawIndex(t, db, "key1", expiryPrefix(expiry))
	if _, found, err := bs.Find("key1"); err != nil || found {
		t.Fatalf("orphaned value: found=%v err=%v, want found=false", found, err)
	}
}

// TestAllSkipsGhostSessions verifies that All ignores one-sided records.
func TestAllSkipsGhostSessions(t *testing.T) {
	bs, db := newTestStore(t)
	expiry := time.Now().Add(time.Minute)

	if err := bs.Commit("healthy", []byte("data"), expiry); err != nil {
		t.Fatal(err)
	}
	// Old-format value record with no expiry index.
	putRawValue(t, db, "legacy", append(expiryPrefix(expiry), []byte("olddata")...))
	// Orphaned expiry index with no value record.
	putRawIndex(t, db, "ghost", expiryPrefix(expiry))

	all, err := bs.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("got %d sessions, want 1: %v", len(all), all)
	}
	if !bytes.Equal(all["healthy"], []byte("data")) {
		t.Fatalf("got %q, want %q", all["healthy"], "data")
	}
}

// TestCommitWritesValueAndIndex verifies that Commit always creates both
// records.
func TestCommitWritesValueAndIndex(t *testing.T) {
	bs, db := newTestStore(t)

	if err := bs.Commit("key1", []byte("value1"), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := bucketCount(t, db, bucketName); got != 1 {
		t.Fatalf("value records: got %d, want 1", got)
	}
	if got := bucketCount(t, db, expiryBucketName); got != 1 {
		t.Fatalf("expiry index entries: got %d, want 1", got)
	}
}

// TestCommitRenewalReplacesIndex verifies that renewing a session replaces
// the old expiry index entry rather than accumulating one-sided entries.
func TestCommitRenewalReplacesIndex(t *testing.T) {
	bs, db := newTestStore(t)

	firstExpiry := time.Now().Add(time.Minute)
	secondExpiry := time.Now().Add(2 * time.Minute)

	if err := bs.Commit("key1", []byte("value1"), firstExpiry); err != nil {
		t.Fatal(err)
	}
	if err := bs.Commit("key1", []byte("value2"), secondExpiry); err != nil {
		t.Fatal(err)
	}

	if got := bucketCount(t, db, expiryBucketName); got != 1 {
		t.Fatalf("expiry index entries after renewal: got %d, want 1", got)
	}
	if got := bucketCount(t, db, bucketName); got != 1 {
		t.Fatalf("value records after renewal: got %d, want 1", got)
	}

	// The surviving index entry must carry the new expiry prefix.
	var indexKey []byte
	db.View(func(tx *bbolt.Tx) error {
		tx.Bucket(expiryBucketName).ForEach(func(k, v []byte) error {
			indexKey = append([]byte{}, k...)
			return nil
		})
		return nil
	})
	if !bytes.Equal(indexKey[:8], expiryPrefix(secondExpiry)) {
		t.Fatal("index entry still points at the old expiry")
	}

	v, found, err := bs.Find("key1")
	if err != nil || !found || !bytes.Equal(v, []byte("value2")) {
		t.Fatalf("Find after renewal: v=%q found=%v err=%v", v, found, err)
	}
}

// TestDeleteRemovesBothSides verifies that Delete removes the value record
// and expiry index entry together.
func TestDeleteRemovesBothSides(t *testing.T) {
	bs, db := newTestStore(t)
	expiry := time.Now().Add(time.Minute)

	if err := bs.Commit("key1", []byte("value1"), expiry); err != nil {
		t.Fatal(err)
	}
	if err := bs.Delete("key1"); err != nil {
		t.Fatal(err)
	}
	if got := bucketCount(t, db, bucketName); got != 0 {
		t.Fatalf("value records after delete: got %d, want 0", got)
	}
	if got := bucketCount(t, db, expiryBucketName); got != 0 {
		t.Fatalf("expiry index entries after delete: got %d, want 0", got)
	}

	// Deleting a missing token is a no-op without error.
	if err := bs.Delete("key1"); err != nil {
		t.Fatalf("delete of missing token: got %v, want nil", err)
	}
}

// TestRepairEmptyDB verifies that Repair on an empty store is a safe no-op.
func TestRepairEmptyDB(t *testing.T) {
	bs, _ := newTestStore(t)

	result, err := bs.Repair(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Total() != 0 {
		t.Fatalf("empty store repaired %d records, want 0: %+v", result.Total(), result)
	}
}

// TestRepairOrphanedIndex verifies that expiry index entries without a value
// record are removed and counted.
func TestRepairOrphanedIndex(t *testing.T) {
	bs, db := newTestStore(t)
	expiry := time.Now().Add(time.Minute)

	if err := bs.Commit("healthy", []byte("data"), expiry); err != nil {
		t.Fatal(err)
	}
	putRawIndex(t, db, "orphan1", expiryPrefix(expiry))
	putRawIndex(t, db, "orphan2", expiryPrefix(expiry))

	result, err := bs.Repair(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.OrphanedIndexes != 2 {
		t.Fatalf("OrphanedIndexes: got %d, want 2", result.OrphanedIndexes)
	}
	if result.OrphanedValues != 0 || result.MalformedValues != 0 {
		t.Fatalf("unexpected repairs: %+v", result)
	}
	if got := bucketCount(t, db, expiryBucketName); got != 1 {
		t.Fatalf("expiry index entries after repair: got %d, want 1", got)
	}
	if _, found, err := bs.Find("healthy"); err != nil || !found {
		t.Fatalf("healthy session lost: found=%v err=%v", found, err)
	}
}

// TestRepairOrphanedValue verifies that value records without an expiry index
// entry are removed and counted, including records written by older package
// versions that predate the index.
func TestRepairOrphanedValue(t *testing.T) {
	bs, db := newTestStore(t)
	expiry := time.Now().Add(time.Minute)

	if err := bs.Commit("healthy", []byte("data"), expiry); err != nil {
		t.Fatal(err)
	}
	// Old-format value record: valid encoding, but no expiry index entry.
	putRawValue(t, db, "legacy", append(expiryPrefix(expiry), []byte("olddata")...))
	// Simulate a value whose index went stale.
	putRawValue(t, db, "stale", append(expiryPrefix(expiry), []byte("staledata")...))

	result, err := bs.Repair(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.OrphanedValues != 2 {
		t.Fatalf("OrphanedValues: got %d, want 2", result.OrphanedValues)
	}
	if got := bucketCount(t, db, bucketName); got != 1 {
		t.Fatalf("value records after repair: got %d, want 1", got)
	}

	// The legacy record must never have been visible as a ghost.
	if _, found, _ := bs.Find("legacy"); found {
		t.Fatal("legacy session returned as a ghost before/after repair")
	}
}

// TestRepairMalformedValue verifies that value records too short to hold an
// expiry prefix are removed and counted, on both sides of the index.
func TestRepairMalformedValue(t *testing.T) {
	bs, db := newTestStore(t)
	expiry := time.Now().Add(time.Minute)

	// Malformed value with a dangling index entry: both sides removed.
	putRawValue(t, db, "broken1", []byte{0x01})
	putRawIndex(t, db, "broken1", expiryPrefix(expiry))
	// Malformed value without any index entry.
	putRawValue(t, db, "broken2", []byte{0x02, 0x03})
	// A healthy session must survive untouched.
	if err := bs.Commit("healthy", []byte("data"), expiry); err != nil {
		t.Fatal(err)
	}

	result, err := bs.Repair(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.MalformedValues != 2 {
		t.Fatalf("MalformedValues: got %d, want 2", result.MalformedValues)
	}
	if result.OrphanedIndexes != 0 {
		t.Fatalf("the broken1 index should be attributed to malformed repair, got OrphanedIndexes=%d", result.OrphanedIndexes)
	}
	if got := bucketCount(t, db, bucketName); got != 1 {
		t.Fatalf("value records after repair: got %d, want 1", got)
	}
	if got := bucketCount(t, db, expiryBucketName); got != 1 {
		t.Fatalf("expiry index entries after repair: got %d, want 1", got)
	}
	if _, found, err := bs.Find("healthy"); err != nil || !found {
		t.Fatalf("healthy session lost: found=%v err=%v", found, err)
	}
}

// TestRepairIdempotent verifies that running Repair repeatedly on a healthy
// store changes nothing.
func TestRepairIdempotent(t *testing.T) {
	bs, _ := newTestStore(t)

	for i := 0; i < 3; i++ {
		if err := bs.Commit("key1", []byte("value1"), time.Now().Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}

	for run := 0; run < 2; run++ {
		result, err := bs.Repair(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if result.Total() != 0 {
			t.Fatalf("run %d repaired %d records on a healthy store: %+v", run, result.Total(), result)
		}
	}
	if _, found, err := bs.Find("key1"); err != nil || !found {
		t.Fatalf("session lost after repeated repair: found=%v err=%v", found, err)
	}
}

// cancelAfterNCallsCtx is a context whose Err flips to context.Canceled once
// Err has been called threshold times, giving deterministic control over the
// point at which a batched Repair stops.
type cancelAfterNCallsCtx struct {
	parent    context.Context
	calls     int
	threshold int
	canceled  bool
	closed    chan struct{}
}

func newCancelAfterNCallsCtx(parent context.Context, threshold int) *cancelAfterNCallsCtx {
	return &cancelAfterNCallsCtx{parent: parent, threshold: threshold, closed: make(chan struct{})}
}

func (c *cancelAfterNCallsCtx) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *cancelAfterNCallsCtx) Done() <-chan struct{} {
	if c.canceled {
		return c.closed
	}
	return nil
}

func (c *cancelAfterNCallsCtx) Err() error {
	if c.canceled {
		return context.Canceled
	}
	c.calls++
	if c.calls >= c.threshold {
		c.canceled = true
		close(c.closed)
		return context.Canceled
	}
	return c.parent.Err()
}

func (c *cancelAfterNCallsCtx) Value(key interface{}) interface{} { return c.parent.Value(key) }

// TestRepairCancellationAndResume verifies that cancellation stops further
// batches, that already-committed batches stay in place, and that a later
// Repair resumes and finishes the remaining work.
func TestRepairCancellationAndResume(t *testing.T) {
	originalBatchSize := repairBatchSize
	repairBatchSize = 5
	t.Cleanup(func() { repairBatchSize = originalBatchSize })

	bs, db := newTestStore(t)
	prefix := expiryPrefix(time.Now().Add(time.Minute))
	// Twelve orphaned index entries: pass 1 walks them in sorted order, five
	// per batch.
	for i := 0; i < 12; i++ {
		putRawIndex(t, db, "orphan"+string(rune('a'+i)), prefix)
	}

	// Err is consulted once per batch: batch 1 and 2 commit, batch 3 is
	// refused before it starts.
	ctx := newCancelAfterNCallsCtx(context.Background(), 3)
	result, err := bs.Repair(ctx)
	if err != context.Canceled {
		t.Fatalf("got err %v, want context.Canceled", err)
	}
	if result.OrphanedIndexes != 10 {
		t.Fatalf("committed-batch repairs: got %d, want 10", result.OrphanedIndexes)
	}
	if got := bucketCount(t, db, expiryBucketName); got != 2 {
		t.Fatalf("remaining index entries: got %d, want 2", got)
	}

	// Resume with a fresh context: the remaining entries are cleaned up.
	result, err = bs.Repair(context.Background())
	if err != nil {
		t.Fatalf("resume repair: got %v, want nil", err)
	}
	if result.OrphanedIndexes != 2 {
		t.Fatalf("resumed repairs: got %d, want 2", result.OrphanedIndexes)
	}
	if got := bucketCount(t, db, expiryBucketName); got != 0 {
		t.Fatalf("index entries after resume: got %d, want 0", got)
	}
	if got := bucketCount(t, db, bucketName); got != 0 {
		t.Fatalf("value records after resume: got %d, want 0", got)
	}

	// A further run reports nothing to do.
	result, err = bs.Repair(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Total() != 0 {
		t.Fatalf("post-resume repair changed %d records: %+v", result.Total(), result)
	}
}

// TestRepairCancelledBeforeFirstBatch verifies that a context already
// canceled before Repair starts changes nothing at all.
func TestRepairCancelledBeforeFirstBatch(t *testing.T) {
	bs, db := newTestStore(t)
	prefix := expiryPrefix(time.Now().Add(time.Minute))
	putRawIndex(t, db, "orphan1", prefix)
	putRawIndex(t, db, "orphan2", prefix)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := bs.Repair(ctx)
	if err != context.Canceled {
		t.Fatalf("got err %v, want context.Canceled", err)
	}
	if result.Total() != 0 {
		t.Fatalf("pre-canceled repair changed %d records: %+v", result.Total(), result)
	}
	if got := bucketCount(t, db, expiryBucketName); got != 2 {
		t.Fatalf("index entries: got %d, want 2 (nothing should have been committed)", got)
	}
}
