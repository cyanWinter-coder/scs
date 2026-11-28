package boltstore

import (
	"context"
	"os"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

// TestRepairOnLegacyOnDiskData simulates an upgrade-and-restart: a database
// file written by an older version of this package (value records only, no
// expiry index bucket) is reopened, queries return no ghost sessions, and
// Repair cleans the legacy records up.
func TestRepairOnLegacyOnDiskData(t *testing.T) {
	path := t.TempDir() + "/legacy.db"

	// Phase 1: write legacy data using raw bbolt, with no expiry index bucket.
	oldDB, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = oldDB.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		legacies := map[string][]byte{
			"legacy1": append(expiryPrefix(time.Now().Add(time.Minute)), []byte("olddata1")...),
			"legacy2": append(expiryPrefix(time.Now().Add(time.Minute)), []byte("olddata2")...),
		}
		for token, val := range legacies {
			if err := bucket.Put([]byte(token), val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := oldDB.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: reopen the file with the current BoltStore as a restart would.
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}

	bs := NewWithCleanupInterval(db, 0)

	// The index bucket is created lazily by the constructor; legacy value
	// records have no matching index and must never be returned as ghosts.
	if _, found, err := bs.Find("legacy1"); err != nil || found {
		t.Fatalf("legacy record before repair: found=%v err=%v, want false", found, err)
	}
	all, err := bs.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Fatalf("All before repair returned %d ghost sessions: %v", len(all), all)
	}

	// New-style commits continue to work alongside the legacy records.
	if err := bs.Commit("modern", []byte("moderndata"), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	result, err := bs.Repair(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.OrphanedValues != 2 {
		t.Fatalf("OrphanedValues: got %d, want 2", result.OrphanedValues)
	}
	if result.Total() != 2 {
		t.Fatalf("Total repairs: got %d, want 2", result.Total())
	}

	if _, found, err := bs.Find("modern"); err != nil || !found {
		t.Fatalf("modern session lost after repair: found=%v err=%v", found, err)
	}
	if _, found, _ := bs.Find("legacy1"); found {
		t.Fatal("legacy1 still visible after repair")
	}

	// A second repair after restart-equivalent reopen is a no-op.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	bs2 := NewWithCleanupInterval(db2, 0)

	result, err = bs2.Repair(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Total() != 0 {
		t.Fatalf("second repair changed %d records: %+v", result.Total(), result)
	}
	if _, found, err := bs2.Find("modern"); err != nil || !found {
		t.Fatalf("modern session not found after reopen: found=%v err=%v", found, err)
	}

	// Ensure no stray files are left behind outside the temp directory.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database file disappeared: %v", err)
	}
}
