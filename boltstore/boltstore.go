package boltstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"time"

	"go.etcd.io/bbolt"
)

var (
	bucketName       = []byte("scs:session")
	expiryBucketName = []byte("scs:session:expiry")
)

// repairBatchSize limits the number of records repaired in a single
// transaction. It is a var so that tests can exercise the batching and
// cancellation behavior deterministically.
var repairBatchSize = 1000

// RepairResult contains the number of repairs carried out by a Repair call.
type RepairResult struct {
	// OrphanedIndexes is the number of expiry index entries which had no
	// matching value record (or whose expiry prefix no longer matched the
	// value record).
	OrphanedIndexes int

	// OrphanedValues is the number of value records which had no matching
	// expiry index entry. Value records written by older versions of this
	// package (which predate the expiry index) are counted here.
	OrphanedValues int

	// MalformedValues is the number of value records which were too short to
	// contain an expiry prefix and could not be parsed.
	MalformedValues int
}

// Total returns the total number of records repaired.
func (r RepairResult) Total() int {
	return r.OrphanedIndexes + r.OrphanedValues + r.MalformedValues
}

// BoltStore represents the session store.
type BoltStore struct {
	db          *bbolt.DB
	stopCleanup chan bool
}

// New returns a new Boltstore instance, with a background cleanup goroutine
// that runs every 1 minute to remove expired session data.
func New(db *bbolt.DB) *BoltStore {
	return NewWithCleanupInterval(db, time.Minute)
}

// NewWithCleanupInterval returns a new Boltstore instance. The cleanupInterval
// parameter controls how frequently expired session data is removed by the
// background cleanup goroutine. Setting it to 0 prevents the cleanup goroutine
// from running (i.e. expired sessions will not be removed).
func NewWithCleanupInterval(db *bbolt.DB, cleanupInterval time.Duration) *BoltStore {
	db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(expiryBucketName)
		return err
	})
	bs := &BoltStore{
		db: db,
	}
	if cleanupInterval > 0 {
		bs.stopCleanup = make(chan bool)
		go bs.startCleanup(cleanupInterval)
	}
	return bs
}

// expiryIndexKey builds the expiry index key for a token with the given
// expiry prefix (the first 8 bytes of a value record). A fresh backing array
// is always allocated, because slices returned by bbolt are only valid for
// the life of the enclosing transaction.
func expiryIndexKey(expiryPrefix []byte, token string) []byte {
	key := make([]byte, 0, 8+len(token))
	key = append(key, expiryPrefix...)
	key = append(key, []byte(token)...)
	return key
}

// Find returns the data for a given session token from the BoltStore instance.
// If the session token is not found or is expired, the returned exists flag will
// be set to false. A session is only returned when both the value record and
// its expiry index entry exist and agree; a missing or inconsistent either
// side is treated as a non-existent (ghost) session.
func (bs *BoltStore) Find(token string) (b []byte, exists bool, err error) {
	var val []byte
	err = bs.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return nil
		}

		rec := bucket.Get([]byte(token))
		// A missing value record, or one too short to hold an expiry prefix,
		// means the session does not exist.
		if rec == nil || len(rec) < 8 {
			return nil
		}

		if uint64(time.Now().UnixNano()) > binary.BigEndian.Uint64(rec[:8]) {
			return nil
		}

		indexBucket := tx.Bucket(expiryBucketName)
		if indexBucket == nil {
			return nil
		}
		if indexBucket.Get(expiryIndexKey(rec[:8], token)) == nil {
			// The expiry index entry is missing: do not return ghost data.
			return nil
		}

		// Copy the payload; bbolt's returned memory is only valid during
		// the transaction.
		val = append([]byte{}, rec[8:]...)
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if val == nil {
		return nil, false, nil
	}
	return val, true, nil
}

// Commit adds a session token and data to the BoltStore instance with the
// given expiry time. If the session token already exists, then the data and expiry
// time are updated. The value record and the expiry index entry are written in
// a single transaction, so a failed commit can never leave a one-sided record.
func (bs *BoltStore) Commit(token string, b []byte, expiry time.Time) error {
	return bs.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		indexBucket := tx.Bucket(expiryBucketName)

		// On renewal (or when healing a record written by an older version),
		// remove the previous expiry index entry first. Everything still
		// happens in this one transaction.
		if old := bucket.Get([]byte(token)); old != nil && len(old) >= 8 {
			err := indexBucket.Delete(expiryIndexKey(old[:8], token))
			if err != nil {
				return err
			}
		}

		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(expiry.UnixNano()))
		val := append(buf, b...)

		if err := bucket.Put([]byte(token), val); err != nil {
			return err
		}

		return indexBucket.Put(expiryIndexKey(buf, token), []byte(token))
	})
}

// Delete removes a session token and corresponding data from the BoltStore
// instance. The value record and expiry index entry are removed in a single
// transaction.
func (bs *BoltStore) Delete(token string) error {
	return bs.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		indexBucket := tx.Bucket(expiryBucketName)

		if rec := bucket.Get([]byte(token)); rec != nil && len(rec) >= 8 {
			err := indexBucket.Delete(expiryIndexKey(rec[:8], token))
			if err != nil {
				return err
			}
		}

		return bucket.Delete([]byte(token))
	})
}

// All returns a map containing the token and data for all active (i.e.
// not expired) sessions in the BoltStore instance. Sessions whose value
// record and expiry index disagree are skipped.
func (bs *BoltStore) All() (map[string][]byte, error) {
	sessions := make(map[string][]byte)

	err := bs.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return nil
		}
		indexBucket := tx.Bucket(expiryBucketName)
		if indexBucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		now := uint64(time.Now().UnixNano())

		for token, rec := cursor.First(); token != nil; token, rec = cursor.Next() {
			if len(rec) < 8 {
				continue
			}
			if now > binary.BigEndian.Uint64(rec[:8]) {
				continue
			}
			if indexBucket.Get(expiryIndexKey(rec[:8], string(token))) == nil {
				continue
			}
			sessions[string(token)] = append([]byte{}, rec[8:]...)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return sessions, nil
}

func (bs *BoltStore) startCleanup(cleanupInterval time.Duration) {
	ticker := time.NewTicker(cleanupInterval)
	for {
		select {
		case <-ticker.C:
			err := bs.deleteExpired()
			if err != nil {
				log.Println(err)
			}
		case <-bs.stopCleanup:
			ticker.Stop()
			return
		}
	}
}

// StopCleanup terminates the background cleanup goroutine for the BoltStore
// instance. It's rare to terminate this; generally BoltStore instances and
// their cleanup goroutines are intended to be long-lived and run for the lifetime
// of your application.
//
// There may be occasions though when your use of the BoltStore is transient.
// An example is creating a new BoltStore instance in a test function. In this
// scenario, the cleanup goroutine (which will run forever) will prevent the
// BoltStore object from being garbage collected even after the test function
// has finished. You can prevent this by manually calling StopCleanup.
func (bs *BoltStore) StopCleanup() {
	if bs.stopCleanup != nil {
		bs.stopCleanup <- true
	}
}

func (bs *BoltStore) deleteExpired() error {
	type expiredRecord struct {
		token        []byte
		expiryPrefix []byte
	}

	var expired []expiredRecord
	bs.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return nil
		}
		now := uint64(time.Now().UnixNano())
		bucket.ForEach(func(token, val []byte) error {
			if len(val) >= 8 && now > binary.BigEndian.Uint64(val[:8]) {
				expired = append(expired, expiredRecord{
					token:        append([]byte{}, token...),
					expiryPrefix: append([]byte{}, val[:8]...),
				})
			}
			return nil
		})
		return nil
	})

	if len(expired) > 0 {
		return bs.db.Update(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(bucketName)
			indexBucket := tx.Bucket(expiryBucketName)
			for _, rec := range expired {
				// Delete the expiry index entry and the value record
				// together, so an expiry can never leave a one-sided
				// record behind.
				if err := indexBucket.Delete(expiryIndexKey(rec.expiryPrefix, string(rec.token))); err != nil {
					return err
				}
				if err := bucket.Delete(rec.token); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return nil
}

// nextSeekKey returns the smallest key that sorts strictly after key. It is
// used to resume a batched cursor scan in a new transaction.
func nextSeekKey(key []byte) []byte {
	seek := make([]byte, len(key)+1)
	copy(seek, key)
	return seek
}

// Repair checks the consistency of value records and expiry index entries and
// removes orphaned or malformed records. It is intended to be called once at
// application startup, before the store serves traffic, to recover from a
// crash that interrupted a write.
//
// Repair works in batches, and each batch is cleaned up in its own write
// transaction. Already-committed batches are never rolled back: if the ctx is
// canceled, Repair stops before starting further batches and returns the
// counts collected so far together with the context error; calling Repair
// again later resumes and completes the remaining work. Running Repair on a
// healthy store changes nothing and returns zero counts, and it is safe to
// run repeatedly.
func (bs *BoltStore) Repair(ctx context.Context) (RepairResult, error) {
	var result RepairResult

	// Pass 1: walk the expiry index (sorted by expiry prefix) and remove
	// index entries without a usable value record, plus value records too
	// malformed to ever match an index again.
	var seek []byte
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		var lastKey []byte
		batch := 0
		err := bs.db.Update(func(tx *bbolt.Tx) error {
			indexBucket := tx.Bucket(expiryBucketName)
			if indexBucket == nil {
				return nil
			}
			bucket := tx.Bucket(bucketName)
			cursor := indexBucket.Cursor()

			for key, _ := cursor.Seek(seek); key != nil && batch < repairBatchSize; key, _ = cursor.Next() {
				if len(key) < 8 {
					// Unparseable index key: treat as an orphan.
					if err := cursor.Delete(); err != nil {
						return err
					}
					result.OrphanedIndexes++
					lastKey = append([]byte{}, key...)
					batch++
					continue
				}

				token := key[8:]
				rec := bucket.Get(token)
				switch {
				case rec == nil:
					// Orphaned expiry index: the value record is gone.
					if err := cursor.Delete(); err != nil {
						return err
					}
					result.OrphanedIndexes++
				case len(rec) < 8:
					// Malformed value record: remove both sides together.
					if err := cursor.Delete(); err != nil {
						return err
					}
					if err := bucket.Delete(token); err != nil {
						return err
					}
					result.MalformedValues++
				case !bytes.Equal(key[:8], rec[:8]):
					// The index points at a stale expiry: drop it. The
					// unmatched value record is cleaned up in pass 2.
					if err := cursor.Delete(); err != nil {
						return err
					}
					result.OrphanedIndexes++
				}

				lastKey = append([]byte{}, key...)
				batch++
			}
			return nil
		})
		if err != nil {
			return result, err
		}
		if batch < repairBatchSize {
			break
		}
		seek = nextSeekKey(lastKey)
	}

	// Pass 2: walk the value records and remove any without a matching
	// expiry index entry. This covers value records written by older
	// versions of the package as well as any remaining one-sided records.
	seek = nil
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		var lastKey []byte
		batch := 0
		err := bs.db.Update(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(bucketName)
			if bucket == nil {
				return nil
			}
			indexBucket := tx.Bucket(expiryBucketName)
			cursor := bucket.Cursor()

			for token, rec := cursor.Seek(seek); token != nil && batch < repairBatchSize; token, rec = cursor.Next() {
				if len(rec) < 8 {
					if err := cursor.Delete(); err != nil {
						return err
					}
					result.MalformedValues++
				} else if indexBucket.Get(expiryIndexKey(rec[:8], string(token))) == nil {
					if err := cursor.Delete(); err != nil {
						return err
					}
					result.OrphanedValues++
				}

				lastKey = append([]byte{}, token...)
				batch++
			}
			return nil
		})
		if err != nil {
			return result, err
		}
		if batch < repairBatchSize {
			break
		}
		seek = nextSeekKey(lastKey)
	}

	return result, nil
}
