# boltstore

A [Bolt](https://github.com/etcd-io/bbolt) based session store for [SCS](https://github.com/alexedwards/scs).

## Setup

You should follow the instructions to [open a Bolt database](https://github.com/etcd-io/bbolt#opening-a-database), and pass the database to `boltstore.New()` to establish the session store.

## Example

```go
package main

import (
	"io"
	"log"
	"net/http"
	"time"

	"github.com/alexedwards/scs/v2"
	"github.com/alexedwards/scs/boltstore"
	"go.etcd.io/bbolt"
)

var sessionManager *scs.SessionManager

func main() {
    // Open a Bolt database.
	db, err := bbolt.Open("/tmp/bolt.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Initialize a new session manager and configure it to use boltstore as the session store.
	sessionManager = scs.New()
	sessionManager.Store = boltstore.NewWithCleanupInterval(db, 20*time.Second)
	sessionManager.Lifetime = time.Minute

	mux := http.NewServeMux()
	mux.HandleFunc("/put", putHandler)
	mux.HandleFunc("/get", getHandler)

	http.ListenAndServe(":4000", sessionManager.LoadAndSave(mux))
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	sessionManager.Put(r.Context(), "message", "Hello from a session!")
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	msg := sessionManager.GetString(r.Context(), "message")
	io.WriteString(w, msg)
}
```

## Expired Session Cleanup

This package provides a background 'cleanup' goroutine to delete expired session data. This stops the database table from holding on to invalid sessions indefinitely and growing unnecessarily large. By default the cleanup runs every 1 minute. You can change this by using the `NewWithCleanupInterval()` function to initialize your session store. For example:

```go
// Run a cleanup every 5 minutes.
boltstore.NewWithCleanupInterval(db, 5*time.Minute)

// Disable the cleanup goroutine by setting the cleanup interval to zero.
boltstore.NewWithCleanupInterval(db, 0)
```

## Crash Recovery and Startup Repair

Each session is stored as two coordinated records in the Bolt database: a **value record** in the `scs:session` bucket (holding the expiry time and session data) and an **expiry index entry** in the `scs:session:expiry` bucket (holding the expiry time as an ordered key prefix). Every write, renewal and delete updates both records in a single Bolt transaction, so a failed operation never leaves a one-sided record behind.

If the process is killed mid-operation, the two records may temporarily disagree. A query will never return data in this state: `Find` and `All` verify that the value record and the expiry index entry exist and agree, and treat a missing either side as a non-existent session.

To remove the leftover records permanently, call `Repair` once at startup, before the store serves traffic:

```go
store := boltstore.NewWithCleanupInterval(db, time.Minute)

result, err := store.Repair(ctx)
if err != nil {
    // ctx was canceled; already-repaired batches are committed and a later
    // Repair call resumes the remaining work.
    log.Fatal(err)
}
log.Printf("repaired %d invalid session records", result.Total())
```

`Repair` removes three kinds of invalid records and reports how many of each it found:

- `OrphanedIndexes` — expiry index entries without a matching value record (ghost sessions)
- `OrphanedValues` — value records without a matching expiry index entry, including records written by older versions of this package
- `MalformedValues` — value records too short to contain an expiry prefix

Repair runs in batches, each committed in its own transaction. If the `context.Context` is canceled, Repair stops before starting further batches and returns the counts collected so far together with the context error; committed batches stay in place and calling `Repair` again with a fresh context resumes the work. It is safe to run on an empty database and safe to run repeatedly — a healthy store reports zero repairs and is left unchanged.

### Terminating the Cleanup Goroutine

It's rare that the cleanup goroutine needs to be terminated --- it is generally intended to be long-lived and run for the lifetime of your application.

However, there may be occasions when your use of a session store instance is transient. A common example would be using it in a short-lived test function. In this scenario, the cleanup goroutine (which will run forever) will prevent the session store instance from being garbage collected even after the test function has finished. You can prevent this by either disabling the cleanup goroutine altogether (as described above) or by stopping it using the `StopCleanup()` method. For example:

```go
func TestExample(t *testing.T) {
	db, err := bbolt.Open("/tmp/testing.db", 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := boltstore.New(db)
	defer store.StopCleanup()

	sessionManager = scs.New()
	sessionManager.Store = store

	// Run test...
}
```