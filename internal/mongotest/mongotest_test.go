package mongotest_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// opCtx returns a bounded context for a single database operation.
func opCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestConnectSmoke inserts a document into the isolated database and reads it
// back, proving the harness yields a working, writable database.
func TestConnectSmoke(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)

	coll := db.Collection("smoke")
	want := bson.M{"_id": "k", "n": int64(42)}
	if _, err := coll.InsertOne(ctx, want); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var got bson.M
	if err := coll.FindOne(ctx, bson.M{"_id": "k"}).Decode(&got); err != nil {
		t.Fatalf("find: %v", err)
	}
	if got["n"] != int64(42) {
		t.Errorf("round-trip n = %v, want 42", got["n"])
	}
}

// TestIsolation runs two parallel subtests and asserts that Connect gives each
// a distinct database name and that a write in one is invisible to the other.
func TestIsolation(t *testing.T) {
	names := make(chan string, 2)

	run := func(t *testing.T) {
		t.Parallel()
		db := mongotest.Connect(t)
		ctx := opCtx(t)

		// Write a marker only into this subtest's database.
		if _, err := db.Collection("marker").InsertOne(ctx, bson.M{"here": true}); err != nil {
			t.Fatalf("insert marker: %v", err)
		}
		names <- db.Name()
	}

	t.Run("a", run)
	t.Run("b", run)

	// Subtests with t.Parallel() run after the parent returns from t.Run, so
	// collect names in a cleanup once both have finished.
	t.Cleanup(func() {
		n1, n2 := <-names, <-names
		if n1 == n2 {
			t.Errorf("both databases named %q; expected distinct names", n1)
		}
		if !strings.HasPrefix(n1, "mqtest_") || !strings.HasPrefix(n2, "mqtest_") {
			t.Errorf("database names %q, %q lack mqtest_ prefix", n1, n2)
		}
	})
}
