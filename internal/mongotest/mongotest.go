// Package mongotest provides a hermetic MongoDB test harness. Each call to
// Connect yields a freshly named database that is dropped when the test ends,
// so parallel tests never collide and no user configuration is touched.
package mongotest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// envURI is the environment variable naming the MongoDB deployment tests run
// against. It defaults to defaultURI so a bare local mongod and a CI service
// container both work with no special-casing.
const envURI = "MONGOQUEUE_TEST_URI"

// defaultURI is used when envURI is unset.
const defaultURI = "mongodb://localhost:27017"

// connectTimeout bounds each harness operation (connect, ping, cleanup) so a
// dead server fails a test promptly rather than hanging.
const connectTimeout = 10 * time.Second

// Connect returns a *mongo.Database with a unique name for the calling test.
// It reads the MONGOQUEUE_TEST_URI environment variable (default
// mongodb://localhost:27017), connects, and pings to fail fast when the server
// is unreachable. It registers a t.Cleanup that drops the database and
// disconnects the client. It is safe under t.Parallel(): each call gets its
// own database whose name carries a random suffix.
func Connect(t *testing.T) *mongo.Database {
	t.Helper()

	uri := os.Getenv(envURI)
	if uri == "" {
		uri = defaultURI
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("mongotest: connect to %q (set %s to override): %v", uri, envURI, err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		// Disconnect the half-open client before failing so we do not leak it.
		if derr := client.Disconnect(ctx); derr != nil {
			t.Logf("mongotest: disconnect after failed ping: %v", derr)
		}
		t.Fatalf("mongotest: MongoDB unreachable at %q (set %s to override): %v", uri, envURI, err)
	}

	db := client.Database("mqtest_" + randomHex(t))

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), connectTimeout)
		defer cleanupCancel()
		if err := db.Drop(cleanupCtx); err != nil {
			t.Errorf("mongotest: drop database %q: %v", db.Name(), err)
		}
		if err := client.Disconnect(cleanupCtx); err != nil {
			t.Errorf("mongotest: disconnect client: %v", err)
		}
	})

	return db
}

// randomHex returns 16 hex characters (8 random bytes) for use as a database
// name suffix. A crypto/rand read failure is fatal to the test rather than
// silently producing collidable names.
func randomHex(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("mongotest: read random bytes: %v", err)
	}
	return hex.EncodeToString(b[:])
}
