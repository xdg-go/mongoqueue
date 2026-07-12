package mongoqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Queue is a handle to a fair, multi-tenant job queue backed by a single
// MongoDB collection. It is constructed with New and carries no live resources
// of its own; the caller owns the *mongo.Collection and its client lifecycle. A
// Queue is safe to share across goroutines once constructed.
type Queue struct {
	coll   *mongo.Collection
	nodeID string
}

// Option configures a Queue at construction time. Options are applied by New in
// the order given.
type Option func(*Queue)

// WithNodeID overrides the default per-process node id. The node id is recorded
// as stamped_by on every enqueue; setting it explicitly is useful for tests
// that simulate several writers from one process and for deployments that want
// a stable, human-meaningful identifier.
func WithNodeID(id string) Option {
	return func(q *Queue) {
		q.nodeID = id
	}
}

// New returns a Queue over coll. It performs no I/O -- no index creation, no
// ping, no round trip -- so it never fails and never blocks on the server.
// Index creation is caller-invoked via EnsureIndexes and EnsureTTLIndex.
//
// By default the queue mints a node id of the form hostname-pid-<random hex>;
// pass WithNodeID to override it.
func New(coll *mongo.Collection, opts ...Option) *Queue {
	q := &Queue{
		coll:   coll,
		nodeID: defaultNodeID(),
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// defaultNodeID builds hostname-pid-<random hex>. A hostname lookup failure
// falls back to "unknown" so the id is always well formed; the random suffix
// keeps two processes on the same host (or a host whose name is unknown)
// distinct.
func defaultNodeID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), randomHex())
}

// randomHex returns 8 hex characters (4 random bytes). A crypto/rand read
// failure is treated as unrecoverable: node ids must be distinct, so a silent
// zero suffix would be worse than a panic at startup.
func randomHex() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("mongoqueue: read random bytes for node id: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// EnsureIndexes creates the claim index required for dispatch:
// {partition: 1, liveness: 1, vstamp: 1, _id: 1, visible_at: 1}. The first four
// keys serve the claim filter and its (vstamp, _id) sort; visible_at rides in
// the index so the visibility predicate filters in-index without fetching
// documents.
//
// The index is created with the driver's auto-generated name, so re-calling
// EnsureIndexes with an identical key specification is a server-side no-op and
// returns no error. Index creation is never automatic; the caller invokes this
// deliberately because runtime credentials often lack createIndex and index
// builds on a populated collection are a scheduled operational event.
func (q *Queue) EnsureIndexes(ctx context.Context) error {
	model := mongo.IndexModel{
		Keys: bson.D{
			{Key: "partition", Value: 1},
			{Key: "liveness", Value: 1},
			{Key: "vstamp", Value: 1},
			{Key: "_id", Value: 1},
			{Key: "visible_at", Value: 1},
		},
	}
	if _, err := q.coll.Indexes().CreateOne(ctx, model); err != nil {
		return fmt.Errorf("mongoqueue: create claim index: %w", err)
	}
	return nil
}

// EnsureTTLIndex creates the garbage-collection TTL index on resolved_at with
// expireAfterSeconds derived from retention. Because resolved_at is set only by
// the terminal write, the TTL never touches pending or claimed jobs.
//
// There is no default retention and no TTL index unless the caller invokes
// this: TTL is destructive policy, kept separate from the required claim index
// so a caller can create the latter without the former.
//
// Re-calling with the same retention is a no-op. Re-calling with a *different*
// retention returns the driver's index-conflict error rather than silently
// dropping and recreating the index; the caller resolves the change with a
// collMod command against expireAfterSeconds.
//
// retention must be at least one second and at most math.MaxInt32 seconds
// (~68 years). MongoDB TTL granularity is whole seconds, so a sub-second
// retention would truncate to expireAfterSeconds: 0 -- deleting every resolved
// job the instant it resolves. Rather than create that silent-data-loss index,
// EnsureTTLIndex rejects an out-of-range retention before any server round trip.
func (q *Queue) EnsureTTLIndex(ctx context.Context, retention time.Duration) error {
	seconds := int64(retention / time.Second)
	if seconds < 1 || seconds > math.MaxInt32 {
		return fmt.Errorf("mongoqueue: TTL retention must be between 1s and %ds (~68 years), got %v", int64(math.MaxInt32), retention)
	}
	model := mongo.IndexModel{
		Keys:    bson.D{{Key: "resolved_at", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(int32(seconds)),
	}
	if _, err := q.coll.Indexes().CreateOne(ctx, model); err != nil {
		return fmt.Errorf("mongoqueue: create TTL index: %w", err)
	}
	return nil
}
