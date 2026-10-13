package mongoqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Queue is a handle to a fair, multi-tenant job queue backed by a single
// MongoDB collection. It is constructed with New and carries no live resources
// of its own; the caller owns the *mongo.Collection and its client lifecycle. A
// Queue is safe to share across goroutines once constructed.
type Queue struct {
	coll   *mongo.Collection
	nodeID string
	vtimes *vtimeCache // node-local per-(tenant, partition) vtime soft state (see vtimecache.go)
	// primary is coll with primary read preference, for reads that must not
	// see a stale secondary (Get, Cancel's diagnostic read).
	primary *mongo.Collection
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
//
// Lease fencing relies on acknowledged writes: the queue never inspects or
// overrides coll's write concern, so that choice is the caller's. An
// unacknowledged write concern (w: 0) breaks fencing, because a fence miss
// cannot be observed. Single resolution across a failover requires
// w: "majority"; a w: 1 write can roll back, letting a second resolution win.
//
// The queue also requires coll's default collation to be simple (binary): a
// non-simple collation makes partition matching and the _id tie-break follow
// it (a case-insensitive one matches partition "A" to "a"). The client must
// not omit empty values (BSONOptions.OmitEmpty or equivalent): the envelope
// relies on zero values such as attempts: 0 and vstamp: 0 being stored, and
// omitting them breaks attempt counting and claim ordering. Seed reads (which
// go to the primary) must observe the queue's own acknowledged inserts: use
// the default "local" read concern, or pair "majority" reads with
// w: "majority" writes. With w: 1 writes and "majority" reads, an
// acknowledged insert can be missing from the snapshot the seed reads, and
// the timeline re-issues that job's vstamp.
func New(coll *mongo.Collection, opts ...Option) *Queue {
	q := &Queue{
		coll:   coll,
		nodeID: defaultNodeID(),
		vtimes: newVtimeCache(),
	}
	if coll != nil {
		q.primary = coll.Clone(options.Collection().SetReadPreference(readpref.Primary()))
	}
	q.vtimes.seedHook = q.seedTenantVtime
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// seedTenantVtime is the vtime cache's seed (see vtimeCache.seedHook): on
// the first enqueue for a (tenant, partition) timeline this node has not
// seeded, it returns the timeline's trailing edge -- the maximum vstamp among
// the tenant's pending jobs in that partition, or 0 if there are none.
// Resolved jobs are excluded -- their timeline slots are already consumed and
// TTL-collected records must not pin the floor.
//
// Today the seed runs once per timeline per node. Reconciliation (Phase
// 6.2-6.3) will invalidate entries so each timeline's next enqueue re-runs
// this query, floored by the partition's dispatch frontier (its low-water
// mark, LWM); re-seeding will track other writers, cancellation refunds, and
// restarts.
//
// The read goes to the primary so it cannot see a trailing edge older than a
// stamp this node issued. Primary routing is not sufficient on its own: the
// read concern must also observe the queue's acknowledged inserts (see New).
//
// Index dependency: this query is served by the seed index
// {tenant: 1, partition: 1, liveness: 1, vstamp: -1} that EnsureIndexes
// creates. The claim index leads with partition and cannot serve the
// per-tenant lookup; without the seed index, seeding a timeline is a filtered
// collection scan under that timeline's cache lock.
func (q *Queue) seedTenantVtime(ctx context.Context, tenant, partition string) (int64, error) {
	filter := bson.D{
		{Key: "tenant", Value: tenant},
		{Key: "partition", Value: partition},
		{Key: "liveness", Value: LivenessPending},
	}
	opts := options.FindOne().
		SetSort(bson.D{{Key: "vstamp", Value: -1}}).
		SetProjection(bson.D{{Key: "vstamp", Value: 1}})

	var doc struct {
		VStamp int64 `bson:"vstamp"`
	}
	err := q.primary.FindOne(ctx, filter, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("mongoqueue: seed vtime for tenant %q partition %q: %w", tenant, partition, err)
	}
	return doc.VStamp, nil
}

// computeLWM returns partition's dispatch frontier (its low-water mark): the
// vstamp the next claim would take, found with the claim filter and sort.
// ok is false when the partition has no visible pending job.
//
// Visibility filtering matters: a leased or delayed job is not dispatchable
// now, so it must not pin the floor below the point of service.
//
// The read goes to the primary, like the seed query, and is served by the
// claim index.
func (q *Queue) computeLWM(ctx context.Context, partition string) (lwm int64, ok bool, err error) {
	opts := options.FindOne().
		SetSort(claimSort()).
		SetProjection(bson.D{{Key: "vstamp", Value: 1}})

	var doc struct {
		VStamp int64 `bson:"vstamp"`
	}
	err = q.primary.FindOne(ctx, claimFilter(partition), opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("mongoqueue: compute frontier for partition %q: %w", partition, err)
	}
	return doc.VStamp, true, nil
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

// EnsureIndexes creates the two indexes the queue's hot paths depend on.
//
// The claim index {partition: 1, liveness: 1, vstamp: 1, _id: 1, visible_at: 1}
// serves dispatch: the first four keys serve the claim filter and its
// (vstamp, _id) sort; visible_at rides in the index so the visibility
// predicate filters in-index without fetching documents.
//
// The seed index {tenant: 1, partition: 1, liveness: 1, vstamp: -1} serves
// the seed query, which reads a (tenant, partition) timeline's maximum
// pending vstamp.
//
// Indexes are created with the driver's auto-generated names, so re-calling
// EnsureIndexes with identical key specifications is a server-side no-op and
// returns no error. Index creation is never automatic; the caller invokes this
// deliberately because runtime credentials often lack createIndex and index
// builds on a populated collection are a scheduled operational event.
func (q *Queue) EnsureIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{Keys: bson.D{
			{Key: "partition", Value: 1},
			{Key: "liveness", Value: 1},
			{Key: "vstamp", Value: 1},
			{Key: "_id", Value: 1},
			{Key: "visible_at", Value: 1},
		}},
		{Keys: bson.D{
			{Key: "tenant", Value: 1},
			{Key: "partition", Value: 1},
			{Key: "liveness", Value: 1},
			{Key: "vstamp", Value: -1},
		}},
	}
	if _, err := q.coll.Indexes().CreateMany(ctx, models); err != nil {
		return fmt.Errorf("mongoqueue: create claim and seed indexes: %w", err)
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

// reconcile bounds vtime cache drift: each timeline reserved since the
// previous reconcile is marked stale, so its next enqueue re-seeds from the
// trailing edge (picking up other writers' stamps and trailing cancellation
// refunds), and each timeline idle since then is evicted. A done ctx returns
// its error and leaves the cache untouched.
func (q *Queue) reconcile(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mongoqueue: reconcile: %w", err)
	}
	q.vtimes.invalidateAndEvict()
	return nil
}
