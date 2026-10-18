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
	q.vtimes.lwmHook = q.computeLWM
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
// The seed runs on a timeline's first enqueue on this node and again on its
// first enqueue after each reconcile, so re-seeding tracks other writers,
// cancellation refunds, and restarts. The cache floors the result by the
// partition's dispatch frontier (its low-water mark, LWM; see computeLWM);
// this query returns only the trailing edge.
//
// The read goes to the primary so it cannot see a trailing edge older than a
// stamp this node issued. Primary routing is not sufficient on its own: the
// read concern must also observe the queue's acknowledged inserts (see New).
//
// Index dependency: this query is covered by the seed index that
// EnsureIndexes creates. The claim index leads with partition and cannot
// serve the per-(tenant, partition) lookup; without the seed index, seeding
// a timeline is a filtered collection scan under that timeline's cache lock.
func (q *Queue) seedTenantVtime(ctx context.Context, tenant, partition string) (int64, error) {
	vstamp, _, err := q.firstVStamp(ctx, seedFilter(tenant, partition), vstampSort(-1))
	if err != nil {
		return 0, fmt.Errorf("mongoqueue: seed vtime for tenant %q partition %q: %w", tenant, partition, err)
	}
	return vstamp, nil
}

// seedFilter selects a (tenant, partition) timeline's pending jobs. The
// liveness predicate must stay literal so the query can use the seed index's
// partial filter.
func seedFilter(tenant, partition string) bson.D {
	return bson.D{
		{Key: "tenant", Value: tenant},
		{Key: "partition", Value: partition},
		{Key: "liveness", Value: LivenessPending},
	}
}

// computeLWM returns partition's dispatch frontier (its low-water mark): the
// partition's service clock, the maximum vstamp among jobs holding a claim
// fence (claim_id set, any liveness). Claim sets claim_id; Complete and
// Cancel keep it; only Release clears it. When no job holds a fence (never
// claimed, all released, or GC'd), it falls back to the minimum vstamp
// among pending jobs, visible or not. ok is false when the partition has
// neither.
//
// Job state (visibility, liveness) deliberately does not enter the clock.
// Filtering by visibility leaves no frontier in a fully leased or
// delayed-only partition, so newcomers start at a stale value or 0 and jump
// ahead of work already scheduled. Taking a minimum over pending jobs lets a
// long lease, a released retry, or a long delay pin the floor for its
// lifetime, handing newcomers unbounded credit. The maximum fenced vstamp
// only advances with service; it dips only when the top claimed job is
// released or GC'd.
//
// Both reads go to the primary, like the seed query, and are covered: the
// clock by the clock index, the fallback by the claim index. The fallback
// runs only when the clock query finds nothing.
func (q *Queue) computeLWM(ctx context.Context, partition string) (lwm int64, ok bool, err error) {
	lwm, ok, err = q.firstVStamp(ctx, clockFilter(partition), vstampSort(-1))
	if err == nil && !ok {
		lwm, ok, err = q.firstVStamp(ctx, pendingFilter(partition), vstampSort(1))
	}
	if err != nil {
		return 0, false, fmt.Errorf("mongoqueue: compute frontier for partition %q: %w", partition, err)
	}
	return lwm, ok, nil
}

// clockFilter selects partition's fenced (claimed, unreleased) jobs. Released and
// never-claimed jobs store an empty claim_id; $gt "" also skips a missing
// field. The claim_id predicate must stay literal so the query can use the
// clock index's partial filter.
func clockFilter(partition string) bson.D {
	return bson.D{
		{Key: "partition", Value: partition},
		{Key: "claim_id", Value: bson.D{{Key: "$gt", Value: ""}}},
	}
}

// pendingFilter selects partition's pending jobs in any visibility state.
// The liveness predicate must stay literal so the query can use the claim
// index's partial filter.
func pendingFilter(partition string) bson.D {
	return bson.D{
		{Key: "partition", Value: partition},
		{Key: "liveness", Value: LivenessPending},
	}
}

// firstVStamp returns the vstamp of the first job matching filter in sort
// order from the primary; ok is false when nothing matches. See
// vstampProjection for how the query is covered.
func (q *Queue) firstVStamp(ctx context.Context, filter, sort bson.D) (vstamp int64, ok bool, err error) {
	opts := options.FindOne().
		SetSort(sort).
		SetProjection(vstampProjection())

	var doc struct {
		VStamp int64 `bson:"vstamp"`
	}
	err = q.primary.FindOne(ctx, filter, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read vstamp: %w", err)
	}
	return doc.VStamp, true, nil
}

// vstampSort orders by vstamp alone: dir 1 ascending, -1 descending.
func vstampSort(dir int) bson.D {
	return bson.D{{Key: "vstamp", Value: dir}}
}

// vstampProjection returns only vstamp. Excluding _id lets an index on the
// filter and sort keys cover the query.
func vstampProjection() bson.D {
	return bson.D{{Key: "vstamp", Value: 1}, {Key: "_id", Value: 0}}
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

// EnsureIndexes creates the three partial indexes the queue's reads depend on.
//
//   - claim {partition: 1, vstamp: 1, _id: 1, visible_at: 1}, partial on
//     {liveness: "pending"}, serves Claim and the frontier's fallback: the
//     leading keys match the claim filter and its (vstamp, _id) sort, and
//     visible_at rides in the index so the visibility predicate filters
//     in-index without fetching documents.
//   - seed {tenant: 1, partition: 1, vstamp: -1}, partial on
//     {liveness: "pending"}, serves the seed query, which reads a (tenant,
//     partition) timeline's maximum pending vstamp.
//   - clock {partition: 1, vstamp: -1}, partial on {claim_id: {$gt: ""}},
//     serves the frontier's service clock, the maximum vstamp among a
//     partition's fenced (claimed, unreleased) jobs.
//
// Each index is partial so it holds only the jobs its queries can match:
// resolved jobs leave the claim and seed indexes, and never-claimed or
// released jobs stay out of the clock index. The predicate in the partial
// filter therefore leaves the keys. MongoDB uses a partial index only when
// the query filter literally implies the partial filter, so every query these
// indexes serve keeps liveness: "pending" or claim_id: {$gt: ""} as a literal
// predicate.
//
// Partiality costs no extra index writes over a job's lifecycle (7, the same
// as two full indexes): enqueue inserts 2 entries; the first claim rewrites
// the claim entry and inserts the clock entry; resolution deletes the claim
// and seed entries; TTL removal deletes the clock entry. A reclaim leaves the
// clock entry alone (its keys do not change); Release deletes it and the next
// claim re-adds it.
//
// Indexes are named, so re-calling EnsureIndexes with identical specifications
// is a server-side no-op and returns no error. The names mongoqueue_claim,
// mongoqueue_seed, and mongoqueue_clock are reserved on the collection.
// Indexes from earlier versions of this library are not dropped: the
// auto-named partition_1_liveness_1_vstamp_1__id_1_visible_at_1_claim_id_1 and
// tenant_1_partition_1_liveness_1_vstamp_-1 add write cost without serving any
// query, and the operator can drop them. Index creation is never automatic; the
// caller invokes this deliberately because runtime credentials often lack
// createIndex and index builds on a populated collection are a scheduled
// operational event.
func (q *Queue) EnsureIndexes(ctx context.Context) error {
	pending := bson.D{{Key: "liveness", Value: LivenessPending}}
	claimed := bson.D{{Key: "claim_id", Value: bson.D{{Key: "$gt", Value: ""}}}}
	models := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "partition", Value: 1},
				{Key: "vstamp", Value: 1},
				{Key: "_id", Value: 1},
				{Key: "visible_at", Value: 1},
			},
			Options: options.Index().SetName(claimIndexName).SetPartialFilterExpression(pending),
		},
		{
			Keys: bson.D{
				{Key: "tenant", Value: 1},
				{Key: "partition", Value: 1},
				{Key: "vstamp", Value: -1},
			},
			Options: options.Index().SetName(seedIndexName).SetPartialFilterExpression(pending),
		},
		{
			Keys: bson.D{
				{Key: "partition", Value: 1},
				{Key: "vstamp", Value: -1},
			},
			Options: options.Index().SetName(clockIndexName).SetPartialFilterExpression(claimed),
		},
	}
	if _, err := q.coll.Indexes().CreateMany(ctx, models); err != nil {
		return fmt.Errorf("mongoqueue: create claim, seed, and clock indexes: %w", err)
	}
	return nil
}

// Index names EnsureIndexes assigns.
const (
	claimIndexName = "mongoqueue_claim"
	seedIndexName  = "mongoqueue_seed"
	clockIndexName = "mongoqueue_clock"
)

// ttlIndexName is the name EnsureTTLIndex assigns.
const ttlIndexName = "mongoqueue_ttl"

// EnsureTTLIndex creates the garbage-collection TTL index, named
// mongoqueue_ttl (reserved on the collection), on resolved_at with
// expireAfterSeconds derived from retention. The index is partial on
// liveness: resolved, so it holds resolved jobs only and the TTL can never
// delete a pending or claimed job, whatever fields that job carries.
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
//
// The dispatch frontier reads resolved jobs' stamps (the service clock), so a
// very short retention can drop the clock back to the min-pending fallback
// once all claimed history is removed; retention should comfortably exceed
// the time between claims in a partition.
func (q *Queue) EnsureTTLIndex(ctx context.Context, retention time.Duration) error {
	seconds := int64(retention / time.Second)
	if seconds < 1 || seconds > math.MaxInt32 {
		return fmt.Errorf("mongoqueue: TTL retention must be between 1s and %ds (~68 years), got %v", int64(math.MaxInt32), retention)
	}
	model := mongo.IndexModel{
		Keys: bson.D{{Key: "resolved_at", Value: 1}},
		Options: options.Index().
			SetName(ttlIndexName).
			SetExpireAfterSeconds(int32(seconds)).
			SetPartialFilterExpression(bson.D{{Key: "liveness", Value: LivenessResolved}}),
	}
	if _, err := q.coll.Indexes().CreateOne(ctx, model); err != nil {
		return fmt.Errorf("mongoqueue: create TTL index: %w", err)
	}
	return nil
}

// Reconcile bounds how far this node's cached virtual times can drift from
// the stored jobs. Callers must run it periodically, typically on a ticker;
// the interval is the staleness bound. Each call refreshes the dispatch
// frontier of every partition in this node's cache, marks every
// (tenant, partition) timeline used since the previous call so that its next
// enqueue re-reads its position from the database, and evicts timelines left
// unused for two consecutive calls (so an idle timeline is dropped within two
// intervals).
//
// After each call, every marked timeline re-seeds before its next stamp,
// which picks up stamps written by other producers and reclaims virtual time
// from canceled jobs at the end of a tenant's backlog. New tenants, and
// tenants whose timelines were evicted, are placed at the partition's
// frontier even without Reconcile, because a timeline's first enqueue
// refreshes that frontier itself. Drift in active timelines, however, is only
// corrected by Reconcile.
//
// Every partition's refresh is attempted; failures are joined into the
// returned error, and the walk (marking and eviction) runs regardless, so one
// failing partition cannot stall drift correction or eviction for the rest.
// Marking a timeline whose partition failed to refresh is safe: its re-seed
// uses the frontier stored by an earlier refresh. Once ctx is done the
// remaining refreshes are skipped; a ctx already done on entry skips the walk
// too. Retrying on the next tick is sufficient. Reconcile is safe to call
// concurrently with itself and with all other Queue methods.
func (q *Queue) Reconcile(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("mongoqueue: reconcile: %w", err)
	}
	var errs []error
	for _, partition := range q.vtimes.partitions() {
		// A ctx canceled during a refresh that still succeeded is caught
		// here, before another refresh is started on a dead ctx.
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := q.vtimes.refreshLWM(ctx, partition); err != nil {
			errs = append(errs, fmt.Errorf("refresh LWM for partition %q: %w", partition, err))
			if ctx.Err() != nil {
				break
			}
		}
	}
	q.vtimes.invalidateAndEvict()
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("mongoqueue: reconcile: %w", err)
	}
	return nil
}
