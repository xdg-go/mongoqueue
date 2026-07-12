package mongoqueue

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// defaultPartition is the partition stored when EnqueueOpts.Partition is
// empty. An unpartitioned queue is one default partition -- claims and
// enqueues that never name a partition all meet here, so the claim path is
// identical whether or not a caller uses partitioning.
const defaultPartition = "default"

// EnqueueOpts carries the caller-supplied envelope fields for one enqueue.
// It is the input counterpart of the Job record: the caller never constructs
// a Job, and every field the library computes (vstamp, liveness, claim state)
// has no home here.
//
// Envelope fields travel in EnqueueOpts, not inside the body -- the library
// must read them and is contractually blind to the body.
type EnqueueOpts struct {
	// JobID is the caller-supplied job id: the record's Mongo "_id" and
	// the idempotency key for enqueue. Required; enqueue rejects an empty
	// id with ErrMissingJobID.
	JobID string

	// TenantID names the tenant whose virtual timeline the job is stamped
	// on. Single-tenant callers leave it empty -- one tenant named "" is
	// the degenerate case, same code path.
	TenantID string

	// Partition scopes claims; empty means the default partition. The
	// literal "default" names that same partition, so passing it is
	// equivalent to leaving Partition empty.
	Partition string

	// Cost is the job's declared cost in cost-units. Zero and negative
	// values are floored to 1 (the cheapest possible job), never rejected.
	Cost int64

	// Weight is the tenant's fair-share weight for this enqueue. Zero
	// means 1; otherwise it must be >= 1, and a negative value is
	// rejected with ErrInvalidWeight. Weight consistency across a
	// tenant's producers is a caller obligation.
	Weight int64
}

// normalizedEnqueue is the validated form of EnqueueOpts: cost and weight
// normalized per vtime.go, partition defaulted. Produced by
// EnqueueOpts.normalize so validation is testable without a server.
type normalizedEnqueue struct {
	jobID     string
	tenantID  string
	partition string
	cost      int64
	weight    int64
}

// normalize validates opts and resolves defaults: JobID is required
// (ErrMissingJobID), cost is floored via normalizeCost, weight is checked via
// normalizeWeight (ErrInvalidWeight), and an empty Partition maps to the
// default partition.
func (o EnqueueOpts) normalize() (normalizedEnqueue, error) {
	if o.JobID == "" {
		return normalizedEnqueue{}, ErrMissingJobID
	}
	weight, err := normalizeWeight(o.Weight)
	if err != nil {
		return normalizedEnqueue{}, err
	}
	partition := o.Partition
	if partition == "" {
		partition = defaultPartition
	}
	return normalizedEnqueue{
		jobID:     o.JobID,
		tenantID:  o.TenantID,
		partition: partition,
		cost:      normalizeCost(o.Cost),
		weight:    weight,
	}, nil
}

// enqueue is the non-generic insert path: it stamps the job on its tenant's
// virtual timeline and inserts the durable record. It stays unexported -- the
// typed Kind facade (Phase 3.3) is the only public entry point for bodies,
// because the kind string and the body's Go type must be paired at a single
// declaration site.
//
// The reserve->insert->commit sequence holds the tenant's cache lock for the
// duration of the insert (see vtimecache.go). The deferred abort guarantees
// the lock is released even if InsertOne panics; the cached vtime advances
// only on a successful insert, so a failed or duplicate insert advances
// nothing and the candidate slot is reused. Because the lock is held across
// the insert, callers should bound ctx with a deadline: a hung insert blocks
// only that tenant's enqueue path, but blocks it for the duration.
//
// enqueue trusts its caller for two invariants owned by the Kind facade
// (Phase 3.3): kind is non-empty (NewKind panics on empty), and body is a
// well-formed BSON document (the facade marshals it from T).
func (q *Queue) enqueue(ctx context.Context, kind string, body bson.Raw, opts EnqueueOpts) error {
	n, err := opts.normalize()
	if err != nil {
		return err
	}

	res, err := q.vtimes.reserve(ctx, n.tenantID, n.cost, n.weight)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			res.Abort()
		}
	}()

	job := Job{
		ID:        n.jobID,
		TenantID:  n.tenantID,
		Partition: n.partition,
		Cost:      n.cost,
		VStamp:    res.vstamp,
		Liveness:  LivenessPending,
		VisibleAt: time.Now().UTC(),
		StampedBy: q.nodeID,
		Kind:      kind,
		Body:      body,
	}
	if _, err := q.coll.InsertOne(ctx, job); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrDuplicateJob
		}
		return fmt.Errorf("mongoqueue: insert job %q: %w", n.jobID, err)
	}

	res.Commit()
	committed = true
	return nil
}
