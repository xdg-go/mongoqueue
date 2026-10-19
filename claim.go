package mongoqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ClaimedJob is the handle a worker receives from Claim: the claimed record
// plus the claim ID that authorizes Heartbeat, Release, and Complete. Each
// claim assigns the job a new random claim ID, and those methods succeed only
// while the stored job still carries it, so a worker whose job was reclaimed
// by another worker cannot touch it.
//
// The embedded Job is an immutable claim-time snapshot. Later operations do
// not update it; call Get for fresh state. Mutating its exported fields
// (including Job.ID and Job.ClaimID) changes nothing server-side and does not
// affect later operations on the handle, which use the job id and claim ID
// captured privately at claim time. Methods on a ClaimedJob read only that
// immutable private state and are safe to call concurrently.
type ClaimedJob struct {
	Job

	q       *Queue
	jobID   string // captured at claim time; guards use this, not Job.ID
	claimID string // the fence minted for this claim; unexported so it cannot be forged
}

// ClaimID returns the claim ID assigned by this claim: 24 lowercase hex
// characters, never empty.
func (c *ClaimedJob) ClaimID() string { return c.claimID }

// Claim leases the visible pending job in partition with the lowest
// (VStamp, ID) and returns a handle to it. An empty partition means the
// default partition, as at enqueue.
//
// The claim is one atomic server-side write: it assigns a new claim ID, pushes
// visible_at to server time plus lease, and increments Attempts. Concurrent
// claimers never receive the same job while its lease is live; an expired
// lease makes the job claimable again with no sweeper.
//
// Effects are at-least-once: a worker that stalls past its lease, crashes,
// or loses its Complete to a timeout can leave work done while the job is
// claimed again, so workers must be idempotent.
//
// lease must be positive; lease <= 0 returns ErrInvalidLease before any
// server round trip. Sub-millisecond leases round up to one millisecond. An
// empty visible set returns ErrNoJob. Other errors wrap the driver error; a
// context timeout or network error leaves the outcome unknown (the job may
// have been claimed and will become visible again when the lease lapses).
// On any error the returned handle is nil.
func (q *Queue) Claim(ctx context.Context, partition string, lease time.Duration) (*ClaimedJob, error) {
	if lease <= 0 {
		return nil, ErrInvalidLease
	}
	if partition == "" {
		partition = defaultPartition
	}
	claimID := bson.NewObjectID().Hex()

	filter := claimFilter(partition)
	opts := options.FindOneAndUpdate().
		SetSort(claimSort()).
		SetReturnDocument(options.After)
	// A pipeline update so $$NOW supplies server time. The claim id is
	// wrapped in $literal: in a pipeline a "$"-prefixed string is a field path.
	update := mongo.Pipeline{{{Key: "$set", Value: bson.D{
		{Key: "claim_id", Value: bson.D{{Key: "$literal", Value: claimID}}},
		{Key: "visible_at", Value: bson.D{{Key: "$add", Value: bson.A{"$$NOW", durationMillis(lease)}}}},
		{Key: "attempts", Value: bson.D{{Key: "$add", Value: bson.A{"$attempts", 1}}}},
	}}}}

	var job Job
	err := q.coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&job)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrNoJob
	}
	if err != nil {
		return nil, fmt.Errorf("mongoqueue: claim in partition %q: %w", partition, err)
	}
	return &ClaimedJob{Job: job, q: q, jobID: job.ID, claimID: claimID}, nil
}

// claimFilter selects partition's visible set: pending jobs whose visible_at
// has passed in server time ($$NOW).
func claimFilter(partition string) bson.D {
	return bson.D{
		{Key: "partition", Value: partition},
		{Key: "liveness", Value: LivenessPending},
		{Key: "$expr", Value: bson.D{{Key: "$lte", Value: bson.A{"$visible_at", "$$NOW"}}}},
	}
}

// claimSort orders the visible set for dispatch: lowest (vstamp, _id) first.
func claimSort() bson.D {
	return bson.D{{Key: "vstamp", Value: 1}, {Key: "_id", Value: 1}}
}
