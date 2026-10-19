package mongoqueue

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Complete resolves the claimed job with r: liveness becomes resolved,
// Resolution r, and ResolvedAt server time. A resolved job never becomes
// claimable again. A holder whose lease expired but was not reclaimed still
// holds its claim and may complete.
//
// r must be non-empty; an empty r returns ErrEmptyResolution before any
// server round trip. If the job no longer carries this claim's ID
// (reclaimed, released, canceled, already completed, or deleted) Complete returns ErrLeaseLost.
// Complete is not success-idempotent: repeating it after a success returns
// ErrLeaseLost. Other errors wrap the driver error; a context timeout or
// network error leaves the outcome unknown, and Get can show the current
// state but cannot prove which call caused it.
func (c *ClaimedJob) Complete(ctx context.Context, r Resolution) error {
	if r == "" {
		return ErrEmptyResolution
	}
	return c.fencedUpdate(ctx, "complete", resolveSet(r))
}

// Cancel resolves a pending job with ResolutionCanceled, whether or not it
// is claimed. It needs no claim ID and leaves claim_id intact, so a holder's
// next Heartbeat, Release, or Complete returns ErrLeaseLost.
//
// An empty jobID returns ErrMissingJobID before any server round trip. If
// the update matches no pending job, one primary read classifies the miss:
//
//   - absent: ErrJobNotFound.
//   - resolved: ErrJobResolved.
//   - pending: ErrJobResolved, without retrying against what may be a
//     different job. The id was inserted after the update missed: either
//     the target resolved, was TTL-deleted, and the id was reused, or the
//     id did not exist yet and a concurrent first enqueue inserted it.
//
// See ErrJobResolved for what that error does and does not prove. Other
// errors wrap the driver error; a context timeout or network error
// leaves the outcome unknown.
func (q *Queue) Cancel(ctx context.Context, jobID string) error {
	if jobID == "" {
		return ErrMissingJobID
	}
	filter := bson.D{
		{Key: "_id", Value: jobID},
		{Key: "liveness", Value: LivenessPending},
	}
	res, err := q.coll.UpdateOne(ctx, filter, mongo.Pipeline{{{Key: "$set", Value: resolveSet(ResolutionCanceled)}}})
	if err != nil {
		return fmt.Errorf("mongoqueue: cancel job %q: %w", jobID, err)
	}
	if res.MatchedCount == 1 {
		return nil
	}
	// Get's error is already ErrJobNotFound or a wrapped driver error naming
	// the job; re-wrapping would only duplicate the prefix.
	if _, err := q.Get(ctx, jobID); err != nil {
		return err
	}
	return ErrJobResolved
}

// Get returns a fresh copy of the stored record for jobID, read from the
// primary so a secondary cannot return stale state. It is the source of
// fresh state for ClaimedJob holders, whose handle is a claim-time snapshot.
// Mutating the returned Job changes nothing server-side.
//
// An empty jobID returns ErrMissingJobID before any server round trip. An
// absent job returns ErrJobNotFound. Other errors wrap the driver error. On
// any error the returned Job is nil.
func (q *Queue) Get(ctx context.Context, jobID string) (*Job, error) {
	if jobID == "" {
		return nil, ErrMissingJobID
	}
	var job Job
	err := q.primary.FindOne(ctx, bson.D{{Key: "_id", Value: jobID}}).Decode(&job)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("mongoqueue: get job %q: %w", jobID, err)
	}
	return &job, nil
}

// resolveSet is the terminal write's pipeline $set. The resolution is
// wrapped in $literal: in a pipeline a "$"-prefixed string is a field path
// or variable.
func resolveSet(r Resolution) bson.D {
	return bson.D{
		{Key: "liveness", Value: LivenessResolved},
		{Key: "resolution", Value: bson.D{{Key: "$literal", Value: r}}},
		{Key: "resolved_at", Value: "$$NOW"},
	}
}
