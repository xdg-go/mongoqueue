package mongoqueue

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Heartbeat extends the lease: visible_at becomes server time plus extend.
// The extension is measured from now, not from the current visible_at, so
// repeated heartbeats do not stack. A holder whose lease expired but was not
// reclaimed still holds its claim and may heartbeat.
//
// extend must be positive; extend <= 0 returns ErrInvalidLease before any
// server round trip. Sub-millisecond extensions round up to one millisecond.
// If the job no longer carries this claim's ID (reclaimed, released,
// canceled, completed, or deleted) Heartbeat returns ErrLeaseLost. Other errors wrap the driver
// error; a context timeout or network error leaves the outcome unknown.
func (c *ClaimedJob) Heartbeat(ctx context.Context, extend time.Duration) error {
	if extend <= 0 {
		return ErrInvalidLease
	}
	return c.fencedUpdate(ctx, "heartbeat", bson.D{
		{Key: "visible_at", Value: bson.D{{Key: "$add", Value: bson.A{"$$NOW", durationMillis(extend)}}}},
	})
}

// Release gives up the claim early: visible_at becomes server time plus
// delay and the claim ID is cleared, so the job is reclaimable once delay
// elapses (immediately for zero). The next claim increments Attempts and
// assigns a new claim ID; VStamp is unchanged.
//
// delay must be non-negative; delay < 0 returns ErrInvalidDelay before any
// server round trip. If the job no longer carries this claim's ID, Release
// returns ErrLeaseLost. Release is not success-idempotent: repeating it after a
// success returns ErrLeaseLost, as does a later Heartbeat or Complete. Other
// errors wrap the driver error; a context timeout or network error leaves
// the outcome unknown.
func (c *ClaimedJob) Release(ctx context.Context, delay time.Duration) error {
	if delay < 0 {
		return ErrInvalidDelay
	}
	return c.fencedUpdate(ctx, "release", bson.D{
		{Key: "visible_at", Value: bson.D{{Key: "$add", Value: bson.A{"$$NOW", durationMillis(delay)}}}},
		{Key: "claim_id", Value: bson.D{{Key: "$literal", Value: ""}}},
	})
}

// fencedUpdate applies set as a pipeline $set (so $$NOW is server time),
// guarded on the job id and fence captured at claim time and on pending
// liveness. There is no visibility condition: an expired but unreclaimed
// lease still holds the fence. MatchedCount, not ModifiedCount, decides the
// outcome: a write that stores values identical to the current ones (two
// heartbeats in the same millisecond) matches but modifies nothing.
func (c *ClaimedJob) fencedUpdate(ctx context.Context, op string, set bson.D) error {
	filter := bson.D{
		{Key: "_id", Value: c.jobID},
		{Key: "claim_id", Value: c.claimID},
		{Key: "liveness", Value: LivenessPending},
	}
	res, err := c.q.coll.UpdateOne(ctx, filter, mongo.Pipeline{{{Key: "$set", Value: set}}})
	if err != nil {
		return fmt.Errorf("mongoqueue: %s job %q: %w", op, c.jobID, err)
	}
	if res.MatchedCount == 0 {
		return ErrLeaseLost
	}
	return nil
}
