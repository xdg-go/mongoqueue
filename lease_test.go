package mongoqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// expireLease writes cj's job visible_at an hour into the past, so tests
// expire leases or end delays without sleeping.
func expireLease(ctx context.Context, t *testing.T, q *Queue, cj *ClaimedJob) {
	t.Helper()
	past := time.Now().Add(-time.Hour)
	res, err := q.coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: cj.jobID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "visible_at", Value: past}}}})
	if err != nil {
		t.Fatalf("expire lease on %q: %v", cj.jobID, err)
	}
	if res.MatchedCount != 1 {
		t.Fatalf("expire lease on %q: matched %d, want 1", cj.jobID, res.MatchedCount)
	}
}

// claimOne enqueues job "j1" and claims it with a one-minute lease.
func claimOne(ctx context.Context, t *testing.T, q *Queue) *ClaimedJob {
	t.Helper()
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "j1", TenantID: "t1", Cost: 1})
	return mustClaim(ctx, t, q, "")
}

// TestLeaseRejectsInvalidDurations proves extend <= 0 and delay < 0 fail
// before any round trip: the handle's queue has a nil collection, so any
// driver call would panic.
func TestLeaseRejectsInvalidDurations(t *testing.T) {
	t.Parallel()

	cj := &ClaimedJob{q: New(nil), jobID: "j1", claimID: "fence"}
	for _, extend := range []time.Duration{0, -time.Nanosecond, -time.Minute} {
		if err := cj.Heartbeat(context.Background(), extend); !errors.Is(err, ErrInvalidLease) {
			t.Errorf("Heartbeat(%v) = %v, want ErrInvalidLease", extend, err)
		}
	}
	for _, delay := range []time.Duration{-time.Nanosecond, -time.Minute} {
		if err := cj.Release(context.Background(), delay); !errors.Is(err, ErrInvalidDelay) {
			t.Errorf("Release(%v) = %v, want ErrInvalidDelay", delay, err)
		}
	}
}

// TestHeartbeatExtendsFromServerNow proves heartbeat sets visible_at to
// server now plus extend, back-to-back heartbeats (possibly in the same
// millisecond) both succeed without stacking, and vstamp is untouched.
func TestHeartbeatExtendsFromServerNow(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)

	const extend = 5 * time.Minute
	lo := serverNow(ctx, t, db)
	if err := cj.Heartbeat(ctx, extend); err != nil {
		t.Fatalf("first heartbeat: %v", err)
	}
	if err := cj.Heartbeat(ctx, extend); err != nil {
		t.Fatalf("second heartbeat: %v", err)
	}
	hi := serverNow(ctx, t, db)

	stored := findJob(ctx, t, q, "j1")
	if stored.VisibleAt.Before(lo.Add(extend)) || stored.VisibleAt.After(hi.Add(extend)) {
		t.Errorf("visible_at = %v, want within [%v, %v] (no stacking)", stored.VisibleAt, lo.Add(extend), hi.Add(extend))
	}
	if stored.VStamp != cj.VStamp {
		t.Errorf("vstamp changed: %d -> %d", cj.VStamp, stored.VStamp)
	}
	if stored.ClaimID != cj.ClaimID() {
		t.Errorf("claim_id = %q, want %q", stored.ClaimID, cj.ClaimID())
	}
}

// TestHeartbeatAfterUnreclaimedExpiry proves an expired lease that nobody
// reclaimed still holds the fence.
func TestHeartbeatAfterUnreclaimedExpiry(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)
	expireLease(ctx, t, q, cj)

	lo := serverNow(ctx, t, db)
	if err := cj.Heartbeat(ctx, time.Minute); err != nil {
		t.Fatalf("heartbeat after expiry: %v", err)
	}
	if stored := findJob(ctx, t, q, "j1"); stored.VisibleAt.Before(lo.Add(time.Minute)) {
		t.Errorf("visible_at = %v, want >= %v", stored.VisibleAt, lo.Add(time.Minute))
	}
}

// TestLeaseLostAfterReclaim proves that once another worker reclaims an
// expired job, the old holder's Heartbeat and Release return ErrLeaseLost
// and change nothing.
func TestLeaseLostAfterReclaim(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	old := claimOne(ctx, t, q)
	expireLease(ctx, t, q, old)
	reclaimed := mustClaim(ctx, t, q, "")
	before := findJob(ctx, t, q, "j1")

	if err := old.Heartbeat(ctx, time.Hour); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("old Heartbeat = %v, want ErrLeaseLost", err)
	}
	if err := old.Release(ctx, 0); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("old Release = %v, want ErrLeaseLost", err)
	}

	after := findJob(ctx, t, q, "j1")
	if !after.VisibleAt.Equal(before.VisibleAt) {
		t.Errorf("visible_at changed: %v -> %v", before.VisibleAt, after.VisibleAt)
	}
	if after.ClaimID != reclaimed.ClaimID() {
		t.Errorf("claim_id = %q, want reclaimer's %q", after.ClaimID, reclaimed.ClaimID())
	}
}

// TestReleaseZeroDelayReclaimable proves release clears the fence and, with
// zero delay, the job is immediately reclaimable with attempts incremented,
// a new claim id, and the same vstamp.
func TestReleaseZeroDelayReclaimable(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	first := claimOne(ctx, t, q)

	if err := first.Release(ctx, 0); err != nil {
		t.Fatalf("release: %v", err)
	}
	if stored := findJob(ctx, t, q, "j1"); stored.ClaimID != "" {
		t.Errorf("claim_id after release = %q, want empty", stored.ClaimID)
	}

	second := mustClaim(ctx, t, q, "")
	if second.ID != "j1" {
		t.Fatalf("reclaimed %q, want j1", second.ID)
	}
	if second.Attempts != first.Attempts+1 {
		t.Errorf("attempts = %d, want %d", second.Attempts, first.Attempts+1)
	}
	if second.ClaimID() == first.ClaimID() || second.ClaimID() == "" {
		t.Errorf("reclaim id = %q, want new non-empty id (old %q)", second.ClaimID(), first.ClaimID())
	}
	if second.VStamp != first.VStamp {
		t.Errorf("vstamp changed: %d -> %d", first.VStamp, second.VStamp)
	}
}

// TestReleaseDelayHidesJob proves a multi-minute release delay hides the job
// until visible_at passes.
func TestReleaseDelayHidesJob(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)

	if err := cj.Release(ctx, 10*time.Minute); err != nil {
		t.Fatalf("release: %v", err)
	}
	assertNoJob(ctx, t, q, "")

	expireLease(ctx, t, q, cj)
	if got := mustClaim(ctx, t, q, ""); got.ID != "j1" {
		t.Errorf("claimed %q, want j1", got.ID)
	}
}

// TestReleaseEndsFence proves a releaser cannot un-release: a later
// Heartbeat returns ErrLeaseLost without touching visible_at, and a second
// Release also returns ErrLeaseLost.
func TestReleaseEndsFence(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)

	if err := cj.Release(ctx, 10*time.Minute); err != nil {
		t.Fatalf("release: %v", err)
	}
	before := findJob(ctx, t, q, "j1")

	if err := cj.Heartbeat(ctx, time.Hour); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("Heartbeat after release = %v, want ErrLeaseLost", err)
	}
	if after := findJob(ctx, t, q, "j1"); !after.VisibleAt.Equal(before.VisibleAt) {
		t.Errorf("visible_at changed: %v -> %v", before.VisibleAt, after.VisibleAt)
	}
	if err := cj.Release(ctx, 0); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("second Release = %v, want ErrLeaseLost", err)
	}
}

// TestHeartbeatGuardsOnPrivateFence proves the guard uses the job id and
// fence captured at claim time: forging the exported ID and ClaimID does not
// stop Heartbeat from succeeding against the real job.
func TestHeartbeatGuardsOnPrivateFence(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)
	cj.ID = "forged-id"
	cj.Job.ClaimID = "forged-fence"

	lo := serverNow(ctx, t, db)
	if err := cj.Heartbeat(ctx, time.Hour); err != nil {
		t.Fatalf("heartbeat with forged exported fields: %v", err)
	}
	if stored := findJob(ctx, t, q, "j1"); stored.VisibleAt.Before(lo.Add(time.Hour)) {
		t.Errorf("visible_at = %v, want >= %v", stored.VisibleAt, lo.Add(time.Hour))
	}
}
