package mongoqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// TestResolveRejectsEmptyArgs proves an empty resolution or job id fails
// before any round trip: the queue has a nil collection, so any driver call
// would panic.
func TestResolveRejectsEmptyArgs(t *testing.T) {
	t.Parallel()

	q := New(nil)
	cj := &ClaimedJob{q: q, jobID: "j1", claimID: "fence"}
	if err := cj.Complete(context.Background(), ""); !errors.Is(err, ErrEmptyResolution) {
		t.Errorf(`Complete("") = %v, want ErrEmptyResolution`, err)
	}
	if job, err := q.Get(context.Background(), ""); !errors.Is(err, ErrMissingJobID) || job != nil {
		t.Errorf(`Get("") = %v, %v, want nil, ErrMissingJobID`, job, err)
	}
	if err := q.Cancel(context.Background(), ""); !errors.Is(err, ErrMissingJobID) {
		t.Errorf(`Cancel("") = %v, want ErrMissingJobID`, err)
	}
}

// TestCompleteResolvesJob proves Complete writes liveness, resolution, and
// server-time resolved_at, leaves vstamp alone, and the job never becomes
// claimable again, even with visible_at forced into the past.
func TestCompleteResolvesJob(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)

	lo := serverNow(ctx, t, db)
	if err := cj.Complete(ctx, ResolutionCompleted); err != nil {
		t.Fatalf("complete: %v", err)
	}
	hi := serverNow(ctx, t, db)

	stored := findJob(ctx, t, q, "j1")
	if stored.Liveness != LivenessResolved {
		t.Errorf("liveness = %q, want %q", stored.Liveness, LivenessResolved)
	}
	if stored.Resolution != ResolutionCompleted {
		t.Errorf("resolution = %q, want %q", stored.Resolution, ResolutionCompleted)
	}
	if stored.ResolvedAt.Before(lo) || stored.ResolvedAt.After(hi) {
		t.Errorf("resolved_at = %v, want within [%v, %v]", stored.ResolvedAt, lo, hi)
	}
	if stored.VStamp != cj.VStamp {
		t.Errorf("vstamp changed: %d -> %d", cj.VStamp, stored.VStamp)
	}

	expireLease(ctx, t, q, cj)
	assertNoJob(ctx, t, q, "")
}

// TestCompleteResolutionIsLiteral proves resolutions that look like a field
// path or a variable are stored verbatim as BSON strings, not evaluated.
func TestCompleteResolutionIsLiteral(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	for _, r := range []Resolution{"$foo", "$$NOW"} {
		id := string(r)
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: id, TenantID: "t1", Cost: 1})
		cj := mustClaim(ctx, t, q, "")
		if cj.ID != id {
			t.Fatalf("claimed %q, want %q", cj.ID, id)
		}
		if err := cj.Complete(ctx, r); err != nil {
			t.Fatalf("complete %q: %v", r, err)
		}

		got, err := q.Get(ctx, id)
		if err != nil {
			t.Fatalf("get %q: %v", id, err)
		}
		if got.Resolution != r {
			t.Errorf("resolution = %q, want %q", got.Resolution, r)
		}

		raw, err := q.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Raw()
		if err != nil {
			t.Fatalf("find raw %q: %v", id, err)
		}
		if typ := raw.Lookup("resolution").Type; typ != bson.TypeString {
			t.Errorf("resolution %q stored as BSON type %v, want string", r, typ)
		}
	}
}

// TestCompleteTwiceLosesLease proves Complete is not success-idempotent.
func TestCompleteTwiceLosesLease(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)

	if err := cj.Complete(ctx, ResolutionCompleted); err != nil {
		t.Fatalf("first complete: %v", err)
	}
	if err := cj.Complete(ctx, ResolutionCompleted); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("second Complete = %v, want ErrLeaseLost", err)
	}
}

// TestCompleteAfterReleaseLosesLease proves a releaser cannot complete and
// the job stays pending.
func TestCompleteAfterReleaseLosesLease(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)

	if err := cj.Release(ctx, 10*time.Minute); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := cj.Complete(ctx, ResolutionCompleted); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("Complete after release = %v, want ErrLeaseLost", err)
	}
	if stored := findJob(ctx, t, q, "j1"); stored.Liveness != LivenessPending {
		t.Errorf("liveness = %q, want %q", stored.Liveness, LivenessPending)
	}
}

// TestCompleteAfterUnreclaimedExpiry proves an expired lease that nobody
// reclaimed still holds the fence for Complete.
func TestCompleteAfterUnreclaimedExpiry(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)
	expireLease(ctx, t, q, cj)

	if err := cj.Complete(ctx, ResolutionCompleted); err != nil {
		t.Fatalf("complete after expiry: %v", err)
	}
	if stored := findJob(ctx, t, q, "j1"); stored.Liveness != LivenessResolved {
		t.Errorf("liveness = %q, want %q", stored.Liveness, LivenessResolved)
	}
}

// TestCancelClaimedJob proves Cancel resolves a claimed job with
// ResolutionCanceled, keeps the holder's claim_id, and ends the holder's
// fence for Heartbeat, Release, and Complete.
func TestCancelClaimedJob(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)

	if err := q.Cancel(ctx, "j1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	stored := findJob(ctx, t, q, "j1")
	if stored.Liveness != LivenessResolved || stored.Resolution != ResolutionCanceled {
		t.Errorf("liveness/resolution = %q/%q, want %q/%q",
			stored.Liveness, stored.Resolution, LivenessResolved, ResolutionCanceled)
	}
	if stored.ClaimID == "" || stored.ClaimID != cj.ClaimID() {
		t.Errorf("claim_id = %q, want holder's %q", stored.ClaimID, cj.ClaimID())
	}

	if err := cj.Heartbeat(ctx, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("Heartbeat after cancel = %v, want ErrLeaseLost", err)
	}
	if err := cj.Release(ctx, 0); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("Release after cancel = %v, want ErrLeaseLost", err)
	}
	if err := cj.Complete(ctx, ResolutionCompleted); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("Complete after cancel = %v, want ErrLeaseLost", err)
	}
}

// TestCancelMissClassification proves a Cancel that matches no pending job
// reports ErrJobResolved for a resolved id and ErrJobNotFound for an
// unknown one.
func TestCancelMissClassification(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	insertJob(ctx, t, q, Job{
		ID: "done", TenantID: "t1", Partition: defaultPartition,
		Liveness: LivenessResolved, Resolution: ResolutionCompleted,
		ResolvedAt: time.Now(),
	})

	if err := q.Cancel(ctx, "done"); !errors.Is(err, ErrJobResolved) {
		t.Errorf("Cancel(resolved) = %v, want ErrJobResolved", err)
	}
	if err := q.Cancel(ctx, "nope"); !errors.Is(err, ErrJobNotFound) {
		t.Errorf("Cancel(unknown) = %v, want ErrJobNotFound", err)
	}
}

// TestGetReturnsFreshCopy proves Get returns the stored record, the result
// is a copy (mutating it does not affect a later Get), and an unknown id is
// ErrJobNotFound.
func TestGetReturnsFreshCopy(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)

	got, err := q.Get(ctx, "j1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	want := findJob(ctx, t, q, "j1")
	if got.ID != want.ID || got.ClaimID != cj.ClaimID() || got.VStamp != want.VStamp ||
		got.Attempts != want.Attempts || !got.VisibleAt.Equal(want.VisibleAt) || got.Liveness != want.Liveness {
		t.Errorf("Get = %+v, want %+v", *got, want)
	}

	got.ClaimID = "mutated"
	got.Attempts = 99
	again, err := q.Get(ctx, "j1")
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if again.ClaimID != cj.ClaimID() || again.Attempts != want.Attempts {
		t.Errorf("second Get = claim_id %q attempts %d, want %q %d",
			again.ClaimID, again.Attempts, cj.ClaimID(), want.Attempts)
	}

	if job, err := q.Get(ctx, "nope"); !errors.Is(err, ErrJobNotFound) || job != nil {
		t.Errorf("Get(unknown) = %v, %v, want nil, ErrJobNotFound", job, err)
	}
}

// TestHandleConcurrentHeartbeatAndComplete proves one handle is safe for a
// heartbeat goroutine racing the worker's Complete: Complete succeeds,
// heartbeats see only nil or ErrLeaseLost, and the fence is gone afterward.
func TestHandleConcurrentHeartbeatAndComplete(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	cj := claimOne(ctx, t, q)

	done := make(chan struct{})
	beating := make(chan struct{})
	hbErr := make(chan error, 1)
	go func() {
		first := true
		for {
			select {
			case <-done:
				hbErr <- nil
				return
			default:
			}
			err := cj.Heartbeat(ctx, time.Minute)
			if errors.Is(err, ErrLeaseLost) {
				hbErr <- nil
				return
			}
			if err != nil {
				hbErr <- err
				return
			}
			if first {
				first = false
				close(beating)
			}
		}
	}()

	// Wait for one successful heartbeat so Complete overlaps a live loop.
	select {
	case <-beating:
	case err := <-hbErr:
		t.Fatalf("heartbeat ended before Complete: %v", err)
	}

	err := cj.Complete(ctx, ResolutionCompleted)
	close(done)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := <-hbErr; err != nil {
		t.Errorf("heartbeat returned %v, want only nil or ErrLeaseLost", err)
	}
	if err := cj.Heartbeat(ctx, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("Heartbeat after Complete = %v, want ErrLeaseLost", err)
	}
}
