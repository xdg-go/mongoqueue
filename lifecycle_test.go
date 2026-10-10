package mongoqueue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// raceIterations is how many fresh jobs each race test contends on; enough
// to exercise both orderings without making the suite slow.
const raceIterations = 50

// raceCtx returns a context bounding a whole race test, shorter than the
// one-minute leases the races use; every lease starts after the test does, so
// none can lapse mid-test.
func raceCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestStealThenLateComplete proves a holder whose lease expired and was
// reclaimed cannot resolve the job: A's Complete loses, B's wins, and the
// stored record carries B's resolution and fence.
func TestStealThenLateComplete(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	a := claimOne(ctx, t, q)

	expireLease(ctx, t, q, a)
	b := mustClaim(ctx, t, q, "")
	if b.ClaimID() == a.ClaimID() {
		t.Fatalf("reclaim reused fence %q", a.ClaimID())
	}

	if err := a.Complete(ctx, ResolutionCompleted); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("A's late Complete = %v, want ErrLeaseLost", err)
	}
	const bResolution Resolution = "done-by-b"
	if err := b.Complete(ctx, bResolution); err != nil {
		t.Fatalf("B's Complete: %v", err)
	}

	got, err := q.Get(ctx, "j1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Liveness != LivenessResolved || got.Resolution != bResolution {
		t.Errorf("liveness/resolution = %q/%q, want %q/%q",
			got.Liveness, got.Resolution, LivenessResolved, bResolution)
	}
	if got.ClaimID != b.ClaimID() {
		t.Errorf("claim_id = %q, want B's %q", got.ClaimID, b.ClaimID())
	}
}

// TestRaceCompleteVsCancel races the holder's Complete against an external
// Cancel on a fresh job each iteration: exactly one wins, the loser sees
// its sentinel, and the stored resolution names the winner.
func TestRaceCompleteVsCancel(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := raceCtx(t)
	q := New(db.Collection("jobs"))

	var completeWins, cancelWins int
	for i := range raceIterations {
		id := fmt.Sprintf("j%03d", i)
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: id, TenantID: "t1", Cost: 1})
		cj := mustClaim(ctx, t, q, "")
		if cj.jobID != id {
			t.Fatalf("iteration %d claimed %q, want %q", i, cj.jobID, id)
		}

		gate := make(chan struct{})
		completeErr := make(chan error, 1)
		cancelErr := make(chan error, 1)
		go func() { <-gate; completeErr <- cj.Complete(ctx, ResolutionCompleted) }()
		go func() { <-gate; cancelErr <- q.Cancel(ctx, id) }()
		close(gate)
		errComplete, errCancel := <-completeErr, <-cancelErr

		got, err := q.Get(ctx, id)
		if err != nil {
			t.Fatalf("iteration %d: get: %v", i, err)
		}
		switch {
		case errComplete == nil && errors.Is(errCancel, ErrJobResolved):
			completeWins++
			if got.Resolution != ResolutionCompleted {
				t.Errorf("iteration %d: Complete won but resolution = %q", i, got.Resolution)
			}
		case errCancel == nil && errors.Is(errComplete, ErrLeaseLost):
			cancelWins++
			if got.Resolution != ResolutionCanceled {
				t.Errorf("iteration %d: Cancel won but resolution = %q", i, got.Resolution)
			}
		default:
			t.Fatalf("iteration %d: Complete = %v, Cancel = %v; want exactly one nil and the loser's sentinel",
				i, errComplete, errCancel)
		}
		if got.Liveness != LivenessResolved {
			t.Errorf("iteration %d: liveness = %q, want %q", i, got.Liveness, LivenessResolved)
		}
	}
	t.Logf("Complete won %d, Cancel won %d of %d", completeWins, cancelWins, raceIterations)
}

// TestRaceCompleteVsReclaim races the holder's Complete against a reclaim
// after forced expiry on a fresh job each iteration. Either Complete wins
// and the claim finds nothing, or the claim wins and Complete loses its
// lease; never both. Which ordering occurs is up to the scheduler and server,
// so the distribution is logged, not asserted.
func TestRaceCompleteVsReclaim(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := raceCtx(t)
	q := New(db.Collection("jobs"))

	type claimResult struct {
		cj  *ClaimedJob
		err error
	}

	var completeWins, reclaimWins int
	for i := range raceIterations {
		id := fmt.Sprintf("j%03d", i)
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: id, TenantID: "t1", Cost: 1})
		holder := mustClaim(ctx, t, q, "")
		if holder.jobID != id {
			t.Fatalf("iteration %d claimed %q, want %q", i, holder.jobID, id)
		}
		expireLease(ctx, t, q, holder)

		gate := make(chan struct{})
		completeErr := make(chan error, 1)
		claimed := make(chan claimResult, 1)
		// Complete (UpdateOne by _id) is far cheaper server-side than Claim
		// (findAndModify with $expr and sort) and wins nearly every unaided
		// race. On odd iterations Complete first spends one round trip so the
		// reclaim-wins branch runs too. This shifts odds only; every outcome
		// is still checked against the same invariant.
		go func() {
			<-gate
			if i%2 == 1 {
				_ = db.Client().Ping(ctx, nil)
			}
			completeErr <- holder.Complete(ctx, ResolutionCompleted)
		}()
		go func() {
			<-gate
			cj, err := q.Claim(ctx, "", time.Minute)
			claimed <- claimResult{cj, err}
		}()
		close(gate)
		errComplete, reclaim := <-completeErr, <-claimed

		got, err := q.Get(ctx, id)
		if err != nil {
			t.Fatalf("iteration %d: get: %v", i, err)
		}
		switch {
		case errComplete == nil && errors.Is(reclaim.err, ErrNoJob) && reclaim.cj == nil:
			completeWins++
			if got.Liveness != LivenessResolved || got.Resolution != ResolutionCompleted ||
				got.ClaimID != holder.ClaimID() {
				t.Errorf("iteration %d: Complete won but stored liveness/resolution/claim_id = %q/%q/%q",
					i, got.Liveness, got.Resolution, got.ClaimID)
			}
		case reclaim.err == nil && errors.Is(errComplete, ErrLeaseLost):
			reclaimWins++
			if reclaim.cj.jobID != id {
				t.Fatalf("iteration %d: reclaimed %q, want %q", i, reclaim.cj.jobID, id)
			}
			if got.Liveness != LivenessPending || got.ClaimID != reclaim.cj.ClaimID() {
				t.Errorf("iteration %d: reclaim won but stored liveness/claim_id = %q/%q, want %q/%q",
					i, got.Liveness, got.ClaimID, LivenessPending, reclaim.cj.ClaimID())
			}
			// Resolve the reclaimed job so later iterations start clean.
			if err := reclaim.cj.Complete(ctx, ResolutionCompleted); err != nil {
				t.Fatalf("iteration %d: reclaimer's Complete: %v", i, err)
			}
		default:
			t.Fatalf("iteration %d: Complete = %v, Claim = (%v, %v); want exactly one winner",
				i, errComplete, reclaim.cj, reclaim.err)
		}
	}
	t.Logf("Complete won %d, reclaim won %d of %d", completeWins, reclaimWins, raceIterations)
}

// lifecycleBody is a typed body for the end-to-end smoke test.
type lifecycleBody struct {
	Name  string `bson:"name"`
	Count int    `bson:"count"`
}

// TestLifecycleSmoke drives one job through the typed facade and the whole
// lease lifecycle: enqueue, claim, decode, heartbeat, complete, get.
func TestLifecycleSmoke(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	kind := NewKind[lifecycleBody]("smoke")
	want := lifecycleBody{Name: "widget", Count: 3}

	if err := kind.Enqueue(ctx, q, want, EnqueueOpts{JobID: "j1", TenantID: "t1", Cost: 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	cj := mustClaim(ctx, t, q, "")
	body, err := kind.Decode(&cj.Job)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body != want {
		t.Errorf("decoded body = %+v, want %+v", body, want)
	}
	if err := cj.Heartbeat(ctx, time.Minute); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := cj.Complete(ctx, ResolutionCompleted); err != nil {
		t.Fatalf("complete: %v", err)
	}

	got, err := q.Get(ctx, "j1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Liveness != LivenessResolved || got.Resolution != ResolutionCompleted {
		t.Errorf("liveness/resolution = %q/%q, want %q/%q",
			got.Liveness, got.Resolution, LivenessResolved, ResolutionCompleted)
	}
	if got.Kind != kind.Kind() || got.ClaimID != cj.ClaimID() {
		t.Errorf("kind/claim_id = %q/%q, want %q/%q", got.Kind, got.ClaimID, kind.Kind(), cj.ClaimID())
	}
}
