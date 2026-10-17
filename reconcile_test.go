package mongoqueue

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// assertVStamp fails unless job id is stored with vstamp want.
func assertVStamp(ctx context.Context, t *testing.T, q *Queue, id string, want int64) {
	t.Helper()
	job, err := q.Get(ctx, id)
	if err != nil {
		t.Fatalf("get %q: %v", id, err)
	}
	if job.VStamp != want {
		t.Fatalf("job %q vstamp = %d, want %d", id, job.VStamp, want)
	}
}

func mustReconcile(ctx context.Context, t *testing.T, q *Queue) {
	t.Helper()
	if err := q.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func mustCancel(ctx context.Context, t *testing.T, q *Queue, id string) {
	t.Helper()
	if err := q.Cancel(ctx, id); err != nil {
		t.Fatalf("cancel %q: %v", id, err)
	}
}

// TestReconcileRefundsTrailingCancellation proves a canceled trailing job's
// slot is reused only after reconcile: the cached vtime ignores cancels, and
// the re-seed reads the trailing edge of the remaining pending jobs. Each
// tenant has its own partition so the values hold under a frontier floor.
func TestReconcileRefundsTrailingCancellation(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	enqueueTenant(ctx, t, q, 3, EnqueueOpts{TenantID: "b", Partition: "pb", Cost: 1, Weight: 1})
	mustCancel(ctx, t, q, "b-002")
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "b-003", TenantID: "b", Partition: "pb", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "b-003", 4*scale)

	enqueueTenant(ctx, t, q, 3, EnqueueOpts{TenantID: "a", Partition: "pa", Cost: 1, Weight: 1})
	mustCancel(ctx, t, q, "a-002")
	mustReconcile(ctx, t, q)
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-003", TenantID: "a", Partition: "pa", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-003", 3*scale)
}

// TestReconcileCorrectsExternalWriterDrift proves reconcile picks up a stamp
// another writer placed on the timeline, and only from the same partition.
func TestReconcileCorrectsExternalWriterDrift(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-000", TenantID: "a", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-000", scale)

	insertJob(ctx, t, q, Job{
		ID: "ext-default", TenantID: "a", Partition: defaultPartition, Cost: 1,
		VStamp: 50 * scale, Liveness: LivenessPending, VisibleAt: time.Now(),
	})
	insertJob(ctx, t, q, Job{
		ID: "ext-other", TenantID: "a", Partition: "other", Cost: 1,
		VStamp: 90 * scale, Liveness: LivenessPending, VisibleAt: time.Now(),
	})
	mustReconcile(ctx, t, q)

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-001", TenantID: "a", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-001", 51*scale)
}

// TestReconcileEvictsIdleTimeline proves a timeline with no enqueue between
// two reconciles is dropped from the cache.
func TestReconcileEvictsIdleTimeline(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-000", TenantID: "a", Cost: 1, Weight: 1})
	mustReconcile(ctx, t, q)
	mustReconcile(ctx, t, q)

	key := timelineKey{tenant: "a", partition: defaultPartition}
	q.vtimes.mu.Lock()
	_, ok := q.vtimes.tenants[key]
	q.vtimes.mu.Unlock()
	if ok {
		t.Fatalf("timeline %v still cached after idle reconcile", key)
	}
}

// TestReconcileDoneContextLeavesCacheUntouched proves a reconcile with a done
// context returns the context error without invalidating or evicting.
func TestReconcileDoneContextLeavesCacheUntouched(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-000", TenantID: "a", Cost: 1, Weight: 1})

	doneCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := q.Reconcile(doneCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("reconcile error = %v, want context.Canceled", err)
	}

	key := timelineKey{tenant: "a", partition: defaultPartition}
	q.vtimes.mu.Lock()
	tl, ok := q.vtimes.tenants[key]
	q.vtimes.mu.Unlock()
	if !ok {
		t.Fatalf("timeline %v evicted by reconcile with done context", key)
	}
	if !tl.touched.Load() {
		t.Fatalf("timeline %v touched cleared by reconcile with done context", key)
	}
	if tl.stale.Load() {
		t.Fatalf("timeline %v marked stale by reconcile with done context", key)
	}
}

// claimComplete claims from partition once per wantIDs, asserting each claim
// dispatches the next id in order, and completes it.
func claimComplete(ctx context.Context, t *testing.T, q *Queue, partition string, wantIDs ...string) {
	t.Helper()
	for _, want := range wantIDs {
		cj := mustClaim(ctx, t, q, partition)
		if cj.ID != want {
			t.Fatalf("claim in %q dispatched %q, want %q", partition, cj.ID, want)
		}
		if err := cj.Complete(ctx, ResolutionCompleted); err != nil {
			t.Fatalf("complete %q: %v", cj.ID, err)
		}
	}
}

// jobIDs returns prefix-NNN for each n in [from, to).
func jobIDs(prefix string, from, to int) []string {
	ids := make([]string, 0, to-from)
	for i := from; i < to; i++ {
		ids = append(ids, fmt.Sprintf("%s-%03d", prefix, i))
	}
	return ids
}

// setupIdleReturn drains tenant a (cached vtime 3e3), then stamps b-000..b-019
// at 4e3..23e3 (its cold seed floors at the clock, a-002 at 3e3) and drains
// b-000..b-009, leaving the default partition's clock at 13e3, then
// reconciles.
func setupIdleReturn(ctx context.Context, t *testing.T, q *Queue) {
	t.Helper()
	enqueueTenant(ctx, t, q, 3, EnqueueOpts{TenantID: "a", Cost: 1, Weight: 1})
	claimComplete(ctx, t, q, "", jobIDs("a", 0, 3)...)

	enqueueTenant(ctx, t, q, 20, EnqueueOpts{TenantID: "b", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "b-000", 4*scale)
	assertVStamp(ctx, t, q, "b-019", 23*scale)
	claimComplete(ctx, t, q, "", jobIDs("b", 0, 10)...)

	mustReconcile(ctx, t, q)
}

// TestLWMIdleTenantResumesAtFrontier proves a returning tenant is floored to
// the partition's service clock: no credit for idle time (stale vtime would
// stamp a-003 at 4e3) and no burial behind the backlog (a trailing-edge
// floor would stamp it at 24e3). The stale re-seed is max(a's trailing edge
// 0, clock 13e3) + stride = 14e3.
func TestLWMIdleTenantResumesAtFrontier(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	setupIdleReturn(ctx, t, q)

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-003", TenantID: "a", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-003", 14*scale)

	// a-003 ties b-010 at 14e3 and wins on id.
	want := append([]string{"a-003"}, jobIDs("b", 10, 20)...)
	assertOrder(t, drainOrder(ctx, t, q, ""), want)
}

// TestLWMReturningTenantFloodsAtShare proves fairness is memoryless: the floor
// sets where a returning tenant starts; its stride sets its share, so a flood
// interleaves one-for-one with the incumbent from its first job.
func TestLWMReturningTenantFloodsAtShare(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	setupIdleReturn(ctx, t, q)

	for i := 3; i < 23; i++ {
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: fmt.Sprintf("a-%03d", i), TenantID: "a", Cost: 1, Weight: 1})
	}
	assertVStamp(ctx, t, q, "a-003", 14*scale)
	assertVStamp(ctx, t, q, "a-022", 33*scale)

	// a-(3+i) ties b-(10+i) at (14+i)e3 and wins on id.
	var want []string
	for i := range 10 {
		want = append(want, fmt.Sprintf("a-%03d", 3+i), fmt.Sprintf("b-%03d", 10+i))
	}
	want = append(want, jobIDs("a", 13, 23)...)
	// b drains at claim 20 (b-019), a at claim 30 (a-022).
	assertOrder(t, drainOrder(ctx, t, q, ""), want)
}

// TestLWMColdNodeFloorsNewTenant proves a cold seed refreshes the frontier, so
// a node that never reconciles still floors a new tenant: b-000..b-009 at
// 1e3..10e3 were claimed, so the clock is 10e3 and c-000 is 11e3, not 1e3.
func TestLWMColdNodeFloorsNewTenant(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)

	q1 := New(db.Collection("jobs"))
	enqueueTenant(ctx, t, q1, 20, EnqueueOpts{TenantID: "b", Cost: 1, Weight: 1})
	claimComplete(ctx, t, q1, "", jobIDs("b", 0, 10)...)

	q2 := New(db.Collection("jobs"))
	enqueueJob(ctx, t, q2, EnqueueOpts{JobID: "c-000", TenantID: "c", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q2, "c-000", 11*scale)
}

// claimN claims n jobs in partition and leaves them leased.
func claimN(ctx context.Context, t *testing.T, q *Queue, partition string, n int) {
	t.Helper()
	for range n {
		mustClaim(ctx, t, q, partition)
	}
}

// TestLWMColdNodeFullyLeasedFloorsAtClock proves the frontier does not depend
// on the visible set: every b job is leased, so nothing is visible, yet a
// cold seed floors c at the clock (b-019 at 20e3) rather than at 0, which
// would stamp c-000 at 1e3, ahead of all b's future work.
func TestLWMColdNodeFullyLeasedFloorsAtClock(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)

	q1 := New(db.Collection("jobs"))
	enqueueTenant(ctx, t, q1, 20, EnqueueOpts{TenantID: "b", Cost: 1, Weight: 1})
	claimN(ctx, t, q1, "", 20)

	q2 := New(db.Collection("jobs"))
	enqueueJob(ctx, t, q2, EnqueueOpts{JobID: "c-000", TenantID: "c", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q2, "c-000", 21*scale)
}

// TestLWMWarmNodeReconcileAdvancesClock proves a reconcile advances the stored
// frontier to the clock even when the partition is fully leased and nothing
// is visible. Tenant a stays cached (touched between reconciles), so its last
// enqueue re-seeds from the stored frontier rather than refreshing it.
// a-000 (1e3) is claimed first, so b's cold seed stamps b-000..b-019 at
// 2e3..21e3. Keeping the frontier from before the partition went fully
// leased (11e3) would stamp a-002 at max(a-001 12e3, 11e3) + 1e3 = 13e3.
func TestLWMWarmNodeReconcileAdvancesClock(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-000", TenantID: "a", Partition: "p", Cost: 1, Weight: 1})
	claimComplete(ctx, t, q, "p", "a-000")
	enqueueTenant(ctx, t, q, 20, EnqueueOpts{TenantID: "b", Partition: "p", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "b-019", 21*scale)
	claimN(ctx, t, q, "p", 10)
	mustReconcile(ctx, t, q) // clock: b-009 at 11e3

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-001", TenantID: "a", Partition: "p", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-001", 12*scale)
	claimN(ctx, t, q, "p", 11)
	mustReconcile(ctx, t, q) // a-001 ties b-010 and wins; nothing visible; clock b-019 at 21e3

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-002", TenantID: "a", Partition: "p", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-002", 22*scale)
}

// TestLWMLanesAreIndependent proves each partition floors against its own
// frontier: a tenant's position in one lane does not move it in another (a
// shared timeline would have stamped c's slow job at 12e3). The fast lane's
// clock is b-fast-009 at 10e3, so c-fast is 11e3; the slow lane was never
// claimed, so it falls back to min pending (b-slow-000 at 1e3): c-slow 2e3.
func TestLWMLanesAreIndependent(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	for i := range 20 {
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: fmt.Sprintf("b-slow-%03d", i), TenantID: "b", Partition: "slow", Cost: 1, Weight: 1})
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: fmt.Sprintf("b-fast-%03d", i), TenantID: "b", Partition: "fast", Cost: 1, Weight: 1})
	}
	claimComplete(ctx, t, q, "fast", jobIDs("b-fast", 0, 10)...)

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "c-fast", TenantID: "c", Partition: "fast", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "c-fast", 11*scale)
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "c-slow", TenantID: "c", Partition: "slow", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "c-slow", 2*scale)
}

// TestReconcileConcurrent runs Reconcile in a loop on several goroutines while
// two producers enqueue for one tenant in one partition. Under -race this
// checks Reconcile is safe alongside itself and enqueue, and that the tenant's
// stamps stay exactly {1..N}x1e3 with no gap or duplicate. Each producer
// waits after every enqueue until at least one Reconcile has started and
// finished since, so every stamp after the first is a re-seed and the overlap
// is guaranteed rather than left to the scheduler.
func TestReconcileConcurrent(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	const producers, perProducer, reconcilers = 2, 25, 4
	done := make(chan struct{})
	errs := make(chan error, reconcilers)
	var passes atomic.Int64 // completed Reconcile calls
	var reconcileWG sync.WaitGroup
	for range reconcilers {
		reconcileWG.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				if err := q.Reconcile(ctx); err != nil {
					errs <- err
					return
				}
				passes.Add(1)
			}
		})
	}

	var enqueueWG sync.WaitGroup
	for p := range producers {
		enqueueWG.Go(func() {
			for i := range perProducer {
				opts := EnqueueOpts{JobID: fmt.Sprintf("a-%d-%03d", p, i), TenantID: "a", Partition: "p", Cost: 1, Weight: 1}
				if err := q.enqueue(ctx, "test", rawBody(t, bson.D{}), opts); err != nil {
					t.Errorf("enqueue %q: %v", opts.JobID, err)
					return
				}
				// At most `reconcilers` calls were in flight when after was read,
				// so the next completion beyond those started after this enqueue.
				after := passes.Load()
				for passes.Load() < after+reconcilers+1 {
					if len(errs) > 0 {
						return
					}
					runtime.Gosched()
				}
			}
		})
	}
	enqueueWG.Wait()
	close(done)
	reconcileWG.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Reconcile: %v", err)
	}
	if t.Failed() {
		t.FailNow()
	}

	var got []int64
	for p := range producers {
		for i := range perProducer {
			id := fmt.Sprintf("a-%d-%03d", p, i)
			job, err := q.Get(ctx, id)
			if err != nil {
				t.Fatalf("get %q: %v", id, err)
			}
			got = append(got, job.VStamp)
		}
	}
	slices.Sort(got)
	for i, v := range got {
		if want := int64(i+1) * scale; v != want {
			t.Fatalf("sorted vstamps[%d] = %d, want %d (all: %v)", i, v, want, got)
		}
	}
}

// TestLWMDelayedOnlyPartitionInterleaves proves the frontier is the service
// clock (max ever-claimed vstamp), not the min visible job: with only a's
// delayed jobs pending, a cold seed floors b at the clock (10e3) instead of 0,
// so when the delayed batch wakes, a and b alternate rather than b jumping
// ahead of all of it.
func TestLWMDelayedOnlyPartitionInterleaves(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")
	const part = "p"

	q1 := New(coll)
	enqueueTenant(ctx, t, q1, 10, EnqueueOpts{TenantID: "a", Partition: part, Cost: 1, Weight: 1})
	claimComplete(ctx, t, q1, part, jobIDs("a", 0, 10)...)
	for _, id := range jobIDs("a", 10, 15) {
		enqueueJob(ctx, t, q1, EnqueueOpts{JobID: id, TenantID: "a", Partition: part, Cost: 1, Weight: 1, Delay: time.Hour})
	}
	assertVStamp(ctx, t, q1, "a-010", 11*scale)

	q2 := New(coll)
	enqueueTenant(ctx, t, q2, 5, EnqueueOpts{TenantID: "b", Partition: part, Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q2, "b-000", 11*scale)

	past := time.Now().Add(-time.Minute)
	if _, err := coll.UpdateMany(ctx, bson.D{{Key: "tenant", Value: "a"}, {Key: "liveness", Value: LivenessPending}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "visible_at", Value: past}}}}); err != nil {
		t.Fatalf("wake delayed jobs: %v", err)
	}

	// Equal stamps tie-break on id, so a leads each pair.
	var want []string
	for i := range 5 {
		want = append(want, fmt.Sprintf("a-%03d", 10+i), fmt.Sprintf("b-%03d", i))
	}
	assertOrder(t, drainOrder(ctx, t, q2, part), want)
}

// TestLWMLongLeaseDoesNotPullFloorBack proves a long-held lease on a low
// stamp does not lower the frontier: the clock is the max claimed vstamp
// (10e3), so b's cold seed is not credited back to a-000's 1e3.
func TestLWMLongLeaseDoesNotPullFloorBack(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")
	const part = "p"

	q1 := New(coll)
	enqueueTenant(ctx, t, q1, 10, EnqueueOpts{TenantID: "a", Partition: part, Cost: 1, Weight: 1})
	if cj := mustClaim(ctx, t, q1, part); cj.ID != "a-000" {
		t.Fatalf("claim dispatched %q, want a-000", cj.ID)
	}
	claimComplete(ctx, t, q1, part, jobIDs("a", 1, 10)...)

	q2 := New(coll)
	enqueueJob(ctx, t, q2, EnqueueOpts{JobID: "b-000", TenantID: "b", Partition: part, Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q2, "b-000", 11*scale)
}
