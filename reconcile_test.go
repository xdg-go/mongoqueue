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
	assertVStamp(ctx, t, q, "b-003", 4_000_000)

	enqueueTenant(ctx, t, q, 3, EnqueueOpts{TenantID: "a", Partition: "pa", Cost: 1, Weight: 1})
	mustCancel(ctx, t, q, "a-002")
	mustReconcile(ctx, t, q)
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-003", TenantID: "a", Partition: "pa", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-003", 3_000_000)
}

// TestReconcileCorrectsExternalWriterDrift proves reconcile picks up a stamp
// another writer placed on the timeline, and only from the same partition.
func TestReconcileCorrectsExternalWriterDrift(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-000", TenantID: "a", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-000", 1_000_000)

	insertJob(ctx, t, q, Job{
		ID: "ext-default", TenantID: "a", Partition: defaultPartition, Cost: 1,
		VStamp: 50_000_000, Liveness: LivenessPending, VisibleAt: time.Now(),
	})
	insertJob(ctx, t, q, Job{
		ID: "ext-other", TenantID: "a", Partition: "other", Cost: 1,
		VStamp: 90_000_000, Liveness: LivenessPending, VisibleAt: time.Now(),
	})
	mustReconcile(ctx, t, q)

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-001", TenantID: "a", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-001", 51_000_000)
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

// setupIdleReturn drains tenant a (cached vtime 3e6), then stamps b-000..b-019
// at 1e6..20e6 (its cold seed sees no visible job) and drains b-000..b-009,
// leaving the default partition's frontier at 11e6, then reconciles.
func setupIdleReturn(ctx context.Context, t *testing.T, q *Queue) {
	t.Helper()
	enqueueTenant(ctx, t, q, 3, EnqueueOpts{TenantID: "a", Cost: 1, Weight: 1})
	claimComplete(ctx, t, q, "", jobIDs("a", 0, 3)...)

	enqueueTenant(ctx, t, q, 20, EnqueueOpts{TenantID: "b", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "b-000", 1_000_000)
	assertVStamp(ctx, t, q, "b-019", 20_000_000)
	claimComplete(ctx, t, q, "", jobIDs("b", 0, 10)...)

	mustReconcile(ctx, t, q)
}

// TestLWMIdleTenantResumesAtFrontier proves a returning tenant is floored to
// the partition's dispatch frontier: no credit for idle time (stale vtime
// would stamp a-003 at 4e6) and no burial behind the backlog (a trailing-edge
// floor would stamp it at 21e6).
func TestLWMIdleTenantResumesAtFrontier(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	setupIdleReturn(ctx, t, q)

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-003", TenantID: "a", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-003", 12_000_000)

	// a-003 ties b-011 at 12e6 and wins on id.
	want := append([]string{"b-010", "a-003"}, jobIDs("b", 11, 20)...)
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
	assertVStamp(ctx, t, q, "a-003", 12_000_000)
	assertVStamp(ctx, t, q, "a-022", 31_000_000)

	want := []string{"b-010"}
	for i := range 9 {
		want = append(want, fmt.Sprintf("a-%03d", 3+i), fmt.Sprintf("b-%03d", 11+i))
	}
	want = append(want, jobIDs("a", 12, 23)...)
	// b drains at claim 19 (b-019), a at claim 30 (a-022).
	assertOrder(t, drainOrder(ctx, t, q, ""), want)
}

// TestLWMColdNodeFloorsNewTenant proves a cold seed refreshes the frontier, so
// a node that never reconciles still floors a new tenant.
func TestLWMColdNodeFloorsNewTenant(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)

	q1 := New(db.Collection("jobs"))
	enqueueTenant(ctx, t, q1, 20, EnqueueOpts{TenantID: "b", Cost: 1, Weight: 1})
	claimComplete(ctx, t, q1, "", jobIDs("b", 0, 10)...)

	q2 := New(db.Collection("jobs"))
	enqueueJob(ctx, t, q2, EnqueueOpts{JobID: "c-000", TenantID: "c", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q2, "c-000", 12_000_000)
}

// TestLWMLanesAreIndependent proves each partition floors against its own
// frontier: a tenant's position in one lane does not move it in another (a
// shared timeline would have stamped c's slow job at 13e6).
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
	assertVStamp(ctx, t, q, "c-fast", 12_000_000)
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "c-slow", TenantID: "c", Partition: "slow", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "c-slow", 2_000_000)
}

// TestLWMOtherLanePendingDoesNotRefund proves a re-seed reads only its own
// lane's trailing edge: a's pending slow-lane job does not pull its fast-lane
// timeline back (a shared timeline would re-seed to 1e6 and stamp a-f01 2e6).
func TestLWMOtherLanePendingDoesNotRefund(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	for i := 10; i < 20; i++ {
		insertJob(ctx, t, q, Job{
			ID: fmt.Sprintf("b-%03d", i), TenantID: "b", Partition: "fast", Cost: 1,
			VStamp: int64(i+1) * 1_000_000, Liveness: LivenessPending, VisibleAt: time.Now(),
		})
	}

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-slow", TenantID: "a", Partition: "slow", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-slow", 1_000_000)
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-f00", TenantID: "a", Partition: "fast", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-f00", 12_000_000)

	claimComplete(ctx, t, q, "fast", "b-010", "a-f00")
	mustReconcile(ctx, t, q)

	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-f01", TenantID: "a", Partition: "fast", Cost: 1, Weight: 1})
	assertVStamp(ctx, t, q, "a-f01", 13_000_000)

	want := append([]string{"b-011", "a-f01"}, jobIDs("b", 12, 20)...)
	assertOrder(t, drainOrder(ctx, t, q, "fast"), want)
}

// TestReconcileConcurrent runs Reconcile in a loop on several goroutines while
// two producers enqueue for one tenant in one partition. Under -race this
// checks Reconcile is safe alongside itself and enqueue, and that the tenant's
// stamps stay exactly {1..N}x1e6 with no gap or duplicate. Each producer
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
		if want := int64(i+1) * 1_000_000; v != want {
			t.Fatalf("sorted vstamps[%d] = %d, want %d (all: %v)", i, v, want, got)
		}
	}
}
