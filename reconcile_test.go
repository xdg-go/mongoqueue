package mongoqueue

import (
	"context"
	"errors"
	"testing"
	"time"

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
	if err := q.reconcile(ctx); err != nil {
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
	if err := q.reconcile(doneCtx); !errors.Is(err, context.Canceled) {
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
