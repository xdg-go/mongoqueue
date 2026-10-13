package mongoqueue

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// drainOrder claims from partition until ErrNoJob and returns job ids in
// dispatch order. One-minute leases keep claimed jobs invisible for the
// rest of the test, so nothing is dispatched twice.
func drainOrder(ctx context.Context, t *testing.T, q *Queue, partition string) []string {
	t.Helper()
	var ids []string
	for {
		cj, err := q.Claim(ctx, partition, time.Minute)
		if errors.Is(err, ErrNoJob) {
			return ids
		}
		if err != nil {
			t.Fatalf("claim %d in %q: %v", len(ids), partition, err)
		}
		ids = append(ids, cj.ID)
	}
}

// plannedJob is a job id with the vstamp the stamping arithmetic assigns it.
type plannedJob struct {
	id     string
	vstamp int64
}

// planTenant returns n jobs prefix-000.. on a fresh tenant with vstamps
// k*stride(cost, weight) for k = 1..n, where stride = max(1, cost*1e6/weight).
func planTenant(prefix string, n int, cost, weight int64) []plannedJob {
	stride := max(1, cost*1_000_000/weight)
	jobs := make([]plannedJob, n)
	for i := range jobs {
		jobs[i] = plannedJob{id: fmt.Sprintf("%s-%03d", prefix, i), vstamp: int64(i+1) * stride}
	}
	return jobs
}

// expectedOrder sorts planned jobs by (vstamp, id), the claim order.
func expectedOrder(jobs ...[]plannedJob) []string {
	all := slices.Concat(jobs...)
	slices.SortFunc(all, func(a, b plannedJob) int {
		return cmp.Or(cmp.Compare(a.vstamp, b.vstamp), strings.Compare(a.id, b.id))
	})
	ids := make([]string, len(all))
	for i, j := range all {
		ids[i] = j.id
	}
	return ids
}

// enqueueTenant enqueues n jobs for opts.TenantID with ids <tenant>-000..,
// in id order; every other field comes from opts.
func enqueueTenant(ctx context.Context, t *testing.T, q *Queue, n int, opts EnqueueOpts) {
	t.Helper()
	for i := range n {
		opts.JobID = fmt.Sprintf("%s-%03d", opts.TenantID, i)
		enqueueJob(ctx, t, q, opts)
	}
}

func assertOrder(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("dispatch order:\n got %v\nwant %v", got, want)
	}
}

// TestFairnessFloodingTenant proves a tenant enqueuing after another's
// backlog is not queued behind it: with equal cost and weight, B's jobs
// interleave with A's from the start.
func TestFairnessFloodingTenant(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	enqueueTenant(ctx, t, q, 50, EnqueueOpts{TenantID: "a", Cost: 1, Weight: 1})
	enqueueTenant(ctx, t, q, 5, EnqueueOpts{TenantID: "b", Cost: 1, Weight: 1})

	// Ties at each k*1e6 break by _id: a-00k before b-00k.
	var want []string
	for i := range 50 {
		want = append(want, fmt.Sprintf("a-%03d", i))
		if i < 5 {
			want = append(want, fmt.Sprintf("b-%03d", i))
		}
	}
	assertOrder(t, drainOrder(ctx, t, q, ""), want)
}

// TestFairnessWeightTwoToOne proves weight 2 doubles a tenant's share at
// equal cost: every 3k prefix holds exactly 2k A jobs while both have work.
func TestFairnessWeightTwoToOne(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	const n = 30
	enqueueTenant(ctx, t, q, n, EnqueueOpts{TenantID: "a", Cost: 1, Weight: 2})
	enqueueTenant(ctx, t, q, n, EnqueueOpts{TenantID: "b", Cost: 1, Weight: 1})

	got := drainOrder(ctx, t, q, "")
	assertOrder(t, got, expectedOrder(planTenant("a", n, 1, 2), planTenant("b", n, 1, 1)))

	// A's last job (a-029, vstamp 15e6) ties b-014 and wins by _id, so both
	// tenants have work through the first 45 claims: k = 1..15.
	for k := 1; 3*k <= 45; k++ {
		aCount := 0
		for _, id := range got[:3*k] {
			if strings.HasPrefix(id, "a-") {
				aCount++
			}
		}
		if aCount != 2*k {
			t.Errorf("prefix %d: %d A jobs, %d B jobs; want %d and %d", 3*k, aCount, 3*k-aCount, 2*k, k)
		}
	}
}

// TestFairnessCheapVsExpensive proves cost scales a tenant's stride at equal
// weight: one cost-4 job from B takes the slot of four cost-1 jobs from A.
func TestFairnessCheapVsExpensive(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	enqueueTenant(ctx, t, q, 20, EnqueueOpts{TenantID: "a", Cost: 1, Weight: 1})
	enqueueTenant(ctx, t, q, 5, EnqueueOpts{TenantID: "b", Cost: 4, Weight: 1})

	// A: 1e6..20e6; B: 4e6, 8e6, ..., 20e6. Ties break by _id, so b-00k
	// follows A's 4(k+1)th job (a-003 before b-000 at 4e6).
	want := expectedOrder(planTenant("a", 20, 1, 1), planTenant("b", 5, 4, 1))
	assertOrder(t, drainOrder(ctx, t, q, ""), want)
}

// TestFairnessSameTenantCostNoReorder proves cost only spaces a tenant
// against others: within one tenant, vstamps grow monotonically with
// enqueue order whatever the costs. Ids descend so _id order cannot
// produce the expected result by accident.
func TestFairnessSameTenantCostNoReorder(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	// Vstamps 10e6, 11e6, 16e6.
	enqueued := []string{"a-002", "a-001", "a-000"}
	for i, cost := range []int64{10, 1, 5} {
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: enqueued[i], TenantID: "a", Cost: cost, Weight: 1})
	}
	assertOrder(t, drainOrder(ctx, t, q, ""), enqueued)
}

// TestFairnessSingleTenant proves the degenerate one-tenant case runs the
// shared path as a FIFO: stored vstamps are the running sum of strides, an
// enqueue sequence. Ids descend so _id order differs from enqueue order.
func TestFairnessSingleTenant(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	costs := []int64{3, 1, 7, 2, 1}
	enqueued := make([]string, len(costs))
	wantVStamps := make([]int64, len(costs))
	var vtime int64
	for i, cost := range costs {
		enqueued[i] = fmt.Sprintf("job-%03d", len(costs)-1-i)
		vtime += max(1, cost*1_000_000/1) // default weight is 1
		wantVStamps[i] = vtime
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: enqueued[i], Cost: cost})
	}

	for i, id := range enqueued {
		if got := findJob(ctx, t, q, id).VStamp; got != wantVStamps[i] {
			t.Errorf("%s vstamp = %d, want %d", id, got, wantVStamps[i])
		}
	}
	assertOrder(t, drainOrder(ctx, t, q, ""), enqueued)
}

// TestFairnessPartitionScoped proves a tenant's timeline is per partition:
// enqueues in one partition do not advance the tenant's vtime in another, so
// a tenant's first job in a new partition seeds fresh, and each partition
// drains its own jobs by (vstamp, _id).
func TestFairnessPartitionScoped(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	unit := EnqueueOpts{Cost: 1, Weight: 1}
	enqueue := func(id, tenant, partition string) {
		opts := unit
		opts.JobID, opts.TenantID, opts.Partition = id, tenant, partition
		enqueueJob(ctx, t, q, opts)
	}
	// Vstamps (x1e6): a-000..a-003 = 1..4 in p1; a-004 = 1 in p2 (fresh
	// timeline, not a's p1 vtime); b-000 = 1 in p2; b-001 = 1 in p1.
	for i := range 4 {
		enqueue(fmt.Sprintf("a-%03d", i), "a", "p1")
	}
	enqueue("a-004", "a", "p2")
	enqueue("b-000", "b", "p2")
	enqueue("b-001", "b", "p1")

	assertOrder(t, drainOrder(ctx, t, q, "p1"), []string{"a-000", "b-001", "a-001", "a-002", "a-003"})
	assertOrder(t, drainOrder(ctx, t, q, "p2"), []string{"a-004", "b-000"})
}

// TestRaceFairnessConcurrentTenants enqueues two tenants concurrently, one
// goroutine each, exercising cross-tenant cache locking under real
// interleaving. Each tenant's vstamps depend only on its own enqueue count,
// so the dispatch order is fixed however the enqueues interleaved.
// TestFairnessFloodingTenant is the sequenced test of the same rule.
func TestRaceFairnessConcurrentTenants(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := raceCtx(t)
	q := New(db.Collection("jobs"))
	body := rawBody(t, bson.D{})

	const n = 50
	tenants := []string{"a", "b"}
	errs := make([]error, len(tenants))
	var wg sync.WaitGroup
	for i, tenant := range tenants {
		wg.Go(func() {
			for k := range n {
				id := fmt.Sprintf("%s-%03d", tenant, k)
				opts := EnqueueOpts{JobID: id, TenantID: tenant, Cost: 1, Weight: 1}
				if err := q.enqueue(ctx, "test", body, opts); err != nil {
					errs[i] = fmt.Errorf("enqueue %q: %w", id, err)
					return
				}
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}

	// Ties at each k*1e6 break by _id: a-00k before b-00k.
	var want []string
	for k := range n {
		want = append(want, fmt.Sprintf("a-%03d", k), fmt.Sprintf("b-%03d", k))
	}
	assertOrder(t, drainOrder(ctx, t, q, ""), want)
}
