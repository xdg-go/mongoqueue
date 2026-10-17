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

// planTenant returns n jobs prefix-000.. on a tenant seeded at base, with
// vstamps base + k*stride(cost, weight) for k = 1..n, where
// stride = max(1, cost*scale/weight). A tenant's base is the partition's
// frontier when its cold seed runs: its service clock (max claimed vstamp),
// else its min pending vstamp, else 0.
func planTenant(prefix string, n int, base, cost, weight int64) []plannedJob {
	stride := max(1, cost*scale/weight)
	jobs := make([]plannedJob, n)
	for i := range jobs {
		jobs[i] = plannedJob{id: fmt.Sprintf("%s-%03d", prefix, i), vstamp: base + int64(i+1)*stride}
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
// interleave with A's from the start. B's cold seed floors it to the
// frontier (a-000 at 1e3), so B joins one stride past the service point.
func TestFairnessFloodingTenant(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	enqueueTenant(ctx, t, q, 50, EnqueueOpts{TenantID: "a", Cost: 1, Weight: 1})
	enqueueTenant(ctx, t, q, 5, EnqueueOpts{TenantID: "b", Cost: 1, Weight: 1})

	// b-00k is stamped (k+2)e3 and ties a-(k+1); ties break by _id, so
	// a-(k+1) comes first.
	var want []string
	for i := range 50 {
		want = append(want, fmt.Sprintf("a-%03d", i))
		if i >= 1 && i <= 5 {
			want = append(want, fmt.Sprintf("b-%03d", i-1))
		}
	}
	assertOrder(t, drainOrder(ctx, t, q, ""), want)
}

// TestFairnessWeightTwoToOne proves weight 2 doubles a tenant's share at
// equal cost: every 3k prefix holds exactly 2k+1 A jobs while both have
// work. A's extra job is the half-stride head start the frontier floor gives
// the incumbent: B's cold seed finds a-000 at 0.5e3.
func TestFairnessWeightTwoToOne(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	const n = 30
	enqueueTenant(ctx, t, q, n, EnqueueOpts{TenantID: "a", Cost: 1, Weight: 2})
	enqueueTenant(ctx, t, q, n, EnqueueOpts{TenantID: "b", Cost: 1, Weight: 1})

	got := drainOrder(ctx, t, q, "")
	assertOrder(t, got, expectedOrder(planTenant("a", n, 0, 1, 2), planTenant("b", n, scale/2, 1, 1)))

	// B is stamped (k+1.5)e3, so 14 B jobs precede A's last job (a-029,
	// vstamp 15e3) and both tenants have work through the first 44 claims:
	// k = 1..14.
	for k := 1; 3*k <= 42; k++ {
		aCount := 0
		for _, id := range got[:3*k] {
			if strings.HasPrefix(id, "a-") {
				aCount++
			}
		}
		if aCount != 2*k+1 {
			t.Errorf("prefix %d: %d A jobs, %d B jobs; want %d and %d", 3*k, aCount, 3*k-aCount, 2*k+1, k-1)
		}
	}
}

// TestFairnessCheapVsExpensive proves cost scales a tenant's stride at equal
// weight: one cost-4 job from B takes the slot of four cost-1 jobs from A,
// starting from the frontier B's cold seed finds (a-000 at 1e3).
func TestFairnessCheapVsExpensive(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	enqueueTenant(ctx, t, q, 20, EnqueueOpts{TenantID: "a", Cost: 1, Weight: 1})
	enqueueTenant(ctx, t, q, 5, EnqueueOpts{TenantID: "b", Cost: 4, Weight: 1})

	// A: 1e3..20e3; B: 5e3, 9e3, ..., 21e3. Ties break by _id, so for
	// k < 4, b-00k follows a-(4k+4) (a-004 before b-000 at 5e3); b-004 at
	// 21e3 comes after A's last job.
	want := expectedOrder(planTenant("a", 20, 0, 1, 1), planTenant("b", 5, scale, 4, 1))
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

	// Vstamps 10e3, 11e3, 16e3.
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
		vtime += max(1, cost*scale/1) // default weight is 1
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
// a tenant's first job in a new partition seeds fresh, floored to that
// partition's frontier, and each partition drains its own jobs by
// (vstamp, _id).
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
	// Vstamps (x1e3): a-000..a-003 = 1..4 in p1; a-004 = 1 in p2 (fresh
	// timeline, not a's p1 vtime); b-000 = 2 in p2 (floored to a-004's
	// frontier at 1); b-001 = 2 in p1 (floored to a-000's frontier at 1).
	for i := range 4 {
		enqueue(fmt.Sprintf("a-%03d", i), "a", "p1")
	}
	enqueue("a-004", "a", "p2")
	enqueue("b-000", "b", "p2")
	enqueue("b-001", "b", "p1")

	assertOrder(t, drainOrder(ctx, t, q, "p1"), []string{"a-000", "a-001", "b-001", "a-002", "a-003"})
	assertOrder(t, drainOrder(ctx, t, q, "p2"), []string{"a-004", "b-000"})
}

// TestRaceFairnessConcurrentTenants enqueues two tenants concurrently, one
// goroutine each, exercising cross-tenant cache locking under real
// interleaving. Both tenants' first jobs are enqueued sequentially first, so
// the cold seeds (and b's frontier floor) are fixed; after that each
// tenant's vstamps depend only on its own enqueue count, so the dispatch
// order is fixed however the enqueues interleaved.
// TestFairnessFloodingTenant is the sequenced test of the same rule.
func TestRaceFairnessConcurrentTenants(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := raceCtx(t)
	q := New(db.Collection("jobs"))
	body := rawBody(t, bson.D{})

	const n = 50
	tenants := []string{"a", "b"}
	// a-000 = 1e3; b-000's cold seed finds a-000 at the frontier: 2e3.
	for _, tenant := range tenants {
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: tenant + "-000", TenantID: tenant, Cost: 1, Weight: 1})
	}
	errs := make([]error, len(tenants))
	var wg sync.WaitGroup
	for i, tenant := range tenants {
		wg.Go(func() {
			for k := 1; k < n; k++ {
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

	// b-(k-1) ties a-k at (k+1)e3 and breaks by _id: a-k first.
	want := []string{"a-000"}
	for k := 1; k < n; k++ {
		want = append(want, fmt.Sprintf("a-%03d", k), fmt.Sprintf("b-%03d", k-1))
	}
	want = append(want, fmt.Sprintf("b-%03d", n-1))
	assertOrder(t, drainOrder(ctx, t, q, ""), want)
}
