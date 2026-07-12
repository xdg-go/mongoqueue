package mongoqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// mustReserve calls reserve with a background context and fails the test on
// error. With no seed hook wired (the pure-unit configuration) reserve cannot
// fail, so the error branch exists only to satisfy the signature honestly.
func mustReserve(t *testing.T, c *vtimeCache, tenant string, cost, weight int64) *vtimeReservation {
	t.Helper()
	r, err := c.reserve(context.Background(), tenant, cost, weight)
	if err != nil {
		t.Fatalf("reserve(%q, %d, %d): %v", tenant, cost, weight, err)
	}
	return r
}

// TestVtimeCacheAbortAdvancesNothing verifies that aborting a reservation
// (failed or duplicate insert) leaves the tenant's vtime untouched: the next
// reserve recomputes from the same base and yields the same candidate.
func TestVtimeCacheAbortAdvancesNothing(t *testing.T) {
	cases := []struct {
		name         string
		cost, weight int64
	}{
		{"unit cost and weight", 1, 1},
		{"heavy cost", 100, 1},
		{"high weight", 10, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newVtimeCache()

			r1 := mustReserve(t, c, "tenant-a", tc.cost, tc.weight)
			first := r1.vstamp
			r1.Abort()

			r2 := mustReserve(t, c, "tenant-a", tc.cost, tc.weight)
			if r2.vstamp != first {
				t.Errorf("reserve after abort: got vstamp %d, want %d (same base reused)", r2.vstamp, first)
			}
			r2.Commit()

			// After a commit the base has moved; a subsequent abort still
			// preserves the committed value.
			r3 := mustReserve(t, c, "tenant-a", tc.cost, tc.weight)
			afterCommit := r3.vstamp
			r3.Abort()
			r4 := mustReserve(t, c, "tenant-a", tc.cost, tc.weight)
			if r4.vstamp != afterCommit {
				t.Errorf("reserve after post-commit abort: got vstamp %d, want %d", r4.vstamp, afterCommit)
			}
			r4.Abort()
		})
	}
}

// TestVtimeCacheStridesAccumulate verifies that successive commits produce
// strictly increasing vstamps matching stride arithmetic from a zero base.
func TestVtimeCacheStridesAccumulate(t *testing.T) {
	cases := []struct {
		name    string
		enqs    []struct{ cost, weight int64 }
		tenants []string // parallel tenant per enqueue; independent timelines
	}{
		{
			name: "uniform unit strides",
			enqs: []struct{ cost, weight int64 }{
				{1, 1}, {1, 1}, {1, 1},
			},
			tenants: []string{"a", "a", "a"},
		},
		{
			name: "mixed cost and weight",
			enqs: []struct{ cost, weight int64 }{
				{5, 1}, {1, 3}, {7, 2},
			},
			tenants: []string{"a", "a", "a"},
		},
		{
			name: "tenants advance independently",
			enqs: []struct{ cost, weight int64 }{
				{2, 1}, {3, 1}, {2, 1},
			},
			tenants: []string{"a", "b", "a"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newVtimeCache()
			want := make(map[string]int64) // expected vtime per tenant
			last := make(map[string]int64) // last vstamp seen per tenant
			for i, e := range tc.enqs {
				tenant := tc.tenants[i]
				r := mustReserve(t, c, tenant, e.cost, e.weight)
				want[tenant] += stride(e.cost, e.weight)
				if r.vstamp != want[tenant] {
					t.Errorf("enqueue %d tenant %q: got vstamp %d, want %d", i, tenant, r.vstamp, want[tenant])
				}
				if prev, ok := last[tenant]; ok && r.vstamp <= prev {
					t.Errorf("enqueue %d tenant %q: vstamp %d not strictly greater than %d", i, tenant, r.vstamp, prev)
				}
				last[tenant] = r.vstamp
				r.Commit()
			}
		})
	}
}

// TestVtimeCacheConcurrentReserveCommit races many goroutines enqueueing for
// one tenant and asserts every committed vstamp is unique and, in aggregate,
// forms the exact strictly increasing sequence stride arithmetic predicts.
// Run with -race.
func TestVtimeCacheConcurrentReserveCommit(t *testing.T) {
	const (
		goroutines = 16
		perG       = 50
		cost       = 3
		weight     = 2
	)
	c := newVtimeCache()
	stamps := make(chan int64, goroutines*perG)

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range perG {
				// No seed hook is wired, so reserve cannot fail; assert
				// anyway rather than discard the error. t.Errorf is safe
				// from a goroutine (t.Fatalf is not).
				r, err := c.reserve(context.Background(), "tenant-a", cost, weight)
				if err != nil {
					t.Errorf("reserve: %v", err)
					return
				}
				stamps <- r.vstamp
				r.Commit()
			}
		})
	}
	wg.Wait()
	close(stamps)

	seen := make(map[int64]bool, goroutines*perG)
	for s := range stamps {
		if seen[s] {
			t.Errorf("duplicate vstamp %d", s)
		}
		seen[s] = true
	}
	// With a fixed stride, N commits from a zero base must occupy exactly
	// the slots stride, 2*stride, ..., N*stride -- uniqueness plus this
	// coverage implies the committed sequence was strictly increasing.
	st := stride(cost, weight)
	for i := int64(1); i <= goroutines*perG; i++ {
		if !seen[i*st] {
			t.Errorf("missing expected vstamp %d", i*st)
		}
	}
}

// TestVtimeCacheSeedErrorRetries verifies that a failing seed hook fails the
// reserve, leaves the tenant unseeded, and a later reserve retries the seed
// and succeeds with the seeded base.
func TestVtimeCacheSeedErrorRetries(t *testing.T) {
	const seed = int64(42 * scale)
	calls := 0
	c := newVtimeCache()
	c.seedHook = func(_ context.Context, _ string) (int64, error) {
		calls++
		if calls == 1 {
			return 0, errors.New("transient seed failure")
		}
		return seed, nil
	}

	if _, err := c.reserve(context.Background(), "tenant-a", 1, 1); err == nil {
		t.Fatal("reserve with failing seed hook: got nil error, want failure")
	}

	r, err := c.reserve(context.Background(), "tenant-a", 1, 1)
	if err != nil {
		t.Fatalf("reserve after seed retry: %v", err)
	}
	defer r.Abort()
	if want := seed + stride(1, 1); r.vstamp != want {
		t.Errorf("seeded reserve: got vstamp %d, want %d", r.vstamp, want)
	}
	if calls != 2 {
		t.Errorf("seed hook calls: got %d, want 2 (one failure, one retry)", calls)
	}
}
