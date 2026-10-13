package mongoqueue

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"
)

// mustReserve calls reserve with a background context and fails the test on
// error. With no seed hook wired (the pure-unit configuration) reserve cannot
// fail, so the error branch exists only to satisfy the signature honestly.
func mustReserve(t *testing.T, c *vtimeCache, tenant string, cost, weight int64) *vtimeReservation {
	t.Helper()
	r, err := c.reserve(context.Background(), tenant, defaultPartition, cost, weight)
	if err != nil {
		t.Fatalf("reserve(%q, %q, %d, %d): %v", tenant, defaultPartition, cost, weight, err)
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
				r, err := c.reserve(context.Background(), "tenant-a", defaultPartition, cost, weight)
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
	c.seedHook = func(_ context.Context, _, _ string) (int64, error) {
		calls++
		if calls == 1 {
			return 0, errors.New("transient seed failure")
		}
		return seed, nil
	}

	if _, err := c.reserve(context.Background(), "tenant-a", defaultPartition, 1, 1); err == nil {
		t.Fatal("reserve with failing seed hook: got nil error, want failure")
	}

	r, err := c.reserve(context.Background(), "tenant-a", defaultPartition, 1, 1)
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

// TestVtimeCachePartitionsAreIndependentTimelines verifies that one tenant
// in two partitions gets two independent timelines, each seeded once.
func TestVtimeCachePartitionsAreIndependentTimelines(t *testing.T) {
	seeds := map[timelineKey]int{}
	c := newVtimeCache()
	c.seedHook = func(_ context.Context, tenant, partition string) (int64, error) {
		seeds[timelineKey{tenant: tenant, partition: partition}]++
		return 0, nil
	}

	st := stride(1, 1)
	partitions := []string{"p1", "p2"}
	for _, p := range partitions {
		for i := int64(1); i <= 2; i++ {
			r, err := c.reserve(context.Background(), "tenant-a", p, 1, 1)
			if err != nil {
				t.Fatalf("reserve(%q): %v", p, err)
			}
			if want := i * st; r.vstamp != want {
				t.Errorf("partition %q reserve %d: got vstamp %d, want %d", p, i, r.vstamp, want)
			}
			r.Commit()
		}
	}
	for _, p := range partitions {
		if got := seeds[timelineKey{tenant: "tenant-a", partition: p}]; got != 1 {
			t.Errorf("seed hook calls for partition %q: got %d, want 1", p, got)
		}
	}
	if len(seeds) != len(partitions) {
		t.Errorf("seed hook called for %d timelines, want %d", len(seeds), len(partitions))
	}
}

// seedSequence is a fake seedHook that returns seeds[i] on its i-th call
// (repeating the last value once exhausted) and counts calls.
type seedSequence struct {
	seeds []int64
	calls int
}

func (s *seedSequence) hook(context.Context, string, string) (int64, error) {
	i := min(s.calls, len(s.seeds)-1)
	s.calls++
	return s.seeds[i], nil
}

var defaultKey = timelineKey{tenant: "tenant-a", partition: defaultPartition}

// TestVtimeCacheTouchedEntryIsInvalidated verifies that a walk marks an entry
// reserved since the previous walk stale, so its next reserve re-runs the
// seed and stamps from the new seed rather than the cached vtime.
func TestVtimeCacheTouchedEntryIsInvalidated(t *testing.T) {
	seeds := &seedSequence{seeds: []int64{0, 50_000_000}}
	c := newVtimeCache()
	c.seedHook = seeds.hook

	mustReserve(t, c, "tenant-a", 1, 1).Commit()
	c.invalidateAndEvict()

	r := mustReserve(t, c, "tenant-a", 1, 1)
	got := r.vstamp
	r.Commit()

	if seeds.calls != 2 {
		t.Errorf("seed calls = %d, want 2", seeds.calls)
	}
	if want := int64(51_000_000); got != want {
		t.Errorf("vstamp after invalidation = %d, want %d", got, want)
	}
}

// TestVtimeCacheUntouchedEntryIsEvicted verifies the two-walk eviction
// lifetime: the first walk clears touched, the second removes the entry, and
// the next reserve creates a fresh entry that seeds.
func TestVtimeCacheUntouchedEntryIsEvicted(t *testing.T) {
	seeds := &seedSequence{seeds: []int64{0, 7_000_000}}
	c := newVtimeCache()
	c.seedHook = seeds.hook

	mustReserve(t, c, "tenant-a", 1, 1).Commit()
	orig := c.tenants[defaultKey]

	c.invalidateAndEvict()
	if c.tenants[defaultKey] != orig {
		t.Fatal("entry left the map after the first walk")
	}
	if orig.touched.Load() || !orig.stale.Load() {
		t.Fatalf("after first walk: touched=%v stale=%v, want false, true",
			orig.touched.Load(), orig.stale.Load())
	}

	c.invalidateAndEvict()
	if _, ok := c.tenants[defaultKey]; ok {
		t.Fatal("entry still in the map after the second walk")
	}
	if !orig.evicted {
		t.Error("evicted entry not marked evicted")
	}

	r := mustReserve(t, c, "tenant-a", 1, 1)
	got := r.vstamp
	r.Commit()

	fresh := c.tenants[defaultKey]
	if fresh == nil || fresh == orig {
		t.Fatalf("reserve after eviction did not create a fresh entry")
	}
	if seeds.calls != 2 {
		t.Errorf("seed calls = %d, want 2", seeds.calls)
	}
	if want := int64(8_000_000); got != want {
		t.Errorf("vstamp after eviction = %d, want %d", got, want)
	}
}

// TestVtimeCacheBusyEntryIsNotEvicted verifies that a walk finding an
// untouched entry whose lock is held (the window between lockTimeline and
// reserve setting touched) neither blocks nor evicts it, but marks it stale
// so the next reserve re-seeds.
func TestVtimeCacheBusyEntryIsNotEvicted(t *testing.T) {
	seeds := &seedSequence{seeds: []int64{0, 30_000_000}}
	c := newVtimeCache()
	c.seedHook = seeds.hook

	mustReserve(t, c, "tenant-a", 1, 1).Commit()
	c.invalidateAndEvict() // clears touched
	orig := c.tenants[defaultKey]
	orig.stale.Store(false) // so the busy walk's stale mark is observable

	locked := make(chan struct{})
	release := make(chan struct{})
	released := make(chan struct{})
	go func() {
		e := c.lockTimeline(defaultKey)
		close(locked)
		<-release
		e.mu.Unlock()
		close(released)
	}()

	<-locked
	// The walk must return while the lock is held. The timeout is a failure
	// deadline for a walk that blocks on the entry lock, not an ordering sleep.
	walked := make(chan struct{})
	go func() {
		c.invalidateAndEvict()
		close(walked)
	}()
	select {
	case <-walked:
	case <-time.After(5 * time.Second):
		t.Fatal("walk blocked on a held entry lock")
	}
	if c.tenants[defaultKey] != orig {
		t.Error("busy entry left the map")
	}
	if !orig.stale.Load() {
		t.Error("busy entry not marked stale")
	}

	close(release)
	<-released

	r := mustReserve(t, c, "tenant-a", 1, 1)
	got := r.vstamp
	r.Commit()
	if seeds.calls != 2 {
		t.Errorf("seed calls = %d, want 2", seeds.calls)
	}
	if want := int64(31_000_000); got != want {
		t.Errorf("vstamp after busy walk = %d, want %d", got, want)
	}
}

// TestVtimeCacheEvictedEntryRetry forces an eviction between lockTimeline's
// map lookup and its entry lock, and verifies the reserve retries onto a
// fresh entry and stamps from its new seed, never from the orphan's vtime.
func TestVtimeCacheEvictedEntryRetry(t *testing.T) {
	seeds := &seedSequence{seeds: []int64{0, 90_000_000}}
	c := newVtimeCache()
	c.seedHook = seeds.hook

	mustReserve(t, c, "tenant-a", 1, 1).Commit()
	c.invalidateAndEvict() // clears touched so the seam's walk evicts
	orphan := c.tenants[defaultKey]
	// The walk above also marked the orphan stale, which would make it
	// re-seed even if lockTimeline ignored evicted. Clear it so only the
	// evicted check keeps the reserve off the orphan.
	orphan.stale.Store(false)

	fired := false
	c.afterLookup = func(timelineKey) {
		if fired {
			return
		}
		fired = true
		c.invalidateAndEvict()
	}

	r := mustReserve(t, c, "tenant-a", 1, 1)
	got := r.vstamp
	r.Commit()

	if !fired {
		t.Fatal("afterLookup seam never fired")
	}
	if !orphan.evicted {
		t.Error("orphan not marked evicted")
	}
	fresh := c.tenants[defaultKey]
	if fresh == nil || fresh == orphan {
		t.Fatal("reserve did not land on a fresh map entry")
	}
	if seeds.calls != 2 {
		t.Errorf("seed calls = %d, want 2", seeds.calls)
	}
	// Stamping from the orphan (vtime 1e6, not stale) would have given 2e6.
	if want := int64(91_000_000); got != want {
		t.Errorf("vstamp = %d, want %d (from new seed)", got, want)
	}
}

// TestVtimeCacheReconcileRace runs concurrent reserve+commit on two
// timelines against a walk loop that refreshes both partitions' LWM before
// each walk. A seed hook that returns the durable
// maximum must keep each timeline's stamps contiguous however walks
// interleave with reservations.
func TestVtimeCacheReconcileRace(t *testing.T) {
	const (
		perWorker = 100
		workers   = 2
		unit      = int64(1_000_000) // stride(1, 1)
	)
	partitions := []string{"p1", "p2"}

	var mu sync.Mutex
	durable := map[string]int64{}
	stamps := map[string][]int64{}
	seedCalls := map[string]int{}

	c := newVtimeCache()
	c.seedHook = func(_ context.Context, _, partition string) (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		seedCalls[partition]++
		return durable[partition], nil
	}
	// A zero floor keeps the expected stamps at {1..200}*stride while still
	// exercising the cold-seed and reconcile refresh paths concurrently.
	c.lwmHook = func(context.Context, string) (int64, bool, error) {
		return 0, true, nil
	}

	var wg sync.WaitGroup
	for _, p := range partitions {
		for range workers {
			wg.Go(func() {
				for range perWorker {
					r, err := c.reserve(context.Background(), "tenant-a", p, 1, 1)
					if err != nil {
						t.Errorf("reserve: %v", err)
						return
					}
					mu.Lock()
					durable[p] = max(durable[p], r.vstamp)
					stamps[p] = append(stamps[p], r.vstamp)
					mu.Unlock()
					r.Commit()
				}
			})
		}
	}

	done := make(chan struct{})
	// The walker counts evictions per partition by diffing map snapshots taken
	// around each walk: a key whose entry pointer vanished was evicted.
	type walkStats struct {
		walks     int
		evictions map[string]int
	}
	snapshot := func() map[timelineKey]*tenantVtime {
		c.mu.Lock()
		defer c.mu.Unlock()
		return maps.Clone(c.tenants)
	}
	walked := make(chan walkStats)
	go func() {
		st := walkStats{evictions: map[string]int{}}
		for {
			select {
			case <-done:
				walked <- st
				return
			default:
				for _, p := range partitions {
					if err := c.refreshLWM(context.Background(), p); err != nil {
						t.Errorf("refreshLWM(%q): %v", p, err)
					}
				}
				before := snapshot()
				c.invalidateAndEvict()
				after := snapshot()
				st.walks++
				for k, v := range before {
					if after[k] != v {
						st.evictions[k.partition]++
					}
				}
			}
		}
	}()

	wg.Wait()
	close(done)
	ws := <-walked

	for _, p := range partitions {
		seen := make(map[int64]bool, len(stamps[p]))
		for _, s := range stamps[p] {
			if seen[s] {
				t.Errorf("partition %s: duplicate vstamp %d", p, s)
			}
			seen[s] = true
		}
		want := workers * perWorker
		if len(seen) != want {
			t.Errorf("partition %s: %d distinct vstamps, want %d", p, len(seen), want)
		}
		for i := 1; i <= want; i++ {
			if !seen[int64(i)*unit] {
				t.Errorf("partition %s: missing vstamp %d", p, int64(i)*unit)
			}
		}
	}
	// Re-seeds after the initial one are driven by invalidation or eviction;
	// subtracting evictions approximates the invalidation-driven share (an
	// eviction not followed by a reserve overcounts by one).
	t.Logf("walks=%d", ws.walks)
	for _, p := range partitions {
		reseeds := seedCalls[p] - 1
		t.Logf("partition %s: seed calls=%d evictions=%d invalidation re-seeds~=%d",
			p, seedCalls[p], ws.evictions[p], reseeds-ws.evictions[p])
	}
}

// lwmCalls is a fake lwmHook that records calls per partition and returns a
// fixed frontier.
type lwmCalls struct {
	lwm   int64
	calls map[string]int
}

func (f *lwmCalls) hook(_ context.Context, partition string) (lwm int64, ok bool, err error) {
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[partition]++
	return f.lwm, true, nil
}

// TestVtimeCacheColdSeedRefreshesLWM verifies that every cold seed (an
// entry's first) refreshes its partition's frontier, while a re-seed after a
// walk uses the stored LWM without querying.
func TestVtimeCacheColdSeedRefreshesLWM(t *testing.T) {
	ctx := context.Background()
	lwm := &lwmCalls{}
	c := newVtimeCache()
	c.lwmHook = lwm.hook

	reserveCommit := func(tenant, partition string) {
		t.Helper()
		r, err := c.reserve(ctx, tenant, partition, 1, 1)
		if err != nil {
			t.Fatalf("reserve(%q, %q): %v", tenant, partition, err)
		}
		r.Commit()
	}

	reserveCommit("a", "p1")
	if got := lwm.calls["p1"]; got != 1 {
		t.Fatalf("after cold seed (a, p1): lwmHook(p1) calls = %d, want 1", got)
	}
	reserveCommit("a", "p1")
	if got := lwm.calls["p1"]; got != 1 {
		t.Fatalf("after seeded reserve (a, p1): lwmHook(p1) calls = %d, want 1", got)
	}
	reserveCommit("b", "p1")
	if got := lwm.calls["p1"]; got != 2 {
		t.Fatalf("after cold seed (b, p1): lwmHook(p1) calls = %d, want 2", got)
	}
	reserveCommit("a", "p2")
	if got := lwm.calls["p2"]; got != 1 {
		t.Fatalf("after cold seed (a, p2): lwmHook(p2) calls = %d, want 1", got)
	}

	c.invalidateAndEvict()
	reserveCommit("a", "p1")
	if got := lwm.calls["p1"]; got != 2 {
		t.Errorf("after re-seed (a, p1): lwmHook(p1) calls = %d, want 2 (re-seed must not query)", got)
	}
}

// TestVtimeCacheColdSeedLWMErrorRetries verifies that a cold seed whose
// frontier refresh fails also fails the reserve and leaves the entry unseeded, so
// the next reserve retries the refresh and the seed.
func TestVtimeCacheColdSeedLWMErrorRetries(t *testing.T) {
	const lwm = int64(10 * scale)
	ctx := context.Background()
	lwmCalls, seedCalls := 0, 0
	c := newVtimeCache()
	c.lwmHook = func(context.Context, string) (int64, bool, error) {
		lwmCalls++
		if lwmCalls == 1 {
			return 0, false, errors.New("transient frontier failure")
		}
		return lwm, true, nil
	}
	c.seedHook = func(context.Context, string, string) (int64, error) {
		seedCalls++
		return 0, nil
	}

	if _, err := c.reserve(ctx, "tenant-a", defaultPartition, 1, 1); err == nil {
		t.Fatal("reserve with failing lwmHook: got nil error, want failure")
	}
	if seedCalls != 0 {
		t.Errorf("seed hook calls after failed refresh = %d, want 0", seedCalls)
	}

	r, err := c.reserve(ctx, "tenant-a", defaultPartition, 1, 1)
	if err != nil {
		t.Fatalf("reserve after refresh retry: %v", err)
	}
	defer r.Abort()
	if want := lwm + stride(1, 1); r.vstamp != want {
		t.Errorf("vstamp = %d, want %d", r.vstamp, want)
	}
	if lwmCalls != 2 {
		t.Errorf("lwmHook calls = %d, want 2 (one failure, one retry)", lwmCalls)
	}
}

// TestVtimeCacheLWMTracksFrontier verifies the stored LWM is the last
// computed frontier with no monotone hold: it follows a dip, and a refresh
// that finds no visible job keeps the prior value.
func TestVtimeCacheLWMTracksFrontier(t *testing.T) {
	type result struct {
		lwm int64
		ok  bool
	}
	results := []result{{10 * scale, true}, {6 * scale, true}, {0, false}}
	want := []int64{10 * scale, 6 * scale, 6 * scale}

	i := 0
	c := newVtimeCache()
	c.lwmHook = func(context.Context, string) (int64, bool, error) {
		r := results[i]
		i++
		return r.lwm, r.ok, nil
	}

	for n, w := range want {
		if err := c.refreshLWM(context.Background(), "p"); err != nil {
			t.Fatalf("refresh %d: %v", n, err)
		}
		if got := c.lwmFor("p"); got != w {
			t.Errorf("after refresh %d: lwmFor = %d, want %d", n, got, w)
		}
	}
}

// TestVtimeCacheSeedTakesMax verifies a seed takes max(trailing edge, LWM).
func TestVtimeCacheSeedTakesMax(t *testing.T) {
	const lwm = int64(10 * scale)
	cases := []struct {
		name string
		edge int64
		want int64
	}{
		{"edge below floor", 3 * scale, lwm + stride(1, 1)},
		{"edge above floor", 12 * scale, 12*scale + stride(1, 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newVtimeCache()
			c.lwmHook = (&lwmCalls{lwm: lwm}).hook
			c.seedHook = func(context.Context, string, string) (int64, error) {
				return tc.edge, nil
			}
			r := mustReserve(t, c, "tenant-a", 1, 1)
			defer r.Abort()
			if r.vstamp != tc.want {
				t.Errorf("first vstamp = %d, want %d", r.vstamp, tc.want)
			}
		})
	}
}

// TestReconcileFailedRefreshLeavesCacheUntouched verifies that a reconcile
// whose LWM refresh fails for one of two partitions skips the walk: a
// touched entry stays seeded, touched, and not stale. The Queue has only a
// cache with fake hooks; reconcile touches nothing else, so no database is
// needed.
func TestReconcileFailedRefreshLeavesCacheUntouched(t *testing.T) {
	ctx := context.Background()
	failing := false
	c := newVtimeCache()
	c.lwmHook = func(_ context.Context, partition string) (int64, bool, error) {
		if failing && partition == "p2" {
			return 0, false, errors.New("frontier failure")
		}
		return 0, true, nil
	}
	q := &Queue{vtimes: c}

	for _, p := range []string{"p1", "p2"} {
		r, err := c.reserve(ctx, "tenant-a", p, 1, 1)
		if err != nil {
			t.Fatalf("reserve(%q): %v", p, err)
		}
		r.Commit()
	}

	failing = true
	if err := q.reconcile(ctx); err == nil {
		t.Fatal("reconcile with failing lwmHook: got nil error, want failure")
	}

	for _, p := range []string{"p1", "p2"} {
		key := timelineKey{tenant: "tenant-a", partition: p}
		c.mu.Lock()
		tl, ok := c.tenants[key]
		c.mu.Unlock()
		if !ok {
			t.Fatalf("timeline %v evicted by failed reconcile", key)
		}
		if !tl.touched.Load() {
			t.Errorf("timeline %v: touched cleared by failed reconcile", key)
		}
		if tl.stale.Load() {
			t.Errorf("timeline %v: marked stale by failed reconcile", key)
		}
		tl.mu.Lock()
		seeded := tl.seeded
		tl.mu.Unlock()
		if !seeded {
			t.Errorf("timeline %v: unseeded by failed reconcile", key)
		}
	}
}
