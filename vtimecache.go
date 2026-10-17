package mongoqueue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// vtimeCache is the node-local virtual-time cache, with one timeline per
// (tenant, partition). It is soft state: durable job records carry the
// authoritative vstamps, and the cache exists only so the enqueue path can
// stamp the next job without a read.
// Entries are rebuildable: a timeline seeds on first reserve (see seedHook),
// and invalidateAndEvict, run by reconcile, makes each timeline re-seed.
//
// Reconcile walks the cache without blocking on any timeline. An entry
// reserved since the previous walk is marked stale, so its next reserve
// re-runs the seed under the timeline lock, after every prior insert from
// this node has been acknowledged; this is what bounds drift. An entry not
// reserved since the previous walk is evicted, so an idle entry is removed
// on the second consecutive walk that finds it untouched and lives up to
// two reconcile intervals plus walk time. An evicted timeline re-seeds as a
// fresh entry on its next reserve.
//
// Locking granularity is two-level. A global mutex (mu) guards only the
// timeline map itself -- lookup and lazy creation of entries -- and is never
// held across I/O. Each timeline then has its own mutex, held from reserve
// through the caller's insert until commit or abort. Per-timeline locking is
// required, not merely an optimization: the strictly-increasing vstamp
// invariant is per timeline, so exclusivity must span the whole
// reserve->insert->commit sequence for that timeline, while enqueues on
// different timelines proceed concurrently. A single global lock held across
// inserts would serialize all timelines behind one slow write.
//
// Lock order is global mu, then entry mu, then lwmMu. Only eviction holds the
// global lock while taking an entry lock, and only with TryLock; reserve
// releases the global lock before taking the entry lock. Only the seed path
// takes lwmMu under an entry lock, and only the walk takes it under the
// global lock. Never acquire in the reverse direction. No I/O is done while
// holding mu or lwmMu.
type vtimeCache struct {
	mu      sync.Mutex
	tenants map[timelineKey]*tenantVtime

	// afterLookup, when non-nil, runs between the map lookup and the entry
	// lock in lockTimeline. It is a test seam for racing an eviction against
	// that window; nil in production.
	afterLookup func(key timelineKey)

	// seedHook, when non-nil, supplies the initial vtime for a timeline on
	// first touch. Its contract is to return the max pending vstamp for the
	// timeline (0 if none); Queue wires it to seedTenantVtime, the minimal
	// cold-start seed. It is called with the timeline's lock held, before the
	// first reserve computes a candidate, so exactly one enqueue per timeline
	// pays the query. Nil means start at 0. A hook error fails the reserve and
	// leaves the timeline unseeded, so the next reserve retries the seed.
	// It runs again on the first reserve after a reconcile marks the entry
	// stale, and on the first reserve after an eviction, so it must be safe
	// to call repeatedly for the same timeline. The seed is the trailing
	// edge only; reserve floors it by the partition's LWM (see lwmHook).
	seedHook func(ctx context.Context, tenant, partition string) (int64, error)

	// lwmHook, when non-nil, returns a partition's dispatch frontier (its
	// low-water mark, LWM): its service clock, the highest fenced (claimed,
	// unreleased) vstamp there, or with no fenced job the lowest pending vstamp.
	// ok is false when the partition has neither. Queue wires it to
	// computeLWM. Nil means the LWM is always 0. Every seed takes
	// max(trailing edge, LWM), so a new, evicted, or returning timeline joins
	// at the point of service rather than claiming credit for idle time.
	lwmHook func(ctx context.Context, partition string) (lwm int64, ok bool, err error)

	// lwm holds each partition's last computed frontier; a missing key reads
	// as 0. It is refreshed by a cold seed (refreshLWM before the first seed of
	// an entry) and by reconcile for every cached partition. A partition's key
	// is pruned by any reconcile walk that finds no cached entry for it (a late
	// refresh may re-store a pruned value; the next walk drops it). lwmMu
	// guards only this map, never across I/O.
	lwmMu sync.Mutex
	lwm   map[string]int64
}

// timelineKey identifies one virtual-time timeline. partition is the
// normalized value stored on the job.
type timelineKey struct {
	tenant, partition string
}

// tenantVtime is one timeline's cache entry. mu serializes the full
// reserve->insert->commit sequence for the timeline; vtime is the last
// committed virtual timestamp on the timeline.
//
// touched and stale are atomics so the reconcile walk can read and set them
// without taking mu, and so never blocks on an in-flight reservation.
type tenantVtime struct {
	mu     sync.Mutex
	vtime  int64
	seeded bool // guarded by mu; cleared when stale is applied
	// everSeeded is guarded by mu and set by the entry's first successful
	// seed; a seed with it unset is a cold seed, which refreshes the LWM.
	everSeeded bool
	evicted    bool        // guarded by mu; set once, before the entry leaves the map
	touched    atomic.Bool // set by every reserve; cleared by the walk
	stale      atomic.Bool // set by the walk; applied to seeded by the next reserve
}

// newVtimeCache returns an empty cache; timelines are created lazily on
// first reserve.
func newVtimeCache() *vtimeCache {
	return &vtimeCache{
		tenants: make(map[timelineKey]*tenantVtime),
		lwm:     make(map[string]int64),
	}
}

// refreshLWM recomputes partition's frontier with lwmHook and stores it. The
// stored value tracks the frontier, dips included (plain overwrite, no max);
// a computation that finds no frontier (ok false) leaves the prior value.
// A hook error is returned and nothing is stored. The hook runs without
// lwmMu held, so a late refresh may overwrite a newer value with an older one
// (an accepted, bounded dip), or re-store a value a walk just pruned; the
// next walk drops it.
func (c *vtimeCache) refreshLWM(ctx context.Context, partition string) error {
	if c.lwmHook == nil {
		return nil
	}
	lwm, ok, err := c.lwmHook(ctx, partition)
	if err != nil {
		return err
	}
	if ok {
		c.lwmMu.Lock()
		c.lwm[partition] = lwm
		c.lwmMu.Unlock()
	}
	return nil
}

// lwmFor returns partition's last computed frontier, or 0 if none.
func (c *vtimeCache) lwmFor(partition string) int64 {
	c.lwmMu.Lock()
	defer c.lwmMu.Unlock()
	return c.lwm[partition]
}

// partitions returns the distinct partitions present in the cache.
func (c *vtimeCache) partitions() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := make(map[string]bool)
	var out []string
	for key := range c.tenants {
		if !seen[key.partition] {
			seen[key.partition] = true
			out = append(out, key.partition)
		}
	}
	return out
}

// vtimeReservation is the exclusivity guard returned by reserve. It holds the
// timeline's lock, so the holder MUST call exactly one of Commit or Abort;
// dropping a reservation on the floor deadlocks that timeline's enqueue path.
//
// vstamp is the candidate virtual timestamp for the job being inserted. The
// cache state is untouched until Commit.
type vtimeReservation struct {
	tenant *tenantVtime
	vstamp int64
}

// reserve computes the candidate vstamp for the next job on the (tenant,
// partition) timeline -- vtime + stride(cost, weight) -- without mutating
// cache state, and returns a reservation holding the timeline's lock. The
// caller performs its insert while holding the reservation, then calls Commit
// on success or Abort on failure (including duplicate-key: an idempotent
// re-enqueue that inserted nothing must advance nothing).
//
// cost and weight must already be normalized (see normalizeCost and
// normalizeWeight).
//
// A seed sets vtime to max(trailing edge, lwmFor(partition)). A cold seed
// (the entry's first) refreshes the partition's LWM first; a re-seed after
// reconcile uses the LWM reconcile stored.
//
// reserve fails when the seed hook or the cold seed's LWM refresh fails;
// the timeline's lock is released and the timeline stays unseeded, so a
// later reserve retries the seed. ctx bounds only those queries and is
// unused after a timeline is seeded. It also fails with ErrVTimeOverflow
// when the candidate would exceed int64 (see nextVStamp); the lock is
// released and vtime is unchanged.
func (c *vtimeCache) reserve(ctx context.Context, tenant, partition string, cost, weight int64) (*vtimeReservation, error) {
	t := c.lockTimeline(timelineKey{tenant: tenant, partition: partition})
	t.touched.Store(true)
	if t.stale.Swap(false) {
		t.seeded = false
	}
	if !t.seeded {
		if err := c.seed(ctx, t, tenant, partition); err != nil {
			t.mu.Unlock()
			return nil, err
		}
	}
	vstamp, err := nextVStamp(t.vtime, cost, weight)
	if err != nil {
		t.mu.Unlock()
		return nil, fmt.Errorf("mongoqueue: stamp tenant %q partition %q: %w", tenant, partition, err)
	}
	return &vtimeReservation{tenant: t, vstamp: vstamp}, nil
}

// seed sets t's vtime to max(trailing edge, LWM) and marks it seeded. The
// caller holds t.mu. On error t is left unseeded.
func (c *vtimeCache) seed(ctx context.Context, t *tenantVtime, tenant, partition string) error {
	if !t.everSeeded {
		if err := c.refreshLWM(ctx, partition); err != nil {
			return err
		}
	}
	var edge int64
	if c.seedHook != nil {
		var err error
		if edge, err = c.seedHook(ctx, tenant, partition); err != nil {
			return err
		}
	}
	t.vtime = max(edge, c.lwmFor(partition))
	t.seeded = true
	t.everSeeded = true
	return nil
}

// lockTimeline returns the map entry for key, creating it if absent, with its
// lock held. The global lock is released before the entry lock is taken, so
// the entry may be evicted in between; locking an evicted entry retries the
// lookup. Stamping on an orphan alongside its replacement would put two
// writers on one timeline inside one process.
func (c *vtimeCache) lockTimeline(key timelineKey) *tenantVtime {
	for {
		c.mu.Lock()
		t, ok := c.tenants[key]
		if !ok {
			t = &tenantVtime{}
			c.tenants[key] = t
		}
		c.mu.Unlock()

		if c.afterLookup != nil {
			c.afterLookup(key)
		}
		t.mu.Lock()
		if !t.evicted {
			return t
		}
		t.mu.Unlock()
	}
}

// invalidateAndEvict is the cache half of reconcile. Each entry reserved
// since the previous walk is marked stale so its next reserve re-seeds; each
// entry not reserved since then is evicted. It never blocks on an entry lock:
// a locked entry has an in-flight reservation that will set touched, so it is
// marked stale instead of evicted. After the walk, every partition with no
// cached entry has its stored LWM pruned, which also drops any value a late
// refreshLWM re-stored after an earlier prune. No I/O is done under the global
// lock.
func (c *vtimeCache) invalidateAndEvict() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, t := range c.tenants {
		if t.touched.Swap(false) {
			t.stale.Store(true)
			continue
		}
		if !t.mu.TryLock() {
			t.stale.Store(true)
			continue
		}
		delete(c.tenants, key)
		t.evicted = true
		t.mu.Unlock()
	}
	cached := make(map[string]bool, len(c.tenants))
	for key := range c.tenants {
		cached[key.partition] = true
	}
	c.lwmMu.Lock()
	for partition := range c.lwm {
		if !cached[partition] {
			delete(c.lwm, partition)
		}
	}
	c.lwmMu.Unlock()
}

// Commit records a successful insert: the timeline's vtime advances to the
// reserved candidate, and the timeline's lock is released.
func (r *vtimeReservation) Commit() {
	r.tenant.vtime = r.vstamp
	r.tenant.mu.Unlock()
}

// Abort releases the timeline's lock without advancing vtime. Used when the
// insert failed or was a duplicate; the next reserve recomputes from the same
// base, so the candidate slot is reused rather than leaked.
func (r *vtimeReservation) Abort() {
	r.tenant.mu.Unlock()
}
