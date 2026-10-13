package mongoqueue

import (
	"context"
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
// Lock order is global mu, then entry mu, then the frontier lock (lwmMu, the
// LWM lock added in Phase 6.3).
// Only eviction holds the global lock while taking an entry lock, and only
// with TryLock; reserve releases the global lock before taking the entry
// lock. Never acquire in the reverse direction.
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
	// to call repeatedly for the same timeline.
	seedHook func(ctx context.Context, tenant, partition string) (int64, error)
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
	mu      sync.Mutex
	vtime   int64
	seeded  bool        // guarded by mu; cleared when stale is applied
	evicted bool        // guarded by mu; set once, before the entry leaves the map
	touched atomic.Bool // set by every reserve; cleared by the walk
	stale   atomic.Bool // set by the walk; applied to seeded by the next reserve
}

// newVtimeCache returns an empty cache; timelines are created lazily on
// first reserve.
func newVtimeCache() *vtimeCache {
	return &vtimeCache{tenants: make(map[timelineKey]*tenantVtime)}
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
// normalizeWeight); reserve applies stride directly.
//
// reserve fails only when the one-shot seed hook fails; the timeline's lock
// is released and the timeline stays unseeded, so a later reserve retries
// the seed. ctx bounds only the seed query and is unused after a timeline is
// seeded.
func (c *vtimeCache) reserve(ctx context.Context, tenant, partition string, cost, weight int64) (*vtimeReservation, error) {
	t := c.lockTimeline(timelineKey{tenant: tenant, partition: partition})
	t.touched.Store(true)
	if t.stale.Swap(false) {
		t.seeded = false
	}
	if !t.seeded {
		if c.seedHook != nil {
			seed, err := c.seedHook(ctx, tenant, partition)
			if err != nil {
				t.mu.Unlock()
				return nil, err
			}
			t.vtime = seed
		}
		t.seeded = true
	}
	return &vtimeReservation{tenant: t, vstamp: t.vtime + stride(cost, weight)}, nil
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
// marked stale instead of evicted. No I/O is done under the global lock.
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
