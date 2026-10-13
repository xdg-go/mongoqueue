package mongoqueue

import (
	"context"
	"sync"
)

// vtimeCache is the node-local virtual-time cache, with one timeline per
// (tenant, partition). It is soft state: durable job records carry the
// authoritative vstamps, and the cache exists only so the enqueue path can
// stamp the next job without a read.
// Entries are rebuildable (cold-start seeding and periodic reconciliation
// arrive in later phases; see seedHook below).
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
type vtimeCache struct {
	mu      sync.Mutex
	tenants map[timelineKey]*tenantVtime

	// seedHook, when non-nil, supplies the initial vtime for a timeline on
	// first touch. Its contract is to return the max pending vstamp for the
	// timeline (0 if none); Queue wires it to seedTenantVtime, the minimal
	// cold-start seed. It is called with the timeline's lock held, before the
	// first reserve computes a candidate, so exactly one enqueue per timeline
	// pays the query. Nil means start at 0. A hook error fails the reserve and
	// leaves the timeline unseeded, so the next reserve retries the seed.
	// Reconciliation will invalidate entries so each timeline re-seeds.
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
type tenantVtime struct {
	mu     sync.Mutex
	vtime  int64
	seeded bool // set under mu by the first reserve; guards one-shot seeding
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
	key := timelineKey{tenant: tenant, partition: partition}
	c.mu.Lock()
	t, ok := c.tenants[key]
	if !ok {
		t = &tenantVtime{}
		c.tenants[key] = t
	}
	c.mu.Unlock()

	t.mu.Lock()
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
