package mongoqueue

import (
	"context"
	"sync"
)

// vtimeCache is the node-local, per-tenant virtual-time cache. It is soft
// state: durable job records carry the authoritative vstamps, and the cache
// exists only so the enqueue path can stamp the next job without a read.
// Entries are rebuildable (cold-start seeding and periodic reconciliation
// arrive in later phases; see seedHook below).
//
// Locking granularity is two-level. A global mutex (mu) guards only the
// tenant map itself -- lookup and lazy creation of tenant entries -- and is
// never held across I/O. Each tenant then has its own mutex, held from
// reserve through the caller's insert until commit or abort. Per-tenant
// locking is required, not merely an optimization: the strictly-increasing
// vstamp invariant is per tenant, so exclusivity must span the whole
// reserve->insert->commit sequence for that tenant, while enqueues for
// different tenants proceed concurrently. A single global lock held across
// inserts would serialize all tenants behind one slow write.
type vtimeCache struct {
	mu      sync.Mutex
	tenants map[string]*tenantVtime

	// seedHook, when non-nil, supplies the initial vtime for a tenant on
	// first touch. Queue wires it to seedTenantVtime, the minimal cold-start
	// seed: max pending vstamp for the tenant (0 if none). It is called with
	// the tenant's lock held, before the first reserve computes a candidate,
	// so exactly one enqueue per tenant pays the query. Nil means start at 0.
	// A hook error fails the reserve and leaves the tenant unseeded, so the
	// next reserve retries the seed. Superseded by full reconciliation in
	// Phase 6.
	seedHook func(ctx context.Context, tenant string) (int64, error)
}

// tenantVtime is one tenant's cache entry. mu serializes the full
// reserve->insert->commit sequence for the tenant; vtime is the last
// committed virtual timestamp on the tenant's timeline.
type tenantVtime struct {
	mu     sync.Mutex
	vtime  int64
	seeded bool // set under mu by the first reserve; guards one-shot seeding
}

// newVtimeCache returns an empty cache; tenants are created lazily on first
// reserve.
func newVtimeCache() *vtimeCache {
	return &vtimeCache{tenants: make(map[string]*tenantVtime)}
}

// vtimeReservation is the exclusivity guard returned by reserve. It holds the
// tenant's lock, so the holder MUST call exactly one of Commit or Abort;
// dropping a reservation on the floor deadlocks that tenant's enqueue path.
//
// vstamp is the candidate virtual timestamp for the job being inserted. The
// cache state is untouched until Commit.
type vtimeReservation struct {
	tenant *tenantVtime
	vstamp int64
}

// reserve computes the candidate vstamp for the next job on tenant's
// timeline -- vtime + stride(cost, weight) -- without mutating cache state,
// and returns a reservation holding the tenant's lock. The caller performs
// its insert while holding the reservation, then calls Commit on success or
// Abort on failure (including duplicate-key: an idempotent re-enqueue that
// inserted nothing must advance nothing).
//
// cost and weight must already be normalized (see normalizeCost and
// normalizeWeight); reserve applies stride directly.
//
// reserve fails only when the one-shot seed hook fails; the tenant's lock is
// released and the tenant stays unseeded, so a later reserve retries the
// seed. ctx bounds only the seed query and is unused after a tenant is
// seeded.
func (c *vtimeCache) reserve(ctx context.Context, tenant string, cost, weight int64) (*vtimeReservation, error) {
	c.mu.Lock()
	t, ok := c.tenants[tenant]
	if !ok {
		t = &tenantVtime{}
		c.tenants[tenant] = t
	}
	c.mu.Unlock()

	t.mu.Lock()
	if !t.seeded {
		if c.seedHook != nil {
			seed, err := c.seedHook(ctx, tenant)
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

// Commit records a successful insert: the tenant's vtime advances to the
// reserved candidate, and the tenant's lock is released.
func (r *vtimeReservation) Commit() {
	r.tenant.vtime = r.vstamp
	r.tenant.mu.Unlock()
}

// Abort releases the tenant's lock without advancing vtime. Used when the
// insert failed or was a duplicate; the next reserve recomputes from the same
// base, so the candidate slot is reused rather than leaked.
func (r *vtimeReservation) Abort() {
	r.tenant.mu.Unlock()
}
