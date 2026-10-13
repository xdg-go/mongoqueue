# TODO: Phase 6 -- Reconciliation and Cache Lifecycle

Expanded from [docs/implementation-plan.md](docs/implementation-plan.md)
Phase 6. Moves virtual-time timelines to per-(tenant, partition), bounds
vtime-cache drift, floors new and returning tenants to each partition's
dispatch frontier (the low-water mark, LWM), bounds cache memory, and
settles how callers drive reconciliation. Resolves open questions #2
(reconciliation/LWM access path) and #3 (reconciliation surface) in
[docs/mongodb-go-library-design.md](docs/mongodb-go-library-design.md).
Design references: "Core model" (memoryless fairness), "Two orthogonal
axes", "Node-local cache model", "Reconciliation", "Low-water mark and the
idle floor", "Cancellation and vtime", and the GC note under "Storage
requirements" in
[docs/fair-job-queue-design.md](docs/fair-job-queue-design.md).

Section 6.0 holds the decisions and contracts every subphase relies on.
Read it before starting any subphase; do not improvise around it.

## Testing Philosophy

- **Pure unit tests for cache mechanics**: per-(tenant, partition) keying,
  invalidation, eviction, the evicted-entry retry, and LWM refresh live in
  `vtimeCache` and are tested with fake `seedHook`/`lwmHook` functions --
  no database.
- **Integration tests against real MongoDB** for anything whose answer
  comes from a query: trailing-edge re-seed, the frontier query, and
  end-to-end dispatch order after a floor. Tests connect via
  `internal/mongotest` (`MONGOQUEUE_TEST_URI`, default
  `mongodb://localhost:27017`).
- **Hermetic tests**: each test uses a uniquely named database/collection
  (random suffix) so parallel runs never collide; drop on cleanup.
- **Exact assertions**: vstamps are integer arithmetic (`stride =
  max(1, cost*1_000_000/weight)`), ties break by `_id`. Assert exact
  vstamps and exact dispatch sequences, never ratios.
- **Concurrency tests**: every outcome a race test accepts also has a
  deterministic, sequenced test that forces it. Race tests run under
  `-race`, log the outcome distribution, and assert only the invariant.
- **Control time through data, not a clock**: no sleeps, no tickers in
  tests. Tests call reconcile directly. Make a job invisible by writing
  `visible_at` into the future (or claiming it with a one-minute lease);
  make it visible by writing `visible_at` into the past.
- **Fixtures carry a partition**: the seed filter and the frontier filter
  both name a partition, so every `insertJob`/`seedDoc` fixture sets
  `Partition` explicitly (`defaultPartition` for unpartitioned tests);
  `insertJob` does not normalize an empty partition.

Test what new code changes. Enqueue validation, stride arithmetic, claim
ordering, and the Phase 3 cold-start seed query are already covered. Use
them as fixtures; don't retest them. The Phase 5 fairness tests change
where 6.0 says they change, and nowhere else.

## Verification Checklist

Before marking a subphase complete and committing it:

1. `go build ./...` and `make vet` pass
2. `make test` passes (including integration tests against Mongo); run the
   race tests with `go test -race ./...`
3. `make lint` clean (config in `.golangci.yml`)
4. New exported identifiers have godoc comments
5. Design docs updated in the same commit when the subphase settles a
   decision the docs left open (each subphase names its doc task)

When verification of a subphase is complete, commit all relevant
newly-created and modified files as one logical unit.

## Dependencies Between Subphases

```
6.0 Decisions and contracts (read first)
       │
       ▼
6.1 Per-(tenant, partition) timelines: cache key, seed query, seed index
       │
       ▼
6.2 Reconcile: invalidate, evict, re-seed from trailing edge
       │
       ▼
6.3 LWM: per-partition frontier, cold-seed refresh, idle floor
       │
       ▼
6.4 Public surface and docs
```

Strictly sequential: 6.2 keys its walk on 6.1's entries, 6.3 extends the
reconcile pass 6.2 builds, and 6.4 exports what 6.1-6.3 built.

---

## 6.0 Decisions and Contracts

### D1 (decided 2026-10-12): memoryless fairness, per-partition frontier, per-(tenant, partition) timelines

**Fairness is memoryless.** Capacity in a partition divides among tenants
with pending work there by their present weights. Past idleness earns no
credit and incurs no penalty. Within a tenant, dispatch is FIFO. Across
tenants, already-enqueued work is slowed by another tenant's arrival,
never jumped: a tenant that becomes backlogged is placed one stride past
the job the partition will serve next, exactly as if it had been active
all along and had just finished its previous work. The floor sets where a
returning tenant *starts*; its stride sets its *share* from then on, so a
returning tenant that floods interleaves with incumbents at its weighted
share from its first job.

**The LWM is the dispatch frontier of a partition**: the minimum vstamp
among that partition's visible pending jobs (`partition`, `liveness:
pending`, `visible_at <= $$NOW`), i.e. the vstamp the next claim in that
partition would take. Visibility filtering matters: a leased or delayed
low-vstamp job must not pin the floor, or every newcomer would inherit
stale credit.

**Timelines are per (tenant, partition).** Each partition is an
independent fair queue over the tenants enqueueing into it. The cache key,
the trailing-edge seed, the single-writer obligation, and the weight
consistency obligation are all per (tenant, partition). A tenant's load in
one partition does not move its position in another. The public API does
not change; `EnqueueOpts.Partition` already names the partition.

Rejected, recorded in the design doc's "Rejected alternatives":
- *Min active trailing edge* (the design doc's former definition): places a
  newcomer behind the shallowest active backlog; contradicts the doc's own
  "idealized fair server" sentence.
- *Idle credit* (no floor, or `frontier - k strides`): unbounded, or a knob
  with little latency gain since the frontier already places a sparse
  tenant's job within one stride of the front.
- *One global frontier over a shared tenant timeline*: stale credit bounded
  only by the spread between lane frontiers, and trailing-edge re-seed
  forgets a tenant's fast-lane consumption whenever it has a slow-lane job
  pending.
- *Per-partition floors on a shared timeline*: a reserve into a hot lane
  drags the tenant's single vtime up, burying its work in cooler lanes
  behind backlog it never consumed.
- *Monotone hold* (`lwm = max(lwm, computed)`): protected a multi-query,
  non-snapshot computation from dips. The per-partition frontier is one
  query, and memoryless says the floor tracks the actual service point,
  dips included. The LWM for a partition is simply its last computed
  frontier; a computation that finds no visible job leaves the prior
  value (initially 0).

### D2: reconciliation is lazy invalidation, not eager rewrite

Reconcile does **not** query each timeline and write the result into the
cache. It (1) refreshes the LWM of every partition present in the cache
(6.3), then (2) walks the cache: an entry touched since the previous
reconcile is **invalidated** (marked stale, which clears `seeded` at its
next reserve), so the timeline's next reserve re-runs the seed under its
own lock; an entry untouched since the previous reconcile is **evicted**.

Why: the seed already runs under the entry lock, after every prior insert
for that timeline on this node has been acknowledged, so -- given the read
requirement in "Seed reads from the primary" below -- it can never read a
trailing edge older than a stamp this node issued. An eager rewrite would
run the query outside the entry lock and need a commit-epoch fence to
avoid lowering vtime below a concurrently inserted job. Lazy invalidation
has no such race. The cost is one indexed `FindOne` (the existing seed
query, served by the seed index) on each active timeline's first enqueue
after each reconcile, paid on the enqueue path.

Consequences an implementer must preserve:
- A seed failure fails that enqueue (existing behavior,
  `TestVtimeCacheSeedErrorRetries`). A reconcile that fails to refresh any
  LWM returns its error and **leaves the cache untouched** -- no
  invalidation, no eviction.
- Re-seeding sets vtime to `max(trailing edge, lwm[partition])` where
  trailing edge is 0 when the timeline has no pending jobs. This one rule
  covers restart, trailing-cancellation refund (the edge drops),
  external-writer drift (the edge rises), and drained/returning tenants
  (no edge, so LWM).
- **Open question #2 answer**: reconciliation adds no index. The
  trailing edge is the seed query on the seed index
  `{tenant: 1, partition: 1, liveness: 1, vstamp: -1}` (6.1 adds
  `partition`); the frontier reads the claim index with the claim filter
  (6.3).

### D3: the surface is caller-driven, and cold seeds refresh the floor

Open question #3 answer: `func (q *Queue) Reconcile(ctx context.Context)
error`, exported in 6.4. No internal goroutine, no `Close`, no interval
option -- `New` stays I/O-free and goroutine-free. The caller runs
`Reconcile` on a ticker; the godoc example shows the loop.

A **cold seed** (a reserve that creates a new cache entry) refreshes the
LWM for its partition before seeding, so the floor is correct for a new or
evicted tenant even on a node that never reconciles. The cost is one extra
indexed read on a rare event. A **re-seed** (a stale entry after a walk)
uses the LWM the reconcile stored; it does not query the frontier again.
A caller that never calls `Reconcile` therefore still floors newcomers
correctly, but cache drift for active timelines is never corrected and
the cache map never shrinks. Godoc states that.

### Cache state and locking

The cache map is keyed by `timelineKey{tenant, partition string}`
(partition already normalized to `defaultPartition` when empty). Each
entry (`tenantVtime`, renamed `timeline` if that reads better; keep the
diff small) gains three fields:
- `touched atomic.Bool` -- set by every `reserve` (seeded or not); cleared
  by the walk.
- `stale atomic.Bool` -- set by the walk; `reserve`, holding the entry
  lock, does `if t.stale.Swap(false) { t.seeded = false }` before its seed
  check. This is how invalidation reaches `seeded` without the walk taking
  the entry lock.
- `evicted bool` -- guarded by the entry's `mu`; set once, by eviction,
  before the entry leaves the map.

Lock order is **global `mu` → entry `mu` → `lwmMu`**. Only eviction takes
global then entry; only the seed path takes entry then `lwmMu`. Never
acquire in the reverse direction. `reserve` releases the global lock before
taking the entry lock; keep it that way. No I/O is ever performed while
holding the global lock or `lwmMu`.

The cache walk takes the global lock and, for each entry:
- if `touched.Swap(false)` returned true: `stale.Store(true)` (invalidate);
- else `TryLock` the entry; on success delete it from the map, set
  `evicted = true`, unlock. On failure (an in-flight reservation, which
  will set `touched`), `stale.Store(true)` and move on.

The walk never blocks on an entry lock, so a slow insert for one timeline
cannot stall reconcile for others, and it does no I/O under the global
lock.

**Reconcile is not atomic.** Between `refreshLWM` publishing a new floor
and the walk marking an entry stale, a reserve for an already-seeded entry
still uses its old base. The new floor and re-seed take effect at each
entry's first reserve after the walk. An entry is evicted on the second
consecutive walk that finds it untouched, so an idle entry lives up to two
reconcile intervals plus walk time.

`reserve` must handle an entry evicted between its map lookup and its
entry lock: after `t.mu.Lock()`, if `t.evicted`, unlock and redo the
lookup (which creates a fresh entry). Without this, a goroutine stamping
on an orphaned entry and another stamping on its replacement would be two
writers for one timeline inside one process. Factor the
lookup-lock-retry loop into `lockTimeline(key) *tenantVtime`, and give
`vtimeCache` an unexported test seam `afterLookup func(key timelineKey)`
(nil in production) called between the map lookup and `t.mu.Lock()`, so a
unit test can run an eviction at exactly that point.

### LWM state

The LWM lives in `vtimeCache`, beside the seed hook it feeds:
- `lwmHook func(ctx, partition string) (lwm int64, ok bool, err error)` --
  wired by `New` to `q.computeLWM`; `ok == false` means no visible pending
  job in that partition. Nil means LWM is always 0 (pure-unit
  configuration, as with `seedHook`).
- `lwm map[string]int64` guarded by a dedicated `lwmMu` (not the global
  map lock). A missing key reads as 0.
- `refreshLWM(ctx, partition) error`: call the hook *without* holding
  `lwmMu`, then under it store the value when `ok` (plain overwrite, no
  max); `ok == false` leaves the stored value. A hook error is returned
  and nothing is stored.
- `lwmFor(partition) int64`: read under `lwmMu`.

`reserve`'s seed branch distinguishes the two seed paths:
- **Cold seed** (entry just created, `seeded == false` and never set):
  `refreshLWM(ctx, partition)` first; an error fails the reserve exactly
  like a seed error, and the entry stays unseeded so the next reserve
  retries. Then seed hook, then `vtime = max(edge, lwmFor(partition))`.
- **Re-seed** (`stale` applied): seed hook, then
  `vtime = max(edge, lwmFor(partition))`. No frontier query.

Put the `max` in `reserve`'s seed branch (seed hook result vs `lwmFor`),
so the seed hook stays the plain trailing-edge query and both are
unit-testable with fakes. Between reconciles the LWM does not change and
every entry re-seeds after a reconcile, so a floor at seed time is
equivalent to a floor on every reserve; do not add a second floor in
`reserve`.

### Seed reads from the primary

`seedTenantVtime` currently reads through `q.coll`, which inherits the
caller's read preference. A secondary read can return a trailing edge older
than a stamp this node issued, and periodic re-seeding makes that a
recurring risk rather than a one-time cold-start one. Switch it (and the
frontier query) to `q.primary`. This is the only read-side change to the
seed query besides the `partition` filter 6.1 adds.

Primary routing is necessary but not sufficient: with `w: 1` writes and a
collection read concern of `"majority"`, an acknowledged insert can be
missing from the majority snapshot the seed reads, and the re-seed reissues
its stamp. Per AGENTS.md ("documented, not defended"), add this to `New`'s
godoc and the mongodb design doc as a caller requirement -- seed reads must
observe the queue's own acknowledged inserts (e.g. default `"local"` read
concern, or `w: "majority"` with `"majority"` reads). Do not detect or
override read concern.

### Known effect: dispatched top-of-backlog lowers the edge

Within one (tenant, partition) timeline, the highest-vstamp job can
dispatch before lower ones only when the lower ones are delayed or leased.
Once it resolves, the trailing edge drops below it, and the timeline's
next job can be stamped *below* an already-dispatched job. This is a
small, bounded over-service the trailing-edge design accepts, and it is
confined to one partition now that timelines are per partition. Do not
write a test asserting that new stamps exceed every stamp the timeline
ever had; the invariant is **a new stamp exceeds every pending stamp on
its timeline**.

### Expected changes to existing tests

Per-partition timelines (6.1) and cold-seed LWM refresh (6.3) change the
expected values of some Phase 5 tests. These changes are the behavior the
design now specifies; make them exactly as listed, and update each test's
comment to state the rule it now proves. **Any existing test not listed
here that fails after a subphase is a regression. Stop and report it; do
not edit its expectations.**

After 6.1 (timelines per partition; no LWM yet):
- `TestFairnessPartitionScoped`: its stated purpose inverts. It now proves
  a tenant's timeline is per partition: `a-004` in `p2` seeds fresh at
  `1e6`, not at a's `p1` vtime. Expected `p2` order `[a-004, b-000]`
  (tie at `1e6`, `a-` first); `p1` order `[a-000, b-001, a-001, a-002,
  a-003]` (`b-001` seeds fresh at `1e6`, ties `a-000`). 6.3 changes `p1`
  again (below).
- `TestEnsureIndexesCreatesClaimAndSeedIndexes`: seed index keys become
  `{tenant: 1, partition: 1, liveness: 1, vstamp: -1}`.
- `TestColdStartSeed`: `seedRecord` gains `Partition` (the seed filter
  names it) and the seed call passes the partition. Assertions unchanged.
- `vtimecache_test.go`: `reserve` and the fake `seedHook` take a
  partition; assertions unchanged.

After 6.3 (cold seeds refresh the partition's frontier):
- `TestFairnessFloodingTenant`: b's cold seed finds a's frontier at `1e6`,
  so `b-00k` is stamped `(k+2)e6` and ties `a-(k+1)`. Expected order:
  `a-000`, then for `i = 1..49`: `a-i` followed by `b-(i-1)` while
  `i <= 5`. The comment keeps its claim (B interleaves from its first
  job) and adds that B joins one stride past the service point.
- `TestFairnessWeightTwoToOne`: b's cold seed finds a's frontier at
  `0.5e6`, so `b-k` is stamped `(k+1.5)e6`. Both tenants have work
  through the first 44 claims; every `3k` prefix with `3k <= 42` holds
  exactly `2k+1` A jobs (A's extra one is the half-stride head start the
  floor gives the incumbent). Keep `expectedOrder` for the full sequence
  with b planned from base `0.5e6`.
- `TestFairnessCheapVsExpensive`: b's cold seed finds a's frontier at
  `1e6`, so B is stamped `5e6, 9e6, 13e6, 17e6, 21e6`. Give `planTenant` a
  base-vstamp parameter (or a sibling helper) rather than hand-writing the
  list.
- `TestFairnessPartitionScoped`: `b-001`'s cold seed in `p1` finds the
  frontier `1e6`, so `b-001 = 2e6` and `p1` is `[a-000, a-001, b-001,
  a-002, a-003]`; `b-000`'s cold seed in `p2` finds `a-004` at `1e6`, so
  `b-000 = 2e6` and `p2` stays `[a-004, b-000]`.
- `TestRaceFairnessConcurrentTenants`: with concurrent cold seeds the
  second tenant's floor depends on the race, so the order is no longer
  fixed. Enqueue `a-000` then `b-000` sequentially before starting the
  goroutines (`b-000 = 2e6`), then race `a-001..a-049` and
  `b-001..b-049`. Expected order: `a-000`, then `a-k, b-(k-1)` for
  `k = 1..49`, then `b-049`.
- `TestColdStartSeed`: switch fixtures to full `Job` records via
  `insertJob` with `Partition: defaultPartition` and `VisibleAt` an hour
  in the future, so the frontier query deliberately finds nothing
  (`ok == false`, LWM 0) and the test keeps asserting the seed alone.
  Assertions unchanged.
- `TestFairnessSingleTenant`, `TestFairnessSameTenantCostNoReorder`: one
  tenant into an empty partition; unchanged.

---

## 6.1 Per-(tenant, partition) Timelines

Cache key, seed query, and seed index move to (tenant, partition). No
reconcile, no LWM yet.

### 6.1.1 Cache key (`vtimecache.go`)
- [x] `timelineKey{tenant, partition string}`; `tenants map[string]` →
  `map[timelineKey]*tenantVtime`; `reserve(ctx, tenant, partition, cost,
  weight)`; `seedHook(ctx, tenant, partition)`
- [x] `enqueue` passes `n.partition` (the normalized value that is stored)
- [x] Update the `vtimeCache` doc comment: timelines are per (tenant,
  partition); the strictly-increasing invariant is per timeline
- [x] **Test** (unit): one tenant, two partitions -- reserve+commit twice
  in each; vstamps are `1,2` in both partitions (independent timelines);
  the fake seed hook is called once per (tenant, partition)
- [x] **Test**: existing `vtimecache_test.go` tests pass with the new
  signatures

### 6.1.2 Seed query and index (`queue.go`)
- [x] `seedTenantVtime(ctx, tenant, partition)`: filter adds `partition`;
  read via `q.primary`; doc comment rewritten (drop "Phase 6 supersedes
  it"; describe the re-seed cycle to come, and the read-concern caveat)
- [x] Seed index becomes `{tenant: 1, partition: 1, liveness: 1,
  vstamp: -1}` in `EnsureIndexes` and its doc comment
- [x] Add the read-concern caller requirement to `New`'s godoc (6.0)
- [x] **Test**: `TestColdStartSeed` -- fixtures gain `Partition`; add a
  pending record for the same tenant in another partition at a higher
  vstamp and assert it does not raise the seed
- [x] **Test**: `TestEnsureIndexesCreatesClaimAndSeedIndexes` expected
  keys updated; explain check that the seed query uses the new index
  (`IXSCAN`, no blocking `SORT`), recorded in the mongodb design doc
- [x] **Test**: `TestFairnessPartitionScoped` rewritten per 6.0 (6.1
  values); comment states the rule it now proves
- [x] **Doc**: mongodb design doc -- seed index keys and the
  per-(tenant, partition) seed; generic design doc -- confirm the 6.0 D1
  wording landed in "Core model", "Two orthogonal axes", "Node-local cache
  model", and "The conserved single-writer requirement" (the drafts were
  written with D1; fix anything the implementation contradicts); AGENTS.md
  "Node-local cache" bullet

---

## 6.2 Reconcile: invalidate, evict, re-seed

No LWM yet: `reconcile` skips the LWM step until 6.3 adds it, and the
seed is still `max pending or 0`.

### 6.2.1 Cache mechanics (`vtimecache.go`)
- [x] Add `touched`, `stale` (both `atomic.Bool`) and `evicted` to the
  entry; `reserve`, holding the entry lock, sets `touched` and applies
  `stale` to `seeded` per 6.0
- [x] Factor lookup-lock-retry into `lockTimeline(key) *tenantVtime` with
  the `afterLookup` test seam (6.0); `reserve` uses it
- [x] `(*vtimeCache).invalidateAndEvict()` implementing the walk in 6.0
  (no I/O, never blocks on an entry lock, returns nothing)
- [x] Update the `vtimeCache` and `seedHook` doc comments: describe the
  re-seed-after-reconcile cycle, the two-walk eviction lifetime, and the
  lock order
- [x] **Test**: touched entry is invalidated -- reserve+commit, walk, next
  reserve calls `seedHook` again and stamps from the new seed (fake hook
  returning a different value the second time)
- [x] **Test**: untouched entry is evicted -- reserve+commit, walk (clears
  `touched`, marks stale), walk again: entry gone from the map; next
  reserve creates a fresh entry and seeds
- [x] **Test**: busy entry is not evicted, and the walk does not block --
  reserve+commit, walk (clears `touched`). Then, from one goroutine, call
  `lockTimeline` directly and hold the entry lock without setting
  `touched` (simulating the window before `reserve` sets it). Run a walk
  on the test goroutine: it returns while the lock is still held, the
  entry is still in the map, and `stale` is set. Release the lock; the
  next reserve re-seeds. Order steps with channels, not sleeps.
- [x] **Test**: evicted-entry retry -- set `afterLookup` to run a walk that
  evicts the entry (pre-clear `touched` with an earlier walk); assert the
  reserve lands on a fresh map entry (seed hook called again), the orphan
  has `evicted == true`, and the resulting vstamp comes from the new seed,
  not the orphan's vtime
- [x] **Test**: race -- 2 goroutines per timeline × 2 timelines × 100
  reserve+commit each, while a fifth goroutine calls the walk in a loop.
  The fake seed hook returns the timeline's current durable maximum, which
  the test tracks under a mutex and updates **after reserve and before
  Commit** (mirroring insert-before-commit). Assert each timeline's
  committed vstamps are exactly `{1..200}×stride`, no duplicates. Log
  eviction and invalidation counts.

### 6.2.2 Queue-level reconcile (`queue.go`)
- [x] Unexported `(q *Queue) reconcile(ctx) error`: calls
  `q.vtimes.invalidateAndEvict()` and returns nil (6.3 adds the LWM step
  ahead of it)
- [x] **Test**: trailing cancellation is refunded. Isolate the tenants in
  their own partitions so the values survive 6.3's cold-seed refresh:
  (1) tenant `b` in partition `pb` enqueues `b-000`..`b-002`
  (`1e6..3e6`), `Cancel(b-002)`, enqueue `b-003`: `4e6` (no reconcile;
  cached vtime is unaffected by cancel). (2) tenant `a` in partition `pa`
  enqueues `a-000`..`a-002` (`1e6..3e6`), `Cancel(a-002)`, `reconcile`,
  enqueue `a-003`: `3e6` (edge `2e6` + stride). Under 6.3 the frontier of
  `pa` at reconcile is `1e6`, below `a`'s edge, so the value holds.
- [x] **Test**: external-writer drift is corrected -- enqueue `a-000`
  (`1e6`), `insertJob` a full pending `Job` for tenant `a` at vstamp
  `50e6` (`Partition: defaultPartition`, another writer's stamp),
  `reconcile`, enqueue: `51e6`. A second `insertJob` for `a` at `90e6` in
  partition `other` must not affect the default-partition stamp.
- [x] **Test**: idle timeline is evicted end to end -- enqueue for tenant
  `a`, `reconcile` twice with no enqueue between; `a`'s key is absent from
  `q.vtimes.tenants`
- [x] **Doc**: design doc "Reconciliation" -- confirm the lazy-invalidation
  paragraph (D2) and the eviction rule match the implementation; mongodb
  doc open question #2 -- record the answer (no new index; seed index
  serves the trailing edge; frontier access path pending 6.3)

---

## 6.3 LWM: Per-Partition Frontier and Idle Floor

### 6.3.1 Frontier query (`queue.go`)
- [x] `(q *Queue) computeLWM(ctx, partition string) (int64, bool, error)`
  reading `q.primary`: one `FindOne` with the **claim filter**
  (`partition`, `liveness: pending`, `$expr: {$lte: ["$visible_at",
  "$$NOW"]}`), sort `{vstamp: 1, _id: 1}`, projection `{vstamp: 1}`;
  `ok = false` on `ErrNoDocuments`
- [x] Explain check, recorded in the mongodb design doc next to the claim
  explain check: expect the claim plan (`IXSCAN` on the claim index, no
  blocking `SORT`, one document fetched). Include keys examined with a
  leased prefix, as the claim check did, and for a partition whose pending
  jobs are all invisible (the scan walks the partition's whole pending
  range in the index and returns nothing).
- [x] **Stop rule**: if the plan is a `COLLSCAN` or needs a blocking
  `SORT`, stop and report the explain output. Do not add an index without
  user approval -- an index on the enqueue/resolve write path is a
  write-amplification decision (open question #2).
- [x] **Test**: `computeLWM` -- partition `p1` with visible pending jobs
  at `7e6` and `4e6`; a leased job at `1e6` (`insertJob` with a `ClaimID`
  and `VisibleAt` a minute ahead -- the stored shape of a lease), a
  delayed job at `2e6` (`VisibleAt` an hour ahead), and a resolved job at
  `3e6` are all excluded; a visible job at `1e6` in partition `p2` is
  ignored: result for `p1` is `(4e6, true)`, for `p2` `(1e6, true)`, for
  an empty partition `(0, false)`. All fixtures are full `Job` records
  with an explicit `Partition`.

### 6.3.2 Floor in the cache (`vtimecache.go`, `queue.go`)
- [ ] Add `lwmHook`, `lwm`, `lwmMu`, `refreshLWM`, `lwmFor` per 6.0; `New`
  wires `lwmHook = q.computeLWM`
- [ ] `reserve` seed branch: cold seed refreshes the partition's LWM first;
  both seed paths take `max(edge, lwmFor(partition))` (6.0 "LWM state")
- [ ] `reconcile` snapshots the set of partitions present in the cache
  (under the global lock, no I/O), calls `refreshLWM` for each, and on
  the first error returns it wrapped and skips the walk
- [ ] Rework `TestColdStartSeed` fixtures per 6.0 (assertions unchanged)
- [ ] Update the Phase 5 tests listed under 6.0 "After 6.3", exactly as
  listed, with comments stating the rule each now proves
- [ ] **Test** (unit): cold seed refreshes -- first reserve for `(a, p1)`
  calls `lwmHook("p1")` once; first reserve for `(b, p1)` calls it again;
  first reserve for `(a, p2)` calls `lwmHook("p2")`; a re-seed after a
  walk does not call `lwmHook`
- [ ] **Test** (unit): cold-seed hook failure fails the reserve and is not
  memoized -- hook fails once, reserve fails, next reserve retries and
  succeeds
- [ ] **Test** (unit): tracks the frontier, no hold -- `refreshLWM("p")`
  with the hook returning `10e6`, then `6e6`, then `(0, false)`: `lwmFor`
  reads `10e6`, `6e6`, `6e6`
- [ ] **Test** (unit): seed takes the max -- seed hook `3e6`, LWM `10e6`
  → first vstamp `10e6 + stride`; seed hook `12e6`, LWM `10e6` →
  `12e6 + stride`
- [ ] **Test** (unit): failed refresh leaves the cache untouched -- touched
  entry stays seeded after a `reconcile` whose `lwmHook` fails for one of
  two partitions
- [ ] **Test**: idle tenant resumes at the frontier, not stale vtime and not
  buried -- one Queue, default partition. Tenant `a` enqueues
  `a-000`..`a-002` (`1e6..3e6`); claim and complete all three (`a`
  drained, cached vtime `3e6`). Tenant `b` enqueues `b-000`..`b-019`: its
  cold seed finds no visible job (`a` is drained), so `b` is stamped
  `1e6..20e6`; claim and complete `b-000`..`b-009`. Frontier is now
  `11e6`. `reconcile`. Tenant `a` enqueues `a-003`: vstamp `12e6`. Drain
  with `drainOrder`: `b-010, a-003, b-011, ..., b-019` (`a-003` ties
  `b-011` at `12e6`; `a-` < `b-`). Stale credit would stamp `a-003` at
  `4e6`; the trailing-edge LWM would stamp it at `21e6`.
- [ ] **Test**: returning tenant that floods interleaves at its share
  (memoryless) -- same setup through `reconcile`, then `a` enqueues
  `a-003`..`a-022` (`12e6..31e6`). Drain: `b-010`, then `a-003, b-011,
  a-004, b-012, ..., a-011, b-019`, then `a-012`..`a-022`. `b` drains at
  claim 19, `a` at claim 30. Comment: the floor sets where `a` starts; the
  stride sets its share.
- [ ] **Test**: cold node floors a new tenant -- Queue 1 enqueues `b-000`
  ..`b-019` and claims+completes 10; Queue 2 (fresh, same collection,
  Queue 1 discarded) enqueues new tenant `c`: vstamp `12e6` (its cold seed
  refreshes the frontier; no `reconcile` is called on Queue 2)
- [ ] **Test**: lanes are independent -- tenant `b` enqueues 20 jobs in
  partition `slow` (none claimed; frontier `1e6`) and 20 in partition
  `fast` (claim+complete 10; frontier `11e6`). Tenant `c` enqueues one job
  in `fast`: `12e6`; then one in `slow`: `2e6`. A shared timeline would
  have stamped the `slow` job `13e6`.
- [ ] **Test**: a pending job in one lane does not refund consumption in
  another -- `insertJob` pending visible jobs for tenant `b` in `fast` at
  `11e6..20e6`. Tenant `a` enqueues `a-slow` in `slow` (`1e6`) and
  `a-f00` in `fast` (cold seed finds frontier `11e6`: `12e6`). Claim
  `fast` twice (`b-010`, `a-f00`) and complete both. `reconcile`. `a`
  enqueues `a-f01` in `fast`: `13e6` (edge none, LWM `12e6`). Drain
  `fast`: `b-011, a-f01, b-012, ..., b-019`. Under a shared timeline the
  re-seed would have landed on `a-slow`'s `1e6` and stamped `a-f01` at
  `2e6`.
- [ ] **Test**: extend 6.2.1's race test with an `lwmHook` returning
  `(0, true)` and the walk loop calling `refreshLWM` for both partitions
  before each walk; contiguity `{1..200}×stride` must still hold. (A
  nonzero floor would legitimately shift the first stamp, so use 0.)
- [ ] **Doc**: design doc "Low-water mark and the idle floor" and
  "Sparse-tenant responsiveness" -- confirm the D1 wording (frontier per
  partition, no hold, cold-seed refresh plus reconcile) matches the
  implementation and add the worked example from the idle-tenant test;
  mongodb doc open question #2 -- the frontier access path, explain
  results
- [ ] **Doc audit**: grep both design docs for "low-water", "LWM",
  "floor", "pending set", "trailing edge", and "per-tenant" and fix every
  statement the per-partition, memoryless definition contradicts

---

## 6.4 Public Surface and Docs

- [ ] Rename `reconcile` to exported `Reconcile(ctx) error`; update tests.
  Godoc: what it does (refresh each cached partition's frontier,
  invalidate touched entries, evict idle ones), that callers run it on a
  ticker, that after a successful call each timeline re-seeds before its
  next stamp, that an idle entry is evicted after two consecutive
  untouched passes (so up to two intervals), that an error leaves the
  cache as it was, that new and evicted tenants are floored correctly
  even without it (cold-seed refresh) but active timelines' drift is only
  corrected by it, and that concurrent calls are safe
- [ ] `ExampleQueue_Reconcile` in `example_test.go`: ticker loop in a
  goroutine with ctx cancellation. No `// Output:` comment, so it compiles
  but does not run (it needs a server)
- [ ] Package godoc (`doc.go`): one paragraph naming `Reconcile` as a
  caller obligation alongside `EnsureIndexes`; one sentence that fairness
  is per partition and memoryless
- [ ] **Test**: concurrent `Reconcile` -- 4 goroutines call `Reconcile`
  in a loop while 2 goroutines enqueue for one tenant in one partition;
  under `-race`, assert no error and the tenant's stamps are exactly
  `{1..N}×1e6`. The example compiles under `go vet`; other behavior is
  covered by 6.1-6.3
- [ ] **Doc**: mongodb doc -- close open questions #2 and #3 (move the
  answers into the body, leave a one-line pointer); design doc "Why a
  cache, and what it costs" -- staleness bound is the caller's reconcile
  interval; implementation plan Phase 6 -- note the subphase reshaping
  (per-partition timelines added; lazy invalidation replaced eager
  rebuild; "rebuild on startup" is the lazy seed plus cold-seed frontier
  refresh)

---

## Future Phases (Deferred)

- Multi-writer detection (per-timeline distinct `stamped_by` count during
  reconcile) -- the `Reconcile` pass is its natural home; not in v1
- Internal reconcile goroutine or `RunReconciler` helper -- add only if
  callers ask; D3 keeps `New` goroutine-free
- Batched re-seed (one `$in` query for many invalidated timelines) -- only
  if per-timeline re-seed latency shows up in enqueue measurements
- Backlog reconstruction (LWM + sum of pending strides) -- rejected in the
  design in favor of trailing edge; revisit only with multi-writer
- Bounded idle credit (`frontier - k strides`, CFS-style sleeper bonus) --
  rejected for v1 (D1); revisit only with evidence that sparse tenants
  need more than one-stride-from-the-front latency
- Weight-change transition behavior -- documented, not tested
