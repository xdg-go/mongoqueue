# TODO: Phase 5 -- Fairness and Contention Verification

Expanded from [docs/implementation-plan.md](docs/implementation-plan.md)
Phase 5. No new queue mechanism: multi-tenant dispatch-order tests that prove
the fairness properties the design promises, and a claim-throughput
benchmark measured against the stated "low hundreds of claims/sec per
partition" target. Design references: "Scheduling model" and "Two orthogonal
axes" in [docs/fair-job-queue-design.md](docs/fair-job-queue-design.md);
"Indexes, contention, and GC" in
[docs/mongodb-go-library-design.md](docs/mongodb-go-library-design.md).

Section 5.0 holds the contracts every subphase relies on. Read it before
starting any subphase; do not improvise around it.

## Testing Philosophy

- **Integration tests against real MongoDB**: dispatch order is the order
  `Claim`'s `FindOneAndUpdate` returns; mocks would test nothing. Tests
  connect via `internal/mongotest` (`MONGOQUEUE_TEST_URI`, default
  `mongodb://localhost:27017`).
- **Hermetic tests**: each test uses a uniquely named database/collection
  (random suffix) so parallel runs never collide; drop on cleanup.
- **Exact assertions, not ratios**: vstamps are deterministic integer
  arithmetic and ties break by `_id`, so a sequenced enqueue-then-claim
  test has exactly one correct dispatch order. Assert the exact sequence
  (or exact per-tenant counts over a prefix). Never assert "approximately
  2:1" -- an approximate assertion would hide an off-by-one stride bug.
- **Concurrency tests**: every outcome a race test accepts also has a
  deterministic, sequenced test that forces it. Race tests run under
  `-race`, log the outcome distribution, and assert only the invariant.
- **Control time through data, not a clock**: no sleeps. Fairness tests
  enqueue every job before the first claim and use one-minute leases
  (`mustClaim`), so claimed jobs stay invisible for the whole test.
- **Benchmarks are measurement, not tests**: they do not run under
  `make test` and assert nothing about speed. Their numbers go in the
  design doc.

Test what new code changes. Phase 5 adds no production code paths;
enqueue validation, stamping arithmetic, claim ordering by `(vstamp, _id)`,
and single-partition scoping are covered by Phases 2–4. Use them as
fixtures; don't retest them.

## Verification Checklist

Before marking a subphase complete and committing it:

1. `go build ./...` and `make vet` pass
2. `make test` passes (including integration tests against Mongo; run the
   concurrency tests with `-race`)
3. `make lint` clean (config in `.golangci.yml`)
4. New exported identifiers have godoc comments (none expected this phase)
5. Design docs updated if implementation forced a decision the docs left open

When verification of a subphase is complete, commit all relevant
newly-created and modified files as one logical unit.

## Dependencies Between Subphases

```
5.0 Contracts (read first)
       │
       ├─► 5.1 Fairness behavior tests
       │
       └─► 5.2 Contention: cross-partition race + claim benchmark
```

5.1 and 5.2 are independent; either may land first.

---

## 5.0 Contracts

### Starting vtime: all tenants enqueue before any claim
Every tenant in a fairness test is **new to the queue**: it has no
pending records, so `seedTenantVtime` seeds its vtime at 0, and its k-th
job (1-based) gets vstamp `Σ stride(cost_i, weight_i)` over its first k
enqueues -- `k * stride` when cost and weight are constant. Phase 5 has no
low-water-mark floor (that is Phase 6): a tenant first enqueued *after*
others have advanced still seeds at 0 and may jump ahead of their backlog.
Therefore:
- Enqueue all jobs for all tenants before the first Claim.
- Do not write a test that depends on a late-arriving or idle-returning
  tenant being floored. That behavior does not exist yet; it belongs to
  Phase 6.2.

### Fixed-point arithmetic for expected orders
`stride(cost, weight) = max(1, cost*1_000_000/weight)`. Compute expected
dispatch orders from this formula in the test (or as a literal list derived
from it), not from intuition. Worked example, equal cost 1:
- weight 2: vstamps 500_000, 1_000_000, 1_500_000, 2_000_000, ...
- weight 1: vstamps 1_000_000, 2_000_000, ...

Every weight-1 vstamp **ties** a weight-2 vstamp. Ties break by `_id`
ascending (byte-wise string compare under the simple collation). Choose job
ids so the tie order is intended and obvious, e.g. prefix tenant A's ids
`a-` and tenant B's `b-` and zero-pad sequence numbers (`a-007`, not
`a-7`, which sorts after `a-10`).

### Cheap-jobs-first is cross-tenant only
Within one tenant, vstamps strictly increase in enqueue order regardless
of cost: a cheap job enqueued after an expensive one on the **same** tenant
still dispatches after it. "Cheap jobs sort earlier" means a tenant
submitting cheap jobs gets more of them dispatched per unit of virtual
time than a peer submitting expensive ones. Do not write a test expecting
intra-tenant reordering by cost.

### Fixtures
- Enqueue through `q.enqueue` via the existing `enqueueJob` helper
  (claim_test.go), never `insertJob` -- the point is to test stamps the
  enqueue path assigns.
- Claim with a one-minute lease and never Complete in 5.1 tests: leased
  jobs stay invisible, so successive claims read out the dispatch order.
  `drainOrder` (5.1) calls `q.Claim` directly, because `mustClaim` fails
  the test on every error including the expected terminal `ErrNoJob`.
- One `Queue` per test. Two `Queue` handles on the same collection would
  each hold their own vtime cache and violate single-writer-per-tenant.
- No `EnsureIndexes` needed in 5.1 (tiny collections); 5.2's benchmark
  **must** call it.

### Benchmark harness
- `mongotest.Connect` takes `*testing.T`. Widen it to `testing.TB` so a
  benchmark can use it; this is the only change to `internal/mongotest`.
  Check that `randomHex` and any other helpers take `testing.TB` too.
- Add `make bench`. It must not be a dependency of `all` or `test`, and CI
  does not run it.

---

## 5.1 Fairness behavior

New file `fairness_test.go`. Every test here is sequenced: enqueue all,
then claim one at a time until `ErrNoJob`, recording `(TenantID, ID)` per
claim.

- [x] Helper `drainOrder(ctx, t, q, partition) []string` -- calls
  `q.Claim(ctx, partition, time.Minute)` until `ErrNoJob`, returns job ids
  in dispatch order; fails on any other error. Do not build it on
  `mustClaim`.
- [x] **Test**: equal weight, flooding tenant -- tenant A enqueues 50 jobs
  (`a-000`..`a-049`), then tenant B enqueues 5 (`b-000`..`b-004`), all
  cost 1 weight 1. Expected order: `a-000, b-000, a-001, b-001, ...,
  a-004, b-004, a-005, ..., a-049` (ties at each `k * 1e6` break by `_id`,
  `a-` < `b-`). Assert the exact sequence. This proves B is not queued
  behind A's backlog despite enqueuing last.
- [x] **Test**: weight 2 vs 1, equal cost -- tenant A weight 2 and tenant
  B weight 1 each enqueue 30 jobs. Assert the exact dispatch sequence
  derived from 5.0's arithmetic, and additionally that every prefix of
  length `3k` (while both tenants have work) contains exactly `2k` A jobs
  and `k` B jobs.
- [x] **Test**: cheap vs expensive, equal weight -- tenant A enqueues 20
  jobs at cost 1, tenant B enqueues 5 jobs at cost 4. Assert exact order:
  A's vstamps are `1e6..20e6`, B's are `4e6, 8e6, ...`; B's jobs land
  after A's 4th, 8th, ... (tie at `4e6` breaks `a-003` before `b-000`).
- [x] **Test**: same-tenant cost does not reorder -- one tenant enqueues
  cost 10, then cost 1, then cost 5; dispatch is enqueue order. This guards
  the 5.0 "cross-tenant only" rule.
- [x] **Test**: single-tenant degenerate case -- `TenantID` empty for all
  jobs, mixed costs and default weight; dispatch is FIFO by enqueue order,
  and the stored vstamps equal the running sum of strides (the vstamp is an
  enqueue sequence). No special-casing in code to verify; the test shows
  the shared path produces FIFO.
- [x] **Test**: partition-scoped ordering over shared tenant timelines --
  vtime is per tenant, not per partition, so a tenant's stamps advance
  across every partition it enqueues into. All cost 1, weight 1, enqueued
  in this exact order: A enqueues `a-000`..`a-003` into `p1`, then `a-004`
  into `p2`; then B enqueues `b-000` into `p2`, then `b-001` into `p1`.
  Vstamps (×1e6): a-000..a-004 = 1..5; b-000 = 1, b-001 = 2. Expected
  drains: `p1 = [a-000, a-001, b-001, a-002, a-003]`,
  `p2 = [b-000, a-004]`. The fixture is deliberately asymmetric: a
  per-partition timeline would stamp `a-004` at 1 and put it before
  `b-000`. This shows ordering within a partition uses the shared tenant
  timeline; it does not claim independent proportional shares per
  partition.
- [x] **Test**: race -- two tenants enqueue concurrently (one goroutine
  per tenant, ids `a-000`..`a-049` and `b-000`..`b-049`, cost 1, weight
  1), then drain. Per-tenant vstamps depend only on each tenant's own
  enqueue count, so the order is exactly `a-000, b-000, a-001, b-001, ...,
  a-049, b-049` no matter how the enqueues interleaved. Assert it. This
  exercises cross-tenant cache locking under real interleaving; the
  flooding test above is the sequenced test of the same ordering rule.

## 5.2 Contention

### 5.2.1 Cross-partition claims under concurrency
`TestClaimPartitionScoping` (Phase 4.1) already proves sequenced scoping.
This adds only the race counterpart.

- [x] **Test**: race -- seed partitions `p1` and `p2` with M jobs each
  (job ids encode the partition, e.g. `p1-000`); run N goroutines per
  partition (N > M), each claiming once from its partition with a
  one-minute lease. Assert every returned job's `Partition` matches the
  claimer's partition, exactly M successes per partition with distinct ids,
  and `N−M` `ErrNoJob` per partition. Use `raceCtx`.

### 5.2.2 Claim throughput benchmark
New file `claim_bench_test.go`.

**What is measured:** a full dispatch cycle -- `Claim` then `Complete` --
per job, with G concurrent workers on one partition. Complete is included
deliberately: a claim-only loop leaves every claimed job pending and leased,
so the claim scan walks a leased prefix that grows with b.N (see the explain
check in the design doc) and later claims slow for an artifactual reason.
Real workers resolve jobs, which removes them from the `liveness: pending`
index range.

- [ ] Widen `mongotest.Connect` (and helpers) to `testing.TB` per 5.0
- [ ] `BenchmarkClaimComplete` with sub-benchmarks for G = 1, 4, 16, 64
  workers (`b.Run(fmt.Sprintf("workers=%d", g), ...)`)
- [ ] Per sub-benchmark: fresh database via `mongotest.Connect(b)`;
  `EnsureIndexes`; seed b.N pending jobs with `InsertMany` in batches,
  bypassing enqueue. Insert full `Job` structs, not hand-built documents --
  direct inserts skip enqueue's normalization, and missing fields break
  claim: unique string `ID` (e.g. `fmt.Sprintf("j-%07d", i)`; an omitted
  `_id` becomes an ObjectID that cannot decode into `Job.ID`),
  `Partition: defaultPartition` (empty does not normalize here),
  `Liveness: LivenessPending`, `VStamp: int64(i+1) * scale`, `VisibleAt`
  an hour in the past, `Attempts: 0` (the claim pipeline's `$add` yields
  null on a missing field), `Kind: "bench"`, and an empty-document `Body`.
  Run one warm-up `Ping` so connection setup is outside the timer, then
  `b.ResetTimer()`
- [ ] Workers reserve work with an atomic **ticket** taken *before* each
  Claim: `if tickets.Add(1) > int64(b.N) { return }`, then
  Claim(1-minute lease) → `cj.Complete(ctx, ResolutionCompleted)`. Counting
  completions instead would let a worker Claim while the last jobs are
  leased but not yet completed, and hit `ErrNoJob` spuriously. With
  tickets, `ErrNoJob` is a real failure. Do not call `b.Fatal` from worker
  goroutines -- record the first error, wait for all workers, then fail
  from the main goroutine. After the wait, `b.StopTimer()` and verify
  completed cycles == b.N.
- [ ] `b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "cycles/s")` --
  label it cycles (Claim+Complete), not claims
- [ ] `make bench`: `go test -run '^$$' -bench BenchmarkClaimComplete
  -benchtime 2000x .` (fixed iteration count, so seeding cost is bounded
  and runs are comparable). Not part of `all`.
- [ ] Run it three times against a local `mongod` and record the median
  per G in the design doc's "Indexes, contention, and GC" section, next to
  the explain check: date, MongoDB version, topology (standalone vs
  replica set), write concern, host hardware summary, and a table of
  cycles/s per G. State what it measures: Claim+Complete cycles with
  immediate completion, so the leased prefix stays near G entries. It
  does not measure claims against a large in-flight leased prefix (the
  explain check covers that scan cost).
- [ ] **Stop rule**: the target is "low hundreds" of cycles/s per
  partition. If any G ≥ 4 median is below 100 cycles/s, stop and report
  the numbers to the user. Do not attribute it to min-document contention
  without evidence (compare against G = 1; a low G = 1 number points at
  round-trip latency, not contention). Do not implement
  bounded-random-skip or any other mitigation in this phase.

---

## Future Phases (Deferred)

- Late-arriving / idle-returning tenant fairness (LWM floor) -- Phase 6.2
- Bounded-random-skip claim among top-K visible -- only if 5.2.2 misses
  the target and the user approves
- Benchmark in CI -- noisy shared runners make numbers meaningless; local
  measurement only
- Multi-node enqueue fairness (multiple `Queue` instances per tenant) --
  violates single-writer; two-stage stamp partitioning territory
- Weight-change transition behavior (old stamps drain at the old rate) --
  documented in the design, not tested here
