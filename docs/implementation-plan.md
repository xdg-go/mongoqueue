# mongoqueue Implementation Plan

Iterative build order for the fair multi-tenant job queue specified in
[fair-job-queue-design.md](fair-job-queue-design.md) and
[mongodb-go-library-design.md](mongodb-go-library-design.md). Each phase ends
with a working, tested slice; later phases layer on without rework.

## Testing Philosophy

- **Integration tests against real MongoDB**: claim atomicity, fencing, and
  visibility semantics depend on server behavior (`FindOneAndUpdate`, `$$NOW`,
  unique `_id`); mocks would test nothing. Tests connect to
  `MONGOQUEUE_TEST_URI`, defaulting to `mongodb://localhost:27017` -- covers
  both a local `mongod` and a CI service container with no special-casing.
- **Hermetic tests**: each test uses a uniquely named database/collection
  (random suffix) so parallel runs never collide; drop on cleanup.
- **Pure unit tests for arithmetic**: vstamp/stride fixed-point math, opts
  validation, and encoding round-trips are plain table-driven `testing` tests
  with no database.
- **Concurrency tests**: mutual exclusion under concurrent claimers is a core
  invariant; test with real goroutine races, not sequenced calls. Every
  outcome a race test accepts also has a deterministic, sequenced test that
  forces it: the race test proves the invariant holds under real
  interleaving, the sequenced test guarantees each branch runs. Race tests
  log the outcome distribution but do not assert it.
- **Control time through data, not a clock**: there is no mock clock --
  visibility compares against server `$$NOW`, which a client-side fake
  cannot move. Force expiry by writing `visible_at` into the past; test
  exclusion with visibility minutes in the future; use leases far longer
  than a race test's deadline. Never sleep. Bound server-time assertions
  with `$$NOW` samples taken before and after the operation, not the client
  clock.

## Verification Checklist

Before marking a phase complete and committing it:

1. `go build ./...` and `go vet ./...` pass
2. `go test ./...` passes (including integration tests against Mongo)
3. `golangci-lint run` clean (config in `.golangci.yml`)
4. New exported identifiers have godoc comments
5. Design docs updated if implementation forced a decision the docs left open

When verification of a phase or subphase is complete, commit all relevant
newly-created and modified files. Add Makefile targets for test/lint in
Phase 1 and use them thereafter.

## Dependencies Between Phases

```
Phase 1 (Scaffolding & test harness)
       │
       ▼
Phase 2 (Job record, lifecycle types, vstamp arithmetic)
       │
       ▼
Phase 3 (Enqueue path + typed facade)
       │
       ▼
Phase 4 (Claim, lease, resolution)   ◄── first usable queue
       │
       ├─► Phase 5 (Fairness & contention verification)
       │
       └─► Phase 6 (Reconciliation & cache lifecycle)
                 │
                 ▼
           Phase 7 (Ops hardening & docs)
```

Phases 5 and 6 are independent of each other; both need Phase 4.

---

## Phase 1: Scaffolding and Test Harness

Module skeleton, MongoDB test infrastructure, and the `Queue` handle with
caller-invoked index creation. No queue semantics yet.

### 1.1 Module and tooling
- `go mod init`; add `go.mongodb.org/mongo-driver/v2` dependency
- Makefile targets: `test`, `lint`, `vet`
- **Test**: CI-runnable `go test ./...` skeleton passes

### 1.2 Mongo test harness
- Test helper: connect to `MONGOQUEUE_TEST_URI` (default
  `mongodb://localhost:27017`); unique per-test database names; cleanup
- GitHub Actions workflow: `mongo` service container, sets
  `MONGOQUEUE_TEST_URI`, runs `make test` and `make lint`
- **Test**: harness smoke test -- insert/read a document in an isolated db

### 1.3 Queue construction and indexes
- `New(...)` / `Queue` type wrapping a `*mongo.Collection`; node id for
  `stamped_by`
- `EnsureIndexes(ctx)` -- claim index
  `{partition, liveness, vstamp, _id, visible_at}`
- `EnsureTTLIndex(ctx, retention)` -- TTL on `resolved_at`
- **Test**: index methods create expected indexes; nothing created on `New`

As built: `EnsureIndexes` creates three named partial indexes (claim
`{partition, vstamp, _id, visible_at}` and seed on pending jobs, clock on
fenced jobs; Phase 6.6), and the TTL index is named `mongoqueue_ttl` and
partial on resolved jobs (Phase 7.1). See the mongodb doc's "Indexes,
contention, and GC".

---

## Phase 2: Job Record and Virtual-Time Arithmetic

The durable shape and the pure math, testable without any queue behavior.
Resolves the Liveness/Resolution representation question, now recorded in the
design doc's "Liveness and Resolution representation" subsection -- it blocks
everything downstream.

### 2.1 Core types
- `Job` struct per the design doc (envelope + `Kind` + `Body bson.Raw`)
- Decide and document `Liveness` and `Resolution` Go types and BSON
  encoding; record the decision in the design doc
- Sentinel errors: `ErrDuplicateJob`, `ErrNoJob`, not-found for `Get`
- **Test**: BSON round-trip of `Job`; `Liveness`/`Resolution` encode as
  decided; `body` stores as a native subdocument queryable by field

### 2.2 Fixed-point virtual time
- Stride function: `max(1, cost*SCALE/weight)`, `SCALE = 1_000` (was
  `1_000_000` until Phase 6 review; see the design doc's SCALE trade-off);
  cost floor; weight zero-means-1 and `>= 1` validation; checked stamp
  arithmetic (`ErrVTimeOverflow`)
- **Test**: table-driven stride cases -- extreme cost/weight ratios,
  floor behavior, strictly-increasing-within-tenant property

---

## Phase 3: Enqueue Path and Typed Facade

Stamp-at-enqueue with the node-local vtime cache, idempotent insert, and the
generic API boundary. After this phase, jobs land in Mongo correctly stamped.

### 3.1 Vtime cache (in-memory)
- Per-tenant vtime map with mutex; advance-after-successful-insert only;
  cold-start seed (max pending vstamp for tenant, or LWM floor -- minimal
  version here, full reconciliation in Phase 6)
- **Test**: failed/duplicate insert advances nothing; concurrent enqueues
  for one tenant produce strictly increasing vstamps

As built: the cache is keyed per (tenant, partition) (Phase 6.1), and a seed
is `max(trailing edge, frontier)`, where the frontier is the partition's
service clock (max fenced vstamp, falling back to min pending), not a
min-visible LWM (Phases 6.3 and 6.6).

### 3.2 Enqueue core
- `EnqueueOpts` validation; insert keyed on caller `JobID`; duplicate key
  maps to `ErrDuplicateJob`; sets `visible_at`, `stamped_by`, initial
  liveness
- **Test**: idempotent retry returns sentinel, stored record wins;
  envelope fields land top-level, body opaque

### 3.3 Typed facade
- Per-kind binding `NewKind[T](kind)` returning `Kind[T]` with
  `Enqueue`/`Decode` methods (binding marshals `T`; no caller-built
  `bson.Raw`); `Decode` returns `ErrKindMismatch` on `kind`↔type drift
- **Test**: round-trip through facade; `kind`↔type drift caught on decode;
  heterogeneous kinds coexist in one collection

As built: nothing restricts `T`, so a `Kind[bson.Raw]` binding accepts a
caller-built body; `bson.Marshal` still rejects malformed framing, and such a
binding opts out of the `kind`↔type pairing (see `docs/rejected-designs.md`).

### 3.4 Amendments (post-review)
- `EnqueueOpts.Delay` -- delayed enqueue: `visible_at = now + Delay`, zero
  means immediately visible, negative rejected (`ErrInvalidDelay`)
- Seed index in `EnsureIndexes`: `{tenant, liveness, vstamp desc}` -- serves
  the Phase 3.1 cold-start seed query, which the claim index cannot

As built: the seed index is `{tenant, partition, vstamp desc}`, partial on
pending jobs, alongside the claim and clock indexes (Phase 6.6; see the
Phase 1.3 note).

---

## Phase 4: Claim, Lease, and Resolution

The lease primitive: atomic claim of the minimum visible `(vstamp, _id)`,
fenced writes, terminal resolution. End of this phase = a usable single-node,
single-tenant queue.

### 4.1 Claim
- `Claim(ctx, partition, lease)`: `FindOneAndUpdate` with server-time
  `now` (`$$NOW` pipeline update), mints claim id, sets fence and
  `visible_at`, increments `attempts`; empty set returns `ErrNoJob`
- `ClaimedJob` handle: promoted `Job`, unexported fence, `ClaimID()`
- **Test**: claims lowest `(vstamp, _id)`; concurrent claimers get
  disjoint jobs; empty partition yields `ErrNoJob`

### 4.2 Lease maintenance
- `Heartbeat(ctx, extend)`: fenced, extends from server now (no stacking)
- `Release(ctx, delay)`: fenced, sets `visible_at = now + delay`,
  **clears `claim_id`**
- **Test**: heartbeat with stale fence fails; released job is reclaimable
  after delay; releaser's subsequent Complete/Heartbeat fails

### 4.3 Resolution and reclaim
- `Complete(ctx, r)`: fenced terminal write, sets `resolved_at`
- `Cancel(ctx, jobID)`: queue-level, guards on pending liveness only
- `Get(ctx, jobID)`: snapshot read, not-found sentinel
- **Test**: expired lease re-enters visible set and is reclaimed with
  incremented attempts; exactly one of {late Complete, steal-then-Complete}
  wins; Cancel of a claimed job revokes per the lifecycle rules

---

## Phase 5: Fairness and Contention Verification

No new mechanism -- multi-tenant behavior tests and the stated throughput
target, proving the fairness properties the design promises.

### 5.1 Fairness behavior
- **Test**: two tenants, equal weight -- interleaved dispatch despite one
  tenant flooding
- **Test**: weight 2 vs 1 -- ~2:1 dispatch ratio; cheap jobs sort earlier
- **Test**: single-tenant degenerate case runs the same code path (FIFO
  by enqueue order)

### 5.2 Contention ceiling
- Benchmark: concurrent claimers on one partition; document measured
  claims/sec against the "low hundreds per partition" target
- **Test**: partition-scoped claims never cross partitions

---

## Phase 6: Reconciliation and Cache Lifecycle

Resolves open questions #2 (reconciliation/LWM access path) and #3 (surface
and scheduling). Bounds vtime-cache drift and handles restart/idle correctly.

As built, `TODO.md` reshaped this phase into 6.1 per-(tenant, partition)
timelines (not in the original plan), 6.2 reconcile, 6.3 LWM, and 6.4
public surface, then added 6.5 review follow-ups (including fixed-point
`SCALE` 1e6 -> 1e3 and checked stamp arithmetic, `ErrVTimeOverflow`) and 6.6
(service-clock frontier and the three partial indexes). Reconcile is lazy
invalidation (mark used entries stale so each re-seeds on its next enqueue;
evict unused ones) instead of an eager aggregation that rewrites the cache.
"Cache rebuild on startup" is the lazy seed: a timeline seeds on its first
enqueue, and that cold seed also refreshes its partition's frontier. The
surface is caller-driven `Reconcile(ctx)`. The subsections below are the
original plan.

### 6.1 Reconciliation
- Per-tenant max-pending-vstamp aggregation; decide and document the
  index/access path and its write-amplification cost
- Cache rebuild on startup; periodic reconcile bounding drift; eviction to
  bound memory
- **Test**: node restart mid-stream preserves within-tenant monotonicity;
  reconcile corrects a deliberately skewed cache

### 6.2 LWM and idle floor
- Low-water-mark computation; idle tenant re-entry does not gain unfair
  credit
- **Test**: tenant idle past the floor resumes at LWM, not stale vtime

### 6.3 Exposure surface
- Decide internal goroutine vs caller-driven tick (or both); document in
  the design doc
- **Test**: chosen surface -- reconcile runs on schedule / on demand

---

## Phase 7: Ops Hardening and Documentation

Production readiness: GC verification, design-doc reconciliation, and public
documentation. As decided 2026-10-17: no observability hooks are built (the
caller can query the collection); `stamped_by` is already wired and tested
(Phase 3); release tagging is separate from this phase. `TODO.md` splits
7.3 into 7.2-7.5 and 7.4 into 7.6-7.8.

### 7.1 GC and retention
Done 2026-10-17.

- Trust the server's TTL monitor; test only the shapes it acts on. No
  short-retention test (the monitor runs every 60s; lowering
  `ttlMonitorSleepSecs` mutates server-wide state)
- Make the TTL index partial on `{liveness: "resolved"}` and name it
  `mongoqueue_ttl` (decided 2026-10-17): pending jobs never enter the
  index, so their safety no longer depends on `resolved_at` being
  `omitempty`, and pending jobs stop costing a null-key TTL entry.
  Nothing is deployed; the old spec is replaced without migration
- **Test**: `TestEnsureTTLIndex` asserts name and partial filter; a
  lifecycle test reads raw BSON and asserts `liveness` and `resolved_at`
  agree -- pending, claimed, heartbeated, released, and lease-expired jobs
  carry no `resolved_at`; completed and canceled jobs carry a date

### 7.3 Design-doc reconciliation against the code
Phases 3-6 changed the design as they went (per-(tenant, partition)
timelines, lazy-invalidation reconcile, service-clock frontier, partial
indexes, SCALE 1e3, checked arithmetic). The docs were patched along the
way; this step confirms the patches are complete. The code is the presumed
reference, but every gap is classified before anything is edited, because
some gaps will be code bugs.

- Fold the decision records from `TODO.md` (6.0 D1-D3, "Cache state and
  locking", "Seed reads from the primary", "Known effect", 6.6 Decision and
  index tables) into the design docs or `docs/rejected-designs.md` before
  `TODO.md` is replaced for Phase 7. Anything that lives only in `TODO.md`
  or the gitignored `REVIEW.md` is lost when the file turns over. Done
  2026-10-17, before `TODO.md` was replaced
- Claim-by-claim pass over `fair-job-queue-design.md` and
  `mongodb-go-library-design.md`: for each normative statement (rules,
  field names, query shapes, index keys, error names, lifecycle
  transitions, invariants), find the code that implements it and the test
  that proves it. Record each in a worksheet (the `REVIEW.md` appendix,
  not committed): statement, doc location, code location, test, verdict
- Verdicts: **doc lags** (fix the doc), **code deviates** (write it to a
  fresh `REVIEW.md`, gitignored; change neither code nor doc until it is
  decided -- deviations are reviewed one by one with the user),
  **untested** (add a test or note it), **undecided** (move to Open
  Questions)
- Reverse pass: walk exported identifiers, sentinel errors, and godoc in
  `*.go`; flag any behavior or caller obligation the docs never mention
- Check the "Correctness invariants" section invariant by invariant:
  each names the test that enforces it, or is marked as unenforced
- Check "Rejected alternatives" and `rejected-designs.md` for alternatives
  that later phases quietly adopted, or that are stated against an older
  model (min-visible LWM, per-tenant timelines, single claim index)
- This plan itself: Phases 1-6 describe superseded specs (1.3 and 3.4 index
  keys, 3.1 per-tenant cache, 6.x original subsections); add as-built notes
  in the style of the Phase 6 preamble rather than rewriting history
- AGENTS.md "The model in brief" and "Mongo/Go specifics" against the code
- Close or update Open Questions: #1 (`Mux` dispatch helper) is deferred
  and closed (decided 2026-10-17). Every deferred item appears in exactly
  one Deferred/Future list across the docs, not three
- **Test**: any behavior found untested gets a test, in this subphase or
  recorded as a follow-up; doc-only fixes need no test

### 7.4 Documentation
- Package godoc: model overview, caller obligations (single-writer,
  weight consistency, index creation, at-least-once effects, delayed jobs jump
  the queue); remove the "Full godoc is deferred" placeholder in `doc.go`
- Runnable example: enqueue/claim/complete loop with a worker pool
- Minimal `README.md` for GitHub visitors: badges (pkg.go.dev reference,
  Go Report Card, CI status, license), one-paragraph pitch, install line,
  link to godoc and design docs. Brief; usage rules live in godoc

---

## Future Phases (Deferred)

### Throughput
- Bounded-random-skip claim among top-K visible (only if Phase 5 benchmark
  hits the contention ceiling; rationale in the mongodb doc's "Contention
  stance")
- Batch enqueue (`InsertMany` + single cache advance); leave API room

### Multi-writer
- Two-stage stamp partitioning (fallback when a single writer per (tenant,
  partition) cannot be guaranteed at enqueue)
- Multi-writer detection consumer: per-(tenant, partition) distinct
  `stamped_by` count during reconciliation, surfaced as a warning metric
  (rationale in the fair doc's "Multi-writer failure analysis")

### Caller conveniences
- `Kind` → handler `Mux` dispatch helper, asynq-style (former mongodb doc
  open question #1, closed 2026-10-17; an optional layer above the
  primitive, not part of it)
- `ClaimOpts` with `claimed_by` observability label
- `CancelOpts.Resolution` override -- only if a cancel-reason need appears
  (see the mongodb doc's "Resolution: Complete vs Cancel")

### Cache and reconcile (from Phase 6)
- Internal reconcile goroutine or `RunReconciler` helper -- only if callers
  ask; `New` stays goroutine-free
- Batched re-seed (one `$in` query for many invalidated timelines) -- only
  if per-timeline re-seed latency shows up in enqueue measurements
- Backlog reconstruction (LWM + sum of pending strides) instead of trailing
  edge -- revisit only with multi-writer
- Weight-change transition behavior -- documented, not tested
