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
  invariant; test with real goroutine races, not sequenced calls.

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
- Stride function: `max(1, cost*SCALE/weight)`, `SCALE = 1_000_000`;
  cost floor; weight zero-means-1 and `>= 1` validation
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

### 3.4 Amendments (post-review)
- `EnqueueOpts.Delay` -- delayed enqueue: `visible_at = now + Delay`, zero
  means immediately visible, negative rejected (`ErrInvalidDelay`)
- Seed index in `EnsureIndexes`: `{tenant, liveness, vstamp desc}` -- serves
  the Phase 3.1 cold-start seed query, which the claim index cannot

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
- `Cancel(ctx, jobID, r)`: queue-level, guards on pending liveness only
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

### 6.1 Reconciliation
- Per-tenant max-pending-vstamp aggregation; decide and document the
  index/access path and its write-amplification cost
- Cache rebuild on startup; periodic reconcile bounding drift
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

Production readiness: GC, observability hooks, and public documentation.

### 7.1 GC and retention
- **Test**: TTL index ages out resolved jobs only; pending/claimed jobs
  untouched (short-retention integration test)

### 7.2 Observability seams
- `stamped_by` recorded on every enqueue (verify Phase 3 wiring);
  `ClaimOpts` room for a `claimed_by` label documented, not built
- **Test**: `stamped_by` reflects the enqueueing node id

### 7.3 Documentation
- Package godoc: model overview, caller obligations (single-writer,
  weight consistency, index creation, at-least-once effects)
- Runnable example: enqueue/claim/complete loop with a worker pool
- Reconcile design docs with as-built decisions; update open questions

---

## Future Phases (Deferred)

### Throughput
- Bounded-random-skip claim among top-K visible (only if Phase 5 benchmark
  hits the contention ceiling)
- Batch enqueue (`InsertMany` + single cache advance)

### Multi-writer
- Two-stage stamp partitioning (fallback when single-writer-per-tenant cannot
  be guaranteed at enqueue)
- Multi-writer detection consumer: per-tenant distinct `stamped_by` count
  during reconciliation, surfaced as a warning metric

### Caller conveniences
- `Kind` → handler `Mux` dispatch helper (open question #1; a layer above the
  primitive, not part of it)
- `ClaimOpts` with `claimed_by` observability label
