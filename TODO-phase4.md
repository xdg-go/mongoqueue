# TODO: Phase 4 -- Claim, Lease, and Resolution

Expanded from [docs/implementation-plan.md](docs/implementation-plan.md)
Phase 4. The lease primitive: atomic claim of the minimum visible
`(vstamp, _id)`, fenced lease writes, terminal resolution, and snapshot
lookup. After this phase the queue is usable end to end on a single node.
Design references: "The Claim API", "Resolution: Complete vs Cancel", and
"Lookup" in [docs/mongodb-go-library-design.md](docs/mongodb-go-library-design.md);
"Job lifecycle and leasing" in
[docs/fair-job-queue-design.md](docs/fair-job-queue-design.md).

Section 4.0 holds the contracts every subphase relies on. Read it before
starting any subphase; do not improvise around it.

## Testing Philosophy

- **Integration tests against real MongoDB**: claim atomicity, fencing,
  `$$NOW` server time, and visibility semantics depend on server behavior;
  mocks would test nothing. Tests connect via `internal/mongotest`
  (`MONGOQUEUE_TEST_URI`, default `mongodb://localhost:27017`).
- **Hermetic tests**: each test uses a uniquely named database/collection
  (random suffix) so parallel runs never collide; drop on cleanup.
- **Pure unit tests for validation**: duration checks, millisecond
  conversion, and empty-resolution rejection are table-driven tests with no
  database.
- **Concurrency tests**: mutual exclusion under concurrent claimers and
  single-winner termination are core invariants; test with real goroutine
  races under `-race`, not sequenced calls.
- **Control time explicitly; never rely on timing.**
  - Force expiry by writing `visible_at` into the past directly, not by
    sleeping out a lease.
  - Test exclusion ("not claimable yet") with long future visibility
    (minutes), so the assertion cannot race the clock.
  - In race tests, use leases far longer than the test's deadline (e.g. 1
    minute) so no lease can lapse mid-test, even under `-race` on slow CI.
  - Bound server-time assertions with server-time samples, not the client
    clock: read `$$NOW` (e.g. via an aggregation) before and after the
    operation and assert the stored value lies within
    `[before + d, after + d]`.

Test what new code changes. Enqueue validation, stamping, and the typed
facade are covered by Phase 3; use them as fixtures, don't retest them.

## Verification Checklist

Before marking a subphase complete and committing it:

1. `go build ./...` and `make vet` pass
2. `make test` passes (including integration tests against Mongo; run the
   concurrency tests with `-race`)
3. `make lint` clean (config in `.golangci.yml`)
4. New exported identifiers have godoc comments
5. Design docs updated if implementation forced a decision the docs left open

When verification of a subphase is complete, commit all relevant
newly-created and modified files as one logical unit.

## Dependencies Between Subphases

```
4.0 Contracts (read first; recorded in design doc during 4.1)
       │
       ▼
4.1 Claim + ClaimedJob handle
       │
       ├─► 4.2 Lease maintenance (Heartbeat, Release)
       │         │
       │         ▼
       └─► 4.3 Resolution and lookup (Complete, Cancel, Get)
                 │
                 ▼
           4.4 Lifecycle race tests (cross-operation)
```

4.2 must compile and pass without Complete; tests needing Complete live in
4.3 or 4.4. 4.4 needs all three verbs.

---

## 4.0 Contracts

Settled decisions. The design doc leaves these open; 4.1 records them in
mongodb-go-library-design.md as part of its commit.

### Sentinels
- **`ErrLeaseLost`** -- "claim no longer held". Returned by Heartbeat,
  Release, and Complete on any acknowledged fence miss: stolen, released,
  canceled, already completed, or deleted (TTL). A worker that needs to
  tell steal from cancel calls `Get` and reads the record.
- **`ErrJobResolved`** -- Cancel's diagnostic read saw the job resolved.
  It means "observed resolved at the read", not "your cancel won" -- a
  caller that needs to know whether the job was canceled inspects
  `Resolution` via `Get`.
- **`ErrInvalidLease`** -- `lease <= 0` on Claim or `extend <= 0` on
  Heartbeat. Release reuses **`ErrInvalidDelay`** for `delay < 0`; widen
  its godoc to cover Release.
- **Empty job id** on `Get` or `Cancel` returns `ErrMissingJobID` (widen
  its godoc) before any round trip.
- Add sentinels to errors.go in the existing style.

### Duration → milliseconds
BSON dates and `$$NOW` arithmetic are whole milliseconds. Convert every
duration (lease, extend, delay) with one helper:
- Zero stays zero (Release with zero delay = immediately reclaimable).
- Positive durations round **up** to whole milliseconds, so `1ns` becomes
  `1ms`, never `0ms`. Compute with quotient/remainder
  (`d / ms`, `+1 if d % ms != 0`) so `math.MaxInt64` nanoseconds does not
  overflow.
- Validation (`<= 0` / `< 0`) runs on the original duration, before
  conversion.

### Pipeline updates
- Claim, Heartbeat, Release, Complete, and Cancel all use pipeline updates
  so `$$NOW` supplies server time.
- **Wrap every caller-supplied or computed string value in `$literal`**
  (resolution, claim id). In a pipeline, `"$foo"` is a field path and
  `"$$NOW"` a variable; an unwrapped resolution of `"$foo"` would store the
  wrong value.

### Fenced-write result handling
- Check the driver error first (including write-concern errors) and return
  it wrapped; do not inspect counts on error.
- Only an error-free result with `MatchedCount == 0` means `ErrLeaseLost`.
  `MatchedCount == 1` is success. **Do not check `ModifiedCount`**: two
  heartbeats in the same millisecond write identical values and modify
  nothing.
- The fence check relies on acknowledged writes. The queue does not
  inspect or override the caller's collection write concern; document in
  `New` godoc that unacknowledged write concern breaks fencing and that
  single-resolution across failover requires `w: majority` (a `w: 1` write
  can roll back). Caller's choice.

### Handle state
- A `ClaimedJob` is an immutable claim-time snapshot. Heartbeat, Release,
  and Complete do **not** update its fields. Callers use `Get` for fresh
  state.
- The handle captures the fence and the job id in unexported fields at
  claim time. All guards use those private copies, so mutating the
  exported `Job.ID` or `Job.ClaimID` has no effect on later operations.
- Handle methods are safe to call concurrently (a heartbeat goroutine plus
  the worker's Complete); they read only immutable private state.

### Context, errors, retries
- Pass the caller's `ctx` to every driver call, including Cancel's
  diagnostic read. No library retry loops.
- Wrap driver errors with `%w` so `errors.Is` still matches driver and
  context errors.
- A context timeout or network error leaves the write outcome unknown;
  document this in godoc. `Get` can show current state but cannot prove
  which call caused it.
- Repeating Complete or Release after success returns `ErrLeaseLost`; they
  are not success-idempotent.
- Errors return a nil `*Job` / `*ClaimedJob`.

### Claim id
- `bson.NewObjectID().Hex()` -- 24 lowercase hex characters, never empty.
- Revisit if claim ids ever cross a trust boundary (e.g. a remote worker
  completing by claim id over HTTP): ObjectIDs are predictable, so such an
  API needs 128-bit random ids.

### Reads
- `Get` and Cancel's diagnostic read use primary read preference
  (`coll.Clone` with `readpref.Primary()`, built once in `New`), so a
  secondary cannot return a stale record.
- `Get` maps only `mongo.ErrNoDocuments` to `ErrJobNotFound`; other errors
  pass through wrapped.

## 4.1 Claim

Storage behavior #3: one atomic `FindOneAndUpdate` with a pipeline update so
`now` is server time.

- [x] Record the 4.0 contracts in mongodb-go-library-design.md (Claim API
  and Resolution sections); fix the stale `Cancel(ctx, jobID, r)` signature
  in implementation-plan.md 4.3 to `Cancel(ctx, jobID)`
- [x] Add `ErrLeaseLost`, `ErrJobResolved`, `ErrInvalidLease` to errors.go;
  widen `ErrInvalidDelay` and `ErrMissingJobID` godoc
- [x] Duration→ms helper per 4.0
- [x] `func (q *Queue) Claim(ctx, partition string, lease time.Duration)
  (*ClaimedJob, error)`; empty partition name maps to `defaultPartition`,
  same as enqueue
- [x] Filter: `partition`, `liveness: pending`, and visibility against
  server time (`$expr: {$lte: ["$visible_at", "$$NOW"]}`)
- [x] Sort `{vstamp: 1, _id: 1}`; `ReturnDocument(After)`
- [x] Pipeline update: `claim_id = {$literal: <minted>}`,
  `visible_at = $$NOW + leaseMs`, `attempts = attempts + 1`
- [x] Map `mongo.ErrNoDocuments` to `ErrNoJob` (not wrapped)
- [x] `ClaimedJob` type per 4.0 handle contract: promoted `Job`;
  unexported `q *Queue`, `jobID string`, `claimID string`; `ClaimID()`
  accessor; godoc stating the snapshot is immutable and mutating exported
  fields changes nothing server-side
- [x] Primary-read collection clone on `Queue` (used by 4.3)
- [x] **Explain check** (one-off, record results in the design doc's
  "Indexes, contention, and GC" section). Seed a partition with a large
  block (e.g. 10k) of invisible low-vstamp jobs followed by visible
  higher-vstamp jobs; run explain on the exact `findAndModify`. Record
  server version, winning plan, blocking sort (must be absent), keys
  examined, docs examined. If docs examined ≈ the invisible block, the
  `$expr` predicate is evaluated after fetch, contradicting the
  `EnsureIndexes` godoc: stop and raise it with the user. Do not switch to
  client time as a fix.
- [x] **Test**: unit -- duration→ms table: `0`, `1ns`, `999µs`, `1ms`,
  `1500µs`, `math.MaxInt64`; `lease <= 0` rejected before any round trip
- [x] **Test**: integration -- claims lowest `(vstamp, _id)`; equal vstamps
  across tenants break ties by `_id`
- [x] **Test**: integration -- claimed record has the minted `claim_id`,
  `visible_at` within server-time bounds of `now + lease`, `attempts == 1`,
  unchanged `vstamp`, liveness still pending
- [x] **Test**: integration -- job with `visible_at` minutes in the future
  is skipped; resolved jobs (written directly as fixtures) are skipped
- [x] **Test**: integration -- partition with no jobs yields `ErrNoJob`;
  claim in partition A never returns a job from partition B
- [x] **Test**: integration -- partition name `""` and `"default"` both
  claim a job enqueued with empty `Partition`
- [x] **Test**: race -- N goroutines, one Claim each, against M jobs
  (N > M), 1-minute lease: exactly M successes with distinct job ids and
  N−M `ErrNoJob`
- [x] **Test**: integration -- claim id is 24-char hex, non-empty, and
  differs between two claims (smoke test only)
- [x] **Test**: unit -- mutating exported `Job.ID`/`Job.ClaimID` on a
  handle does not change its private fence or job id

## 4.2 Lease maintenance

Fenced writes guarded on `{_id: c.jobID, claim_id: c.claimID, liveness:
pending}`. No visibility condition in the guard: a holder whose lease
expired but was not reclaimed still holds the fence. Neither operation is a
virtual-time event.

- [ ] `(c *ClaimedJob) Heartbeat(ctx, extend time.Duration) error`:
  `visible_at = $$NOW + extendMs` (from server now, not from current
  `visible_at`, so heartbeats cannot stack)
- [ ] `(c *ClaimedJob) Release(ctx, delay time.Duration) error`:
  `visible_at = $$NOW + delayMs` and set `claim_id` to `""`
- [ ] Result handling per 4.0 (driver error first; `MatchedCount == 0` →
  `ErrLeaseLost`; ignore `ModifiedCount`)
- [ ] **Test**: unit -- `extend <= 0` → `ErrInvalidLease`; `delay < 0` →
  `ErrInvalidDelay`; both before any round trip
- [ ] **Test**: integration -- heartbeat sets `visible_at` within
  server-time bounds of `now + extend`; two back-to-back heartbeats both
  succeed (same-millisecond case) and do not stack; `vstamp` unchanged
- [ ] **Test**: integration -- holder whose lease expired (forced) but was
  not reclaimed can still Heartbeat successfully
- [ ] **Test**: integration -- after forced expiry and reclaim by another
  handle, the old holder's Heartbeat and Release both return
  `ErrLeaseLost` and leave `visible_at` and `claim_id` unchanged
- [ ] **Test**: integration -- released job has `claim_id == ""`; with
  zero delay it is immediately reclaimable, and the reclaim increments
  `attempts`, mints a new claim id, keeps `vstamp`
- [ ] **Test**: integration -- job released with a multi-minute delay
  returns `ErrNoJob` on Claim; after forcing `visible_at` into the past it
  is claimable
- [ ] **Test**: integration -- releaser's later Heartbeat returns
  `ErrLeaseLost` and does not change `visible_at` (no un-release); second
  Release also returns `ErrLeaseLost`

## 4.3 Resolution and lookup

Terminal writes set `liveness: resolved`, `resolution`, and
`resolved_at = $$NOW`. A resolved job never re-enters the visible set.

- [ ] `(c *ClaimedJob) Complete(ctx, r Resolution) error`: empty `r` →
  `ErrEmptyResolution` before any round trip; guard
  `{_id, claim_id, liveness: pending}`; `resolution: {$literal: r}`;
  result handling per 4.0
- [ ] `func (q *Queue) Cancel(ctx, jobID string) error`: guard
  `{_id, liveness: pending}` only; writes `ResolutionCanceled`; leaves
  `claim_id` intact. On `MatchedCount == 0`, one primary read classifies:
  absent → `ErrJobNotFound`; resolved → `ErrJobResolved`; pending →
  `ErrJobResolved`. The pending case is reachable only if the targeted job
  resolved, was TTL-deleted, and a new job reused the id between the
  update and the read (Release cannot cause a miss -- it leaves liveness
  pending). The record the update missed was resolved, so report that; do
  not retry the update against what may be a different job. Document all
  three outcomes in godoc.
- [ ] `func (q *Queue) Get(ctx, jobID string) (*Job, error)`: primary read
  by `_id`; per 4.0 error rules
- [ ] **Test**: unit -- `Complete("")` returns `ErrEmptyResolution`;
  `Get("")` and `Cancel("")` return `ErrMissingJobID`
- [ ] **Test**: integration -- Complete sets resolved, resolution,
  `resolved_at` within server-time bounds; `vstamp` unchanged; job never
  claimable again
- [ ] **Test**: integration -- resolutions `"$foo"` and `"$$NOW"`
  round-trip through Get as exact strings (stored BSON type string)
- [ ] **Test**: integration -- second Complete on the same handle returns
  `ErrLeaseLost`
- [ ] **Test**: integration -- Complete after the same handle's Release
  returns `ErrLeaseLost`; job stays pending
- [ ] **Test**: integration -- holder whose lease expired (forced) but was
  not reclaimed can still Complete
- [ ] **Test**: integration -- Cancel of a claimed job resolves it with
  `ResolutionCanceled` and preserves the holder's non-empty `claim_id`;
  holder's next Heartbeat, Release, and Complete each return
  `ErrLeaseLost`
- [ ] **Test**: integration -- Cancel of a resolved id → `ErrJobResolved`;
  unknown id → `ErrJobNotFound`
- [ ] **Test**: integration -- Get returns the stored record; mutating the
  returned `Job` and calling Get again shows the stored values; unknown id
  → `ErrJobNotFound`

## 4.4 Lifecycle race tests

Cross-operation invariants from the generic design's "Termination,
cancellation, and races" and "Reclaim by expiry".

- [ ] **Test**: integration -- steal then late Complete: A's lease forced
  expired, B claims; A's Complete → `ErrLeaseLost`, B's succeeds; exactly
  one resolution recorded
- [ ] **Test**: race -- concurrent Complete (holder) vs Cancel (external)
  on one job, repeated many iterations: exactly one returns nil each time,
  and the stored resolution matches the winner
- [ ] **Test**: race -- holder's Complete vs reclaim after forced expiry:
  valid outcomes are (a) Complete wins and Claim returns `ErrNoJob` or
  (b) Claim wins and Complete returns `ErrLeaseLost`; never both succeed
- [ ] **Test**: integration -- end-to-end smoke: enqueue via `Kind[T]`,
  claim, `Decode`, heartbeat, complete, `Get` shows the resolution

---

## Future Phases (Deferred)

- Multi-tenant fairness behavior tests and contention benchmark -- Phase 5
- Reconciliation, LWM, idle floor, cache rebuild surface -- Phase 6
- `ClaimOpts` (e.g. `claimed_by` worker label) -- only when a second
  per-claim knob appears
- `CancelOpts.Resolution` override for cancel reasons -- only on demand
- Remote/HTTP lease operations by claim id -- would require unguessable
  (128-bit random) claim ids; see 4.0
- Blocking/long-poll claim (change streams or backoff loop) -- caller layer
  or later phase
- `Kind` → handler `Mux` dispatch helper -- a layer above the primitive
