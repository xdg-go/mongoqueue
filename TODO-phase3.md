# TODO: Phase 3 -- Enqueue Path and Typed Facade

Expanded from [docs/implementation-plan.md](docs/implementation-plan.md)
Phase 3. Stamp-at-enqueue with the node-local vtime cache, idempotent insert,
and the generic API boundary. After this phase, jobs land in Mongo correctly
stamped; nothing claims them until Phase 4.

## Testing Philosophy

- **Integration tests against real MongoDB**: insert semantics, duplicate-key
  mapping, and body subdocument storage depend on server behavior. Tests
  connect to `MONGOQUEUE_TEST_URI`, defaulting to `mongodb://localhost:27017`.
- **Hermetic tests**: each test uses a uniquely named database/collection
  (random suffix) so parallel runs never collide; drop on cleanup.
- **Pure unit tests for arithmetic and validation**: vtime-cache advance
  logic and `EnqueueOpts` validation are table-driven `testing` tests with no
  database where possible.
- **Concurrency tests**: strictly-increasing vstamps under concurrent
  enqueues is a core invariant; test with real goroutine races, not
  sequenced calls.

## Verification Checklist

Before marking a subphase complete and committing it:

1. `go build ./...` and `go vet ./...` pass
2. `make test` passes (including integration tests against Mongo)
3. `make lint` clean (config in `.golangci.yml`)
4. New exported identifiers have godoc comments
5. Design docs updated if implementation forced a decision the docs left open

When verification of a subphase is complete, commit all relevant
newly-created and modified files as one logical unit.

## Dependencies Between Subphases

```
3.1 Vtime cache (in-memory)
       │
       ▼
3.2 Enqueue core (uses cache for stamping)
       │
       ▼
3.3 Typed facade (wraps enqueue; adds Decode)
```

---

## 3.1 Vtime cache (in-memory)

Per-tenant soft state on the `Queue`. Advance only after a successful insert;
minimal cold-start seed here, full reconciliation and LWM in Phase 6.

- [x] Per-tenant vtime map guarded by a mutex on `Queue` (or a small
  `vtimeCache` type owned by `Queue`)
- [x] Reserve-next-vstamp operation: given `(tenant, cost, weight)`, compute
  candidate `vstamp = vtime + stride(cost, weight)` without mutating state
- [x] Commit operation: advance the tenant's vtime to the candidate only
  after the caller reports a successful insert; failed/duplicate insert
  advances nothing
- [x] Serialize reserve→insert→commit per tenant so concurrent enqueues for
  one tenant yield strictly increasing vstamps (document the chosen locking
  granularity: global vs per-tenant)
- [x] Cold-start seed: on first touch of an unseen tenant, query max pending
  `vstamp` for that tenant (0 if none); document as the minimal seed,
  superseded by Phase 6 reconciliation
- [x] **Test**: unit -- failed/duplicate insert advances nothing; next
  reserve reuses the same base vtime
- [x] **Test**: unit -- strides accumulate: successive commits for one tenant
  produce strictly increasing vstamps matching `stride` arithmetic
- [x] **Test**: race -- concurrent enqueues (goroutines) for one tenant
  produce unique, strictly increasing vstamps; run with `-race`
- [x] **Test**: integration -- cold-start seed picks up max pending vstamp
  from pre-inserted records; unseen tenant starts at the floor

## 3.2 Enqueue core

Non-generic insert path on `Queue`; the facade in 3.3 is its only public
entry point for bodies.

- [x] `EnqueueOpts` struct per the design doc: `JobID`, `TenantID`,
  `Partition`, `Cost`, `Weight`
- [x] Validation: required `JobID`; `normalizeCost` floor; `normalizeWeight`
  (`ErrInvalidWeight`); empty `Partition` maps to the default partition
- [x] Internal enqueue: build the `Job` record -- envelope fields top-level,
  `Kind` string, `Body bson.Raw` subdocument; set initial liveness,
  `visible_at` from client clock, `stamped_by` from the node id
- [x] Insert keyed on caller `JobID` as `_id`; map duplicate-key error to
  `ErrDuplicateJob` (match server error via driver helpers, not string
  matching)
- [x] Wire cache commit: advance tenant vtime only on successful insert
- [x] **Test**: unit -- opts validation table: missing JobID, cost floor,
  weight zero-means-1, invalid weight
- [x] **Test**: integration -- idempotent retry with same JobID returns
  `ErrDuplicateJob`; stored record wins (fields unchanged after retry with
  different body)
- [x] **Test**: integration -- stored record has envelope fields top-level
  and body as a native subdocument queryable by field (e.g. find on
  `body.customer_id`)
- [x] **Test**: integration -- `stamped_by` reflects the enqueueing node id;
  `visible_at` set; liveness pending

## 3.3 Typed facade

Generics only at the boundary; storage and queue core stay non-generic. The
facade is a per-kind binding: the kind↔type pairing has exactly one
declaration site (decided; see design doc and rejected-designs.md).

- [ ] `Kind[T any]` type with unexported kind string; `NewKind[T](kind)`
  constructor panics on empty kind (init-time programmer error, per
  `regexp.MustCompile` precedent)
- [ ] `Kind()` accessor for worker-side dispatch switches
- [ ] `(Kind[T]) Enqueue(ctx, q, body T, opts EnqueueOpts) error` -- binding
  marshals `T` to `bson.Raw`; callers never build raw BSON; non-document
  bodies (scalar/array) fail at `bson.Marshal` and the error is surfaced,
  no extra check
- [ ] `(Kind[T]) Decode(j *Job) (T, error)` -- asserts `j.Kind` matches the
  binding's kind; `ErrKindMismatch` sentinel wrapped with both kind strings
- [ ] Add `ErrKindMismatch` to errors.go, matching existing sentinel style
- [ ] Godoc: the binding is the single declaration site of the kind↔type
  contract; the queue stores `kind` without interpreting it
- [ ] **Test**: unit/integration -- round-trip: `k.Enqueue` then read back
  and `k.Decode` yields the original value
- [ ] **Test**: kind↔type drift -- decoding a job through a different
  binding returns `ErrKindMismatch`, not silent garbage
- [ ] **Test**: integration -- heterogeneous kinds coexist in one
  collection and decode independently via their bindings
- [ ] **Test**: non-document body (e.g. `int`) rejected at enqueue
- [ ] **Test**: `NewKind("")` panics

---

## Future Phases (Deferred)

- Claim/lease/resolution -- Phase 4
- Fairness behavior tests and contention benchmark -- Phase 5
- Full reconciliation, LWM, idle-floor, cache rebuild surface -- Phase 6
- Batch enqueue (`InsertMany` + single cache advance) -- deferred per plan
- `Kind` → handler `Mux` dispatch helper -- a layer above the primitive
