# TODO: Phase 2 -- Job Record and Virtual-Time Arithmetic

Expanded from [docs/implementation-plan.md](docs/implementation-plan.md)
Phase 2. The durable `Job` shape and the pure fixed-point math, testable
without any queue behavior. Resolves the `Liveness` / `Resolution`
representation question, now recorded in the "Liveness and Resolution
representation" subsection of
[docs/mongodb-go-library-design.md](docs/mongodb-go-library-design.md) --
it blocks everything downstream.

## Testing Philosophy

- **Integration tests against real MongoDB**: BSON encoding and body
  queryability depend on server behavior; use the Phase 1 harness
  (`MONGOQUEUE_TEST_URI`, default `mongodb://localhost:27017`).
- **Hermetic tests**: each test uses a uniquely named database/collection
  (random suffix); drop on cleanup.
- **Pure unit tests for arithmetic**: stride/vstamp fixed-point math and
  encoding round-trips are plain table-driven `testing` tests with no
  database.

## Verification Checklist

Before marking a subphase complete and committing it:

1. `go build ./...` and `make vet` pass
2. `make test` passes (including integration tests against Mongo)
3. `make lint` clean (config in `.golangci.yml`)
4. New exported identifiers have godoc comments
5. Design docs updated where this phase forces a decision the docs left
   open (the `Liveness`/`Resolution` encoding, subphase 2.1)

When verification of a subphase is complete, commit all relevant
newly-created and modified files.

## Dependencies Between Subphases

```
2.1 Core types (Job, Liveness, Resolution, sentinel errors)
       │  (blocks Phase 3 enqueue and Phase 4 claim/resolve)
2.2 Fixed-point virtual time (independent of 2.1; ordered for convenience)
       │  (blocks Phase 3 stamping)
       ▼
Phase 3 (Enqueue path + typed facade)
```

---

## Phase 2.1: Core Types

The durable record and its lifecycle fields. Decide the `Liveness` /
`Resolution` representation first -- the `Job` struct depends on it.

### 2.1.1 Liveness and Resolution representation

- [x] Decide the `Liveness` Go type and BSON encoding. Constraints from the
  design docs: exactly two values (pending / resolved); claim, heartbeat,
  release, and cancel guard on pending liveness in server-side conditional
  writes, so the encoding must be cheap to match in a filter
- [x] Decide the `Resolution` Go type and BSON encoding. Constraints:
  caller-set, queue-established but never interpreted by queue code;
  terminal write records exactly one; usable for resolution lookup via
  `Get`
- [x] Record the decision in
  [docs/mongodb-go-library-design.md](docs/mongodb-go-library-design.md):
  update the `Job` struct listing (currently omits both fields) and close
  the open question on `Liveness`/`Resolution` representation
- [x] **Test**: `Liveness` values encode/decode as decided; round-trip
  through BSON preserves them; the pending value is filter-matchable as
  a literal (table-driven, plus a harness test inserting and filtering)

### 2.1.2 Job struct

- [x] `Job` struct per the design doc: envelope fields (`ID`, `TenantID`,
  `Partition`, `Cost`, `VStamp`, `ClaimID`, `VisibleAt`, `Attempts`,
  `StampedBy`, `ResolvedAt`) plus `Liveness`/`Resolution` from 2.1.1,
  plus payload (`Kind`, `Body bson.Raw`); exported fields, documented
  read-only snapshot
- [x] Godoc on `Job` and each field, including the read-only-snapshot
  contract and the `bson` tag mapping
- [x] **Test**: BSON round-trip of a fully populated `Job` -- every field
  survives marshal/unmarshal with the expected wire names
- [x] **Test**: zero-value / omitempty behavior -- `resolved_at` absent on
  a pending job's stored document
- [x] **Test** (integration): insert a `Job` whose `Body` is a native
  subdocument; query it by a body field (e.g. `body.customer`) and get
  the document back -- body is opaque to queue code but queryable in the
  database

### 2.1.3 Sentinel errors

- [x] `ErrDuplicateJob` (enqueue idempotency collision), `ErrNoJob` (empty
  claim set), and a not-found sentinel for `Get`; godoc stating which
  operations return each
- [x] **Test**: sentinels are distinct and match via `errors.Is`

---

## Phase 2.2: Fixed-Point Virtual Time

Pure integer arithmetic; no database.

- [ ] Stride function: `max(1, cost*SCALE/weight)` with
  `SCALE = 1_000_000`; pure integer math
- [ ] Cost floor and validation; weight validation: zero means 1,
  otherwise `>= 1` required
- [ ] Godoc explaining the fixed-point representation (int64, 1e-6 units)
  and why the floor preserves strict monotonicity
- [ ] **Test**: table-driven stride cases -- typical costs/weights, extreme
  ratios (huge cost / weight 1, cost 1 / huge weight), floor engagement,
  weight zero-means-1
- [ ] **Test**: validation rejects weight < 0 and (if decided) weight
  between 0 and 1 semantics; cost floor behavior
- [ ] **Test**: strictly-increasing-within-tenant property -- repeated
  stride advances from any starting vtime never repeat or regress, even
  when the floor is active

---

## Future Phases (Deferred)

- **Phase 3**: enqueue path, vtime cache, typed `Enqueue[T]`/`Decode[T]`
  facade -- consumes 2.1 types and 2.2 stride
- **Phase 4**: claim/lease/resolution -- consumes the liveness guard
  encoding decided in 2.1.1
- Overflow guard beyond documentation: not needed (~292,000 years at
  sustained 10^6 cost-units/sec at weight 1); document, don't code
