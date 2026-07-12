# Phase 1 Implementation Plan: Scaffolding and Test Harness

Expands Phase 1 of [docs/implementation-plan.md](docs/implementation-plan.md):
module skeleton, MongoDB test infrastructure, CI, and the `Queue` handle with
caller-invoked index creation. No queue semantics yet -- no enqueue, claim, or
lifecycle code.

## Design Context

Read before starting; these are the Phase-1-relevant facts from the design
docs ([mongodb-go-library-design.md](docs/mongodb-go-library-design.md)):

- **Module path**: `github.com/xdg-go/mongoqueue` (from the git remote).
- **Index creation is caller-invoked, never automatic.** `New` creates
  nothing. `EnsureIndexes` and `EnsureTTLIndex` are split because the claim
  index is required plumbing while TTL is destructive policy; runtime
  credentials often lack `createIndex`.
- **Claim index**: `{partition: 1, liveness: 1, vstamp: 1, _id: 1,
  visible_at: 1}`. First four serve the claim filter and `(vstamp, _id)`
  sort; `visible_at` rides along so the visibility predicate filters
  in-index.
- **TTL index**: on `resolved_at` with caller-supplied retention. The field
  is `omitempty` -- absent until the terminal write -- so TTL never touches
  pending or claimed jobs.
- **Node id**: the queue records `stamped_by` on every enqueue (Phase 3);
  Phase 1 only needs the id minted and stored on `Queue`.

## Testing Philosophy

- **Integration tests against real MongoDB**: index and harness behavior
  depend on the server; mocks test nothing. Tests connect to
  `MONGOQUEUE_TEST_URI`, defaulting to `mongodb://localhost:27017` -- covers
  both a local `mongod` and a CI service container with no special-casing.
- **Hermetic tests**: each test uses a uniquely named database (random
  suffix) so parallel runs never collide; drop the database in cleanup. No
  test reads or writes user configuration.
- **Pure unit tests** for anything with no server dependency (e.g. node-id
  format), as table-driven `testing` tests.

## Verification Checklist

Before marking a sub-phase complete and committing it:

1. `go build ./...` and `go vet ./...` pass
2. `make test` passes against local `mongod`
3. `make lint` (`golangci-lint run`, config in `.golangci.yml`) is clean
4. New exported identifiers have godoc comments
5. Design docs updated if implementation forced a decision the docs left open

When verification of a sub-phase is complete, commit all relevant
newly-created and modified files as one atomic commit.

## Dependencies Between Sub-phases

```
1.1 (Module and tooling)
       │
       ▼
1.2 (Mongo test harness + CI)
       │
       ▼
1.3 (Queue construction and indexes)
```

---

## Phase 1: Scaffolding and Test Harness

### 1.1 Module and tooling

- [x] `go mod init github.com/xdg-go/mongoqueue`; set `go 1.26`
- [x] Add `go.mongodb.org/mongo-driver/v2` dependency
- [x] Placeholder `doc.go` with package comment (one-paragraph model summary;
      full godoc is Phase 7)
- [x] Makefile: `test` (`go test ./...`), `vet` (`go vet ./...`), `lint`
      (`golangci-lint run`), `all` = vet + lint + test
- [x] **Test**: `go test ./...` runs and passes with a trivial placeholder
      test (deleted in 1.2)

### 1.2 Mongo test harness and CI

- [x] `internal/mongotest` (or `_test.go` helper -- prefer the internal
      package so later phases reuse it): `mongotest.Connect(t)` returns a
      `*mongo.Database` with a unique name (`mqtest_` + random hex suffix),
      registers `t.Cleanup` to drop it and disconnect; reads
      `MONGOQUEUE_TEST_URI`, defaults to `mongodb://localhost:27017`
- [x] Fail fast with a clear message when Mongo is unreachable (connection
      error names the URI and the env var; do not skip silently)
- [x] GitHub Actions workflow `.github/workflows/test.yml`: ubuntu-latest,
      `mongo:8` service container on 27017, `actions/setup-go` with
      `go-version-file: go.mod`, sets `MONGOQUEUE_TEST_URI`, runs
      `make vet lint test` (use `golangci-lint-action` for the lint step)
- [x] **Test**: harness smoke test -- insert and read back a document in the
      isolated database
- [x] **Test**: two parallel tests (`t.Parallel()`) get distinct database
      names and do not interfere
- [x] **Manual**: push a branch; CI workflow goes green

### 1.3 Queue construction and indexes

- [x] `Queue` struct holding the `*mongo.Collection` and node id;
      `New(coll *mongo.Collection, opts ...Option) *Queue` -- performs no I/O
- [x] Default node id: `hostname-pid-<random hex>`; `WithNodeID(string)`
      functional option overrides (needed later for multi-writer tests)
- [x] `EnsureIndexes(ctx)`: creates the claim index
      `{partition: 1, liveness: 1, vstamp: 1, _id: 1, visible_at: 1}`,
      idempotent on re-call
- [x] `EnsureTTLIndex(ctx, retention time.Duration)`: TTL index on
      `resolved_at`, `expireAfterSeconds` from retention; document that
      calling again with a *different* retention returns the driver's
      index-conflict error (caller resolves via `collMod` -- record this
      decision in the design doc's open questions)
- [x] **Test**: `New` creates no indexes (collection has only `_id_` after
      construction and a write)
- [x] **Test**: `EnsureIndexes` creates exactly the claim index with the
      specified keys; second call succeeds without error
- [x] **Test**: `EnsureTTLIndex(ctx, 24*time.Hour)` creates a TTL index on
      `resolved_at` with `expireAfterSeconds == 86400`
- [x] **Test**: node id defaults to non-empty and unique across two `New`
      calls; `WithNodeID` overrides it

---

## Future Phases (Deferred)

Everything else lives in [docs/implementation-plan.md](docs/implementation-plan.md):

- Phase 2: `Job` record, `Liveness`/`Resolution` types, vstamp arithmetic
- Phase 3: enqueue path, vtime cache, typed facade
- Phase 4: claim/lease/resolution
- Phases 5-7: fairness verification, reconciliation, ops hardening

Do not implement any queue semantics in Phase 1, even where a stub seems
convenient -- the `Queue` struct gains behavior in later phases.
