# mongoqueue

A Go library implementing a **fair, multi-tenant job queue on MongoDB**. It is a
durable lease primitive: callers enqueue jobs and supply their own worker pool;
the queue dispatches fairly, leases jobs, and records how each resolves. It does
not run workers, retry, dead-letter, or orchestrate workflows -- those belong to
the caller above the primitive.

## The model in brief

- **Virtual-time fairness.** Each job gets a virtual timestamp (`vstamp`) on its
  tenant's timeline; enqueue advances the tenant's `vtime` by `cost / weight`.
  Workers claim the lowest visible `(vstamp, job id)`. Heavier weight and cheaper
  jobs sort earlier; a tenant flooding work pushes its own later jobs back.
- **Stamp at enqueue, claim at dispatch.** Fairness is computed on the
  lower-contention enqueue path; dispatch is a single atomic "claim the minimum"
  (`FindOneAndUpdate`).
- **Node-local cache.** Per-tenant `vtime` is rebuildable soft state; durable
  job records carry the authoritative stamps. Periodic reconciliation bounds
  drift.
- **Lease, not removal.** A claim attaches a fenced `claim_id` and pushes
  `visible_at` forward; the job stays pending. Expired leases re-enter the
  visible set, so reclaim is lazy (no sweeper). Terminal writes guard on the
  fence -- exactly one resolution wins.
- **Two orthogonal axes.** *Tenancy* (fairness on/off; single-tenant is the
  one-tenant degenerate case, same code path) and *partitioning* (scoped claims;
  unpartitioned is one default partition). Two-stage stamp partitioning is the
  fallback when single-writer-per-tenant cannot be guaranteed at enqueue.

## Mongo/Go specifics

- **Envelope/body split.** The library owns the envelope (`JobID`, `TenantID`,
  `Cost`, `Partition`, `VStamp`, `Liveness`, `Resolution`, `ClaimID`,
  `VisibleAt`, `Attempts`, `StampedBy`, `ResolvedAt`, `Kind`); the body is the
  caller's domain data, stored as a native BSON subdocument (`bson.Raw`) --
  opaque to queue code, queryable in the database.
- **Virtual time is fixed-point.** VStamps/vtimes are `int64` in 1e-6 units;
  stride = `max(1, cost*SCALE/weight)`, weight an `int64` per-enqueue field
  (zero means 1). Weight consistency across a tenant's producers is a caller
  obligation, like single-writer.
- **Typed facade over an opaque core.** Generics live only at the API boundary
  as per-kind bindings (`NewKind[T](kind)` returns a `Kind[T]` whose
  `Enqueue`/`Decode` methods carry the kind↔type pairing declared at one
  site); storage and the
  queue core are non-generic. One queue carries unlimited heterogeneous kinds
  over a shared fairness timeline.
- **Authorization asymmetry in signatures.** `Complete`/`Heartbeat`/`Release`
  ride on the `ClaimedJob` handle and capture the minted fence; `Cancel` stays
  queue-level (external caller, no lease, guards on pending liveness alone).
  `Release` (early nack with caller-chosen delay) clears the fence -- the one
  transition that would otherwise leave a stale claim id live.

## Documentation index

- [docs/fair-job-queue-design.md](docs/fair-job-queue-design.md) -- the
  generic, storage-agnostic design: goals, scheduling model, cache and
  reconciliation, stamp partitioning, job lifecycle and leasing, storage
  contract, correctness invariants, rejected alternatives. Specifies *what* and
  *why*, not *how*.
- [docs/mongodb-go-library-design.md](docs/mongodb-go-library-design.md) --
  the Go API and MongoDB storage design: the envelope/body split, BSON-native
  body storage, the `Job` record, the `Enqueue`/`Claim`/`Cancel` API, and open
  questions.
- [docs/rejected-designs.md](docs/rejected-designs.md) -- Go/Mongo design
  alternatives considered and rejected (body typing options, body storage
  forms, weight resolver, automatic index creation), with reasoning.
- [docs/implementation-plan.md](docs/implementation-plan.md) -- phased,
  iterative build order with per-phase tasks, test tasks, and verification
  checklist; per-phase TODO files are expanded from it just-in-time.

**Always keep this index current: whenever a file is added to or removed from
`docs/`, update the links above in the same change.**
