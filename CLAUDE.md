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
  `VisibleAt`, `Attempts`, `Kind`); the body is the caller's domain data, stored
  as a native BSON subdocument (`bson.Raw`) -- opaque to queue code, queryable in
  the database.
- **Typed facade over an opaque core.** Generics live only at the API boundary
  (`Enqueue[T]`, `Decode[T]`) keyed by a stable `Kind` string; storage and the
  queue core are non-generic. One queue carries unlimited heterogeneous kinds
  over a shared fairness timeline.
- **Authorization asymmetry in signatures.** `Complete`/`Heartbeat` ride on the
  `ClaimedJob` handle and capture the minted fence; `Cancel` stays queue-level
  (external caller, no lease, guards on pending liveness alone).

## Documentation index

- [docs/fair-job-queue-design.md](docs/fair-job-queue-design.md) -- the
  generic, storage-agnostic design: goals, scheduling model, cache and
  reconciliation, stamp partitioning, job lifecycle and leasing, storage
  contract, correctness invariants, rejected alternatives. Specifies *what* and
  *why*, not *how*.
- [docs/mongodb-go-library-design.md](docs/mongodb-go-library-design.md) --
  Go-specific decisions for the MongoDB implementation: the envelope/body split,
  BSON-native body storage, the `Claim` signature, and exported-field rationale.

**Always keep this index current: whenever a file is added to or removed from
`docs/`, update the links above in the same change.**
