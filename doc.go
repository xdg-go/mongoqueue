// Package mongoqueue implements a fair, multi-tenant job queue on MongoDB.
// It is a durable lease primitive: callers enqueue jobs and run their own
// workers, and the queue dispatches fairly, leases jobs, and records how each
// one ends. It does not run workers, retry, or dead-letter.
//
// # Model
//
// Every job is stamped at enqueue with a virtual timestamp on its (tenant,
// partition) timeline, which advances by cost/weight per job. [Queue.Claim]
// takes the lowest visible stamp in a partition, so heavier weight and cheaper
// jobs run sooner and a tenant that floods work pushes back only its own later
// jobs. Fairness is memoryless: a new or returning tenant starts at the
// partition's dispatch frontier, earning no credit for idle time and paying no
// penalty behind other tenants' backlogs.
//
// Fairness is computed at enqueue so that dispatch is one atomic
// claim-the-minimum write. A claim leases the job rather than removing it: the
// job stays pending under a fresh fence and becomes visible again when the
// lease lapses, with no sweeper. Complete guards on that fence and Cancel on
// the job still being pending, so exactly one resolution wins.
//
// # Lifecycle
//
// Enqueue a body with [Kind.Enqueue]; [NewKind] pairs a kind name with its Go
// type, and [Kind.Decode] reads it back. A worker calls [Queue.Claim] to get a
// [ClaimedJob], then [ClaimedJob.Heartbeat] to extend the lease,
// [ClaimedJob.Release] to return the job early, or [ClaimedJob.Complete] to
// resolve it. [Queue.Cancel] resolves a pending job from outside any lease.
// [Queue.Get] reads current state.
//
// # Obligations
//
// The queue states its requirements rather than detecting violations.
//
//   - Call [Queue.EnsureIndexes] before use; the queue never creates indexes.
//   - Call [Queue.Reconcile] on a ticker to bound cached virtual-time drift.
//   - Call [Queue.EnsureTTLIndex] if resolved jobs should be deleted.
//   - Enqueue to each (tenant, partition) from a single writer ([EnqueueOpts]).
//   - Pass the same weight from all of a tenant's producers ([EnqueueOpts.Weight]).
//   - Use acknowledged writes, and w: "majority" for single resolution across failover ([New]).
//   - Use a read concern that observes the queue's own inserts ([New]).
//   - Give the collection the simple default collation ([New]).
//   - Do not set BSONOptions.OmitEmpty on the client ([New]).
//   - Make workers idempotent, because effects are at-least-once ([Queue.Claim]).
//   - Expect a delayed job to run ahead of its tenant's later work when it comes due ([EnqueueOpts.Delay]).
//   - Bound retries in the worker by checking [Job.Attempts].
package mongoqueue
