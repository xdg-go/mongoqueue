// Package mongoqueue implements a fair, multi-tenant job queue on MongoDB.
// Callers enqueue jobs and run their own workers; the queue stores the jobs,
// hands them to workers in a fair order, leases each job to one worker at a
// time, and records how each one ends. It does not run workers, retry failed
// jobs, or move them to a dead-letter queue.
//
// # Jobs and kinds
//
// Each job is one document in a MongoDB collection. The library owns the
// envelope: the job id, tenant, partition, and cost the caller supplies at
// enqueue, plus the fields the queue maintains for ordering and leasing (see
// [Job]). The rest is the payload: a kind name and a body. The body is the
// caller's own data, stored as a BSON subdocument that the queue never reads.
//
// A [Kind] ties a kind name to the Go type of its body. Declare each one once
// with [NewKind], typically as a package-level variable. [Kind.Enqueue] stores
// a body of that type, and [Kind.Decode] reads it back, failing if the job
// was enqueued under a different kind. One queue can carry jobs of many kinds.
//
// # Fair ordering
//
// Each tenant has its own virtual clock in each partition. Enqueue stamps a
// job with the tenant's virtual time, then moves that clock forward by the
// job's cost divided by the tenant's weight. [Queue.Claim] hands out the job
// with the lowest stamp in a partition. A tenant with a higher weight moves
// its clock forward more slowly, so its jobs keep lower stamps and are
// claimed more often; for the same reason, cheap jobs move a clock less than
// expensive ones. A tenant that enqueues a flood of work pushes back only its
// own later jobs.
//
// Fairness has no memory. A tenant that is new, or returns after being idle,
// starts at the point the partition is currently serving: it gets no credit
// for the time it was idle and does not wait behind other tenants' backlogs.
// Ordering is decided at enqueue, so a claim is a single atomic update.
//
// # Leases
//
// A claim does not remove the job. It leases it: the job is hidden from other
// workers until the lease expires, and it gets a new claim ID. The worker
// then extends the lease with [ClaimedJob.Heartbeat], gives the job back early
// with [ClaimedJob.Release], or finishes it with [ClaimedJob.Complete]. These
// succeed only while the job still carries the worker's claim ID. If the
// lease expires, the job becomes claimable again without any background
// process, and the next claim replaces the claim ID, so a stalled worker
// cannot complete a job that another worker now holds.
//
// [Queue.Cancel] resolves a pending job from outside any lease. Whether a job
// ends by Complete or Cancel, only one resolution is recorded. [Queue.Get]
// reads a job's current state.
//
// # Caller requirements
//
// The library does not check or enforce the requirements below. If a caller
// ignores one, the queue can misbehave without returning an error.
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
