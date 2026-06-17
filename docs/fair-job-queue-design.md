# Fair Multi-Tenant Job Queue — High-Level Design

## Purpose and scope

This document describes the behavior a multi-tenant fair job queue library
must exhibit. It defines goals, the scheduling model, the node-local cache
mechanism, stamp partitioning, the job lifecycle and lease model, supporting
behaviors, the storage contract, and correctness invariants. It specifies
*what* the system does and *why*, not *how* to build it.

The two centerpieces are the **node-local cache model** for virtual-time
fairness and the **stamp partitioning** that makes fairness hold when the
system cannot control how work arrives. The **job lifecycle and lease model**
is the scaffolding beneath dispatch: how a job is claimed, kept alive,
reclaimed, and resolved.

## Goals

- Provide a durable, multi-tenant job queue consumed by a shared pool of
  concurrent workers (externally provided by library consumers).
- Allocate worker capacity among tenants in proportion to weight, measured by
  work (job cost), not job count.
- Minimize persistent state, cross-node coordination, and storage round trips.
- Minimize the conditions a caller must satisfy for fairness to hold. A
  generic library cannot assume control over load balancing or connection
  routing.
- Degrade gracefully. Approximation loosens fairness within bounds; it never
  starves a tenant or corrupts ordering.
- Stay portable across storage backends wherever the fairness model permits.

## Non-goals

- **Strict per-tenant FIFO.** Concurrent workers over jobs with varying
  durations may not execute in FIFO fashion.
- **Exact instantaneous fairness.** The system targets bounded long-run
  fairness with tunable staleness.
- **Preemption.** Jobs run to completion. The design bounds the damage long
  jobs cause rather than interrupting them.
- **Outcome handling.** The queue records how a job terminates but does not
  act on it. Retry orchestration, dead-lettering, compensation, and workflow
  continuation are caller responsibilities layered above the primitive. The
  library's only obligation is that termination records a resolution. This is
  the primitive-versus-engine boundary: a workflow engine can be built atop
  this queue, with the resolution load-bearing for what runs next; from the
  queue's vantage, that is user behavior.

## Core model: virtual-time fair queueing

Each job carries a virtual timestamp (**vstamp**) marking a position on its
tenant's virtual timeline. Enqueuing a job advances the tenant's virtual time
(**vtime**) by a **stride** equal to cost divided by weight, and the job takes
the new vtime as its vstamp. Workers dispatch jobs in vstamp order, lowest
first.

Fairness emerges from the advance rate. A heavier weight advances vtime more
slowly per unit work, yielding lower stamps and earlier service. A tenant
submitting more work races its vtime ahead, pushing its later jobs behind its
peers — automatic backpressure. A cheaper job advances vtime less, so small
jobs receive near-term stamps and jump ahead, gaining low latency without
claiming throughput share. Sorting by vstamp serves jobs in the order an
idealized fair server would finish them.

Cost may be known at enqueue or estimated; the cost section below covers
estimation.

For a given tenant, we define the trailing edge as follows:

* **Trailing edge** = its highest pending vstamp = its current vtime, the most recently enqueued job (last to be served).

A tenant without pending jobs has an undefined trailing edge.

## Two orthogonal axes: tenancy and partitioning

The queue exposes two independent capabilities. They compose, and a deployment
selects each separately.

**Tenancy — whether fairness is on.** A *multi-tenant* queue carries a tenant id
and a cost on every job, stamps vstamps on per-tenant timelines, and dispatches
by minimum vstamp to divide capacity by weight. A *single-tenant* queue is the
degenerate case of exactly one tenant: vtime advances uniformly, the vstamp
reduces to an enqueue sequence, and dispatch is FIFO over the visible set.
Fairness is then trivially satisfied — one tenant holds the whole share — so the
fairness machinery is a no-op, not a separate mode. Single-tenant is
multi-tenant with one tenant, the same code path.

**Partitioning — whether claims are scoped.** An *unpartitioned* queue is a
single default partition. A *partitioned* queue routes jobs to named partitions
and scopes each claim to one. Partitioning narrows a claim's candidate set; it
does not change the ordering rule within a partition.

The axes are independent and serve different cases:

- *Multi-tenant, unpartitioned* — the canonical fair queue.
- *Multi-tenant, partitioned* — a fair queue with size-class lanes; fairness
  holds within each lane (see Correctness invariants).
- *Single-tenant, partitioned* — a durable lease queue sharded or laned for
  throughput, with no fairness.
- *Single-tenant, unpartitioned* — a plain durable lease queue.

The two-stage stamping pipeline (below) is itself assembled from these: stage
one is a *single-tenant, tenant-hash-partitioned* queue — fairness off — feeding
stampers that emit into a *multi-tenant* fair queue.

## Stamp at enqueue, not at dispatch

The system computes a job's vstamp when it enqueues the job and stores the
stamp on the durable record. Dispatch then reduces to claiming the lowest
visible vstamp. This moves fairness computation and contention off the hot
dispatch path, where many workers contend, onto the enqueue path, where a
tenant's own writers contend far less. It also eliminates explicit
non-empty-queue detection: an index ordered by vstamp over visible jobs skips
idle tenants for free.

## Node-local cache model

Per-tenant vtime lives as in-memory soft state on the node that stamps the
tenant's jobs — the enqueuing node in single-stage operation, or the
partition's owning stamper under two-stage partitioning (below). Stamping
advances the local vtime and writes the vstamp onto the job. Stage-one
ingestion nodes hold no vtime; they only append raw requests. The durable job
records, carrying their vstamps, are the authoritative state; the cache is a
rebuildable accelerator, never the source of truth.

### Reconciliation

A node periodically rebuilds its cache from durable state, bounding any drift
between cache and truth. Reconciliation rebuilds each tenant's vtime — the
cache's soft state — from its pending jobs (pending includes leased, in-flight
jobs, which are still owed service). Two reconstructions of that vtime trade
cost against robustness:

- **Trailing edge** (the tenant's highest pending vstamp): cheap, and
  self-corrects trailing job cancellation, since a cancelled top job drops the
  max. Trusts the stored stamps, so correct only under
  single-writer-per-tenant.
- **Backlog** (LWM plus the sum of the tenant's pending strides): re-derives
  vtime from queued work rather than stored stamps, bounding multi-writer
  compression to one interval and flooring a drained tenant automatically.
  Costs a per-tenant aggregation.

This model chooses to use **trailing edge**, with the expectation that
single-writer-per-tenant is satisfied or that the deviations described later
are acceptable.

A drained tenant has no pending jobs and therefore no reconstructed vtime; on
its next enqueue the system treats it as returning and uses the low-water mark
to seed its vtime. (See below.)

### Low-water mark and the idle floor

The **low-water mark** (LWM) is the **minimum active trailing edge** — the
lowest trailing edge among tenants with pending work, i.e. the least advanced
active tenant. The minimum ranges over the tenant set, not over nodes: the LWM
is computed periodically and cached locally on the stamping node, soft state
like vtime itself, not a value held consistent across the fleet. On enqueue the
system floors a tenant's vtime to
the LWM: vtime becomes the greater of its cached value and the LWM. This stops
a returning idle tenant, whose stale vtime sits far below other tenants, from
dominating resources to "catch up" on credit it never used.

The floor raises a lagging tenant only. It does not pull down a tenant whose
vtime sits too high — that case is cancellation.

### Cancellation and vtime

A cancelled job advanced vtime at enqueue but will never dispatch, stranding
vtime above the tenant's real backlog. Canceled jobs at the end of the queue
will be 'refunded' at the next reconciliation because reconstruction from the
pending set excludes cancelled jobs. Each reconciliation re-anchors vtime to
surviving work. The vtime of canceled jobs at the head or middle of the queue
are not recovered and the tenant will be underserved until lower vtime jobs
are processed. Because cancellation is expected to be rare, the complexity to
account for all forms of vtime 'refund' are not justified.

### Why a cache, and what it costs

The cache removes per-enqueue reads of durable state and per-dispatch
coordination. Its cost is bounded staleness: an enqueuing node's view lags
truth by up to one reconciliation interval, and fairness error is bounded
accordingly. Because the cache is rebuildable, node restart and partition
handoff are self-healing — a cold node reconstructs from durable stamps before
it stamps again.

## The conserved single-writer requirement

Virtual-time fairness requires that a single writer advance each tenant's
vtime. With several writers advancing independent copies from a shared
baseline, their stamps overlap the same virtual-time range instead of summing;
the tenant's trailing edge then advances too slowly and the tenant is over-served.

A caller satisfies this requirement at the enqueue boundary when either only
one node enqueues, or when enqueuers consistently hash tenants so each tenant
has one writer. When a caller cannot guarantee this — a generic library behind
an uncontrolled load balancer — the system may alternatively satisfy it
internally through two-stage stamp partitioning.

## Multi-writer failure analysis

When several nodes independently stamp one tenant, the tenant's stamps
**compress**: its trailing edge advances at roughly one over its fan-out — the
number of distinct writers — so it is over-served by that factor. The
consequence depends on whether fan-out is uniform across tenants.

- **Uniform fan-out** (every tenant sprayed across all nodes): compression is
  equal, so relative fairness survives in expectation. The absolute virtual
  clock merely runs slow. Costs are stamp collisions and higher variance for
  low-volume tenants.
- **Heterogeneous fan-out** (the balancer pins tenants to subsets, or fan-out
  scales with volume): compression differs per tenant, biasing service toward
  high-fan-out — typically larger — tenants. This is a genuine, persistent
  deviation from fairness, and reconstruction from trailing edge stamps does not
  repair it, because the stored stamps are already compressed.

## Two-stage stamp partitioning

When the enqueue boundary cannot guarantee one writer per tenant, nor uniform
fan-out, a caller may split ingestion from stamping. This is a workaround the
caller builds, not behavior the library performs: the library supplies the
primitives — a partition-routed durable queue, fenced exclusive ownership, and
keyed idempotent insert — and the caller assembles them into a stamping tier.
The library does not operate the stampers, schedule them, or decide partition
ownership.

**Stage one** accepts raw job *requests* on any node and appends them durably
to a queue, routed to a partition by a hash of the tenant id. The hash is a
stateless function any node computes locally, so stage one imposes no
condition on connection routing or load balancing. Each request carries the
caller-supplied job id, which the stamper later writes onto the stamped record
unchanged. Stage one carries no fairness logic: it is a **single-tenant**
queue whose only "tenant" is the ingestion stream itself. The caller-supplied
tenant id rides along as a payload field and as the partition routing key,
never as a fairness dimension — stage one neither stamps vstamps nor advances
any per-tenant vtime. Multiple writers per partition are acceptable. The
multi-tenant timeline begins only at stage two. The cost of stamping is
assumed marginal, and fairness goals apply to the stamped work, not the
stage-one queue.

**Stage two** assigns vstamps. Each partition has one exclusive owning
**stamper**, which consumes its partition's requests, advances per-tenant
vtime as the single writer for those tenants, and emits stamped jobs into the
fair queue. Single-writer correctness now holds by construction, independent
of how requests arrived.

This relocates the single-writer requirement from a boundary the caller cannot
control to a partition the caller controls via library primitives. The only
condition the library imposes is a tenant id — irreducible for tenant fairness.

Three behaviors the caller's stamping stage must exhibit. The library makes
each enforceable but does not implement them:

- **Interleave co-partitioned tenants.** Hash collisions place several tenants
  in one partition. A stamper that drains one tenant's backlog before reaching
  another starves the latter at the stamping gate, because unstamped requests
  are invisible to the fair queue downstream. The stamper must round-robin (or
  deficit-round-robin) across the tenants present in its partition. This
  interleaving needs no virtual time; it only prevents monopoly of the
  stamper's read cursor.
- **Fence ownership handoff.** Partition reassignment — a stamper added,
  removed, or recovered — is the only window where two stampers might own one
  partition and reintroduce compression. A fenced handoff (ownership
  revocation plus a monotonic token that rejects the stale owner) bounds this
  to rare, controlled transitions rather than a per-request hazard.
- **Emit idempotently.** A stamper that emits a stamped job and then crashes
  before retiring the request will, on recovery, re-read the request and
  re-emit — duplicating the job and advancing vtime twice, a single-writer
  violation against its own restart. Carrying the job id from request through to
  the stamped record makes the emit a keyed insert: the re-emit conflicts on the
  job id and is a no-op. This is why the job id originates at stage-one
  admission, not at stamp time.

Two-stage adds a durable write and a latency hop before a job becomes
dispatchable. It is therefore the **fallback**, engaged only when
single-writer-at-enqueue cannot be guaranteed, not the default.

## Partitioning as an orthogonal primitive

Partition-and-claim — route records to partitions, let a worker claim only
within a partition — is a mechanism distinct from virtual-time fairness, which
is policy layered above. Every job carries a **partition** field and every
claim names exactly one partition; an unpartitioned queue is simply a single
default partition, so the claim path is identical whether or not a caller uses
partitions. Within a partition the claim still takes the minimum `(vstamp, job
id)` among visible records — partitioning narrows the claim's candidate set, it
does not change the ordering rule. The system could use this primitive for
various situations that might benefit. E.g.

- **Tenant-hash partitions** for stamping shards. These require **exclusive**
  ownership: one consumer per partition, or compression returns.
- **Size-class lanes** that keep long jobs from blocking short ones. These
  require **shared** consumers: many workers per lane, since the lane is a
  blocking-avoidance filter, not a single-writer constraint.

Partitioning is a feature that is orthogonal to job lifecycle and leasing.

## Job lifecycle and leasing

A job's durable record carries a **liveness** status and, once terminal, a
**resolution**. Liveness takes two values: **pending** (awaiting or undergoing
service) and **resolved** (terminal). The system collapses every terminal
outcome into the single resolved status and records the outcome separately as
a resolution set by the caller.

Resolution is scaffolding the queue establishes on behalf of the caller but
does not interpret. Termination *requires* recording a resolution. Acting
on a resolution — retrying, dead-lettering, continuing a workflow — belongs to
the caller above. The queue records *how* a job ended, never *what happens
next*.

A single terminal status makes the no-double-resolution property structural:
exactly one terminal write succeeds per job. Concurrent terminal attempts race
on a guard that admits the first and rejects the rest, and the loser observes
the winner's resolution.

### Claims, visibility, and three sets

Dispatch does not move a job out of pending. A **claim** instead attaches a
**claim id** to the pending record and pushes a **visibility time** into the
future; the job stays pending. This factoring forces a distinction the bare
fairness model elided — between jobs that exist, jobs that can be claimed, and
jobs in flight:

- **pending** — every non-terminal job. Reconstruction and the LWM read this
  set. A leased, in-flight job belongs here: it is still owed service, and
  excluding it would let reconciliation re-floor the tenant as if drained,
  losing the tenant's place.
- **visible** — pending jobs whose visibility time has passed. This is the
  claim set: the dispatch index orders by vstamp over visible jobs within the
  claimed partition and claims the minimum.
- **leased** — pending jobs whose visibility time lies in the future. In
  flight, excluded from claim, still counted as backlog.

A fresh job is visible at once; a claimed job is invisible until its lease
lapses; a lapsed lease makes the job visible again. *Visible* therefore
unifies never-claimed and lease-expired under one predicate — pending and
visibility-time elapsed — so the claim path needs no separate reclaim branch
and no knowledge of whether it is claiming fresh work or stealing.

Reclaim preserves the job's original vstamp. Re-stamping would charge the
tenant's vtime twice for one job and would be a single-writer violation by a
worker that is not the tenant's stamper. Consequently lease expiry, stealing,
and heartbeats are **not** virtual-time events. Enqueue is the only event that
advances vtime.

### Heartbeats and fencing

A worker holding a lease **heartbeats** to push its visibility time further
out, so a long job is not stolen mid-flight. The heartbeat extends the lease
only while the claim id still matches; a heartbeat that fails to extend tells
the worker its job was stolen or cancelled.

The claim id is a **fence**, and its load-bearing role is to make the durable
store reject a stale holder's *terminal* write, not merely its heartbeat. Were
only the heartbeat fenced, a worker stolen from — paused past its visibility
time, never heartbeating again — could still finish and record completion
under its dead claim, double-resolving the job. Every lease-touching write
therefore guards on the claim id and on pending liveness: extend, complete,
and the cancellation a worker discovers. Heartbeat failure is the
**detection** path that lets a preempted worker abort early and save wasted
effort; the fence on the terminal write is the **safety** property. The two
are independent, and only the second is required for correctness — heartbeats
are a latency optimization, not a safety mechanism.

Claim ids must be unique per claim and never reused, so a stale holder's id
can never match a later claim.

### Job identity

Every job carries a caller-supplied **job id**, stored as the durable record's
primary key (the `_id`). It is the job's stable identity for its whole life: it
does not change across reclaim, steal, or retry, and it is distinct from the
claim id. The claim id is a per-claim fence that rotates on every claim; the job
id names the job itself.

The job id is the caller's handle for the operations the queue exposes but does
not drive — **cancel** (an external caller naming the job to resolve) and
resolution lookup (polling how a job ended). Because the caller supplies it,
enqueue is **idempotent**: submitting under an id already present is a no-op
insert, not a duplicate, so a caller can safely retry an enqueue whose outcome
it never observed. It is likewise the natural key for the effect-level
deduplication job bodies must perform, since delivery is at-least-once, not
exactly-once.

The job id also completes the dispatch order. Within a tenant, vstamps strictly
increase — each enqueue advances vtime by a positive stride — so a tenant's own
jobs never tie. Ties arise only across tenants, when two hold a job at the same
vstamp; both are equally entitled at that virtual instant, so any rule ordering
them is fair. The dispatch index is therefore ordered by `(vstamp, job id)`: the
job id breaks ties deterministically, giving a definite minimum and a total
order, while the tie itself is fairness-neutral. That the caller supplies the
tiebreaking id is harmless — it arbitrates only between equally-entitled jobs of
*different* tenants, never the order of a tenant's own work.

### Reclaim by expiry, not by sweep

Because a lapsed lease re-enters the visible set, reclaim folds into the
existing claim of the minimum visible vstamp. No separate sweeper process
scans for dead leases; the next claimer reclaims an expired job without
knowing it did. This relocates reclaim work from a background scan onto the
claim and heartbeat paths the system already runs.

The visibility timeout must be tuned. Too short a timeout steals from
slow-but-live workers; too long delays recovery of genuinely dead ones.
Heartbeats let the timeout stay short relative to total job runtime while
still tolerating long jobs, since each heartbeat re-extends.

### Termination, cancellation, and races

Two transitions reach the resolved status, differing only in authorization:

- **Complete** is issued by the lease holder and guards on a matching claim
  id. Only the worker doing the work resolves it as done.
- **Cancel** may be issued externally, by a caller holding no lease, and
  guards on pending liveness alone. An in-flight worker is not stopped
  directly; it discovers the cancellation at its next heartbeat or terminal
  write, both of which fail the pending guard. Cancellation is therefore
  cooperative — the worker aborts within one heartbeat interval, or its
  terminal write is rejected and its output discarded.

ClaimID need not be cleared on cancellation as long as heartbeats and terminal
writes guard on pending state as well as ClaimID.  If ClaimID has semantic
meaning (e.g. a unique worker ID), cancelation preserves such information for
observability purposes.

From the worker's vantage, a steal and a cancel are the same fence failure,
distinguished only by what it reads back. Both land in resolved with a
recorded resolution, and the sink's mutual exclusion guarantees a single
winner. Cancellation is not a virtual-time event: the cancelled job leaves the
pending set, and reconciliation may re-anchor the tenant's vtime within one
reconciliation interval, as described above.

### Attempts and poison jobs

Each claim increments a durable **attempt count** on the record.
Steal-on-expiry retries crashed work for free, which is the intent; absent a
bound, a job that crashes its worker every time would retry forever and burn
capacity. A worker should read the attempt count on claim and, past a
worker-configured threshold, resolve the job terminally with an exhaustion
resolution rather than running it again. The queue supplies the count; the
retry count checking and what becomes of an exhausted job — a dead-letter
lane, an alert, a manual replay — is the caller's responsibility.

### Effects are at-least-once

A lease bounds duplicate *dispatch* and *termination*; it cannot bound duplicate
external *effects*. A worker paused past its visibility time may already have
acted on the world — sent the email, changed a database — before its fenced
terminal write is rejected. The fence protects the queue record's consistency,
not side effects. Job bodies must therefore be idempotent or carry their own
effect-level deduplication: the queue guarantees at-least-once delivery, never
exactly-once execution.

## Supporting behaviors

**Cost and estimation.** When cost is known at enqueue, the vtime stride is
exact. When unknown, the caller provides an estimate. Estimation errors are
uncorrected. Recording actual cost and updating vtimes or pending vstamps adds
complexity and coordination costs inconsistent with the design goal. Cost must
be strictly positive — the system floors it to a small minimum — so every
stride advances vtime.

**Long, non-preemptible jobs.** Worst-case unfairness is bounded by the
largest job cost times the worker count. A caller could bound it further with
size-class lanes, runtime caps, and per-tenant concurrency caps.

**Sparse-tenant responsiveness.** The idle LWM floor gives a returning or low-rate
tenant prompt service rather than burying it behind a heavy backlog — the
property that keeps interactive work responsive amid batch floods.  A
returning tenant is floored to the LWM — the lowest trailing edge among active
tenants — so it queues with priority comparable to the least advanced active
tenant, but does not otherwise jump the queue.

**Work conservation.** The system never idles a worker to preserve fairness.
It dispatches whatever is runnable and corrects share retroactively; idle
capacity costs more than transient unfairness.

**At-least-once delivery.** Claims are leases reclaimed by expiry, not
removals; resolution is the fenced terminal write. The job lifecycle and
leasing section specifies both. Delivery is at-least-once and execution is not
exactly-once, so job effects must be idempotent.

## Storage requirements

The storage backend must provide three behaviors:

1. **Durable insert** of a single record, keyed by job id, rejecting a
duplicate id. The duplicate-key rejection is the basis for idempotent enqueue
and for idempotent stamper re-emit.
2. **Atomic conditional transition** of a single record — claim, heartbeat,
complete, cancel expressed as guarded state changes, where guards include
claim-id fencing and pending-state checks.
3. **Ordered claim of the minimum key** — `(vstamp, job id)` — among
**visible** records **within a named partition** — pending records in that
partition whose visibility time has elapsed — with mutual exclusion under
concurrent claimers. A claim names exactly one partition; an unpartitioned
queue uses a single default partition, so the claim path is identical either
way. A claim is itself an atomic transition of the selected record. The job id
breaks vstamp ties to give a total order; the tie order is fairness-neutral
(see Job identity).

The job lifecycle and lease model add no requirements beyond atomic update.
Visibility is a predicate on the visible set (behavior three). Claim-id
fencing and attempt-count increment are guarded conditional transitions
(behavior two). Heartbeats are likewise guarded conditional transitions.

The third behavior is the **portability boundary**. Transactional indexed
stores provide it; pure message queues do not, because they own their ordering
and cannot claim the minimum by an arbitrary mutable key. The library is
therefore portable across indexed stores and inexpressible on append-only or
fixed-order queue services, which can offer only native ordering or partition
routing.

**Note — garbage collection (not a required behavior).** Neither the store nor
the cache is self-pruning. Resolved job records (completed or cancelled,
including attempt-exhausted) accumulate unless reclaimed, and the per-tenant
cache map grows with every tenant ever seen, retaining entries for tenants
long gone idle. Neither threatens correctness — the design tolerates both —
but both grow unbounded without a sweep. (An expired lease is not a resolved
record; it re-enters the visible set and still dispatches, so it is not GC's
concern.) An implementation should reclaim resolved records on some cadence
and evict idle-tenant cache entries (an evicted tenant simply re-floors to the
LWM on its next enqueue, the onboarding path, so eviction is safe). Treat this
as an operational obligation to specify, not a fairness mechanism.

## Correctness invariants

A model or test suite should establish:

- **Fair share (within a partition):** in a multi-tenant queue, among
  continuously backlogged tenants whose visible jobs share a partition,
  cumulative work served approaches the weight-proportional share within a
  bounded error. Fairness is a within-partition property.
- **No starvation:** every enqueued job in a partition with a live consumer
  eventually dispatches. Within a partition no tenant's backlog can indefinitely
  defer another's, and no job is passed over forever. The guarantee is
  per-partition: a partition with no consumer (e.g. an unmanned size-class lane)
  starves its jobs, which is an operational obligation to staff every partition,
  not a property the queue can supply.
- **No duplication:** no job dispatches twice within the duration of a lease;
  the lease plus conditional transition admits one claimer at a time.
- **Single resolution:** exactly one terminal write succeeds per job. A stale
  lease holder — stolen from or cancelled out from under — cannot resolve the
  job, because the fence rejects its terminal write.
- **Lease accounting:** an in-flight (leased) job counts toward its tenant's
  backlog; reclaim preserves the job's original vstamp; lease expiry,
  stealing, and heartbeats do not alter virtual time.
- **Bounded retries:** a job that repeatedly fails its worker is resolved as
  exhausted at a finite attempt threshold, never retried forever. (This
  property must be enforced within the worker; the library only provides the
  supporting attempt count.)
- **No loss:** a job persists until resolved.
- **Work conservation:** no worker idles while a dispatchable job exists.
- **Bounded staleness:** cache divergence from durable truth, and therefore
  fairness error, is bounded by the reconciliation interval.
- **Bounded degradation:** under uncontrolled writer distribution, fairness
  error is bounded by fan-out in the uniform case, or relocated to
  within-interval compression by stamp partitioning; no distribution starves a
  tenant.

## Rejected alternatives

**Dispatch-time stamping.** Computing vstamps when claiming jobs places
fairness computation and shared-state contention on the hot path every worker
traverses. Enqueue-time stamping moves it to the lower-contention enqueue
path.

**Backlog ceiling clamp.** Bounding vtime by an estimated ceiling drawn from
periodic aggregation was considered and rejected. A stale ceiling cannot
distinguish vtime rising from real new work from vtime stranded by
cancellation, so it squashes legitimate bursts; maintaining it between
refreshes reintroduces the coordination the design avoids.

**Refund-on-cancel.** Decrementing a tenant's vtime by a cancelled job's
stride at cancel time was considered and dropped. It wanted per-cancel
bookkeeping and, for co-located exactness, a multi-record transaction the
contract otherwise avoids. Cancellation is expected to be rare — a rare,
bounded, self-correcting error not worth the machinery.

**Sweeper-based lease reclaim.** A background sweeper that scans claimed jobs
and releases expired leases was considered and rejected. Steal-on-expiry
reclaims lazily on the claim path the system already runs, removing a
coordinating background process and its periodic global scan, at the cost of
per-job heartbeat writes and a tuned visibility timeout. The trade favors the
design's goal of minimal cross-node coordination.

**Distinct terminal states.** Modeling completed, cancelled, and failed (and
timed-out, exhausted, and so on) as separate durable states — as workflow
engines do — was considered and rejected for this primitive. Those engines
branch on the terminal state: cancellation runs compensation, failure drives
retry policy, and result APIs decide error propagation by terminal-state
membership. This queue owns none of that. It dispatches fairly and leases
durably, and treats the terminal outcome as recorded metadata, not a
control-flow input. Collapsing liveness to pending-or-resolved while recording
the outcome as a closed resolution keeps the state machine minimal and
preserves the distinction for any engine built above. The boundary is a
deliberate scope choice: were the library to take on retry, dead-lettering, or
result propagation, distinct terminal states would re-earn their place, and
this decision should be revisited then.

**Stochastic fairness by hashing tenants to buckets.** Hashing bounds per-flow
state in network schedulers facing vast, short-lived flow counts. Here tenant
state is already small and the job records hold it; hashing adds
bucket-collision unfairness without removing the hard parts.

**Message-queue backends.** Systems that own their ordering cannot express
claim-by-minimum-vstamp and so cannot carry the fairness model.
