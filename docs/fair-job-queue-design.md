# Fair Multi-Tenant Job Queue -- High-Level Design

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
timeline -- its tenant in its partition (below). Enqueuing a job advances that
timeline's virtual time (**vtime**) by a **stride** equal to cost divided by
weight, and the job takes the new vtime as its vstamp. Workers dispatch
jobs in vstamp order, lowest first.

Fairness emerges from the advance rate. A heavier weight advances vtime more
slowly per unit work, yielding lower stamps and earlier service. A tenant
submitting more work races its vtime ahead, pushing its later jobs behind its
peers -- automatic backpressure. A cheaper job advances vtime less, so small
jobs receive near-term stamps and jump ahead, gaining low latency without
claiming throughput share. Sorting by vstamp serves jobs in the order an
idealized fair server would finish them.

**Fairness is memoryless.** Capacity in a partition divides among the tenants
that have pending work there, by their present weights. Past idleness earns a
tenant no credit and costs it no penalty. Within a tenant, dispatch is FIFO.
Across tenants, already-enqueued work is slowed by another tenant's arrival,
never jumped: a tenant that becomes backlogged is placed one stride past the
last job the partition served, exactly where it would be had it been
active all along and just finished its previous work. This is the placement
rule of start-time fair queueing (`start = max(own finish, system virtual
time)`); the low-water mark below is the system virtual time. The floor sets
where a returning tenant *starts*; its stride sets its *share* from then on,
so a returning tenant that floods interleaves with incumbents at its weighted
share from its first job rather than catching up on unused credit or waiting
behind their backlogs.

Cost may be known at enqueue or estimated; the cost section below covers
estimation.

**Weight input.** Weight is supplied by the caller on each enqueue, not
registered with the queue; callers that manage weights centrally build their
own registry above the API. Fairness therefore rests on a caller obligation:
all enqueues for a tenant must carry a consistent weight over any interval
where its share should be well-defined. Inconsistent weights blend the
tenant's stride rate, distorting its share much as multi-writer stamping does;
this obligation stands alongside the single-writer requirement. A weight
change takes effect on subsequent stamps only -- stamps are immutable, so a
mid-backlog change leaves already-stamped jobs in place, a bounded transition
effect (a raised weight underserves the tenant until its old stamps drain).

**Virtual-time representation.** Virtual time is fixed-point: vstamps and
vtimes are 64-bit integers in units of 1/SCALE, with SCALE = 10^3. The stride
is `max(1, cost * SCALE / weight)` in pure integer arithmetic (weight is a
positive integer; zero means one). The floor guarantees every enqueue advances
vtime -- preserving the invariant that a tenant's vstamps strictly increase --
and binds only when `cost < weight/SCALE`.

SCALE divides the 64-bit range between sub-unit precision and headroom. Cost
units are abstract, so SCALE and the caller's choice of unit are
interchangeable: SCALE only sets the precision a small integer cost gets.
Truncation error in a stride is below `weight / (cost * SCALE)`, under 1%
while weight is at most ten times cost; callers who need finer weight ratios
express cost in finer units. Headroom is about `2^63 / SCALE` (9.2 x 10^15)
cumulative cost per unit weight. Because memoryless placement carries every
new timeline up to its partition's frontier, that budget belongs to the
partition's virtual clock, which advances while the partition has work. At a
sustained 10^6 cost-units per second at weight 1 it lasts about 292 years; at
10^3 per second, about 292,000. Stamp arithmetic is checked: an enqueue whose
stamp would exceed the range fails rather than wrapping to a negative stamp
that would sort ahead of every pending job.

Floating-point is rejected for the stamp: it puts rounding and comparability
hazards inside a sorted index key for no expressive gain.

Virtual time is kept per **timeline**, and a timeline is one tenant in one
partition (see the next section). For a given timeline:

* **Trailing edge** = its highest pending vstamp = its current vtime, the most
  recently enqueued job (last to be served).

A timeline without pending jobs has an undefined trailing edge.

## Two orthogonal axes: tenancy and partitioning

The queue exposes two independent capabilities. They compose, and a deployment
selects each separately.

**Tenancy -- whether fairness is on.** A *multi-tenant* queue carries a tenant id
and a cost on every job, stamps vstamps on one timeline per tenant and
partition (below), and dispatches
by minimum vstamp to divide capacity by weight. A *single-tenant* queue is the
degenerate case of exactly one tenant: vtime advances uniformly, the vstamp
reduces to an enqueue sequence, and dispatch is FIFO over the visible set.
Fairness is then trivially satisfied -- one tenant holds the whole share -- so the
fairness machinery is a no-op, not a separate mode. Single-tenant is
multi-tenant with one tenant, the same code path.

**Partitioning -- whether claims are scoped.** An *unpartitioned* queue is a
single default partition. A *partitioned* queue routes jobs to named partitions
and scopes each claim to one. Partitioning narrows a claim's candidate set; it
does not change the ordering rule within a partition.

**Timelines are per tenant per partition.** Because every claim is scoped to
one partition, each partition is its own dispatch queue with its own point of
service. Virtual time follows that boundary: a tenant's vtime in one partition
is independent of its vtime in another, so each partition is a complete fair
queue over the tenants present in it, and a tenant's load in one partition
does not move its position in another. The axes stay independent -- a
single-tenant queue has one timeline per partition and an unpartitioned queue
has one timeline per tenant -- and the fairness invariants below are stated
per partition for this reason.

The axes are independent and serve different cases:

- *Multi-tenant, unpartitioned* -- the canonical fair queue.
- *Multi-tenant, partitioned* -- a fair queue with size-class lanes; fairness
  holds within each lane (see Correctness invariants).
- *Single-tenant, partitioned* -- a durable lease queue sharded or laned for
  throughput, with no fairness.
- *Single-tenant, unpartitioned* -- a plain durable lease queue.

The two-stage stamping pipeline (below) is itself assembled from these: stage
one is a *single-tenant, tenant-hash-partitioned* queue -- fairness off -- feeding
stampers that emit into a *multi-tenant* fair queue.

## Stamp at enqueue, not at dispatch

The system computes a job's vstamp when it enqueues the job and stores the
stamp on the durable record. Dispatch then reduces to claiming the lowest
visible vstamp. This moves fairness computation and contention off the hot
dispatch path, where many workers contend, onto the enqueue path, where a
tenant's own writers contend far less. It also eliminates explicit
non-empty-queue detection: an index ordered by vstamp over pending jobs, with
visibility checked in the index, skips idle tenants for free.

## Node-local cache model

Each timeline's vtime lives as in-memory soft state on the node that stamps
the tenant's jobs -- the enqueuing node in single-stage operation, or the
partition's owning stamper under two-stage partitioning (below). Stamping
advances the local vtime and writes the vstamp onto the job. Stage-one
ingestion nodes hold no vtime; they only append raw requests. The durable job
records, carrying their vstamps, are the authoritative state; the cache is a
rebuildable accelerator, never the source of truth.

### Reconciliation

A node periodically rebuilds its cache from durable state, bounding any drift
between cache and truth. Reconciliation rebuilds each timeline's vtime -- the
cache's soft state -- from that tenant's pending jobs in that partition
(pending includes leased, in-flight jobs, which are still owed service). Two
reconstructions of that vtime trade cost against robustness:

- **Trailing edge** (the timeline's highest pending vstamp): cheap, and
  self-corrects trailing job cancellation, since a canceled top job drops the
  max. Trusts the stored stamps, so correct only under
  single-writer-per-timeline.
- **Backlog** (LWM plus the sum of the tenant's pending strides): re-derives
  vtime from queued work rather than stored stamps, bounding multi-writer
  compression to one interval and flooring a drained tenant automatically.
  Costs a per-timeline aggregation.

This model chooses to use **trailing edge**, with the expectation that
single-writer-per-timeline is satisfied or that the deviations described later
are acceptable.

Trailing-edge re-seeding has one accepted side effect. A timeline's highest
job can dispatch before its lower ones when those are delayed or leased. Once
it resolves, the trailing edge drops below it, and the timeline's next job
can be stamped below a job already served. The over-service is small and
confined to one partition. The ordering guarantee is therefore that a new
stamp exceeds every *pending* stamp on its timeline, not every stamp the
timeline ever issued.

Reconciliation need not rewrite the cache eagerly. Marking each entry stale so
that its next enqueue re-runs the seed, under the same per-timeline exclusion
that protects stamping, achieves the same drift bound without a second writer
to the cache and without the fence an eager rewrite would need to avoid
lowering vtime below a concurrently inserted job. The re-seed sets vtime to
the greater of the trailing edge and the low-water mark (below); a timeline
with no pending jobs has no trailing edge and takes the low-water mark alone,
which is how a drained or returning tenant is placed. An entry enqueued to
since the previous reconciliation is marked stale; an entry untouched since
then is evicted, so an idle entry is evicted on the second consecutive
untouched pass, up to two intervals after its last enqueue. This bounds cache
memory; an evicted tenant is re-seeded the same way on its next enqueue.

### Low-water mark and the idle floor

The **low-water mark** (LWM) of a partition is its **dispatch frontier**,
defined as the partition's **service clock**: the maximum vstamp among the
partition's jobs that have ever been claimed. A claim sets the job's claim
fence; completion and cancellation keep it; only an early release clears it.
It is the system virtual time of start-time fair queueing (the start tag of
the work most recently put into service), and it is per partition because
each partition has its own point of service.

Only when nothing in the partition has ever been claimed, or its claimed
history has been garbage-collected, does the LWM fall back to the minimum
vstamp among the partition's pending jobs, visible or not. A partition with
neither -- no claimed history and no pending job -- has no frontier.

Visibility and lease timing do not enter the clock. A frontier read from the
visible set vanishes whenever workers keep up (every pending job leased) or
only delayed work remains, so newcomers fall to a stale value or 0 and jump
ahead of work already scheduled. A minimum over pending jobs lets one long
lease, a released retry, or a long delay pin the floor for its lifetime. The
maximum ever claimed advances only with service.

The clock overshoots in one accepted way. In a busy partition it sits about
one stride below the next visible job, which is negligible. When low-stamped
work re-enters the visible set -- an expired lease, a delayed batch coming
due, a released retry other than the top claimed job (see the dip below) --
the clock stays where service left it, and a newcomer lands behind that work: approximately FIFO, never credit.

The LWM is soft state like vtime itself, computed on the stamping node and not
held consistent across the fleet. A node refreshes a partition's LWM when it
first creates a timeline in that partition (so a new or returning tenant is
floored correctly even on a node that never reconciles) and on every
reconciliation. It is the last computed frontier, not a running maximum. The
clock is monotone with two exceptions. A release of the top claimed job is a
small dip. Garbage collection lowers the clock only once the partition's
claimed history is gone; the LWM then falls back to the minimum pending vstamp
(see above), which can be much lower. A computation that finds no frontier
leaves the previous value in place (0 on a node with none).

Refreshes are not sequenced against each other. A refresh that finishes late
may overwrite a newer frontier with an older, lower one. The dip is bounded by
how far the frontier moved while that refresh was in flight, and the next
refresh corrects it; this is accepted rather than fenced. A partition's LWM is
dropped once the node has no cached timeline in that partition: any
reconciliation walk that finds none prunes it, so a stale value does not
outlive the partition's presence in the cache. The next timeline created there
is a cold one and refreshes the LWM first.

A timeline whose cache entry is current is not re-seeded, so the floor
reaches it only at its next re-seed. A tenant that drains and returns within
one reconciliation interval keeps its cached vtime, and a re-seed uses the LWM
from the last reconciliation, which may lag the live frontier. Both errors are
bounded by how far the frontier advances in one interval, the same staleness
bound as the cache itself.

When a timeline is seeded or re-seeded, its vtime becomes the greater of its
trailing edge and the partition's LWM. A tenant whose stale vtime sits far
below the frontier is lifted to it and takes its weighted share from there;
it cannot "catch up" on credit it never used. A tenant whose trailing edge
sits above the frontier keeps its edge; it is not pushed behind anyone.

The floor raises a lagging timeline only. It does not pull down a timeline
whose vtime sits too high -- that case is cancellation.

Worked example, one partition, equal weights. Tenant b has jobs stamped 1..20
and the first ten have been served, so the frontier is 10. Tenant a drained
long ago with a cached vtime of 3. A reconciliation has since run, so a's
entry is stale (or evicted) and the partition's LWM is 10. a now enqueues
again; it has no trailing edge, so its re-seed takes the LWM alone. With no
floor its job is stamped 4 and dispatches before b's ten remaining jobs (stale
credit). Floored to the frontier it is stamped 11 and interleaves with b from
the next claim. If a enqueues twenty jobs they are stamped 11..30 and
alternate with b's 11..20 one for one: b finishes at about the twentieth claim
instead of the tenth, a at the thirtieth, each having had half the capacity
while both were backlogged. A floor at b's trailing edge (20) would instead
have stamped a's first job 21, behind b's entire remaining backlog, which is
tenant-level FIFO rather than fair sharing.

### Cancellation and vtime

A canceled job advanced vtime at enqueue but will never dispatch, stranding
vtime above the tenant's real backlog. Canceled jobs at the end of the queue
will be 'refunded' at the next reconciliation because reconstruction from the
pending set excludes canceled jobs. Each reconciliation re-anchors vtime to
surviving work. The vtime of canceled jobs at the head or middle of the queue
are not recovered and the tenant will be underserved until lower vtime jobs
are processed. Because cancellation is expected to be rare, the complexity to
account for all forms of vtime 'refund' are not justified.

### Why a cache, and what it costs

The cache removes per-enqueue reads of durable state and per-dispatch
coordination. Its cost is bounded staleness: an enqueuing node's view lags
truth by up to one reconciliation interval, and fairness error is bounded
accordingly. Reconciliation is caller-driven, so that interval is whatever
the caller chooses; a caller that never reconciles gets no bound on drift for
active timelines (new and returning tenants are still floored, because a cold
timeline refreshes its partition's LWM itself). Because the cache is rebuildable, node restart and partition
handoff are self-healing -- a cold node reconstructs from durable stamps before
it stamps again.

## The conserved single-writer requirement

Virtual-time fairness requires that a single writer advance each timeline's
vtime. With several writers advancing independent copies from a shared
baseline, their stamps overlap the same virtual-time range instead of summing;
the timeline's trailing edge then advances too slowly and the tenant is
over-served. Because timelines are per tenant per partition, the requirement is
per tenant per partition: two nodes may stamp one tenant's jobs without
conflict if they stamp into different partitions. The same scope applies to
the weight-consistency obligation above.

A caller satisfies this requirement at the enqueue boundary when either only
one node enqueues, or when enqueuers consistently hash tenants so each tenant
has one writer. When a caller cannot guarantee this -- a generic library behind
an uncontrolled load balancer -- the system may alternatively satisfy it
internally through two-stage stamp partitioning.

## Multi-writer failure analysis

When several nodes independently stamp one tenant, the tenant's stamps
**compress**: its trailing edge advances at roughly one over its fan-out -- the
number of distinct writers -- so it is over-served by that factor. The
consequence depends on whether fan-out is uniform across tenants.

- **Uniform fan-out** (every tenant sprayed across all nodes): compression is
  equal, so relative fairness survives in expectation. The absolute virtual
  clock merely runs slow. Costs are stamp collisions and higher variance for
  low-volume tenants.
- **Heterogeneous fan-out** (the balancer pins tenants to subsets, or fan-out
  scales with volume): compression differs per tenant, biasing service toward
  high-fan-out -- typically larger -- tenants. This is a genuine, persistent
  deviation from fairness, and reconstruction from trailing edge stamps does not
  repair it, because the stored stamps are already compressed.

Both failure modes are silent: compressed stamps look like ordinary stamps.
The stamping node therefore records its identity on each job at stamp time,
making the failure detectable after the fact. A periodic per-tenant
distinct-writer count during reconciliation, surfaced as a warning metric, is
deferred future work; the recorded writer id is the durable hook it will need.

## Two-stage stamp partitioning

When the enqueue boundary cannot guarantee one writer per tenant, nor uniform
fan-out, a caller may split ingestion from stamping. This is a workaround the
caller builds, not behavior the library performs: the library supplies a
partition-routed durable queue and keyed idempotent insert, and the caller
assembles them into a stamping tier. Fenced exclusive partition ownership is
not yet supplied: two-stage support is deferred (see "Future Phases" in the
implementation plan), and until then ownership fencing is the caller's
responsibility (for example, an external lock service or its own fenced lease
record). The ownership primitive described below is the design for that
deferred work.
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
never as a fairness dimension -- stage one neither stamps vstamps nor advances
any timeline's vtime. Multiple writers per partition are acceptable. Stage-one
requests carry no vstamp; the stamper consumes them in best-effort insertion
order (any monotone storage key serves), which is irrelevant to fairness --
fairness begins at stamping. The multi-tenant timeline begins only at stage
two. The cost of stamping is
assumed marginal, and fairness goals apply to the stamped work, not the
stage-one queue.

**Stage two** assigns vstamps. Each partition has one exclusive owning
**stamper**, which consumes its partition's requests, advances those
tenants' timelines as their single writer, and emits stamped jobs into the
fair queue. Single-writer correctness now holds by construction, independent
of how requests arrived.

This relocates the single-writer requirement from a boundary the caller cannot
control to a partition the caller controls. The only
condition the library imposes is a tenant id -- irreducible for tenant fairness.

Three behaviors the caller's stamping stage must exhibit. The library does not
implement them; once the deferred primitives land, it makes each enforceable:

- **Interleave co-partitioned tenants.** Hash collisions place several tenants
  in one partition. A stamper that drains one tenant's backlog before reaching
  another starves the latter at the stamping gate, because unstamped requests
  are invisible to the fair queue downstream. The stamper must round-robin (or
  deficit-round-robin) across the tenants present in its partition. This
  interleaving needs no virtual time; it only prevents monopoly of the
  stamper's read cursor.
- **Fence ownership handoff.** Partition reassignment -- a stamper added,
  removed, or recovered -- is the only window where two stampers might own one
  partition and reintroduce compression. A fenced handoff (ownership
  revocation plus a monotonic token that rejects the stale owner) bounds this
  to rare, controlled transitions rather than a per-request hazard.
- **Emit idempotently.** A stamper that emits a stamped job and then crashes
  before retiring the request will, on recovery, re-read the request and
  re-emit -- duplicating the job and advancing vtime twice, a single-writer
  violation against its own restart. Carrying the job id from request through to
  the stamped record makes the emit a keyed insert: the re-emit conflicts on the
  job id and is a no-op. This is why the job id originates at stage-one
  admission, not at stamp time.

Two-stage adds a durable write and a latency hop before a job becomes
dispatchable. It is therefore the **fallback**, engaged only when
single-writer-at-enqueue cannot be guaranteed, not the default.

## Partitioning as an orthogonal primitive

Partition-and-claim -- route records to partitions, let a worker claim only
within a partition -- is a mechanism distinct from virtual-time fairness, which
is policy layered above. Every job carries a **partition** field and every
claim names exactly one partition; an unpartitioned queue is simply a single
default partition, so the claim path is identical whether or not a caller uses
partitions. Within a partition the claim still takes the minimum `(vstamp, job
id)` among visible records -- partitioning narrows the claim's candidate set, it
does not change the ordering rule. The system could use this primitive for
various situations that might benefit. E.g.

- **Tenant-hash partitions** for stamping shards. These require **exclusive**
  ownership: one consumer per partition, or compression returns.
- **Size-class lanes** that keep long jobs from blocking short ones. These
  require **shared** consumers: many workers per lane, since the lane is a
  blocking-avoidance filter, not a single-writer constraint. Because timelines
  are per partition, a tenant's long jobs in a slow lane do not push its short
  jobs behind other tenants' in a fast lane; each lane shares its own capacity
  fairly among the tenants present in it.

Partitioning is a feature that is orthogonal to job lifecycle and leasing.

## Job lifecycle and leasing

A job's durable record carries a **liveness** status and, once terminal, a
**resolution**. Liveness takes two values: **pending** (awaiting or undergoing
service) and **resolved** (terminal). The system collapses every terminal
outcome into the single resolved status and records the outcome separately as
a resolution -- supplied by the caller on complete, supplied by the queue on
cancel (the canceler invokes a queue verb rather than reporting an outcome).

Resolution is scaffolding the queue establishes on behalf of the caller but
does not interpret. Termination *requires* recording a resolution. Acting
on a resolution -- retrying, dead-lettering, continuing a workflow -- belongs to
the caller above. The queue records *how* a job ended, never *what happens
next*.

A single terminal status makes the no-double-resolution property structural:
exactly one terminal write succeeds per job. Concurrent terminal attempts race
on a guard that admits the first and rejects the rest; the loser is told it
lost and can read the winner's resolution from the record.

### Claims, visibility, and three sets

Dispatch does not move a job out of pending. A **claim** instead attaches a
**claim id** to the pending record and pushes a **visibility time** into the
future; the job stays pending. This factoring forces a distinction the bare
fairness model elided -- between jobs that exist, jobs that can be claimed, and
jobs in flight:

- **pending** -- every non-terminal job. Trailing-edge reconstruction reads
  this set. A leased, in-flight job belongs here: it is still owed service, and
  excluding it would let reconciliation re-floor the tenant as if drained,
  losing the tenant's place.
- **visible** -- pending jobs whose visibility time has passed. This is the
  claim set: the dispatch index orders by vstamp over pending jobs within the
  claimed partition, with visibility checked in the index, and claims the
  minimum. The low-water mark does not read
  this set; it is the partition's service clock, read from claim fences (see
  "Low-water mark and the idle floor").
- **leased** -- pending jobs that hold a claim fence whose visibility time
  lies in the future. In flight, excluded from claim while the lease lasts,
  still counted as backlog. A lapsed lease keeps its fence but is visible
  again. A job whose visibility time lies in the future without a fence
  (enqueued with a delay, or released with one) is delayed, not leased, and
  has not been put into service.

A fresh job is visible at once; a claimed job is invisible until its lease
lapses; a lapsed lease makes the job visible again. *Visible* therefore
unifies never-claimed and lease-expired under one predicate -- pending and
visibility-time elapsed -- so the claim path needs no separate reclaim branch
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
the worker its job was stolen or canceled.

The claim id is a **fence**, and its load-bearing role is to make the durable
store reject a stale holder's *terminal* write, not merely its heartbeat. Were
only the heartbeat fenced, a worker stolen from -- paused past its visibility
time, never heartbeating again -- could still finish and record completion
under its dead claim, double-resolving the job. Every lease-touching write
therefore guards on the claim id and on pending liveness: extend, release,
complete, and the cancellation a worker discovers. Heartbeat failure is the
**detection** path that lets a preempted worker abort early and save wasted
effort; the fence on the terminal write is the **safety** property. The two
are independent, and only the second is required for correctness -- heartbeats
are a latency optimization, not a safety mechanism.

Claim ids must be unique per claim and never reused, so a stale holder's id
can never match a later claim. A *minted* claim id is never the empty value;
the stored field, by contrast, is empty before the first claim and again
after a release, so an empty field matches no holder and a cleared fence (see
early release) matches nothing.

### Early release

A worker that cannot finish a job -- a transient failure, a dependency down --
may **release** its lease rather than completing terminally or abandoning the
lease to expire. Release is a fenced write, guarded like a heartbeat on claim
id and pending liveness, that sets the visibility time to now plus a
caller-chosen delay (zero means immediately reclaimable) and **clears the
claim id**. The delay is the caller's backoff lever; the queue supplies the
primitive, never the retry policy. The next claim of a released job is an
ordinary claim -- the attempt count still increments, and the job keeps its
original vstamp. Release is not a virtual-time event, for the same reasons
reclaim is not: the releasing worker does not hold the tenant's timeline, so
a fresh stamp would be a second writer. Because the frontier is the service
clock, a released low-stamped retry does not hold the floor back either.

Release must clear the claim id, unlike cancellation. It is the one transition
that would otherwise leave a stale fence live: after a steal the new claim
overwrites the claim id, and after a cancel the liveness guard rejects the old
holder, but a released job is still pending and unclaimed, so an uncleared
claim id would still match every guard. The releasing worker -- or a goroutine
it left behind -- could complete the job it gave up, or heartbeat it and push
the visibility time back out, silently un-releasing it. Clearing makes fence
revocation structural: the worker's authority ends when it declares it is
done, not at the next claim.

### Job identity

Every job carries a caller-supplied **job id**, stored as the durable record's
primary key (the `_id`). It is the job's stable identity for its whole life: it
does not change across reclaim, steal, or retry, and it is distinct from the
claim id. The claim id is a per-claim fence that rotates on every claim; the job
id names the job itself.

The job id is the caller's handle for the operations the queue exposes but does
not drive -- **cancel** (an external caller naming the job to resolve) and
resolution lookup (polling how a job ended). Because the caller supplies it,
enqueue is **idempotent**: submitting under an id already present is a no-op
insert, not a duplicate, so a caller can safely retry an enqueue whose outcome
it never observed. The stored record wins -- a retried enqueue carrying a
different body under the same id changes nothing -- and the duplicate is
signaled to the caller rather than silently absorbed, so an id-collision bug
is visible instead of a quiet data loss. It is likewise the natural key for the effect-level
deduplication job bodies must perform, since delivery is at-least-once, not
exactly-once.

The job id also completes the dispatch order. Within a tenant, vstamps strictly
increase -- each enqueue advances vtime by a positive stride -- so a tenant's own
jobs never tie. Ties arise only across tenants, when two hold a job at the same
vstamp; both are equally entitled at that virtual instant, so any rule ordering
them is fair. The dispatch index is therefore ordered by `(vstamp, job id)`: the
job id breaks ties deterministically, giving a definite minimum and a total
order, while the tie itself is fairness-neutral. That the caller supplies the
tiebreaking id is harmless -- it arbitrates only between equally-entitled jobs of
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
  cooperative -- the worker aborts within one heartbeat interval, or its
  terminal write is rejected and its output discarded. Because the canceler
  reports no outcome, the queue supplies cancel's resolution: a fixed
  queue-defined value marking the cancel path.

ClaimID need not be cleared on cancellation as long as heartbeats and terminal
writes guard on pending state as well as ClaimID.  If ClaimID has semantic
meaning (e.g. a unique worker ID), cancelation preserves such information for
observability purposes.

From the worker's vantage, a steal and a cancel are the same fence failure,
distinguished only by what it reads back. A canceled job lands in resolved
with a recorded resolution; a stolen job stays pending under the new
claimant's fence. Either way the sink's mutual exclusion guarantees a single
winner. Cancellation is not a virtual-time event: the canceled job leaves the
pending set, and reconciliation may re-anchor the tenant's vtime within one
reconciliation interval, as described above.

### Attempts and poison jobs

Each claim increments a durable **attempt count** on the record.
Steal-on-expiry retries crashed work for free, which is the intent; absent a
bound, a job that crashes its worker every time would retry forever and burn
capacity. A worker should read the attempt count on claim and, past a
worker-configured threshold, resolve the job terminally with an exhaustion
resolution rather than running it again. The queue supplies the count; the
retry count checking and what becomes of an exhausted job -- a dead-letter
lane, an alert, a manual replay -- is the caller's responsibility.

### Effects are at-least-once

A lease bounds duplicate *dispatch* and *termination*; it cannot bound duplicate
external *effects*. A worker paused past its visibility time may already have
acted on the world -- sent the email, changed a database -- before its fenced
terminal write is rejected. The fence protects the queue record's consistency,
not side effects. Job bodies must therefore be idempotent or carry their own
effect-level deduplication: the queue guarantees at-least-once delivery, never
exactly-once execution.

## Supporting behaviors

**Cost and estimation.** When cost is known at enqueue, the vtime stride is
exact. When unknown, the caller provides an estimate. Estimation errors are
uncorrected. Recording actual cost and updating vtimes or pending vstamps adds
complexity and coordination costs inconsistent with the design goal. Cost must
be strictly positive -- the system floors it to a small minimum -- so every
stride advances vtime.

**Long, non-preemptible jobs.** Worst-case unfairness is bounded by the
largest job cost times the worker count. A caller could bound it further with
size-class lanes, runtime caps, and per-tenant concurrency caps.

**Sparse-tenant responsiveness.** The idle LWM floor gives a returning or
low-rate tenant prompt service rather than burying it behind a heavy backlog --
the property that keeps interactive work responsive amid batch floods. A
returning tenant is floored to its partition's dispatch frontier, so its first
job lands one stride past the last job served: it joins at the point of
service, neither jumping work already being served nor waiting behind another
tenant's backlog. This gives a sparse tenant latency (its one job is near the
front) without giving it throughput it has not earned (its tenth job in a burst
sits behind the incumbents' tenth). The worked example under "Low-water mark
and the idle floor" shows the placement.

**Clock discipline.** Visibility comparisons mix timestamps written at
enqueue with the clock consulted at claim, and no storage backend guarantees
one consistent clock (a sharded cluster's server-side "now" varies by shard).
The design therefore assumes operationally bounded clock skew (NTP-class)
rather than engineering around it: skew shifts a job's visibility by at most
the skew bound -- benign against lease granularity -- and never affects fencing,
which compares identities, not clocks. This is a documented operational
assumption, not a correctness dependency.

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
2. **Atomic conditional transition** of a single record -- claim, heartbeat,
release, complete, cancel expressed as guarded state changes, where guards
include claim-id fencing and pending-state checks.
3. **Ordered claim of the minimum key** -- `(vstamp, job id)` -- among
**visible** records **within a named partition** -- pending records in that
partition whose visibility time has elapsed -- with mutual exclusion under
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

**Note -- garbage collection (not a required behavior).** Neither the store nor
the cache is self-pruning. Resolved job records (completed or canceled,
including attempt-exhausted) accumulate unless reclaimed, and the timeline
cache map grows with every (tenant, partition) ever seen, retaining entries
for tenants long gone idle. Neither threatens correctness -- the design tolerates both --
but both grow unbounded without a sweep. (An expired lease is not a resolved
record; it re-enters the visible set and still dispatches, so it is not GC's
concern.) The terminal write records a resolution timestamp so reclamation
has a durable key to age on. Resolved records also carry the service clock
(the LWM reads claimed jobs of any liveness), so retention should comfortably
exceed the time between claims in a partition; once a partition's claimed
history is reclaimed, its LWM falls back to the minimum pending vstamp. An
implementation should reclaim resolved records on some cadence
and evict idle-tenant cache entries (an evicted tenant is re-seeded to the
greater of its trailing edge and the LWM on its next enqueue, the onboarding
path, so eviction is safe). Treat this
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
  lease holder -- stolen from or canceled out from under -- cannot resolve the
  job, because the fence rejects its terminal write.
- **Memoryless placement:** a tenant that becomes backlogged in a partition
  is placed within one stride of that partition's frontier, up to the
  cache's one-interval staleness. It neither
  dispatches ahead of the frontier on credit from past idleness nor waits
  behind another tenant's backlog; from its first job it receives its
  weighted share.
- **Lease accounting:** an in-flight (leased) job counts toward its tenant's
  backlog; reclaim preserves the job's original vstamp; lease expiry,
  stealing, heartbeats, and early release do not alter any stamp or vtime.
  Visibility changes do not move the sampled low-water mark; claims and
  releases do, since the frontier is the maximum vstamp among claimed jobs.
- **Enqueue accounting:** a tenant's cached vtime advances only when the
  job's durable insert succeeds; a failed or duplicate insert advances
  nothing. (Compute the stamp speculatively, commit the cache advance on
  insert success.)
- **Timeline order:** a new stamp exceeds every pending stamp on its
  timeline. It may fall below a stamp already served (see "Reconciliation").
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

**Refund-on-cancel.** Decrementing a tenant's vtime by a canceled job's
stride at cancel time was considered and dropped. It wanted per-cancel
bookkeeping and, for co-located exactness, a multi-record transaction the
contract otherwise avoids. Cancellation is expected to be rare -- a rare,
bounded, self-correcting error not worth the machinery.

**Sweeper-based lease reclaim.** A background sweeper that scans claimed jobs
and releases expired leases was considered and rejected. Steal-on-expiry
reclaims lazily on the claim path the system already runs, removing a
coordinating background process and its periodic global scan, at the cost of
per-job heartbeat writes and a tuned visibility timeout. The trade favors the
design's goal of minimal cross-node coordination.

**Distinct terminal states.** Modeling completed, canceled, and failed (and
timed-out, exhausted, and so on) as separate durable states -- as workflow
engines do -- was considered and rejected for this primitive. Those engines
branch on the terminal state: cancellation runs compensation, failure drives
retry policy, and result APIs decide error propagation by terminal-state
membership. This queue owns none of that. It dispatches fairly and leases
durably, and treats the terminal outcome as recorded metadata, not a
control-flow input. Collapsing liveness to pending-or-resolved while recording
the outcome as an open, uninterpreted resolution keeps the state machine minimal and
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

**Trailing-edge low-water mark.** An earlier revision defined the LWM as the
minimum trailing edge among tenants with pending work. That places a newcomer
behind the entire remaining backlog of the shallowest active tenant -- with one
tenant 600 jobs deep, a newcomer's first job waits behind all 600 -- which is
tenant-level FIFO, not fair sharing, and contradicts the idealized-fair-server
model above. The dispatch frontier is the quantity that model specifies.

**Frontier rules read from job state.** Several definitions of the dispatch
frontier were tried before the service clock, and each failed a common case.
*Minimum visible vstamp only*: a fully leased partition, the normal state when
workers keep up, has no frontier, so a newcomer is floored at a stale value or
zero. *Minimum visible, else maximum leased*: a partition holding only delayed
jobs has no frontier, so a newcomer at 0 beats a scheduled batch when it comes
due; and a long lease outlives later short jobs, so in a lull the frontier
falls back to the long job's old stamp, which is credit. *Adding the minimum
pending vstamp as a third step*: it works, but costs three conditional reads,
and a low-stamped released retry pins the floor in a delayed-only partition.
*Minimum over all pending jobs, any state*: a poison job retried by early
release keeps its old vstamp, and it, a long heartbeated job, or a long delay
pins the floor for its lifetime -- unbounded credit. *First never-claimed job,
any visibility*: a fully leased partition has none, and backoff retries and
long delays pin it even in busy partitions. *Maximum leased first, then the
rest*: finding the top lease means scanning past every unclaimed job above
it, a cost proportional to the backlog. *Clock from resolved jobs only*: it
lags by the in-flight window and must skip every job canceled before claim
that is stamped above the clock. Holding the last computed value instead of
any fallback was also rejected: in a partition that stays fully leased it
ages without bound, a cold node has no value at all, and a newcomer seeded
there monopolizes the partition until its vtime catches up. A running
maximum (`lwm = max(lwm, computed)`) was rejected as well. It protected an
earlier multi-query, non-snapshot computation from dips; the clock is one
query, and memoryless placement wants the floor at the actual point of
service, small dips included.

**Idle credit.** Letting a returning tenant keep its stale vtime (no floor), or
flooring it some fixed allowance below the frontier (a CFS-style sleeper
bonus), rewards idleness with a burst ahead of incumbents. Unbounded credit
lets a tenant idle for a year monopolize the queue for as long as its credit
lasts; a bounded allowance is a knob with little to buy, since the frontier
already places a sparse tenant's job within one stride of the front. Rejected
in favor of memoryless placement.

**Finer fixed-point scale, or unchecked stamp arithmetic.** An earlier
revision used SCALE = 10^6 and left overflow unguarded on the strength of a
miscalculated bound. The real headroom at 10^6 is about 107 days of a sustained
10^6 cost-units per second, reachable when cost is denominated in bytes or
milliseconds. Precision beyond 10^3 serves only weights far above cost, which
callers can reach by choosing finer cost units. Among responses to exhausting
the range, a saturating add was rejected because saturated stamps tie and
lose ordering silently; rebasing stored stamps onto a per-partition epoch was
rejected because it reintroduces cross-node coordination; wider stamps
(decimal or composite keys) were rejected for index size and sort complexity.
A failing enqueue is the loud, local outcome.

**One tenant timeline across partitions.** An earlier revision kept one vtime
per tenant regardless of partition. With several partitions there is no single
point of service, and every floor built on a shared timeline leaks: a single
global frontier gives newcomers stale credit bounded only by the spread between
lane frontiers, and trailing-edge re-seeding forgets a tenant's consumption in
a fast lane whenever it has a job pending in a slow one; per-partition floors
on the shared timeline instead drag a tenant's vtime to the hottest lane it
touches and bury its work in cooler lanes behind backlog it never consumed.
No stated goal depended on the cross-lane coupling, and size-class lanes exist
to remove it. Timelines are therefore per tenant per partition, and each
partition is an independent fair queue.
