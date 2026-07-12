# MongoDB/Go Implementation Design

This document specifies the Go API and MongoDB storage layout for the fair job
queue defined generically in [fair-job-queue-design.md](fair-job-queue-design.md).
The generic design is written backend-agnostic; **this implementation is
Mongo-specific**, and that narrows several choices the generic doc left open.

Alternatives considered and rejected are recorded in
[rejected-designs.md](rejected-designs.md).

## Envelope and payload

Every job's fields fall into two categories, distinguished by **whether the
queue mechanism interprets them**. Both categories are flat, top-level fields;
the split is conceptual, not a nesting.

- **Envelope** — fields the queue *interprets*: `JobID`, `TenantID`, `Cost`,
  `Partition`, `VStamp`, `Liveness`, `Resolution`, `ClaimID`, `VisibleAt`,
  `Attempts`, `StampedBy`, `ResolvedAt`. Dispatch sorts, claims, fences, and
  reconciles on these; the queue reads and writes them. (`JobID` is caller-supplied yet envelope: the queue
  interprets it as the `_id`, the idempotency key, and the vstamp tiebreaker.
  The category is interpretation, not origin.)
- **Payload** — fields the queue stores and returns but never interprets:
  - **`Kind`** — the caller's decode discriminator, a stable string keyed to the
    body type. The queue core never reads it; it is the body's *content-type*,
    used by the caller and the caller-side facade to decode.
  - **`Body`** — the caller's domain data, stored as a native BSON subdocument.

The queue is a transport: it routes on the envelope and carries the payload
untouched. `Kind` is to `Body` as `Content-Type` is to an HTTP entity body — the
transport forwards both; the recipient parses the body according to the kind.

Two constraints from the generic design shape the API:

1. A multi-tenant queue carries **many job kinds** across tenants in one
   vstamp-ordered index. A design that forces one body type per queue is wrong.
2. The library is a **primitive that does not dispatch** — workers are externally
   provided; the caller claims a job and routes it. This weakens any case for
   parameterizing the whole queue on one body type.

## Body storage: BSON-native

The body is stored as a **native BSON subdocument** (`bson.Raw` under a `body`
key), not opaque JSON bytes. The stored document is
`{ _id, tenant, vstamp, ..., kind, body: { ... } }` with `body` a real nested
subdocument and `kind` a top-level scalar beside it.

This gives opacity in code and transparency in storage at once:

- **Opaque to the library** — queue code never interprets the body; the
  primitive boundary holds in the code.
- **Transparent in the database** — a genuine subdocument, so ops inspection,
  aggregations, and `db.jobs.find({"body.url": ...})` work, and the caller can
  index `body.*` without the library knowing.
- **No namespace collision** — the body has its own subdocument; a body field
  named `cost` cannot clobber an envelope field.
- **Ownership intact** — `ClaimID`, `VStamp`, `VisibleAt` stay top-level and
  library-owned; the caller's type never sees them.

**Efficiency.** `bson.Raw` drops a whole codec: one BSON encode native to the
wire protocol instead of struct → JSON bytes → BSON string, and one decode back.
The body stores as a real document, not an escaped blob, so it is smaller on the
wire and on disk and is indexable. The marshal on enqueue is unavoidable in any
scheme, so the JSON layer would be pure overhead.

**Caveats accepted.**

1. A readable body becomes a *de facto* schema once ops query it, so a field
   rename can break dashboards, not just caller code.
2. The facade controls marshaling (`bson.Marshal(T)`); the API does not accept a
   caller-built `bson.Raw`, to keep malformed framing and `kind`↔type drift out
   of the collection.
3. The dispatch hot path touches only envelope fields, so body readability adds
   zero claim-path cost.

## The Job record

Storage and the queue core are non-generic; generics live only at the API
boundary as thin per-kind helpers keyed by a stable `Kind` string. One queue
carries unlimited heterogeneous kinds over the shared fairness timeline while
callers still get typed enqueue and typed decode.

```go
type Job struct {
    // Envelope — queue-interpreted
    ID         string     `bson:"_id"` // caller-supplied job id
    TenantID   string     `bson:"tenant"`
    Partition  string     `bson:"partition"`
    Cost       int64      `bson:"cost"`
    VStamp     int64      `bson:"vstamp"`               // fixed-point virtual time (see arithmetic below)
    Liveness   Liveness   `bson:"liveness"`             // "pending" | "resolved"; always present (guards filter on it)
    Resolution Resolution `bson:"resolution,omitempty"` // open vocabulary; set by the terminal write, absent while pending
    ClaimID    string     `bson:"claim_id"`             // never empty while a claim is live; cleared by Release
    VisibleAt  time.Time  `bson:"visible_at"`
    Attempts   int        `bson:"attempts"`
    StampedBy  string     `bson:"stamped_by"`            // stamping node id — multi-writer detection hook
    ResolvedAt time.Time  `bson:"resolved_at,omitempty"` // set by the terminal write; keys TTL GC
    // Payload — stored and returned, never interpreted by the queue core
    Kind string   `bson:"kind"` // decode discriminator (the body's content-type)
    Body bson.Raw `bson:"body"` // native subdocument, opaque to queue code
}
```

### Virtual-time arithmetic

Vstamps and vtimes are fixed-point `int64` in units of 1/SCALE, `SCALE =
1_000_000`. The stride is

```go
stride := max(1, cost*SCALE/weight)   // pure integer arithmetic
```

Weight is `int64`, must be >= 1, and the zero value means 1 — so the
single-tenant / don't-care case configures nothing. The `max(1, ...)` floor
preserves the strictly-increasing-within-tenant invariant at extreme
cost/weight ratios; overflow is out of reach (~292,000 years at a sustained
10^6 cost-units/sec at weight 1). Weight is an enqueue input used to compute
the stride; it is not stored on the record.

### Liveness and Resolution representation

Both are typed strings — `type Liveness string`, `type Resolution string` —
stored as plain BSON strings.

- **`Liveness`** has exactly two values, `LivenessPending Liveness = "pending"`
  and `LivenessResolved Liveness = "resolved"`, and the field is always present
  (no `omitempty`): claim, heartbeat, complete, release, and cancel are
  server-side conditional writes that filter on the pending literal, so the
  stored value must match the constant exactly.
- **`Resolution`** is an **open vocabulary with two library-defined
  defaults**: `ResolutionCompleted = "completed"` and `ResolutionCanceled =
  "canceled"`. The line for library constants is mechanism-versus-policy: the
  library defines resolutions only for outcomes its own verbs produce, never
  for outcomes only the caller can judge — no `ResolutionFailed`, because the
  queue has no failure concept. Callers define their own values beside the
  defaults. The queue establishes the value at the terminal write but never
  interprets it; it exists for `Get` lookup and ops queries. The field is
  `omitempty`, absent while pending.

Strings over integer codes for ops transparency — `db.jobs.find({liveness:
"pending"})` reads without a decoder ring — and because an open resolution
vocabulary cannot be an enum. A scalar discriminator suffices today; the
representation is extensible later (e.g. an accompanying opaque detail field)
without disturbing the discriminator. Resolution values should stay
low-cardinality, stable, and machine-matchable; human-readable detail belongs
elsewhere (eventually the detail field), not in the discriminator.

### Field visibility

The record's fields are exported. This is a **clarity** concern, not a **safety**
concern.

**Why not safety.** Every guard that matters is enforced in the durable store,
not in Go's type system — claim, heartbeat, complete, and cancel are server-side
conditional writes guarding on the stored `claim_id` and `liveness`. The `Job` a
caller holds is a detached snapshot. Mutating it touches an in-memory copy with
no write-back; the next guarded write compares against durable truth, so a forged
value can only make the caller's *own* write fail the fence. It cannot corrupt
the record, steal another worker's job, or double-resolve.

**Counter-pressure against hiding everything.** The Mongo Go driver marshals only
*exported* fields via reflection, so the stored fields must be exported (or take
on a custom codec / internal shadow struct). Accessor-only records cost
boilerplate and buy no safety, given the durable guards.

**Resolution.**

- **Split `EnqueueOpts` (input) from `Job` (record)** so the caller never
  constructs a `Job` and cannot set a library-computed field expecting effect.
  This is the highest-value clarity fix.
- Keep `Job`'s fields exported, documented read-only — a returned snapshot,
  idiomatic for a DTO, and required by the driver. Treat mutation as a caller bug
  that hurts only the caller.
- Curate one exception: a single read-only `ClaimID()` accessor on `ClaimedJob`
  (below), with no writable claim-id field on the handle.

## The Enqueue API

```go
func Enqueue[T any](ctx context.Context, q *Queue, kind string, body T, opts EnqueueOpts) error
func Decode[T any](j *Job) (T, error)   // bson.Unmarshal(j.Body, &t)

type EnqueueOpts struct {
    JobID     string // caller-supplied; the record _id and idempotency key
    TenantID  string
    Partition string // empty = default partition
    Cost      int64  // floored to a small positive minimum
    Weight    int64  // zero means 1; must otherwise be >= 1
}

// ErrDuplicateJob signals an enqueue under an id already present.
// The stored record wins; match with errors.Is.
var ErrDuplicateJob = errors.New("mongoqueue: job id already exists")
```

Envelope enqueue-fields (`Cost`, `Weight`, `TenantID`, `Partition`, `JobID`)
travel in `EnqueueOpts`, **not** inside the body — the library must read them
and is contractually blind to the body. Non-negotiable.

**Weight is per-enqueue.** Callers that manage weights centrally build their
own registry above the API. Weight consistency across a tenant's producers is
a caller obligation the generic design documents alongside the single-writer
requirement; the library does not police it.

**Duplicate contract.** A duplicate id returns `ErrDuplicateJob`; the stored
record always wins, and the library never compares bodies (that would cost a
read on every duplicate to serve only buggy callers). An honest retry treats
the sentinel as success; a caller with an id-collision bug gets a signal
instead of silent data loss.

**Enqueue-side invariants.** The node's cached vtime advances only after the
insert succeeds — a failed or duplicate insert advances nothing. The insert
sets `visible_at` from the client clock; claims compare against server time,
so visibility can shift by client/server skew. This is a documented
operational assumption (NTP-class skew, benign against lease granularity),
not something server-side stamping could eliminate — `$$NOW` is not
guaranteed consistent across a sharded cluster either.

`kind` is a payload field, so it rides as its own parameter rather than in
`EnqueueOpts`. The facade is its only writer and pairs a stable `kind` with each
body type `T`; the queue stores the string without interpreting it. `Decode[T]`
may assert the stored `kind` matches the type it decodes into, catching
`kind`↔type drift on read.

Enqueue is **idempotent**: insert keyed on the caller-supplied job id, rejecting
a duplicate id, so a caller can safely retry an enqueue whose outcome it never
observed. The duplicate contract above (`ErrDuplicateJob`, stored record wins)
is the surface of that idempotency.

## The Claim API

```go
// ErrNoJob signals an empty visible set in the partition — expected, not
// failure. Returned by Claim; match with errors.Is. It plays the role of
// mongo.ErrNoDocuments at the queue boundary but does not wrap it.
var ErrNoJob = errors.New("mongoqueue: no claimable job")

func (q *Queue) Claim(ctx context.Context, partition string, lease time.Duration) (*ClaimedJob, error)

type ClaimedJob struct {
    Job                  // envelope + Body bson.Raw, promoted
    q       *Queue
    claimID string       // the fence minted for THIS claim; unexported so it can't be forged
}

func (c *ClaimedJob) ClaimID() string                                     { return c.claimID }
func (c *ClaimedJob) Heartbeat(ctx context.Context, extend time.Duration) error
func (c *ClaimedJob) Complete(ctx context.Context, r Resolution) error
func (c *ClaimedJob) Release(ctx context.Context, delay time.Duration) error
```

Under the hood this is storage behavior #3 from the generic contract — one atomic
`FindOneAndUpdate`:

```go
filter := bson.D{
    {"partition", partition},
    {"liveness", LivenessPending},
    {"visible_at", bson.D{{"$lte", now}}},      // visible = pending + elapsed
}
opts := options.FindOneAndUpdate().
    SetSort(bson.D{{"vstamp", 1}, {"_id", 1}}). // minimum (vstamp, job id)
    SetReturnDocument(options.After)
update := bson.D{
    {"$set", bson.D{{"claim_id", claimID}, {"visible_at", now.Add(lease)}}},
    {"$inc", bson.D{{"attempts", 1}}},
}
// no document matched => ErrNoJob
```

This orders by `(vstamp, _id)`, claims the minimum visible record in the
partition, sets the fence, pushes visibility out by `lease`, and increments
`attempts` — atomic and mutually exclusive under concurrent claimers.
Reclaim-by-expiry falls out for free: a lapsed lease re-enters `visible_at <=
now`, so the same query reclaims it with no separate branch.

**Decisions encoded in the signature:**

1. **Empty queue is a sentinel** (`ErrNoJob`), not `(nil, nil)`. An idle
   partition is the common case and must be unmissable; matches
   `mongo.ErrNoDocuments`.
2. **The library mints the claim id**; it is not a caller argument. "Unique per
   claim, never reused" is the fence's whole job and the library's to guarantee.
   For worker-identity observability, add a separate `claimed_by` label via
   `ClaimOpts` rather than overloading the fence.
3. **The fence rides on the handle** so the hot path cannot fumble it —
   `Heartbeat`, `Release`, and `Complete` capture `claimID`.

**`Release` semantics.** A fenced write (guards on `claim_id` + pending, like
`Heartbeat`) that sets `visible_at = now + delay` — zero means immediately
reclaimable — and **clears `claim_id`**. Clearing is a correctness
requirement, not tidiness: release is the one transition that would otherwise
leave a stale fence live (steal overwrites the fence, cancel's liveness guard
revokes it, but a released job is pending and unclaimed). An uncleared fence
would let the releasing worker complete the job it gave up, or heartbeat it
and silently un-release it. The library mints claim ids never-empty, so a
cleared fence matches no guard. The delay is the caller's backoff lever;
retry policy stays above the primitive.

**`Heartbeat` semantics.** Extends from server *now* (`visible_at = now +
extend`), not from the current `visible_at`, so repeated heartbeats cannot
stack extensions into the far future.

**Implementation notes.** Compute `now` from **server time** (ideally `$$NOW` in
a pipeline update) so clock-skewed nodes do not steal early or late. Promote
`lease` to a `ClaimOpts` struct only when a second per-claim knob appears.

## Resolution: Complete vs Cancel

The signature asymmetry encodes the authorization asymmetry from the generic
design:

- **`Complete`** is issued by the lease holder and guards on a matching claim id.
  It rides on the `ClaimedJob` handle. The resolution is required and
  non-empty — an empty value returns `ErrEmptyResolution` before any server
  round trip, enforcing the generic design's "termination records a
  resolution."
- **`Cancel` stays queue-level** — issued by an external caller holding no lease,
  guarding on pending liveness alone, naming only the job id:
  `q.Cancel(ctx, jobID)`. It takes no resolution: the canceller invokes a
  queue verb rather than reporting an outcome, so the library writes
  `ResolutionCanceled` unconditionally. This makes a stored `"canceled"` a
  reliable marker of the cancel path (by convention, callers never pass
  `ResolutionCanceled` to `Complete`). If a cancel-reason need appears, the
  escape hatch is a future `CancelOpts.Resolution` override — the open
  vocabulary means adding it breaks nothing.

The terminal write (complete or cancel) sets `resolved_at`, the durable key
GC ages on.

A resolution is terminal. A failure the caller intends to retry is a
`Release`, not a `Complete` with a "failed"-style resolution — retry policy
lives above the primitive, and a resolved job never re-enters the visible
set.

## Lookup

```go
func (q *Queue) Get(ctx context.Context, jobID string) (*Job, error)
```

The resolution-lookup path the generic design promises: a snapshot read by
job id, the caller's way to poll how a job ended. Returns a not-found
sentinel for an unknown id.

## Indexes, contention, and GC

**Claim index:** `{partition: 1, liveness: 1, vstamp: 1, _id: 1, visible_at:
1}`. The first four fields serve the claim filter and its `(vstamp, _id)`
sort; `visible_at` rides in the index so the visibility predicate filters
in-index without fetching documents. The leased low-vstamp prefix is still
scanned past on every claim, but as index entries only.

**Contention stance.** Concurrent claimers all target the same minimum
visible document; one wins and the rest retry server-side on write conflict.
v1 ships exact-minimum claim and should demonstrate a stated target (low
hundreds of claims/sec per partition) so the ceiling is testable. Further
mitigation — bounded random skip among the top-K visible, fairness-neutral
within vstamp ties and a bounded fairness error across stamps — is documented
contingency, deliberately deferred: if a database queue is hot enough for
this to be a problem, a database might be the wrong coordination mechanism.

**Index creation is caller-invoked, never automatic.** The library exposes
two methods and creates nothing on init:

- `EnsureIndexes(ctx)` — the claim index (and any other operational indexes).
- `EnsureTTLIndex(ctx, retention)` — the GC TTL index, with a caller-supplied
  retention duration.

They are split because a caller may want the first and not the second: the
claim index is required plumbing, while TTL is destructive policy. Runtime
credentials often lack `createIndex`, and index builds on a populated
collection are a scheduled operational event — hence no auto-creation for
either.

**GC.** The terminal write sets `resolved_at`. `EnsureTTLIndex` keys the TTL
index on it; there is no default retention and no TTL index unless the caller
invokes it. Retention is validated to `[1s, math.MaxInt32 s]` (~68 years):
MongoDB's TTL granularity is whole seconds, so a sub-second or non-positive
retention would truncate to `expireAfterSeconds: 0` and delete every resolved
job the instant it resolves. `EnsureTTLIndex` rejects an out-of-range retention
locally rather than create that silent-data-loss index.

**Changing TTL retention.** `EnsureTTLIndex` called again with a *different*
retention returns the driver's index-conflict error rather than silently
dropping and recreating the index. The library will not perform a destructive
drop-and-recreate implicitly -- a retention change is an operational decision,
and the same `resolved_at` key with a new `expireAfterSeconds` is exactly the
conflict the server reports. The caller resolves it with a `collMod` command
against `expireAfterSeconds`, which mutates the existing index in place without
a rebuild.

## Deferred / future work

- **Bounded-random-skip claim** if a measured contention ceiling is hit (see
  above).
- **Batch enqueue.** The per-tenant vtime cache makes batching natural
  (one cache advance, one `InsertMany`); leave API room, not in v1.
- **Multi-writer detection.** `stamped_by` is recorded now (cheap to add
  early, expensive to retrofit). The consumer — a per-tenant distinct-writer
  count during reconciliation surfacing a warning metric — is future work.

## Open Questions

1. **Dispatch helper.** Whether to ship a caller-side `Kind` → handler `Mux`
   (asynq-style) as an optional layer above the primitive. Undecided.
2. **Reconciliation/LWM index.** The claim index is settled (above); the
   access path for the reconciliation and LWM aggregations (per-tenant max
   pending vstamp) and its write-amplification cost are not.
3. **Reconciliation and cache surface.** How the node-local vtime cache, periodic
   reconciliation, and LWM computation are exposed and scheduled — internal
   goroutine, caller-driven tick, or both.
