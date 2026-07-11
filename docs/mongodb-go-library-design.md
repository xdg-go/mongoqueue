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
  `Attempts`. Dispatch sorts, claims, fences, and reconciles on these; the queue
  reads and writes them. (`JobID` is caller-supplied yet envelope: the queue
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
    ID        string    `bson:"_id"`        // caller-supplied job id
    TenantID  string    `bson:"tenant"`
    Partition string    `bson:"partition"`
    Cost      int64     `bson:"cost"`
    VStamp    int64     `bson:"vstamp"`
    ClaimID   string    `bson:"claim_id"`
    VisibleAt time.Time `bson:"visible_at"`
    Attempts  int       `bson:"attempts"`
    // Payload — stored and returned, never interpreted by the queue core
    Kind      string    `bson:"kind"`       // decode discriminator (the body's content-type)
    Body      bson.Raw  `bson:"body"`       // native subdocument, opaque to queue code
}
```

(`Liveness` and `Resolution` join the record per the generic lifecycle model;
omitted above to keep the storage shape in focus.)

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
```

Envelope enqueue-fields (`Cost`, `TenantID`, `Partition`, `JobID`) travel in
`EnqueueOpts`, **not** inside the body — the library must read them and is
contractually blind to the body. Non-negotiable.

`kind` is a payload field, so it rides as its own parameter rather than in
`EnqueueOpts`. The facade is its only writer and pairs a stable `kind` with each
body type `T`; the queue stores the string without interpreting it. `Decode[T]`
may assert the stored `kind` matches the type it decodes into, catching
`kind`↔type drift on read.

Enqueue is **idempotent**: insert keyed on the caller-supplied job id, rejecting
a duplicate id, so a caller can safely retry an enqueue whose outcome it never
observed.

## The Claim API

```go
// ErrNoJob signals an empty visible set in the partition — expected, not failure.
// Mirrors mongo.ErrNoDocuments so callers loop on errors.Is.
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
```

Under the hood this is storage behavior #3 from the generic contract — one atomic
`FindOneAndUpdate`:

```go
filter := bson.D{
    {"partition", partition},
    {"liveness", Pending},
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
   `Heartbeat` and `Complete` capture `claimID`.

**Implementation notes.** Compute `now` from **server time** (ideally `$$NOW` in
a pipeline update) so clock-skewed nodes do not steal early or late. Promote
`lease` to a `ClaimOpts` struct only when a second per-claim knob appears.

## Resolution: Complete vs Cancel

The signature asymmetry encodes the authorization asymmetry from the generic
design:

- **`Complete`** is issued by the lease holder and guards on a matching claim id.
  It rides on the `ClaimedJob` handle.
- **`Cancel` stays queue-level** — issued by an external caller holding no lease,
  guarding on pending liveness alone, naming only the job id:
  `q.Cancel(ctx, jobID, r)`.

## Open Questions

1. **Tenant weight input.** The generic model advances `vtime` by `cost /
   weight`, but `EnqueueOpts` as sketched carries `Cost` with no weight. Decide
   where weight enters — per-enqueue, per-tenant registration, or a caller-
   supplied callback — and where it is stored.
2. **Dispatch helper.** Whether to ship a caller-side `Kind` → handler `Mux`
   (asynq-style) as an optional layer above the primitive. Undecided.
3. **`Liveness` / `Resolution` representation.** Concrete BSON encoding of the
   liveness status and the caller-set resolution, and the `Resolution` Go type.
4. **Index set.** The exact MongoDB indexes backing the claim sort
   (`partition`, `liveness`, `visible_at`, `vstamp`, `_id`) and the
   reconciliation/LWM aggregations, plus their write-amplification cost.
5. **Reconciliation and cache surface.** How the node-local vtime cache, periodic
   reconciliation, and LWM computation are exposed and scheduled — internal
   goroutine, caller-driven tick, or both.
