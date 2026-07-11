# Rejected Designs — MongoDB/Go Implementation

This records design alternatives considered and rejected for the Go/MongoDB
implementation, with the reasoning, so the decisions in
[mongodb-go-library-design.md](mongodb-go-library-design.md) are not relitigated.
For alternatives rejected at the *generic* design level (dispatch-time stamping,
refund-on-cancel, sweeper reclaim, distinct terminal states, and so on), see the
"Rejected alternatives" section of
[fair-job-queue-design.md](fair-job-queue-design.md).

## How the body is typed at the API boundary

The central question: a multi-tenant queue carries **many job kinds** across
tenants in one vstamp-ordered index, and the library is a **primitive that does
not dispatch** (the caller claims and routes). How is the body typed, and how
does the caller supply the envelope's enqueue-time fields?

Prior art surveyed: [River](https://github.com/riverqueue/river) (generics,
`JobArgs.Kind()`, `Job[T]`, per-kind `Worker[T]`; storage stays opaque JSON +
kind string) and [asynq](https://github.com/hibiken/asynq) (opaque `[]byte`
payload + type string, `ServeMux` routing).

The chosen design is an **opaque core with a typed generic facade**, stored
BSON-native. The alternatives below were rejected.

### A. Opaque body + enqueue options

`[]byte` / `json.RawMessage` body plus enqueue options — asynq's model. Trivial
uniform storage, heterogeneous kinds in one queue, smallest surface. Rejected as
the *whole* answer because it forces unmarshal boilerplate and a runtime kind
switch the compiler cannot check. The chosen design keeps this opacity in the
core but adds a typed facade on top.

### B. Generic `Queue[T]` / `Job[T]`

A trap. One `T` per queue collides with multi-tenancy — many kinds share one
fairness timeline. `Queue[any]` throws away the typing; one-queue-per-kind
fractures fairness across separate timelines. This is also *not* how River
actually works; River parameterizes the per-kind worker, not the queue.

### C. Job as an interface the caller implements

`JobID()`, `Cost()`, and so on, implemented by each caller type. Wrong direction
of complexity: it pushes envelope construction up into every caller type, is
useless on the claim side (the library has no caller type to instantiate), and
conflates a value with behavior. Reserve interfaces for the genuinely
polymorphic seam — the storage backend.

#### C-embedding variant

`type MyJob struct { mongoqueue.Job; ... }`. Legal Go — fields and methods
promote, and the BSON driver inlines anonymous structs; it is the `gorm.Model`
pattern. Rejected because it flattens envelope and body into one namespace:

- **Reserves every envelope key across all caller types.** A body field named
  `cost` or `partition` silently collides with a library field.
- **Blurs ownership** of library-managed mutable fields (`VStamp`, `ClaimID`,
  `VisibleAt`).
- **Still needs generics** for typed read-back.

ORMs accept this because they own the full row per type. A queue primitive wants
a uniform envelope plus an opaque body, which embedding cannot give.

## Body storage: JSON bytes vs embedding vs BSON-native

Opacity-in-code and transparency-in-storage are independent axes. The chosen
design stores the body as a native BSON subdocument (`bson.Raw`), which is
opaque to library code yet a real, queryable document in Mongo. The two rejected
storage forms each fail one axis:

- **JSON bytes** — opaque in code *and* opaque in the database: a useless escaped
  string blob that ops cannot query and the caller cannot index. It also adds a
  codec round trip (struct → JSON → BSON string and back).
- **Embedding** — transparent in the database *and* transparent in code, with the
  collision and ownership-blur flaws of the C-embedding variant above.

`bson.Raw` is opaque where opacity is wanted (library code) and transparent
where transparency is wanted (the database).
