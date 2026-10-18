# TODO: Phase 7 -- Ops Hardening and Documentation

Expanded from [docs/implementation-plan.md](docs/implementation-plan.md)
Phase 7. Verifies the TTL contract, reconciles the design docs against the
code, and writes the public documentation (package godoc, a worker-pool
example, a minimal README). Builds no new mechanism.

Section 7.0 holds the decisions every subphase relies on. Read it first.

## Testing Philosophy

- **Integration tests against real MongoDB** for anything the server
  decides: which documents carry `resolved_at`, index shapes. Tests connect
  via `internal/mongotest` (`MONGOQUEUE_TEST_URI`, default
  `mongodb://localhost:27017`).
- **Hermetic tests**: each test uses a uniquely named database/collection
  (random suffix) so parallel runs never collide; drop on cleanup. Never
  change server-wide parameters (no `setParameter`).
- **Trust the server's TTL monitor**: test the index shape and the document
  shapes it acts on, not the deletion itself. No test waits on the monitor.
- **Control time through data, not a clock**: no sleeps. Expire a lease or
  end a delay by writing `visible_at` into the past.
- **Examples compile, they do not run**: examples need a server, so they
  carry no `// Output:` comment and `go vet` checks them. Behavior they show
  is covered by existing tests.
- **Doc-only changes need no test.** A reconciliation finding that exposes
  untested behavior gets a test (or a recorded follow-up), not a doc edit
  alone.

Test what new code changes. Lifecycle, fencing, fairness, frontier, and
reconcile behavior are already covered; don't retest them.

## Verification Checklist

Before marking a subphase complete and committing it:

1. `go build ./...` and `make vet` pass
2. `make test` passes (including integration tests against Mongo)
3. `make lint` clean (config in `.golangci.yml`); remove helpers your
   change leaves unused, since lint fails on them
4. New exported identifiers have godoc comments
5. Every `docs/` file added or removed is reflected in the AGENTS.md
   documentation index in the same commit

When verification of a subphase is complete, commit all relevant
newly-created and modified files as one logical unit. `REVIEW.md` is
gitignored and is never committed.

## Dependencies Between Subphases

```
7.0 Decisions (read first)
       │
       ├─► 7.1 Partial TTL index + shape tests (independent)
       │
       ▼
7.2 Reconciliation: forward pass (both design docs)
       │
       ▼
7.3 Reconciliation: reverse pass, invariants, rejected alternatives
       │
       ▼
7.4 Reconciliation: plan, AGENTS.md, open questions, deferred lists
       │
       ▼
7.5 Deviation review with the user (REVIEW.md, one by one)
       │
       ▼
7.6 Package godoc ──► 7.7 Worker-pool example ──► 7.8 README
```

Docs come after reconciliation so godoc and README describe the model the
code implements. 7.5 may produce code or doc changes of its own; each
decided deviation is its own commit.

---

## 7.0 Decisions (2026-10-17)

- **TTL: shape, not deletion.** The TTL monitor runs every 60s; testing
  actual deletion needs a sleep or a server-wide `ttlMonitorSleepSecs`
  change. The queue's obligation is the index shape and the document
  shapes it acts on; the server does the rest.
- **TTL index is partial on `liveness: "resolved"`.** The current index
  is a plain TTL on `resolved_at`, so pending jobs are safe only because
  `resolved_at` is `omitempty`: a stored zero `time.Time` is year 1, and
  the monitor would delete the pending job on its next pass. A
  `partialFilterExpression` of `{liveness: "resolved"}` keeps pending jobs
  out of the index entirely, so safety rests on the liveness state machine
  (Complete and Cancel set `liveness` and `resolved_at` in one update), not
  on a struct tag. It also drops a null-key index entry per pending job
  (non-sparse indexes store missing fields as null): no TTL write at
  enqueue, one insert at resolve, one delete at removal. Named
  `mongoqueue_ttl`, matching the other three. Nothing is deployed, so the
  old unnamed TTL spec is simply replaced -- no migration note.
  `omitempty` stays (pending documents should not carry a dummy date);
  it is no longer what keeps them alive.
- **Observability: build nothing.** `stamped_by` is wired and tested
  (`enqueue_test.go`, `TestEnqueue...` asserting `node-under-test`). No
  metrics, stats returns, or hooks; callers query the collection.
  `claimed_by`/`ClaimOpts` stays a documented future option.
- **Open question #1 (`Mux`): deferred and closed.** It sits above the
  primitive. Move it to the deferred list; the Open Questions section keeps
  one-line pointers.
- **Deviations go to `REVIEW.md`.** When the code and a doc disagree on
  behavior (not just wording), record it in a fresh `REVIEW.md` and change
  neither side. The user reviews deviations one by one in 7.5.
- **README: minimal.** For GitHub visitors: badges, a short pitch, install
  line, links. Usage rules live in godoc.
- **Tagging is separate.** No release or tag in this phase.

### Reconciliation method

The code is the presumed reference: phases 3-6 changed the design as they
went and the docs were patched along the way. Every gap is classified
before anything is edited.

| Verdict | Meaning | Action |
|---|---|---|
| **doc lags** | Doc states an older or vaguer version of what the code does, and the code's behavior is the decided one | Fix the doc in this subphase |
| **code deviates** | Code does something the docs don't sanction, or the doc's rule is the decided one | Record in `REVIEW.md`; touch nothing |
| **untested** | Code and doc agree, no test proves it | Add a test, or record in `REVIEW.md` as a follow-up |
| **undecided** | Neither side settles it | Add to Open Questions |

When unsure whether a gap is "doc lags" or "code deviates" (e.g. the doc
describes a deliberate rule and the code is simpler), it is a deviation.

A test citation counts only if the test asserts that exact statement;
otherwise the verdict is **untested**. Before recording a deviation, check
the plan's "Future Phases" and the mongodb doc's "Deferred / future work":
a doc that presents a deferred feature as present is **doc lags (deferred
feature)**.

`REVIEW.md` layout: a **Deviations** section (numbered; each with doc
location, code location, what each says, a concrete scenario where they
differ, and a recommendation) and a **Worksheet** appendix holding the
claim-by-claim pass (statement, doc location, code location, test,
verdict). The worksheet lives there, not in a session scratchpad, so it
survives across sessions.

---

## 7.1 Partial TTL Index and Shape Tests

- [x] `EnsureTTLIndex` (`queue.go`): add `SetName("mongoqueue_ttl")` and
  `SetPartialFilterExpression(bson.D{{"liveness", LivenessResolved}})`;
  add `ttlIndexName` beside the other index-name constants
- [x] Rewrite `EnsureTTLIndex` godoc: the index holds resolved jobs only,
  so the TTL can never touch a pending or claimed job regardless of its
  fields; keep the retention-range, conflict/`collMod`, and service-clock
  paragraphs
- [x] **Test**: update `TestEnsureTTLIndex` -- name `mongoqueue_ttl`, key
  `{resolved_at: 1}`, `expireAfterSeconds`, and
  `partialFilterExpression == {liveness: "resolved"}`
- [x] **Test**: `TestResolvedAtOnlyOnResolvedJobs` -- drive one job each
  through: enqueued (pending, never claimed), delayed, claimed,
  heartbeated, released, lease-expired (claim, then write `visible_at`
  into the past), completed, canceled before claim, canceled while
  claimed. Read each raw document (`bson.Raw`, not `Job`, so a zero time
  cannot hide) and assert `liveness` and `resolved_at` agree: the pending
  six have `liveness: "pending"` and no `resolved_at`; the resolved three
  have `liveness: "resolved"` and a BSON date
- [x] **Doc**: mongodb doc "Indexes, contention, and GC" -- TTL row of the
  index table (name, partial filter), the write-cost paragraph (TTL
  writes per lifecycle), and the GC/retention text; fair doc "Storage
  requirements" GC note if it describes which records the TTL sees.
  Grep docs and godoc for `resolved_at` and "TTL" and fix what the
  partial filter changes
- [x] Mark 7.1 done in `docs/implementation-plan.md`

---

## 7.2 Reconciliation: Forward Pass

For each normative statement -- rules, field names, query shapes, index
keys and partial filters, error names, lifecycle transitions, caller
obligations -- find the code that implements it and the test that proves
it. Record every statement in the `REVIEW.md` worksheet.

### 7.2.1 `docs/fair-job-queue-design.md`
- [x] Walk every section in order. Storage-agnostic statements still map
  to code: e.g. "Release clears the claim id" → `lease.go` + test
- [x] Fix every **doc lags** finding; record the rest
- [x] Watch for stale vocabulary: "per-tenant" vtime, "min visible",
  "leased fallback", "trailing-edge LWM", "monotone hold", SCALE 1e6,
  "claim index" with `liveness`/`claim_id` keys

### 7.2.2 `docs/mongodb-go-library-design.md`
- [x] Walk every section in order; check field names and BSON tags
  against `job.go`, query filters/sorts/projections against `claim.go`,
  `queue.go`, `resolve.go`, `lease.go`, index specs against
  `EnsureIndexes`, errors against `errors.go`
- [x] The pre-2026-10-17 explain checks: confirm each is labeled
  historical and none is cited as current behavior
- [x] Fix every **doc lags** finding; record the rest

---

## 7.3 Reconciliation: Reverse Pass, Invariants, Rejected Alternatives

- [x] Reverse pass: list every exported identifier (`go doc -all .`),
  sentinel error, and `Option`. For each, find where the docs describe
  it; flag behavior or caller obligations stated only in godoc (e.g.
  read concern, collation, write concern, `OmitEmpty`, client-clock
  `visible_at` skew)
- [x] Godoc vs docs: where both state a rule, they must agree in
  substance. Disagreement is a deviation unless one is plainly older
- [x] "Correctness invariants": for each invariant, name the enforcing
  test(s) in the worksheet, or mark it "caller obligation" or
  "unenforced" (e.g. Bounded degradation, Work conservation)
- [x] "Rejected alternatives" and `docs/rejected-designs.md`: flag any
  alternative the code adopted, and any argument stated against a model
  the code no longer has
- [x] **Test**: for each **untested** finding, add the test here if it is
  small; otherwise record it in `REVIEW.md`

---

## 7.4 Reconciliation: Plan, AGENTS.md, Open Questions

- [x] `docs/implementation-plan.md` Phases 1-5: add short as-built notes
  where specs were superseded (1.3 and 3.4 index keys → the three partial
  indexes; 3.1 per-tenant cache → per-(tenant, partition); 3.1 "LWM
  floor" wording). Do not rewrite the original subsections
- [x] `docs/implementation-plan.md` Phase 6 preamble: confirm it names
  6.5 and 6.6 (SCALE, checked arithmetic, service clock, partial indexes)
- [x] AGENTS.md "The model in brief" and "Mongo/Go specifics": check each
  bullet against the code
- [x] mongodb doc Open Questions: close #1 (`Mux`) with a pointer to the
  deferred list
- [x] Deferred lists: the plan's "Future Phases", the mongodb doc's
  "Deferred / future work", and the fair doc's deferrals. Each item
  appears in exactly one place; the others point to it
- [x] Doc audit grep across `docs/`, `*.go`, AGENTS.md: "Phase [0-9]",
  "deferred to a later phase", "TODO", "not yet" -- remove references
  that no longer hold

---

## 7.5 Deviation Review

- [x] Walk `REVIEW.md` deviations with the user one at a time. For each,
  record the decision in `REVIEW.md` (fix code / fix doc / accept and
  document / defer)
- [x] Apply each decision as its own commit, with a test for any code
  change. Record any rejected alternative in the design doc or
  `docs/rejected-designs.md`, per AGENTS.md
- [x] Untested follow-ups from 7.3: write or move to Future Phases
- [x] Mark 7.2-7.5 done in `docs/implementation-plan.md`

---

## 7.6 Package Godoc (`doc.go`)

- [ ] Replace the placeholder sentence ("Full godoc is deferred to a
  later phase") with a full package doc. Use the `library-docs` skill
- [ ] Model overview: virtual-time fairness per partition, memoryless
  placement, stamp at enqueue, lease not removal, fenced resolution
- [ ] Caller obligations, each one sentence with a link to the identifier
  that states it in full: `EnsureIndexes` before use; `Reconcile` on a
  ticker; optional `EnsureTTLIndex`; single writer per (tenant,
  partition); weight consistency across producers; acknowledged writes
  (`w: "majority"` for single resolution across failover); reads observe
  own inserts; simple default collation; no `OmitEmpty`; effects are
  at-least-once (idempotent workers); delayed jobs jump the queue when
  they come due; bounded retries are the worker's job (`Attempts`)
- [ ] Lifecycle summary: Enqueue → Claim → Heartbeat / Release /
  Complete; Cancel; Get
- [ ] Check rendering with `go doc .` (headings, links)

---

## 7.7 Worker-Pool Example (`example_test.go`)

- [ ] `Example` (package-level): `NewKind[T]`, `Enqueue`, a fixed pool of
  workers looping `Claim` → `Decode` → work → `Complete`; `ErrNoJob`
  backs off; a heartbeat goroutine for long work; `Release` with a delay
  on transient failure; ctx cancellation for shutdown. No `// Output:`
- [ ] Keep it short enough to read on pkg.go.dev; split heartbeat or
  release into separate `ExampleClaimedJob_Heartbeat` /
  `ExampleClaimedJob_Release` if the main example passes ~60 lines
- [ ] **Test**: `go vet ./...` compiles the examples

---

## 7.8 README (`README.md`)

- [ ] Use the `library-docs` skill. Badges: pkg.go.dev reference
  (`github.com/xdg-go/mongoqueue`), CI status (`.github/workflows/test.yml`),
  Go Report Card, license (Apache-2.0)
- [ ] One-paragraph pitch (what it is, what it deliberately is not), a
  `go get` line, a pointer to godoc for usage and to `docs/` for design
- [ ] Brief: no API replay, no usage rules (those are godoc's)
- [ ] Mark 7.6-7.8 done in `docs/implementation-plan.md`

---

## Future Phases (Deferred)

See [docs/implementation-plan.md](docs/implementation-plan.md) "Future
Phases" -- the single deferred list. Add items there, not here.
