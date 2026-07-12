package mongoqueue

// Liveness is the two-value lifecycle state of a job: pending (awaiting or
// undergoing service) or resolved (terminal). There is no separate "claimed"
// state -- a claimed job is pending with a live lease, and an expired lease
// re-enters the visible set with no state transition.
//
// Liveness is stored under the BSON field "liveness", always present. Claim,
// heartbeat, complete, release, and cancel are server-side conditional writes
// that filter on the pending literal, so the stored value must match the
// constant exactly.
type Liveness string

const (
	// LivenessPending marks a job awaiting or undergoing service. Every
	// guarded write (claim, heartbeat, complete, release, cancel) filters
	// on this literal server-side.
	LivenessPending Liveness = "pending"

	// LivenessResolved marks a terminal job. Exactly one terminal write
	// (complete or cancel) wins the transition; a resolved job never
	// re-enters the visible set.
	LivenessResolved Liveness = "resolved"
)

// Resolution records how a job ended. The vocabulary is open and
// caller-defined -- the library declares no constants. The queue establishes
// the value at the terminal write (Complete or Cancel) but never interprets
// it; it exists for Get lookups and ops queries.
//
// Resolution is stored under the BSON field "resolution" and is absent while
// the job is pending. It is a scalar discriminator today; the representation
// may be extended later (e.g. an accompanying opaque detail field).
type Resolution string
