// Package mongoqueue implements a fair, multi-tenant durable job queue on
// MongoDB. Jobs are stamped with a per-tenant virtual timestamp at enqueue so
// that heavier-weighted and cheaper work sorts earlier and a tenant flooding
// work pushes only its own later jobs back; workers claim the lowest visible
// stamp in a single atomic operation. Claiming leases a job rather than
// removing it -- a fenced claim id and a forward-moved visibility time keep the
// record pending, so expired leases lazily re-enter the visible set and exactly
// one terminal resolution wins. Full godoc is deferred to a later phase.
//
// Fairness is per partition and memoryless: each partition divides its
// capacity among the tenants with pending work there by their present
// weights, and an idle tenant earns no credit and pays no penalty when it
// returns. Callers have two obligations beyond enqueueing and claiming. They
// call [Queue.EnsureIndexes] before use, and they call [Queue.Reconcile]
// periodically, typically on a ticker, to bound how far each node's cached
// virtual times drift from the stored jobs.
package mongoqueue
