// Package saga is a saga orchestrator for Go services backed by Postgres.
//
// Workflows are plain Go functions that call registered steps. Every step
// outcome is checkpointed to Postgres, so a run interrupted by a crash or a
// deploy is resumed by another instance: completed steps are replayed from
// their recorded outputs, and a failed run is compensated in reverse order
// using compensation data persisted with each step.
//
// Ownership of a run is a lease (owner + expiry + version). Every write checks
// the lease version (fencing), so an instance that lost its lease cannot
// overwrite the progress of the instance that took over.
//
// Guarantees and obligations:
//   - Steps run at least once. A step re-executed after a crash receives the
//     same Key; the module it calls must deduplicate on that key.
//   - Compensators run at least once and must be idempotent. A compensator may
//     be asked to undo a step whose outcome was never recorded; see
//     CompensationUncertain.
//   - Code between steps must be deterministic, since it is re-executed on
//     resume.
package saga
