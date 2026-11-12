# Guarantees and failure modes

## What is guaranteed

| Property | Guarantee | Mechanism |
| --- | --- | --- |
| One run per idempotency key | Exactly one, within `Options.Retention` (a purged run's key starts a new run) | Unique `idem_key`, `INSERT ... ON CONFLICT` |
| Concurrent execution of a run | Never two writers | Lease taken only when free; every checkpoint fenced on `owner_id` + `lease_version` under a row lock |
| Step invocation | At least once | Re-executed after crash, retry or takeover, always with the same `Key` |
| Step effect | Once, if the module deduplicates on `Key` | `idem.Do` in the module transaction |
| Compensation | At least once per completed or uncertain step, reverse order (with `Options.AsyncStepStart`, a step whose `started` record was lost in a Postgres crash and that is not re-run before compensating is not known to the engine) | Compensators found by name, durable retries |
| Progress after a crash | Resumed by another instance | Lease expiry + leader recovery scan + River `resume_run` |
| Wake-ups (retry, Sleep, Await timeout, signals) | Not lost | Job inserted in the same transaction as the park or signal; parked runs keep a `lease_until` safety net for recovery |
| Events of a run | Published only if the run is done; at least once per subscriber | Staging released in the `done` transaction; outbox relayed with deterministic BullMQ job ids |
| Time | Database time only | `now()` for leases, wake-ups, deadlines, locks |

## Failure modes

**Instance crash (SIGKILL, OOM, node loss).** Its leases expire after `LeaseTTL`; the leader's recovery scan enqueues the runs; another instance resumes them. The step that was in flight runs again with the same key. Covered by `test/chaos` (SIGKILL mid-step, three instances racing recovery).

**Instance pause (GC, VM freeze) or partition from Postgres.** Another instance takes over after the TTL. When the paused instance wakes up, its next checkpoint fails (`ErrLeaseLost`), its heartbeat cancels the run context, and nothing it does afterwards is recorded. A side effect it performs in the meantime is deduplicated by the module (`idem`), or fenced off by `idem.Abort` if the run was compensated. Covered by `test/chaos` (SIGSTOP past the lease).

**Postgres unavailable.** Checkpoints fail and runs stop (no progress without the source of truth). If renewal fails for `LeaseTTL - LeaseTTL/3`, the instance cancels its runs itself. Everything resumes from the last checkpoint once Postgres is back.

**Redis unavailable.** Saga execution is unaffected. Events accumulate in the outbox and are pushed when Redis returns. Jobs already pushed live in Redis: run it with AOF (`appendfsync always` to lose none).

**Relay crash between push and marking rows sent.** The rows are pushed again with the same job ids; BullMQ ignores them (within `CompletedRetention`).

**Graceful shutdown.** `Stop` drains, then cancels what is left and releases the leases so another instance resumes at once.

**Rolling deploy.** A run whose workflow (or version) is unknown to the instance that picks it is left alone and enqueued again later, for an instance that knows it. Event subscribers must be registered on every relaying instance.

**Non-deterministic workflow change.** Detected on replay (different step at a recorded position, or recorded output that no longer decodes): the run is parked as `needs_attention`. Fix by versioning the workflow, then `RetryCompensation` (compensates what was done).

**Compensation that keeps failing.** Retried with backoff up to its policy, then `needs_attention` with a `saga.needs_attention` event. The run keeps its business locks until their TTL.

**Lock holder parked or dead.** Its lock lapses after `ttl`; another run may take it. When the holder resumes forward it fails with `ErrLockLost` and compensates.

## What is not guaranteed

- Exactly-once side effects without module cooperation: a module that ignores the `Key` may act twice.
- Ordering of events, or of deliveries to one subscriber.
- Progress while no instance has called `Start` (only inline calls with the same key resume runs).
- Bounded latency of durable delays below ~5 seconds (River scheduler granularity).
- Liveness with a permanently failing `RunHook` or an unregistered workflow (retried forever, logged).

See TASKS.md "Known limitations" for the complete, current list.
