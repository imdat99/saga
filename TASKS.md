# Tasks

Progress tracker for the saga orchestrator. Design and decision log: [docs/PLAN.md](docs/PLAN.md). Working conventions: [AGENTS.md](AGENTS.md).

Legend: `[x]` done, `[~]` in progress, `[ ]` todo. Update this file in the same change as the code.

## Overview

| Milestone | Scope | Status |
| --- | --- | --- |
| M0 | Repo skeleton, tooling | Done |
| M1 | Synchronous core | Done |
| M2 | Lease heartbeat, recovery, multi-instance | Done |
| M3 | Retry, Sleep, Await/Signal, timeouts, durable compensation | Done |
| M4 | Business locks, `idem` helper | Done |
| M5 | Event bus: staging, outbox, relay to BullMQ | Done |
| M6 | Example modulith, e2e, benchmarks, observability | Done |

## M0 - Skeleton

- [x] `go.mod` (`github.com/imdat99/saga`), pgx/v5, goose, uuid, testcontainers
- [x] `docker-compose.yml`: Postgres 16, Redis 7 (`noeviction`, AOF)
- [x] `Makefile`: `up`, `down`, `test`, `test-chaos`, `lint`
- [x] goose migrations embedded (`migrations/`), `saga.Migrate` with session lock
- [x] `internal/testdb`: one container per test binary, one database per test, `SAGA_TEST_DSN` override
- [x] `.golangci.yml` with depguard rules (core must not import add-ons; example modules must not import each other)
- [x] CI workflow (lint + `go test -race`, chaos tag)

## M1 - Synchronous core

- [x] Schema `saga.run`, `saga.step` (one row per step, no full-flow rewrite)
- [x] `DefineStep[I,O,C]`, `DefineWorkflow[I,O]`, global registry (duplicate names panic)
- [x] `Workflow.Run` with `idem_key`: start, join finished run, resume expired lease, `ErrRunInProgress`
- [x] Per-step checkpoint and replay of completed steps
- [x] Fencing: every write checks `lease_version` under a row lock (`store.Fenced`)
- [x] Compensation in reverse order from persisted `comp_data`
- [x] Uncertain compensation for steps left `started` (`saga.CompensationUncertain`)
- [x] Inline compensation retry with backoff, then `needs_attention`
- [x] Non-determinism detection parks the run as `needs_attention`
- [x] Tests: happy path, reverse compensation, crash resume (A), fencing (B), concurrent same key across 2 engines (C), compensation failure, non-determinism
- [x] Verified: `go test -race -count=10` green

## M2 - Multi-instance recovery

- [x] Per-instance batched heartbeat (one ticker, `UPDATE ... FROM unnest(ids, versions)`), goroutine only while runs are active; stops renewing interrupted runs
- [x] Cancel the run context (cause `ErrLeaseLost`) when renewal is rejected or has failed for a whole TTL
- [x] Add River (`riverpgxv5`), River migrations in `Migrate` (River tables in schema `saga`, advisory lock)
- [x] Job `resume_run{run_id}`; enqueue reserves the run (owner NULL, grace) so recovery does not duplicate queued jobs; the lease dedupes execution (River uniqueness not used, see decision log)
- [x] Periodic leader-only recovery scan (`lease_until < now()`, `FOR NO KEY UPDATE SKIP LOCKED`, batch limit)
- [x] `Engine.Start(ctx)` / `Engine.Stop(ctx)` lifecycle (River client, heartbeat), graceful shutdown drains, then cancels and releases leases for recovery
- [x] `Handle` API: `Status`, `Wait(ctx)`; `Workflow.Start` (enqueue in one tx), `Workflow.Lookup`
- [x] Chaos tests (`-tags chaos`, real child processes): SIGKILL mid-step, SIGSTOP past the lease, 3 instances racing recovery
- [x] Docs: pool sizing, LISTEN connection and PgBouncer note (`docs/operations.md`)

## M3 - Async steps and durable retries

- [x] `RetryPolicy` per step (`WithRetry`, `Exponential`, `saga.Permanent`), backoff capped, float overflow clamped
- [x] Retry parks the run (`waiting`, owner NULL) and schedules `resume_run` at a database time in the same tx; same `Key` on every attempt
- [x] `saga.Sleep(c, d)` with checkpointed wake time
- [x] `saga.Await[T](c, name, timeout)` and `Engine.Signal(ctx, runID, name, payload)` (`saga.signal` table, run row lock prevents lost wake-ups, first signal wins)
- [x] Step timeouts (`WithTimeout`, cause `ErrStepTimeout`, transient) and run deadline (`WithRunTimeout`, checked at step start and caps wake-ups); no `step_timeout` job (see decision log)
- [x] Durable `CompensatePolicy` via River (`WithCompensatePolicy`, default 10 attempts from 1s); inline retry and `Options.CompensateAttempts/Backoff` removed
- [x] `Engine.RetryCompensation(ctx, runID)` for `needs_attention` runs (fresh attempt budget)
- [x] Checkpointed `saga.Now(c)` (database time) / `saga.Rand(c)` (crypto/rand) helpers
- [x] Tests D (transient payment failures), E (compensation exhausted then manual retry), plus Sleep, Await/Signal (before and after parking), Await timeout, run deadline, step timeout, Now/Rand replay
- [x] `saga.needs_attention` event: emitted by the event bus hook (`eventbus.NeedsAttentionEvent`) in the transaction that parks the run
- [x] `Engine.SignalKey(ctx, key, payload)` and `Key.RunID()`: signal a run when the external party only knows the step `Key` (long-running external work, PLAN item 9)
- [x] Docs: long-running external work as "start step + `Await`" (`docs/patterns.md`)
- [x] Review fixes: error kind persisted (`error_kind`, replay keeps sentinels and `Permanent`), sticky `uncertain` flag (timed-out attempts are compensated as uncertain), signal wake-up reserved for recovery, bad signal payload fails the step, `AcquireLease` returns the row it took, `ErrRunWaiting` for compensation backoff

## M4 - Locks and idempotency helper

- [x] `saga.lock` table (migration 00004), `saga.Lock(c, key, ttl)` pseudo-step, released in the tx that sets done/compensated, TTL fallback; replay re-asserts (extends) the lock or fails with `ErrLockLost`
- [x] Lock contention policy: fail fast with `ErrLockHeld` (default) or `LockWait(RetryPolicy)` parking the run between attempts
- [x] `idem` package: `processed_requests` per module schema, `idem.Do(ctx, tx, table, key, fn)` inside the module tx (unique-key claim, concurrent callers block then read the result), `idem.Abort` for uncertain compensation, `idem.Lookup`, `idem.Purge`
- [x] Test F (fail-fast and waiting contention, TTL release after dead run, lock lost on resume), idem tests (once per key, failure stores nothing, 8 concurrent callers)
- [x] Review fixes: re-assert only extends this run's own row (a lock taken and released meanwhile is lost); the lease heartbeat extends the locks of executing runs (`ttl_us` column); ttl >= `LeaseTTL`, re-entry never shortens; `idem.Do` runs in a savepoint; `idem.Abort` tombstone for uncertain compensation; `ErrLockHeld` stays `Permanent` on replay

## M5 - Event bus

- [x] Tables `eventbus.staging`, `eventbus.outbox` (own goose migrations, schema `eventbus`, `eventbus.Migrate`)
- [x] `Bus.EmitTx(ctx, tx, events...)`: staging when inside a run (`saga.RunIDFrom`), outbox otherwise; UUIDv7 event ids
- [x] Move staging to outbox in the same tx as `status = done`; delete staging on `compensated`/`needs_attention`, via the new core `saga.RunHook` (`Options.Hooks`) so the core does not import eventbus
- [x] Relay: `SKIP LOCKED` batches (every instance may relay), fan-out per subscriber, `bullmq.Queue.AddBulk` with `JobID = eventID.subscriberID` (BullMQ rejects a single ':'), then `sent_at` in the same tx
- [x] Subscriber registry and BullMQ worker dispatch by `job.Name`; unknown subscriber returns retryable error; bad payload is unrecoverable
- [x] Job options: attempts/backoff per subscriber, `RemoveOnComplete` by age (24h), `RemoveOnFail` 7 days (DLQ)
- [x] Outbox retention cleanup (sent rows and staging of ended runs, batches until caught up, own loop every `CleanupInterval`)
- [x] Tests G (no delivery after compensation, exactly one effect per subscriber), H (relay crash between AddBulk and sent_at), needs_attention event
- [x] Review fixes: immutable running snapshot (no `Stop` deadlock with an in-flight pass), queue per subscriber by default and `Start` rejects partly handled queues, retention only drops staging of ended runs and keeps up under load (own loop, batches until caught up, SKIP LOCKED), `RunEnded` in one statement, batched `EmitTx`, nested runs do not inherit the parent's run id

## M6 - Example modulith and hardening

- [x] `examples/modulith`: modules inventory, payment (fake provider with idempotency), order; own schema and NOLOGIN role each (`SET LOCAL ROLE` per module transaction), depguard keeps modules apart
- [x] Flow `order.complete` with cart lock, reservation, payment authorize (retry + timeout), order create emitting `order.created`; uncertain compensation through `idem.Abort`
- [x] HTTP API (`POST /orders` with `Idempotency-Key`, `GET /runs/{id}`, retry-compensation, stock) and `cmd/server` (env config, graceful shutdown); e2e test runs two instances
- [x] Benchmarks (`docs/benchmarks.md`, fsync=on Postgres): engine overhead p95 2.5 ms per step (target < 5 ms met; the target means engine overhead, confirmed by the user on 2026-11-12); durable end-to-end p95 NOT under 5 ms on this disk (2.7 ms per fsync): 11.6 ms by default, 7.7 ms with `AsyncStepStart` (second run). Single-statement hot-path checkpoints: p95 14.7 -> 11.1 ms, 79 -> 119 runs/s (first run)
- [x] Non-durable `started` checkpoint: opt-in `Options.AsyncStepStart` (default off; one durable commit per step, -34% p95, +37% runs/s; risk documented)
- [x] Review fix: re-running a step left `started` marks it uncertain (also in durable mode: a resume after the deadline used to drop the uncertainty); step outcomes are upserted
- [x] Toolchain: `toolchain go1.27.4` (govulncheck GO-2026-6859 in `net/http` fixed; no reachable vulnerability left)
- [x] Metrics/logging hooks: `saga.Observer` (step attempts with outcome and duration, runs ended by status), slog everywhere; example expvar observer at `/debug/vars`
- [x] Retention: `Engine.PurgeRuns`, `Options.Retention` (leader periodic job), index `run_ended_idx` (migration 00005); autovacuum guidance in `docs/operations.md`
- [x] `docs/`: concepts, guarantees and failure modes, operations, patterns; README rewritten

## Known limitations (current)

- Recovery needs at least one instance with `Engine.Start`; inline-only deployments resume abandoned runs only when called again with the same `idem_key`.
- A run whose workflow is registered in no running binary is re-enqueued every `max(LeaseTTL, 5 x RecoveryInterval)` forever, with a warning; it is never parked.
- `Engine.Stop` must be called to close the engine's heartbeat pool.
- `Handle.Wait` polls (20ms doubling to 1s); many concurrent waiters cost one PK read each per interval.
- A step that ignores context cancellation keeps running after its lease is lost; fencing discards its result, its side effect relies on idempotency by `Key`.
- Durable delays (retry backoff, Sleep, Await timeout, compensation backoff) have about 5s granularity: River's scheduler promotes scheduled jobs every 5s.
- `Workflow.Run` returns `ErrRunWaiting` when the run parks; the caller must use `Lookup` + `Handle.Wait`, and some instance must have called `Start`.
- Replay rebuilds application errors from their message only: `errors.Is` against application sentinels does not survive a resume (engine sentinels and `Permanent` do). Branch on step outputs, not on error identity.
- A crash mid-step re-runs it and consumes one attempt of its `RetryPolicy`.
- Every resume loads the whole step history; a workflow looping over `Sleep`/`Await` N times costs O(N) memory per resume (no continue-as-new).
- A business lock lapses after `ttl` while its run is parked (retry, Sleep, Await, compensation backoff) or in needs_attention; another run may take it then. The parked run fails with `ErrLockLost` when it resumes forward, but a run resuming compensation does not re-check its locks.
- If lease renewal stalls for a whole lock `ttl` while nobody takes the run, another run may take the lock and the holder is not told (narrow: the lease would normally be lost first).
- Locks are polled, not queued (no fairness, no deadlock detection): take several locks in a fixed order.
- `idem.Do` under REPEATABLE READ/SERIALIZABLE turns a concurrent duplicate into a serialization error (40001): callers retry.
- Every instance relaying must register the same subscribers: a relay that does not know a subscriber skips it for the events it relays (rolling deploy adding a subscriber).
- The relay holds its outbox rows locked while it calls Redis (bounded batch): a slow Redis slows the relay, it does not lose events. An event that can never be pushed blocks its batch (no quarantine).
- A `RunHook` that keeps failing (e.g. eventbus schema not migrated) makes the run's terminal transition fail on every resume; the run is never parked.
- `eventbus.Unrecoverable` keeps only the error message (BullMQ API).
- One BullMQ queue per subscriber by default: each handled subscriber costs a worker (blocking Redis connection, up to `Concurrency` goroutines), and each relay pass makes about two Redis round trips per subscriber queue, sequentially.
- Each step costs 2 round trips and 2 durable commits (1 durable + 1 async with `AsyncStepStart`); retries, parking and compensation still use multi-statement fenced transactions.
