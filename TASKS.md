# Tasks

Progress tracker for the saga orchestrator. Design and decision log: [docs/PLAN.md](docs/PLAN.md). Working conventions: [AGENTS.md](AGENTS.md).

Legend: `[x]` done, `[~]` in progress, `[ ]` todo. Update this file in the same change as the code.

## Overview

| Milestone | Scope | Status |
| --- | --- | --- |
| M0 | Repo skeleton, tooling | Mostly done (lint config missing) |
| M1 | Synchronous core | Done |
| M2 | Lease heartbeat, recovery, multi-instance | Todo |
| M3 | Retry, Sleep, Await/Signal, timeouts, durable compensation | Todo |
| M4 | Business locks, `idem` helper | Todo |
| M5 | Event bus: staging, outbox, relay to BullMQ | Todo |
| M6 | Example modulith, e2e, benchmarks, observability | Todo |

## M0 - Skeleton

- [x] `go.mod` (`github.com/imdat99/saga`), pgx/v5, goose, uuid, testcontainers
- [x] `docker-compose.yml`: Postgres 16, Redis 7 (`noeviction`, AOF)
- [x] `Makefile`: `up`, `down`, `test`, `test-chaos`, `lint`
- [x] goose migrations embedded (`migrations/`), `saga.Migrate` with session lock
- [x] `internal/testdb`: one container per test binary, one database per test, `SAGA_TEST_DSN` override
- [ ] `.golangci.yml` with depguard rules (modules must not import each other)
- [ ] CI workflow (lint + `go test -race`)

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

- [ ] Per-instance batched heartbeat (one ticker, `UPDATE ... WHERE id = ANY($1)`), stop on terminal status or lease loss
- [ ] Cancel the run context when its lease renewal fails
- [ ] Add River (`riverpgxv5`), River migrations in `Migrate`
- [ ] Job `resume_run{run_id}` with `UniqueOpts` by run id
- [ ] Periodic leader-only recovery scan (`lease_until < now()`, `FOR UPDATE SKIP LOCKED`, batch limit)
- [ ] `Engine.Start(ctx)` / `Engine.Stop(ctx)` lifecycle (River client, heartbeat), graceful shutdown leaves runs for recovery
- [ ] `Handle` API: `Status`, `Wait(ctx)` for runs executing elsewhere
- [ ] Chaos tests (`-tags chaos`): kill instance mid-step, stale owner after pause, 3 instances racing recovery
- [ ] Docs: pool sizing, LISTEN connection and PgBouncer note

## M3 - Async steps and durable retries

- [ ] `RetryPolicy` per step (attempts, exponential backoff, permanent error marker)
- [ ] Retry schedules `resume_run` at backoff in the same tx, status `waiting`, lease released
- [ ] `saga.Sleep(c, d)` with checkpointed wake time
- [ ] `saga.Await(c, name, timeout)` and `Engine.Signal(runID, name, payload)` (`saga.signal` table)
- [ ] Step and run timeouts (`step_timeout` job)
- [ ] Durable `CompensatePolicy` via River (replace inline retry), default 10 attempts
- [ ] `Engine.RetryCompensation(runID)` for `needs_attention` runs
- [ ] Checkpointed `saga.Now(c)` / `saga.Rand(c)` helpers
- [ ] Tests D (transient payment failures), E (compensation exhausted then manual retry)

## M4 - Locks and idempotency helper

- [ ] `saga.lock` table, `saga.Lock(c, key, ttl)` step, release on done/compensated, TTL fallback
- [ ] Lock contention policy (wait with backoff vs fail)
- [ ] `idem` package: `processed_requests` per module schema, `idem.Do(ctx, tx, key, fn)` inside the module tx
- [ ] Test F (two sagas on the same lock key, TTL release after dead run)

## M5 - Event bus

- [ ] Tables `eventbus.staging`, `eventbus.outbox`
- [ ] `Bus.EmitTx(ctx, tx, events...)`: staging when inside a run (`saga.RunIDFrom`), outbox otherwise
- [ ] Move staging to outbox in the same tx as `status = done`; delete staging on `compensated`
- [ ] Relay: `SKIP LOCKED` batches, fan-out per subscriber, `bullmq.Queue.AddBulk` with `JobID = eventID:subscriberID`, then `sent_at`
- [ ] Subscriber registry and BullMQ worker dispatch by `job.Name`; unknown subscriber returns retryable error
- [ ] Job options: attempts/backoff per subscriber, `RemoveOnComplete` by age, `RemoveOnFail` 7 days (DLQ)
- [ ] Outbox retention cleanup job
- [ ] Tests G (no delivery after compensation, exactly one effect per subscriber), H (relay crash between AddBulk and sent_at)

## M6 - Example modulith and hardening

- [ ] `examples/modulith`: modules inventory, payment (fake provider with idempotency), order; own schema and DB role each
- [ ] Flow `complete-order` with lock, reservation, payment authorize, order create, events
- [ ] HTTP API and `cmd/server` running multiple instances locally
- [ ] Benchmarks: per-step overhead (target p95 < 5ms local), runs/s with 3 instances
- [ ] Metrics/logging hooks (slog, counters for runs by status, step latency)
- [ ] Retention cleanup for `saga.run` / `saga.step`, autovacuum guidance
- [ ] `docs/`: concepts, guarantees, operations, failure modes

## Known limitations (current)

- No heartbeat yet: a run longer than `LeaseTTL` (30s default) can be taken over; the first owner then stops with `ErrLeaseLost`.
- No automatic recovery yet: an abandoned run resumes only when called again with the same `idem_key`.
- Compensation retries are in-process only; a crash during compensation resumes it only on the next call.
- Each step costs 2 fenced transactions; not benchmarked yet.
