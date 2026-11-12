# Operations

How to run the saga engine in production: lifecycle, connection pool, and what each instance costs.

## Lifecycle

```go
pool, _ := pgxpool.New(ctx, dsn)
if err := saga.Migrate(ctx, pool); err != nil { ... } // saga tables + River tables, schema "saga"
eng, err := saga.New(pool, saga.Options{})
if err := eng.Start(ctx); err != nil { ... }          // queue workers + leader-only recovery
defer eng.Stop(shutdownCtx)
```

- `Workflow.Run` executes a run inline in the caller's goroutine. It works without `Start`, but then nothing recovers runs abandoned by a crash. Every instance that should take part in recovery must call `Start`.
- `Workflow.Start` creates the run and enqueues it in one transaction; any started instance executes it. Use `Handle.Wait` to get the result.
- `Stop(ctx)` stops taking new work and waits for runs in progress until `ctx` is done. Runs still going are then cancelled (`context.Cause` is `saga.ErrShutdown`) and their leases are released (`owner_id = NULL`, `lease_until = now()`): a call with the same idempotency key takes them over at once, and recovery enqueues them on its next scan instead of after `LeaseTTL`. A step interrupted mid-flight is executed again there with the same `saga.Key`.
- Always call `Stop`: besides draining, it closes the engine's dedicated heartbeat pool.

## Leases and recovery timing

| Setting | Default | Effect |
| --- | --- | --- |
| `LeaseTTL` | 30s (min 300ms) | How long a crashed instance keeps its runs. |
| heartbeat | `LeaseTTL/3` | One batched `UPDATE ... FROM unnest(ids, versions)` per instance per tick, whatever the number of runs, on a dedicated 2-connection pool. |
| `RecoveryInterval` | 10s | The River leader enqueues a scan job; whichever instance runs it reserves up to `RecoveryBatch` (500) expired runs per transaction (`FOR NO KEY UPDATE SKIP LOCKED`), at most 10 batches per scan. |

A crashed instance's runs are typically resumed within `LeaseTTL + RecoveryInterval`, plus:

- River leader re-election (seconds) if the crashed instance was the leader,
- queue latency when all `MaxWorkers` slots of the surviving instances are busy,

Enqueuing a run (`Workflow.Start`, recovery) reserves it for its job: `owner_id = NULL`, `lease_until = now() + max(LeaseTTL, 5 x RecoveryInterval)`. Any instance may still take it (the job, or a call with the same idempotency key), but recovery leaves it alone until the reservation expires, so a queue backlog does not pile up duplicate jobs. A job whose workflow is not registered in the picking binary (rolling deploy) leaves the run untouched; recovery enqueues it again after the reservation.

If an instance cannot renew a lease for `LeaseTTL - LeaseTTL/3` (database unreachable), it cancels the run's context with `saga.ErrLeaseLost`. Fencing (`lease_version`) makes any later write from it fail anyway; the cancellation only stops side effects early.

A step that ignores context cancellation keeps running after its lease is gone. Its result is then discarded by fencing, but its side effect happened: this is why steps must be idempotent by `saga.Key`.

## Waiting runs

Retries, `Sleep`, `Await` timeouts and compensation backoff park the run in Postgres without a lease and schedule a River job at a database time. River's scheduler promotes scheduled jobs about every 5 seconds, so a 100ms backoff behaves like up to 5 seconds. Parked runs cost nothing in memory; each is one row in `saga.run` and one scheduled row in `saga.river_job`.

`Workflow.Run` returns `saga.ErrRunWaiting` when its run parks. Use `Workflow.Lookup(...).Wait(ctx)` to get the outcome, and make sure at least one instance has called `Start`.

## Connection pool sizing

Per instance, the engine needs:

- **Inline runs:** a connection only for the duration of each checkpoint transaction (two short transactions per step), never for the whole run. Size for concurrent checkpoints, not concurrent runs.
- **Queue workers:** up to `MaxWorkers` (50) runs resumed concurrently, with the same per-step pattern.
- **River:** one connection held permanently for `LISTEN` (job notifications, leader election), plus short-lived connections for fetching and completing jobs.
- **Heartbeat:** a separate pool of 2 connections (same DSN), one short query every `LeaseTTL/3`. It is separate so that a pool saturated by checkpoints cannot make the instance lose all its leases at once.

A starting point is `max_conns = 10 + concurrent checkpoints`, where concurrent checkpoints is roughly `(inline runs/s + queued runs/s) x steps per run x checkpoint latency`. Watch `pgxpool.Stat().EmptyAcquireCount()` and acquire duration: when a checkpoint waits for a connection, every step pays that wait.

Across instances, total connections are `instances x max_conns`; keep it under the server's `max_connections` with headroom for migrations and admin sessions.

## PgBouncer

River keeps a `LISTEN` connection and uses session-level advisory locks during migrations, neither of which works through PgBouncer in **transaction** pooling mode. Either:

- point the engine's pool directly at Postgres (simplest), or
- use PgBouncer in **session** mode for the engine's pool.

`Migrate` needs a pool of at least 3 connections (lock holder, goose locker, statements). It also takes session-level advisory locks (goose and River migrations); run it over a direct connection.

## Hot-path statements and pgx settings

The step checkpoints, the run insert and the end-of-run statement bypass pgx's `Query`/`Scan` layer to save allocations: they run on `pgconn` as named prepared statements (`saga_*_v1`, prepared once per connection). This fast path is used only when the pool is in `pgx.QueryExecModeCacheStatement` (the default) and has no `ConnConfig.Tracer`; otherwise the same SQL goes through pgx (about twice the allocations per step), so a tracer still sees every statement. On the fast path:

- a cancelled run context interrupts a statement in flight by setting a past deadline on its connection, like pgconn's default `DeadlineContextWatcherHandler`; a custom `BuildContextWatcherHandler` (e.g. sending a cancel request) is not used for these statements;
- a prepared statement invalidated behind the engine's back (`DEALLOCATE`, a schema change: SQLSTATE 26000 or 0A000) fails one call and closes that connection; the next one prepares it again.

## Schema

All engine tables live in the `saga` schema, including River's (`saga.river_job`, ...). An application using River for its own jobs keeps its own River tables in its own schema and is not affected by saga migrations.

Hot tables for autovacuum tuning: `saga.step`, `saga.run`, `saga.river_job`.

Every lease renewal updates `saga.run.lease_until`, an indexed column, so it is not a HOT update: each active run adds one index entry to `run_recoverable_idx` every `LeaseTTL/3`. With many long-running runs, tune autovacuum on `saga.run` accordingly (or raise `LeaseTTL`).

## Event bus

```go
bus := eventbus.New(pool, eventbus.Options{Redis: bullmq.RedisConnectionOptions{URL: redisURL}})
bus.Subscribe(eventbus.Subscriber{ID: "billing.invoice", Event: "order.created", Handle: onOrderCreated})
eng, _ := saga.New(pool, saga.Options{Hooks: []saga.RunHook{bus}})
_ = eventbus.Migrate(ctx, pool)
_ = bus.Start(ctx)
defer bus.Stop(shutdownCtx)
```

- Redis must run with `maxmemory-policy noeviction` and AOF; watch its memory. Losing Redis delays delivery of events still in the outbox; it does not lose them. Events already pushed (rows marked sent) and not yet processed live only in Redis: with `appendfsync everysec` up to one second of them can be lost in a Redis crash, use `appendfsync always` if that matters.
- Every instance runs the relay; batches use `SKIP LOCKED`. Register the same subscribers on every instance, or events relayed by an instance that lacks one skip it.
- Each subscriber gets its own BullMQ queue by default. If you group subscribers on one `Queue`, every instance with a handler for one of them needs handlers for all (`Start` refuses otherwise): a worker that takes a job it cannot handle wastes an attempt.
- A subscriber registered without a handler on any instance accumulates jobs in Redis forever: remove it from every instance (or drain its queue) when retiring it.
- One event that cannot be pushed (e.g. Redis OOM) blocks the relay for the whole batch it is in, on every instance, until it can.
- Delivery is at least once and unordered: handlers deduplicate by `Event.ID` and re-read state.
- Failed jobs stay 7 days in BullMQ's failed set (dead letter queue); completed jobs 24h, which is also the window in which a re-pushed job id is ignored.
- Sent outbox rows, and staging rows of runs that already ended, are deleted after `Retention` (7 days) by a retention loop every `CleanupInterval` (1 minute), in batches of 5000 until caught up.

## Retention and vacuum

- `saga.Options{Retention: 30 * 24 * time.Hour}` purges done and compensated runs (the River leader enqueues the job, any instance runs it, on its own queue) (steps, signals and locks cascade) every `RetentionInterval` (1h), in batches of 1000. `needs_attention` runs are kept. `Engine.PurgeRuns` does the same on demand. Retention is also the deduplication window of idempotency keys: after it, the same key starts a new run. Purge the modules' `processed_requests` (`idem.Purge`) with a longer age.
- River deletes its own completed jobs (24h by default).
- `eventbus` deletes sent outbox rows after `Retention` (7 days).
- Hot tables: `saga.run` (lease renewals are non-HOT updates of an indexed column), `saga.step` (one insert and one or two updates per step), `saga.river_job`, `eventbus.outbox`. Lower their `autovacuum_vacuum_scale_factor` (e.g. 0.01) so vacuum keeps up with the churn instead of waiting for 20% dead tuples.

## Metrics

Implement `saga.Observer` to export step attempts (outcome, duration) and runs ended (by status); the example modulith publishes them with `expvar` at `/debug/vars`. Useful alerts: runs ending `needs_attention` (also an event), step `retrying` rate, step duration percentiles, and the age of the oldest unsent outbox row.
