# 9. Production

## Engine configuration

```go
eng, err := saga.New(pool, saga.Options{
	LeaseTTL:          30 * time.Second, // takeover delay after a crash; renewed every TTL/3
	MaxWorkers:        50,               // queued runs executed concurrently per instance
	RecoveryInterval:  10 * time.Second, // leader scan for expired leases
	ShutdownGrace:     5 * time.Second,  // how long Stop waits after cancelling runs
	Retention:         30 * 24 * time.Hour, // purge ended runs; also the dedup window of keys
	Hooks:             []saga.RunHook{bus},  // event bus
	Observer:          metrics,              // see Monitoring
	Logger:            logger,
	// InstanceID: default "<hostname>-<pid>-<random>", unique per process
	// AsyncStepStart: false (see below)
})
```

| Option | Default | Raise when | Lower when |
| --- | --- | --- | --- |
| `LeaseTTL` | 30 s | GC pauses or network hiccups cause spurious takeovers | You need faster recovery after crashes (min 300 ms) |
| `MaxWorkers` | 50 | Many queued runs wait (retries, Start, signals) | Steps are heavy and saturate the instance |
| `RecoveryInterval` | 10 s | Huge numbers of runs make scans expensive | Faster recovery is needed |
| `Retention` | 0 (keep) | Keys must be deduplicated longer | Tables grow too much |

`AsyncStepStart: true` saves about one fsync per step (one durable commit instead of two) at a precise risk: in a Postgres crash, the record that a step had started can be lost; see its documentation and [benchmarks](../benchmarks.md) before enabling.

Whether every instance serves the API and executes runs, or the roles are split: [chapter 11](11-api-or-worker.md#deployment-topologies).

## Startup and shutdown

```go
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := mustPool(ctx)
	must(saga.Migrate(ctx, pool))        // safe to run on every instance: advisory lock
	must(eventbus.Migrate(ctx, pool))
	eng := mustEngine(pool)
	must(eng.Start(ctx))
	must(events.Start(ctx))      // eventbus producer: relay + retention
	must(consumer.Start(ctx))    // this service's event handlers, if any
	srv := startHTTP()

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)   // 1. stop accepting requests
	_ = eng.Stop(shutdown)       // 2. drain runs; cancel and hand over the rest
	_ = events.Stop(shutdown)    // 3. stop the relay
	_ = consumer.Stop(shutdown)  //    and the event handlers
	pool.Close()
}
```

`Stop` waits for runs in progress until its context is done, then cancels them with `saga.ErrShutdown` and releases their leases so another instance resumes them at once. Give the orchestrator (Kubernetes `terminationGracePeriodSeconds`) more time than the `Stop` timeout plus `ShutdownGrace`.

## Database

- **Migrations:** `saga.Migrate` and `eventbus.Migrate` take session-level advisory locks: run them over a direct connection (not PgBouncer in transaction mode), with a pool of at least 3 connections.
- **Pool size:** each step does two short checkpoints; size for concurrent checkpoints, not concurrent runs. Start with `max_conns = 10 + concurrent checkpoints` and watch acquire waits (`pool.Stat().EmptyAcquireCount()`): a waiting checkpoint delays every step. An undersized pool also adds allocations, one waiter per waiting acquire ([benchmarks](../benchmarks.md)). Details: [Operations](../operations.md#connection-pool-sizing).
- **PgBouncer:** River needs `LISTEN` and advisory locks; point the engine's pool at Postgres directly or use session pooling.
- **pgx settings:** the hot-path checkpoints use a lower-allocation path when the pool is in the default `QueryExecModeCacheStatement` and has no `ConnConfig.Tracer`; with a tracer or another mode they take the regular pgx path (correct, about twice the allocations). See [Operations](../operations.md#hot-path-statements-and-pgx-settings).
- **Vacuum:** `saga.run`, `saga.step`, `saga.river_job` and `eventbus.outbox` churn; lower their `autovacuum_vacuum_scale_factor` (e.g. 0.01).
- **Durability vs latency:** with `fsync=on` each step costs one or two commit flushes; measure your storage (`pg_test_fsync`).

## Deploys

- Workflows and steps must be registered in every binary that may resume their runs. During a rolling deploy, an instance that does not know a run's `name@version` leaves it to another instance.
- Change the step sequence only with a new workflow version, and keep the old version until its runs are done (chapter 3).
- Never rename a step that has recorded rows; never change its output or compensation data types incompatibly.
- Adding a compensator to a step that had none: steps recorded before have no compensation data and are compensated as uncertain (`CompensationUncertain` true, zero data). Write the new compensator so it works from the key.
- Event consumer groups register their routes when they start; a group does not receive the events relayed before its first registration (chapter 7).

## Monitoring

Implement `saga.Observer` (called after the fact, must not block):

```go
type metrics struct {
	stepDuration *prometheus.HistogramVec // labels: workflow, step, outcome
	runsEnded    *prometheus.CounterVec   // labels: workflow, status
}

func (m *metrics) StepAttempt(a saga.StepAttempt) {
	m.stepDuration.WithLabelValues(a.Workflow, a.Step, a.Outcome).Observe(a.Duration.Seconds())
}

func (m *metrics) RunEnded(r saga.RunInfo) {
	m.runsEnded.WithLabelValues(r.Workflow, string(r.Status)).Inc()
}
```

Alert on:

| Signal | Why |
| --- | --- |
| Any run ending `needs_attention` (or the `saga.needs_attention` event) | Manual action required |
| Rate of step outcome `retrying` | A dependency is failing |
| Step duration p95/p99 | Slow dependency or pool waits |
| Count of `running` runs with `lease_until < now()` for longer than a few `RecoveryInterval` | Recovery not keeping up, or no started instance |
| Age of the oldest unsent outbox row | Relay or Redis problem |
| Pool acquire waits | Undersized pool |

Useful queries are in [chapter 10](10-troubleshooting.md).

## Retention

- `Options.Retention` purges `done` and `compensated` runs (with steps, signals, locks); `needs_attention` runs are kept. It is also how long idempotency keys are deduplicated.
- Purge the modules' `processed_requests` with `idem.Purge` using a **longer** age.
- River deletes its own completed jobs (24 h); the event bus deletes sent outbox rows after its `Retention` (7 days).

## Production checklist

- [ ] Every instance calls `eng.Start`, and `eng.Stop` on shutdown, with a grace period longer than the stop timeout.
- [ ] Pool sized for concurrent checkpoints; direct connection or session pooling.
- [ ] Every step's module deduplicates on `Key`; compensators work when uncertain.
- [ ] Workflows are versioned; old versions stay registered while their runs exist.
- [ ] `Retention` set; `idem.Purge` scheduled with a longer age.
- [ ] Alerts on `needs_attention`, retries, durations, recovery lag, outbox age.
- [ ] Redis (if used) with `noeviction` and AOF.
- [ ] A runbook for `needs_attention` (chapter 4, chapter 10).
