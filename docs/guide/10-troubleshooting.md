# 10. Troubleshooting

## Symptoms

### `Run` returns `ErrRunInProgress`

Another caller or instance holds the run's lease right now (a concurrent request with the same key, or a previous attempt still executing). Wait for it: `wf.Lookup(ctx, eng, key)` then `h.Wait(ctx)`. If the holder crashed, its lease expires after `LeaseTTL` and the next call (or recovery) takes over.

### `Run` returns `ErrRunWaiting` and the run never finishes

The run is parked (retry, `Sleep`, `Await`, busy lock) and needs a **started** engine to resume it.

1. Is `eng.Start` called on at least one instance? Inline-only deployments never resume parked runs.
2. What is it waiting for?

```sql
SELECT id, status, wake_at, await_name, deadline, lease_until, owner_id
FROM saga.run WHERE id = $1;
```

- `await_name` set: waiting for that signal (`saga.signal` holds the signals delivered so far). With `wake_at` NULL there is no timeout and no deadline: only the signal wakes it; check the sender.
- `wake_at` in the future: the earliest of what it waits for: a retry backoff, `Sleep`, an `Await` timeout, a `LockWait` retry, or the run deadline (`deadline` column). Durable delays round up to about 5 s.
- Status `compensating`, `owner_id` NULL, `wake_at` set: a compensation backoff.
- `wake_at` in the past: the resume job should run; check River (`SELECT * FROM saga.river_job WHERE args->>'run_id' = $1`) and the instances' logs.

### Run stuck in `running` with nobody executing it

```sql
SELECT id, owner_id, lease_until, now() - lease_until AS expired_for
FROM saga.run WHERE status IN ('running', 'compensating') AND lease_until < now()
ORDER BY lease_until LIMIT 20;
```

Expired leases are picked up by the leader's recovery scan every `RecoveryInterval`. If rows stay here: no instance called `Start`, the River leader is not elected, all `MaxWorkers` are busy, or the workflow `name@version` is registered in no running binary (logged as "resume skipped: workflow not registered").

### Run in `needs_attention`

```sql
SELECT r.id, r.workflow, r.version, r.error, s.seq, s.name, s.status, s.error, s.comp_attempt
FROM saga.run r JOIN saga.step s ON s.run_id = r.id
WHERE r.status = 'needs_attention' AND r.id = $1 ORDER BY s.seq;
```

- A step in `compensation_failed`: its compensator failed until its `CompensatePolicy` ran out (or returned `Permanent`). Fix the cause, then `eng.RetryCompensation(ctx, runID)`.
- Cause mentions `non-deterministic`: the code changed under a running workflow (chapter 3). Restore the old version, or register it under its old version, then `RetryCompensation` to compensate what was done.

### `ErrLockHeld`

Another run holds the business lock. Expected for concurrent operations on the same entity; use `LockWait` to wait instead of failing. To see the holder:

```sql
SELECT key, run_id, until, until < now() AS expired FROM saga.lock WHERE key = $1;
```

### `ErrLockLost`

The run was parked longer than the lock's `ttl` (or its instance died), and another run took the lock meanwhile. The run compensates. Raise the `ttl` above the longest expected wait, or restructure so the lock is taken after long waits.

### `ErrLeaseLost` in logs, or steps executed twice

An instance lost its lease (pause, partition, database unreachable for most of a `LeaseTTL`) and another took over; the step that was in flight runs again with the same key. Expected and safe if the module deduplicates on the key. If it happens often: GC pauses or CPU starvation (raise `LeaseTTL`), or an overloaded database.

### `ErrWorkflowNotRegistered`

The run's workflow `name@version` is not defined in this binary. During a rolling deploy this is normal; otherwise a version was removed while runs still use it. Re-register it.

### `ErrIdemKeyConflict`

The idempotency key is used by a run of another workflow. Prefix keys with the workflow or domain.

### Steps are slow

- Each step is two round trips plus commit flushes; on slow storage, the commit dominates ([benchmarks](../benchmarks.md)).
- Connection waits: check `pool.Stat()` (`EmptyAcquireCount`, `AcquireDuration`); raise `MaxConns`.
- Long-held row locks on `saga.run` (manual queries, long transactions touching a run) block checkpoints.

## Inspection queries

```sql
-- Runs by status
SELECT status, count(*) FROM saga.run GROUP BY status;

-- A run and its steps
SELECT * FROM saga.run WHERE id = $1;
SELECT seq, name, status, attempt, uncertain, error, error_kind, started_at, finished_at
FROM saga.step WHERE run_id = $1 ORDER BY seq;

-- A run by idempotency key
SELECT id, status FROM saga.run WHERE idem_key = $1;

-- Oldest parked runs (waiting, or compensating in a backoff)
SELECT id, workflow, status, wake_at, await_name FROM saga.run
WHERE status = 'waiting' OR (status = 'compensating' AND owner_id IS NULL AND wake_at IS NOT NULL)
ORDER BY updated_at LIMIT 20;

-- Steps retrying the most
SELECT name, count(*), max(attempt) FROM saga.step
WHERE status IN ('retrying', 'started') GROUP BY name ORDER BY count(*) DESC;

-- Business locks held
SELECT key, run_id, until FROM saga.lock ORDER BY until;

-- Outbox backlog (event bus)
SELECT count(*), min(created_at) FROM eventbus.outbox WHERE sent_at IS NULL;
```

Never update `saga.run` or `saga.step` by hand for a run that may be executing: every write of the engine is fenced on the lease, and a manual write can break that. Use `RetryCompensation`, `Signal`, or let the lease expire.
