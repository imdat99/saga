# Benchmarks

`step_bench_test.go`:

```bash
make up   # Postgres 16 with default durability (fsync=on, synchronous_commit=on)
SAGA_TEST_DSN='postgres://saga:saga@localhost:5432/saga?sslmode=disable' \
  go test -run '^$' -bench . -benchtime 1000x -count 6 . | tee bench.txt
# commit-cost check: same, with SAGA_BENCH_ASYNC_COMMIT=1 (engine sessions use synchronous_commit=off)
```

Do not benchmark against the testcontainers Postgres (`make test`'s default): it runs with `fsync=off`, which hides the commit cost.

- `BenchmarkStepRun`: one instance, inline runs of 10 no-op steps; the per-step latency (p50/p95/p99) is the engine's overhead.
- `BenchmarkRunsThreeInstances`: three engines with separate pools (6 connections each) on one database, 24 concurrent callers, in one process sharing 6 CPUs with Postgres; runs/s and per-step latency under contention (includes pool queueing).

Environment: Intel Xeon E5-4669 v3 @ 2.10GHz, 6 CPUs, Go 1.27.3, no `-race`, Postgres 16 in Docker (compose). `pg_test_fsync`: 2.7 ms per synchronous write on this disk.

## Results (2026-11-12, n=6, all deltas p=0.002 unless noted)

"Before" is the hot path through `store.Fenced` (BEGIN, `SELECT ... FOR NO KEY UPDATE`, write, COMMIT per checkpoint); "after" is the single-statement fenced checkpoints (`StartStepFenced`, `FinishStepFenced`). To reproduce "before", replace those two calls in `step.go` with `Fenced` + `StartStep` / `FinishStep`.

| Metric | Before | After | Delta |
| --- | --- | --- | --- |
| StepRun p50 per step | 11.6 ms | 8.2 ms | -29% |
| StepRun p95 per step | 14.7 ms | 11.1 ms | -24% |
| StepRun p99 per step | 20.1 ms | 13.8 ms | -31% |
| StepRun allocs/op (10-step run) | 930 | 489 | -47% |
| 3 instances, runs/s (10 steps) | 79 | 119 | +51% |
| 3 instances, p95 per step | 37.3 ms | 23.9 ms | -36% |

With `synchronous_commit=off` on the engine's sessions (after): StepRun p50 1.9 ms, p95 2.5 ms (-77%).

### `Options.AsyncStepStart` (one durable commit per step)

Second run on the same setup, n=6 (default vs `AsyncStepStart: true`, `SAGA_BENCH_ASYNC_START=1`):

| Metric | Durable (default) | AsyncStepStart | Delta |
| --- | --- | --- | --- |
| StepRun p50 per step | 8.5 ms | 5.6 ms | -34% |
| StepRun p95 per step | 11.6 ms | 7.7 ms | -34% |
| 3 instances, runs/s | 116 | 159 | +37% |

### Allocation diet (M7)

`SAGA_BENCH_ASYNC_COMMIT=1` on the testcontainers Postgres (commit cost removed, so the CPU side shows), `-benchtime 500x -count 6`, HEAD before M7 vs after, run one after the other:

| Metric | Before | After | Delta |
| --- | --- | --- | --- |
| StepRun allocs/op (10-step run) | 500 | 69 | -86% |
| StepRun B/op | 25.6 KiB | 3.6 KiB | -86% |
| StepRun ns/op | 24.9 ms | 22.0 ms | -11.5% |
| StepRun p50 / p95 per step | 1.99 / 2.60 ms | 1.96 / 2.55 ms | ~ (p > 0.05) |
| 3 instances, allocs/op | 568 | 126 | -78% |
| 3 instances, runs/s | 191 | 206 | +8% |

Where the 431 allocations went: the hot-path statements run on `pgconn` directly instead of pgx's `Query`/`Scan` (argument boxing, rows and scan plans per query: ~240), one context watch per execution instead of pgconn's per statement (~80), a run ends with one fenced statement instead of a five-round-trip transaction when there are no hooks, a new run does not re-read its history, step keys and outputs are built without `fmt`/extra copies, and steps without a compensator store no `comp_data` (~110).

What is left per run (~69): one command tag string per statement inside pgconn (~21), the step `Key` string handed to each step (10), `reflect.New` inside `encoding/json` for value outputs (~8; passing pointers would change which `MarshalJSON` methods run, hence the stored bytes), and per-execution setup (run id, contexts, lease watch, heartbeat entry). The fast path applies to pools in the default `QueryExecModeCacheStatement` without a tracer (see `docs/operations.md`); other pools keep the pgx path.

## Reading the numbers

- **Target (p95 < 5 ms per step): met for the engine's own overhead (p95 2.5 ms, commits not waiting for fsync); not met for durable end-to-end latency on this disk**: p95 11.6 ms by default (two durable commits per step), 7.7 ms with `AsyncStepStart` (one), with 2.7 ms per fsync plus Docker's I/O path. Durable latency is roughly the overhead plus one or two fsync times of your storage; measure yours with `pg_test_fsync` and this benchmark. The target means the engine overhead (confirmed by the user, 2026-11-12); durable latency is reported, not targeted.
- **Round trips were the second cost.** Before the change, each checkpoint was four round trips; the single-statement checkpoint removes three per checkpoint, which is the -24% to -36% above.
- **Commit cost dominates.** Group commit (`commit_delay`) helps throughput with many concurrent runs, not single-run latency.

## Durability options

- `Options.AsyncStepStart` (opt-in, default off) commits the `started` checkpoint with `synchronous_commit=off` for that statement only. WAL order makes it safe when the module commits its effect durably to the same Postgres cluster: that commit flushes the earlier `started` record. For effects elsewhere, a Postgres crash during the step may lose the record. A normal resume re-runs the step with the same key (the usual contract), but any path that does not re-run it skips compensating it: a deploy that changed the workflow at that position (the non-determinism is no longer detected) or the run deadline passing before the resume. The lost attempt is not counted against the step's retry budget.
- `synchronous_commit=off` for the whole engine is a deployment choice: after a Postgres crash, the last ~600 ms of checkpoints may be lost and the affected steps re-run with the same key.
