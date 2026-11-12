# User guide

A step-by-step guide to building, running and operating sagas with `github.com/imdat99/saga`. Vietnamese version: [docs/huong-dan](../huong-dan/README.md).

The reference documents stay short and precise; this guide explains how to use them:

| Reference | Covers |
| --- | --- |
| [Concepts](../concepts.md) | Vocabulary: run, step, key, replay, lease, parking, compensation |
| [Guarantees](../guarantees.md) | What holds under crashes, partitions, redeploys |
| [Operations](../operations.md) | Lifecycle, timing, pool sizing, PgBouncer, retention |
| [Patterns](../patterns.md) | Long-running external work, payload size |
| [Benchmarks](../benchmarks.md) | Latency, throughput, allocations |

## Chapters

1. [Getting started](01-getting-started.md): install, migrate, a first saga that compensates, run it.
2. [Steps](02-steps.md): `DefineStep`, the `Key`, outputs and compensation data, retries, timeouts, permanent errors, compensators.
3. [Workflows](03-workflows.md): `DefineWorkflow`, determinism, `Now`/`Rand`, versioning, run deadlines, errors inside a workflow.
4. [Starting and following runs](04-runs.md): idempotency keys, `Run`, `Start`, `Lookup`, `Handle`, outcomes, HTTP integration, manual recovery.
5. [Waiting: Sleep, Await, signals, locks](05-waiting.md): parking, `Sleep`, `Await`, `Signal`, `SignalKey`, `Lock`.
6. [Writing modules with `idem`](06-modules.md): exactly-once effects, compensators that work when uncertain.
7. [Event bus](07-events.md): emitting from steps, subscribers, delivery semantics, the `needs_attention` event.
8. [Testing](08-testing.md): integration tests, failure injection, crash and resume, determinism.
9. [Production](09-production.md): configuration, sizing, shutdown, deploys, monitoring, retention.
10. [Troubleshooting](10-troubleshooting.md): symptoms, causes, fixes, and SQL to inspect runs.

The program of chapter 1 is in [examples/quickstart](../../examples/quickstart/main.go); a complete application with three isolated modules, an HTTP API and several instances is in [examples/modulith](../../examples/modulith).
