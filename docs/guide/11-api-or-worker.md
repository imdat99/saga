# 11. API layer or worker: where workflows run

## What actually runs where

There are three things to place, and they are not the same:

| Thing | Runs in | Notes |
| --- | --- | --- |
| **Starting a run** (`Workflow.Run`, `Workflow.Start`), signals (`Engine.Signal`), status (`Handle.Status`), `RetryCompensation` | Wherever you call it: an HTTP handler, a consumer, a CLI | Cheap: one or a few statements |
| **Executing a run** (the workflow function and its steps) | The process that holds the run's lease: the caller of `Workflow.Run` (inline), or an instance that called `eng.Start` (queued, resumed, recovered) | A run may start in one process and finish in another |
| **Waiting** (retry backoff, `Sleep`, `Await`, busy lock) | Nowhere: the run is parked in Postgres | Woken by a job, executed again by a started instance |

So you do not put a *step* in the API or in a worker: a step runs wherever its run executes at that moment. What you choose is **how a run is started** (inline or queued) and **which processes call `eng.Start`** (and therefore execute queued, resumed and recovered runs).

Two consequences hold whatever you choose:

1. **Every binary that may execute a run must register its workflow and steps**, with the dependencies the steps need (module services, clients). A run started inline by the API is resumed by a worker after a retry backoff; a run started by a worker can be resumed by any started instance.
2. **Steps must not depend on the request.** No `*http.Request`, no user session, no values from the request context (auth claims, trace ids, deadlines): after a resume the step runs in another process, with another context. Put everything a step needs into the workflow input (user id, tenant, amounts) or into step outputs.

## Inline in the API: `Workflow.Run`

```go
out, err := CompleteOrder.Run(ctx, eng, "checkout:"+idemKey, in) // in the HTTP handler
```

The run executes in the request goroutine. Measured on a local test database, a 3-step workflow returns in about 9 ms (p50) inline, against about 27 ms (p50) and 70 ms (p90) when queued and waited for: queue pickup plus the polling of `Handle.Wait`.

**Use it when all of these hold:**

- The client needs the result in the response (an order id, a confirmation, a validation failure).
- The workflow is short: every step is fast, and the whole run fits well within the HTTP timeout at p99.
- It normally does not wait: no `Sleep`, no `Await` on a person, retries only for rare transient failures.
- The request rate is moderate: each in-flight request holds a goroutine and uses connections for its checkpoints.

**Mind the request context.** If it is cancelled (client disconnect, deadline), the run is interrupted mid-way and is only continued by recovery once its lease expires (`LeaseTTL`, 30 s by default). Detach the run from the request and bound it yourself:

```go
ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Second)
defer cancel()
out, err := CompleteOrder.Run(ctx, eng, key, in)
```

And handle the "not finished yet" outcomes (`ErrRunWaiting`, `ErrRunInProgress`) with `202 Accepted` and a status URL ([chapter 4](04-runs.md#http-integration)).

## Queued to workers: `Workflow.Start`

```go
h, err := ExportReport.Start(ctx, eng, "export:"+reqID, in) // returns once the run is enqueued
// respond 202 with h.ID(); the client polls GET /runs/{id}, or gets an event / webhook
```

The run executes on an instance that called `eng.Start`, within its `Options.MaxWorkers` slots.

**Use it when any of these holds:**

- **Slow steps:** calls with long timeouts, file processing, report generation, anything that does not fit an HTTP timeout.
- **Waiting is part of the flow:** approvals (`Await`), timers (`Sleep`), external callbacks (`SignalKey`), long retry policies. The run would return `ErrRunWaiting` inline anyway.
- **No user is waiting:** event consumers, cron jobs, batch backfills, data migrations.
- **Bursts and backpressure:** the queue absorbs spikes; `MaxWorkers` bounds how many runs execute at once per instance, protecting the database pool and downstream services.
- **Isolation:** heavy dependencies (CPU, memory, large SDKs) or risky integrations should not share processes, pools and latency with the API.
- **Fire-and-forget:** the client only needs to know the work was accepted.

## The hybrid: start queued, wait a little

When the result is usually quick but sometimes slow, start the run queued and wait briefly. Unlike an inline run with a short deadline, timing out the *wait* does not interrupt the run: it keeps executing on a worker.

```go
h, err := CompleteOrder.Start(r.Context(), eng, key, in)
if err != nil { ... 503 }

wctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
defer cancel()
out, err := h.Wait(wctx)
var re *saga.RunError
switch {
case err == nil:
	writeJSON(w, http.StatusCreated, out)            // finished within 2 s
case errors.As(err, &re):
	writeJSON(w, http.StatusConflict, re)            // compensated or needs attention
case wctx.Err() != nil:                           // the wait timed out, not the run
	w.Header().Set("Location", "/runs/"+h.ID())      // still running on a worker
	w.WriteHeader(http.StatusAccepted)
default:
	http.Error(w, "try again", http.StatusServiceUnavailable)
}
```

Costs: a few tens of milliseconds more than inline, and the API depends on workers being up to answer quickly.

## What stays in the API layer anyway

Whatever the execution model, these calls belong to the API and are cheap:

| Endpoint | Call |
| --- | --- |
| Start | `Workflow.Run` or `Workflow.Start` with an idempotency key from the client |
| Status / result | `Workflow.Handle(eng, id).Status(ctx)`, or `Wait` with a short timeout |
| Human decisions, webhooks | `Engine.Signal`, `Engine.SignalKey` |
| Operations | `Engine.RetryCompensation` (admin only) |

None of them needs `eng.Start`: an engine that never called `Start` can still enqueue runs, deliver signals and read statuses.

## Deployment topologies

### One binary, every instance does both (default)

Every instance serves HTTP, runs inline runs, and calls `eng.Start`. This is the simplest setup and the one the example modulith uses. Start here.

### Split API and worker roles

Same codebase, two roles (a flag or two `cmd/` binaries), both defining the same workflows and steps:

```go
eng, err := saga.New(pool, saga.Options{MaxWorkers: cfg.MaxWorkers})
if cfg.Role == "worker" || cfg.Role == "all" {
	if err := eng.Start(ctx); err != nil { ... } // executes queued, resumed and recovered runs
}
defer eng.Stop(shutdownCtx)                      // always, in every role
```

| | API instances | Worker instances |
| --- | --- | --- |
| `eng.Start` | No (or yes with a small `MaxWorkers`) | Yes |
| Executes | Inline runs it starts or rejoins | Queued runs, parked runs waking up, recovered runs |
| `MaxWorkers` | Irrelevant without `Start` | Sized to the downstream capacity |
| Pool size | Concurrent inline checkpoints + signals/status | `MaxWorkers` runs' checkpoints + River (one `LISTEN` connection) |
| Scales with | Request rate | Queue depth, run duration |

Rules for the split:

- **At least one worker must always run.** Without a started instance, queued runs never start, parked runs never wake up and crashed runs are never recovered. Inline runs still work, which hides the problem.
- **Workers need everything steps need**, and the API needs it too if it runs workflows inline.
- **Deploy order:** a new workflow version or step must reach the workers before the API starts using it. During a rolling deploy, a run whose `name@version` is unknown to an instance is left for another one.
- **Several worker pools** (e.g. a separate deployment for heavy exports) need separate databases or schemas today: every started engine on a database takes any run. Bound heavy work with `MaxWorkers` and per-step timeouts instead.

## Decision table

| Situation | Choice |
| --- | --- |
| Checkout, booking, transfer: a few fast steps, the user waits for the answer | Inline `Run` in the API, detached context, 202 fallback |
| Same, but a payment provider is sometimes slow | Hybrid: `Start` + `Wait` for 1-2 s, 202 fallback |
| Approval, KYC, anything with `Await` or `Sleep` | `Start`, 202, status endpoint or event |
| Report export, media processing, bulk import | `Start` on workers, `MaxWorkers` sized for the load |
| Reaction to an event or a message | The consumer calls `Start` (or `Run` if it is short), key derived from the event id |
| Nightly batch over many entities | A job calls `Start` once per entity with a deterministic key; workers drain the queue |

## Checklist

- [ ] Workflows and steps are registered in every binary that may execute them.
- [ ] Steps get everything from the input and step outputs, nothing from the request.
- [ ] Inline runs use a detached context with an explicit timeout, and answer 202 when the run waits.
- [ ] At least one instance calls `eng.Start`, always; every instance calls `eng.Stop`.
- [ ] `MaxWorkers` and pool sizes match each role.
- [ ] Clients send an idempotency key and can poll a status endpoint.
