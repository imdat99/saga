# 4. Starting and following runs

## Choosing the idempotency key

Every run has a unique idempotency key. Starting a workflow with a key that already has a run never creates a second one. Choose a key that identifies the business intent, not the attempt:

| Situation | Key |
| --- | --- |
| HTTP request with an `Idempotency-Key` header | `"checkout:" + header` |
| One saga per business entity | `"order.complete:" + orderID` |
| Consumer of a message | `"invoice:" + event.ID` |
| Child run started by a step | `string(k) + ":child"` (k is the step's key) |

Prefix keys with the workflow or domain: a key already used by another workflow fails with `saga.ErrIdemKeyConflict`. Deduplication lasts as long as the run is kept (`Options.Retention`); after the purge the same key starts a new run.

## Three ways to start

### `Workflow.Run`: inline

```go
out, err := CompleteOrder.Run(ctx, eng, key, in)
```

Executes in the caller's goroutine and returns the outcome, unless the run has to wait. Lowest latency. Works without `eng.Start`, but nothing then recovers runs abandoned by a crash, and parked runs never continue: call `Start` on at least one instance (normally all of them).

What `Run` returns:

| Result | Meaning | What to do |
| --- | --- | --- |
| `out, nil` | Run is `done` (now or earlier) | Use `out` |
| `*saga.RunError` with `StatusCompensated` | Workflow failed, effects undone | Report `re.Cause` |
| `*saga.RunError` with `StatusNeedsAttention` | Compensation gave up or non-determinism | Alert; fix; `RetryCompensation` |
| `saga.ErrRunWaiting` (wrapped) | Parked: retry backoff, Sleep, Await, busy lock | `Lookup` + `Handle.Wait`, or answer "accepted" |
| `saga.ErrRunInProgress` | Another caller or instance executes it now | `Lookup` + `Handle.Wait` |
| `saga.ErrIdemKeyConflict` | Key used by another workflow | Fix the key scheme |
| `saga.ErrWorkflowNotRegistered` | The run's `name@version` is unknown here | Rolling deploy; another instance will continue |
| other error (DB, `ErrLeaseLost`, context) | Interrupted | Retry `Run` with the same key later; recovery continues it anyway |

The caller's `ctx` bounds the inline execution: if it is cancelled (client disconnect, deadline), the run is interrupted and continued later by recovery after its lease expires (`LeaseTTL`). If the work should finish even when the caller goes away, run it detached:

```go
runCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
defer cancel()
out, err := CompleteOrder.Run(runCtx, eng, key, in)
```

### `Workflow.Start`: queued

```go
h, err := CompleteOrder.Start(ctx, eng, key, in) // creates and enqueues in one transaction
fmt.Println(h.ID())
out, err := h.Wait(ctx)                           // optional
```

Any started instance executes it (`Options.MaxWorkers` per instance). Use it for work that should not run in a request goroutine, or to spread load. With a key already in use, `Start` returns a handle to the existing run.

### `Workflow.Lookup` and `Workflow.Handle`

```go
h, err := CompleteOrder.Lookup(ctx, eng, key)   // by idempotency key (ErrRunNotFound if none)
h := CompleteOrder.Handle(eng, runID)           // by run id (not checked until used)

st, err := h.Status(ctx)   // running, waiting, compensating, done, compensated, needs_attention
out, err := h.Wait(ctx)    // blocks until a terminal status or ctx is done
```

`Wait` polls Postgres (20 ms doubling to 1 s). Many concurrent waiters cost one primary-key read each per interval; for long waits prefer reacting to an event (chapter 7) or polling `Status` yourself.

## Statuses

```
running ──► done
   │  ▲
   ▼  │ (wake-up, signal, retry)
waiting
   │
   ▼ (workflow error, deadline)
compensating ──► compensated
   │
   ▼ (compensation exhausted, non-determinism)
needs_attention ──(RetryCompensation)──► compensating
```

## HTTP integration

A typical endpoint with an `Idempotency-Key` header (the example modulith implements this in `internal/api`):

```go
func (s *Server) postOrder(w http.ResponseWriter, r *http.Request) {
	key := "checkout:" + r.Header.Get("Idempotency-Key")
	var in flows.CompleteOrderInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil { ... 400 }

	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 20*time.Second)
	defer cancel()
	out, err := s.flows.CompleteOrder.Run(ctx, s.eng, key, in)

	var re *saga.RunError
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, out)
	case errors.As(err, &re):
		writeJSON(w, http.StatusConflict, map[string]string{"run": re.RunID, "status": string(re.Status), "cause": re.Cause})
	case errors.Is(err, saga.ErrRunWaiting), errors.Is(err, saga.ErrRunInProgress):
		h, err := s.flows.CompleteOrder.Lookup(r.Context(), s.eng, key)
		if err != nil {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Location", "/runs/"+h.ID())
		w.WriteHeader(http.StatusAccepted) // client polls GET /runs/{id}
	default:
		http.Error(w, "try again", http.StatusServiceUnavailable) // same key resumes
	}
}
```

The client retries with the same `Idempotency-Key` on network errors and 503: it joins the same run.

## Manual recovery: needs_attention

A run in `needs_attention` stopped because compensation could not finish (or the workflow became non-deterministic). It keeps its locks until their TTL and does nothing until someone acts:

1. Find it: `SELECT id, workflow, error FROM saga.run WHERE status = 'needs_attention'`, or subscribe to `saga.needs_attention` (chapter 7).
2. Read the cause (`saga.run.error`, failing step in `saga.step` with status `compensation_failed`).
3. Fix the cause (deploy a fixed compensator, repair data in the module, restore the external service).
4. `err := eng.RetryCompensation(ctx, runID)`: failed compensators get a fresh attempt budget and the run is enqueued; `Handle.Wait` returns the outcome.

`RetryCompensation` returns `saga.ErrNotNeedsAttention` if the run is not in that status.
