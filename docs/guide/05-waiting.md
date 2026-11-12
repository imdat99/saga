# 5. Waiting: Sleep, Await, signals, locks

## Parking

Whenever a run must wait (retry backoff, a timer, a signal, a busy lock, compensation backoff), the engine **parks** it: status `waiting` (status `compensating` with no owner for a compensation backoff), lease released, no goroutine, no connection. A River job scheduled at a database time (or the signal) wakes it on any started instance, which resumes the workflow from the top with replay. A parked run costs one row in `saga.run`, plus one in `saga.river_job` when it has a wake-up time (an `Await` without timeout on a run without deadline has none: only the signal wakes it), so millions of waiting runs are fine.

Inside the workflow, every waiting primitive returns an error wrapping `saga.ErrRunWaiting` the first time; return it as is. Outside, `Workflow.Run` returns `ErrRunWaiting`; use `Handle.Wait` for the outcome. Durable delays have about 5 seconds of granularity (River's scheduler).

## Sleep

```go
if err := saga.Sleep(c, 24*time.Hour); err != nil {
	return Out{}, err
}
// a day later, possibly on another instance
_, err := SendReminder.Run(c, in.UserID)
```

The wake-up time is recorded (database time), so a replay does not sleep again. `d <= 0` returns immediately. `d` must be computed deterministically (from the input or step outputs, or `saga.Now`).

## Await and signals

`Await[T]` waits for a named signal and returns its payload:

```go
approval, err := saga.Await[Approval](c, "manager-approval", 72*time.Hour)
if errors.Is(err, saga.ErrAwaitTimeout) {
	return Out{}, err          // no answer in 72h: compensate
}
if err != nil {
	return Out{}, err          // includes ErrRunWaiting
}
if !approval.OK {
	return Out{}, saga.Permanent(errors.New("rejected"))
}
```

Someone delivers the signal with the run id:

```go
err := eng.Signal(ctx, runID, "manager-approval", Approval{OK: true, By: "alice"})
```

- A signal sent **before** the run reaches `Await` is kept and consumed at once.
- The **first** signal of a name wins; later ones are ignored, so senders may retry freely.
- Awaiting the same name twice returns the first payload again. To wait for several answers, use several names (`"approval:1"`, `"approval:2"`).
- A payload that does not decode into `T` fails the `Await` permanently.
- `timeout <= 0` waits forever (until the run deadline, if any).

### SignalKey: answers from external systems

For work that runs outside the process (a renderer, a bank, a partner API with callbacks), give the external system the **step key** as reference and let it answer with `SignalKey`; it needs nothing else:

```go
var StartRender = saga.DefineStep("media.render.start",
	func(ctx context.Context, k saga.Key, in RenderIn) (saga.Key, string, error) {
		jobID, err := renderer.Submit(ctx, string(k), in) // reference = key
		return k, jobID, err
	},
	func(ctx context.Context, k saga.Key, _ string) error { return renderer.Cancel(ctx, string(k)) })

// workflow
ref, err := StartRender.Run(c, in)
if err != nil {
	return Out{}, err
}
res, err := saga.Await[RenderResult](c, string(ref), 2*time.Hour)

// webhook handler
err := eng.SignalKey(ctx, saga.Key(req.Reference), RenderResult{URL: req.URL})
```

[Patterns](../patterns.md) shows the timeout-and-retry loop for this case.

## Business locks

`saga.Lock` gives a run exclusive use of a business key (a cart, an account) for the rest of the run:

```go
if err := saga.Lock(c, "cart:"+in.CartID, 5*time.Minute); err != nil {
	return Out{}, err  // ErrLockHeld: another checkout of this cart is in progress
}
```

- Released when the run is `done` or `compensated`.
- Extended by the lease heartbeat while the run executes.
- While the run is parked or dead, the lock lapses after `ttl`: `ttl` bounds how long a waiting run can keep others out. If another run takes it meanwhile, the first one fails with `saga.ErrLockLost` when it resumes, and compensates.
- `ttl` is raised to at least `Options.LeaseTTL`.

By default a busy lock fails the step at once with `saga.ErrLockHeld` (permanent). To wait instead:

```go
err := saga.Lock(c, "account:"+in.AccountID, time.Minute,
	saga.LockWait(saga.Exponential(10, time.Second)))  // parks and retries until the policy runs out
```

Locks are polled, not queued: no fairness and no deadlock detection. When a run needs several locks, take them in a fixed order (e.g. sorted keys).

## Choosing a primitive

| Need | Use |
| --- | --- |
| Try a failing call again later | `WithRetry` on the step |
| Wait a fixed time | `Sleep` |
| Wait for a person or another service | `Await` + `Signal` |
| Wait for an external job that calls back | step that starts it + `Await` on the step key + `SignalKey` |
| Keep two runs from touching the same entity | `Lock` (fail fast or `LockWait`) |
| Give up after a total time | `WithRunTimeout` on the workflow |
