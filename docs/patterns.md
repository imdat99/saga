# Patterns

## Long-running work outside the process

Work that takes minutes or days in another system (video rendering, a bank transfer, a human approval) must not hold a lease, a goroutine or a River worker slot. There is no dedicated primitive: combine a normal step that **starts** the work with `saga.Await` on the step's `Key`.

```go
type Render struct{ URL string }

var StartRender = saga.DefineStep("media.render.start",
	func(ctx context.Context, k saga.Key, in RenderIn) (saga.Key, string, error) {
		// The Key is the external reference: idempotent on the renderer side,
		// and enough to route the answer back to this run.
		jobID, err := renderer.Submit(ctx, string(k), in)
		return k, jobID, err
	},
	func(ctx context.Context, k saga.Key, jobID string) error {
		return renderer.Cancel(ctx, string(k)) // idempotent; jobID may be "" if uncertain
	},
	saga.WithRetry(saga.Exponential(5, time.Second)))

var Publish = saga.DefineWorkflow("media.publish", 1, func(c saga.Ctx, in PublishIn) (Render, error) {
	ref, err := StartRender.Run(c, in.Render)
	if err != nil {
		return Render{}, err
	}
	return saga.Await[Render](c, string(ref), 2*time.Hour)
})

// In the renderer's callback handler, which only knows the reference:
err := eng.SignalKey(ctx, saga.Key(reference), Render{URL: url})
```

What you get:

- **No lease while waiting.** The run is parked in Postgres; any instance resumes it when the signal arrives.
- **Signal before Await is fine.** If the callback beats the run to `Await` (the start step returned but the run has not parked yet), the signal is stored and consumed at once.
- **Duplicates are ignored.** The first signal for a key wins, so the external system may retry its callback.
- **Compensation follows the start step.** If the workflow fails later, the start step's compensator cancels the external work, by `Key`.
- **Timeout.** After the `Await` timeout the step fails with `saga.ErrAwaitTimeout`, and the workflow compensates (cancelling the work). To try again instead, call the start step again in a deterministic loop: each call is a new step with a new `Key`, so the external system sees a new request.

```go
for attempt := range 3 {
	ref, err := StartRender.Run(c, in.Render)
	if err != nil {
		return Render{}, err
	}
	out, err := saga.Await[Render](c, string(ref), time.Hour)
	if !errors.Is(err, saga.ErrAwaitTimeout) || attempt == 2 {
		return out, err
	}
	// Give up on this reference before starting a new one: otherwise it keeps
	// running (and billing) even if the next attempt succeeds.
	if _, err := CancelRender.Run(c, ref); err != nil {
		return Render{}, err
	}
}
```

`CancelRender` is an ordinary idempotent step calling `renderer.Cancel(ctx, string(ref))`. A late answer for a cancelled reference is stored against the old key and never read, so it is harmless.

A runnable version of this pattern, combined with a human approval and a durable timer, is [examples/longrunning](../examples/longrunning/README.md).

## Payload size

Signals, step outputs and compensation data are stored as JSON in `saga.signal` and `saga.step`. Keep them to IDs and small values; put large results (a rendered file) in storage and pass a URL.
