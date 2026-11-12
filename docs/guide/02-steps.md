# 2. Steps

## Anatomy

```go
var ReserveStock = saga.DefineStep(
	"inventory.reserve",                              // name: global, stable forever
	func(ctx context.Context, k saga.Key, in ReserveIn) (string, string, error) {
		id, err := inventory.Reserve(ctx, string(k), in.SKU, in.Qty)
		return id, id, err                            // output O, compensation data C, error
	},
	func(ctx context.Context, k saga.Key, reservationID string) error {
		return inventory.Release(ctx, string(k))     // undo; nil compensator = nothing to undo
	},
	saga.WithRetry(saga.Exponential(5, time.Second)),
	saga.WithTimeout(10*time.Second),
)
```

Define steps at package level (or once at startup). Defining the same name twice panics. Names starting with `saga.` are reserved for the engine's own pseudo-steps.

## The key

Each call of `Step.Run` inside a run gets a `saga.Key` of the form `<runID>:<position>:<step name>`. It is the same every time that position is executed: after a crash, on a retry, on another instance. Pass it to the module as the request's idempotency key and let the module return the first result for a key it has already processed (chapter 6). That is what turns "the step may run several times" into "the effect happens once".

- `k.RunID()` gives the run id.
- A key is a string you can store; if you keep keys in memory for a long time (e.g. a cache map), store `strings.Clone(string(k))`: keys of one execution share a small buffer.

## Output and compensation data

The output (`O`) is what `Step.Run` returns to the workflow, now and on every replay. The compensation data (`C`) is handed to the compensator. Both are stored as JSON in `saga.step`:

- Keep them small: IDs, amounts, references. Not entities, not files.
- They must survive a JSON round trip: exported fields, no channels or functions, no reliance on unexported state.
- They are replayed: changing their types in an incompatible way breaks running workflows (decoding a recorded output that no longer fits parks the run as `needs_attention`). Version the workflow instead (chapter 3).
- A step without compensator stores no compensation data.

## Errors and retries

By default a step is attempted once: an error fails it, and the workflow decides what to do (usually return it, which compensates).

```go
saga.WithRetry(saga.RetryPolicy{
	MaxAttempts:    5,                // all attempts, the first included
	InitialBackoff: time.Second,
	MaxBackoff:     time.Minute,
	Multiplier:     2,
})
// or
saga.WithRetry(saga.Exponential(5, time.Second)) // doubling, capped at 10 minutes
```

Retries are durable: after a failed attempt the run is **parked** (lease released, no goroutine) and a job resumes it after the backoff, on any started instance. Meanwhile `Step.Run` returns an error wrapping `saga.ErrRunWaiting`; the workflow must return it unchanged (see chapter 3). The backoff granularity is about 5 seconds (River's scheduler).

Mark errors that retrying cannot fix with `saga.Permanent`: the step fails at once whatever the policy.

```go
if errors.Is(err, payment.ErrDeclined) {
	err = saga.Permanent(err)
}
```

Use it for validation errors, business refusals, "not found". Leave infrastructure errors (timeouts, connection refused, 5xx) transient.

## Timeouts

`saga.WithTimeout(d)` cancels the step's context after `d` (cause `saga.ErrStepTimeout`). The attempt counts as a transient failure, so it is retried if the policy allows. A timed-out attempt may still have taken effect (the request reached the other side); the step is then marked **uncertain** and will be compensated even if it finally fails. Your invoke function must honour `ctx`.

## Compensators

A compensator undoes the effect of its step. The engine calls it:

- in reverse order of the steps, after the workflow returned an error, or after the run deadline passed;
- with the same `Key` as the step and the recorded compensation data;
- **at least once**: a crash during compensation runs it again. It must be idempotent.

### Uncertain steps

Sometimes the engine does not know whether a step took effect: the process died after the step started and before its result was recorded, a retry was pending, or an attempt timed out. Such a step is still compensated, with `saga.CompensationUncertain(ctx) == true` and the zero value as data (or the data of a successful later attempt). The compensator must then find out by key what was done, if anything:

```go
func(ctx context.Context, k saga.Key, reservationID string) error {
	// Works whether or not reservationID is known: look up by key.
	return inventory.Release(ctx, string(k))
}
```

With the `idem` package, `idem.Abort` does exactly this and also prevents a late attempt from taking effect afterwards (chapter 6).

### Compensation retries and needs_attention

A failing compensator is retried durably:

```go
saga.WithCompensatePolicy(saga.Exponential(10, time.Second)) // default: 10 attempts from 1s
```

When the policy is exhausted (or the compensator returns `saga.Permanent`), the run stops in status `needs_attention`, keeps its business locks until their TTL, and the event bus (if configured) emits `saga.needs_attention`. Fix the cause, then call `eng.RetryCompensation(ctx, runID)` (chapter 4).

## Checklist for a step

- [ ] Name is unique, descriptive (`module.action`) and will never be renamed.
- [ ] The module deduplicates on `Key` (or the effect is naturally idempotent).
- [ ] Output and compensation data are small and JSON-friendly.
- [ ] Permanent failures are wrapped with `saga.Permanent`; transient ones are retried with a policy.
- [ ] The invoke function respects `ctx` (timeouts, shutdown, lease loss).
- [ ] The compensator is idempotent and works when `CompensationUncertain(ctx)` is true.
