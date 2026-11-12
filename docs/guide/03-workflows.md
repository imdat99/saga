# 3. Workflows

## Anatomy

```go
var CompleteOrder = saga.DefineWorkflow("order.complete", 1,
	func(c saga.Ctx, in CompleteIn) (Order, error) {
		if err := saga.Lock(c, "cart:"+in.CartID, 5*time.Minute); err != nil {
			return Order{}, err
		}
		resID, err := ReserveStock.Run(c, ReserveIn{SKU: in.SKU, Qty: in.Qty})
		if err != nil {
			return Order{}, err
		}
		payRef, err := Authorize.Run(c, in.Amount)
		if err != nil {
			return Order{}, err
		}
		return CreateOrder.Run(c, OrderIn{CartID: in.CartID, Reservation: resID, Payment: payRef})
	},
	saga.WithRunTimeout(time.Hour))
```

`DefineWorkflow(name, version, fn, opts...)` registers the function under `name@version`. Input `I` and output `O` are stored as JSON, like step data.

## The three rules of the body

### 1. Call steps with `c`, one after another

`saga.Ctx` carries the run. Steps (and `Sleep`, `Await`, `Lock`, `Now`, `Rand`) are matched to the recorded history **by position**, so they must be called sequentially from the workflow goroutine. Do not call steps from goroutines started in the workflow. Parallel work belongs inside a step (one step can fan out to several calls) or in several runs.

### 2. Return engine errors as they are

`Step.Run`, `Sleep`, `Await` and `Lock` may return an error wrapping `saga.ErrRunWaiting` (the run parked) or a cancellation (lease lost, shutdown). Return those unchanged: the engine recognises them and does not compensate. In practice: `if err != nil { return zero, err }`. If you inspect errors, test with `errors.Is` and still return the original error.

```go
_, err := Authorize.Run(c, in.Amount)
switch {
case err == nil:
case errors.Is(err, payment.ErrDeclined): // permanent business failure, recorded
	return Order{}, err                      // compensate what was done
default:
	return Order{}, err                      // includes ErrRunWaiting: return as is
}
```

### 3. Be deterministic between steps

A resumed run executes the function again from the top. Completed steps return their recorded outputs instantly, so the code between them runs again with the same values and must take the same decisions. Branch only on the input and on step outputs.

| Do not | Do instead |
| --- | --- |
| `time.Now()`, `time.Since(...)` | `now, err := saga.Now(c)` (recorded database time) |
| `rand.Int()`, `uuid.New()` | `n, err := saga.Rand(c)`, or generate IDs inside a step |
| Read a database, call an API, read a file or env var | Do it in a step and use its output |
| Iterate over a map and call steps in that order | Sort the keys first |
| Read package-level mutable state, feature flags | Pass it in the input, or read it in a step |
| `go func() { step.Run(c, ...) }()` | Call steps sequentially; fan out inside a step |

Logging and metrics between steps are fine (they are side effects without consequences), but they repeat on every replay.

## Non-determinism

If a resumed run calls a different step at a recorded position, or a recorded output no longer decodes into the output type, the engine stops the run and parks it as `needs_attention` (`saga.ErrNonDeterministic` in the cause). Nothing more is executed. After fixing the code, `RetryCompensation` compensates what had been done.

## Versioning

Changing a workflow that has runs in flight is a deploy problem: a run started with the old code is resumed with the new one. Rules:

- **Safe without a new version:** changes that do not move or change recorded steps: code after the last step, log lines, a change inside a step's implementation that keeps its input and output types compatible.
- **Needs a new version:** adding, removing or reordering steps, changing a step's input/output types incompatibly, changing which step runs for given inputs.

```go
// Keep v1 registered until its runs have finished; start new runs on v2.
var completeV1 = saga.DefineWorkflow("order.complete", 1, completeOrderV1)
var CompleteOrder = saga.DefineWorkflow("order.complete", 2, completeOrderV2)
```

Runs record their version and are always resumed with the matching function. An instance that does not know a run's `name@version` leaves it alone and it is picked up by an instance that does (rolling deploys). Remove the old version once no run uses it:

```sql
SELECT count(*) FROM saga.run
WHERE workflow = 'order.complete' AND version = 1
  AND status IN ('running', 'waiting', 'compensating');
```

Steps follow the same logic: never rename a step that has recorded rows; define a new step with a new name instead.

## Run deadline

`saga.WithRunTimeout(d)` sets a deadline `d` after the run is created. It is checked when each step starts and while the run waits (a parked run wakes at the deadline at the latest). Past it, the next step fails with `saga.ErrRunTimeout` and the workflow compensates. A step already executing is bounded by its own `WithTimeout`, not by the run deadline.

## Database time and randomness

```go
now, err := saga.Now(c)          // recorded on first execution, replayed afterwards
if err != nil {
	return Out{}, err
}
if now.Weekday() == time.Sunday { ... }   // deterministic branch

n, err := saga.Rand(c)           // uint64 from crypto/rand, recorded
```

Each costs one recorded pseudo-step.

## Nested runs

A step may start another workflow (`Child.Run(ctx, eng, key, in)` with the step's `ctx`). The child is an independent run with its own key and history; it does not inherit the parent's run id. Derive its key from the parent's step key (e.g. `string(k) + ":child"`) so a re-executed parent step joins the same child run.
