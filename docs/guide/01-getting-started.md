# 1. Getting started

## What you need

- Go 1.27 or later.
- Postgres (the repository is tested on 16 only). The engine keeps everything there: runs, steps, signals, locks, and its job queue (River).
- Redis 7 only if you use the event bus (chapter 7).

```bash
go get github.com/imdat99/saga@latest   # tagged releases: vX.Y.Z
make up   # in this repository: Postgres 16 and Redis 7 from docker-compose.yml
```

## The model in one paragraph

A **workflow** is a Go function that calls **steps** one after another. Each step is a function that performs one effect (reserve stock, charge a card) and returns an output plus the data needed to undo it. The engine records every step outcome in Postgres before moving on. If the process dies, another instance picks the run up, re-executes the workflow function from the top, gets recorded outputs back for completed steps without running them, and continues where it stopped. If the workflow returns an error, the engine runs the **compensators** of the completed steps in reverse order. Steps can run more than once (after a crash, on retry), always with the same **key**, so the module performing the effect deduplicates on it.

## A first saga

The full program is [examples/quickstart/main.go](../../examples/quickstart/main.go). It moves money between two accounts with two steps, debit then credit; the credit can be told to fail, which makes the engine undo the debit.

### 1. Define steps

```go
debit := saga.DefineStep("quickstart.debit",
	// invoke: perform the effect once per key, return output and compensation data
	func(ctx context.Context, k saga.Key, in TransferIn) (string, string, error) {
		id, err := l.post(ctx, k, in.From, -in.Amount)
		return id, id, err
	},
	// compensate: undo it, idempotently
	func(ctx context.Context, k saga.Key, _ string) error { return l.reverse(ctx, k) },
	saga.WithRetry(saga.Exponential(3, time.Second)))
```

`DefineStep[I, O, C]` takes the input type `I`, the output type `O` and the compensation data type `C` from the functions you pass. The name is global and must never change once runs exist: recovery finds compensators by name.

### 2. Define the workflow

```go
transfer := saga.DefineWorkflow("quickstart.transfer", 1,
	func(c saga.Ctx, in TransferIn) (TransferOut, error) {
		debitID, err := debit.Run(c, in)
		if err != nil {
			return TransferOut{}, err
		}
		creditID, err := credit.Run(c, in)
		if err != nil {
			return TransferOut{}, err // the engine compensates debit
		}
		return TransferOut{DebitID: debitID, CreditID: creditID}, nil
	})
```

Rules for the body, detailed in chapter 3: call steps with `c` and sequentially, return their errors as they are, and keep the code between steps deterministic.

### 3. Create the engine and run

```go
pool, _ := pgxpool.New(ctx, dsn)
if err := saga.Migrate(ctx, pool); err != nil { ... }   // creates schema "saga" (tables + River)

eng, err := saga.New(pool, saga.Options{})
if err := eng.Start(ctx); err != nil { ... }             // recovery, retries, timers, signals
defer eng.Stop(shutdownCtx)                              // always: drains runs, closes the heartbeat pool

out, err := transfer.Run(ctx, eng, "transfer-42", TransferIn{From: "alice", To: "bob", Amount: 100})
```

The third argument of `Run` is the **idempotency key** of the run. Calling `Run` again with the same key never starts a second run: it returns the stored result of the finished run, resumes it if it was abandoned, or reports that it is in progress.

### 4. Handle the outcome

```go
out, err := transfer.Run(ctx, eng, key, in)
if errors.Is(err, saga.ErrRunWaiting) || errors.Is(err, saga.ErrRunInProgress) {
	h, err2 := transfer.Lookup(ctx, eng, key) // parked for a retry, or running elsewhere
	if err2 != nil { ... }
	out, err = h.Wait(ctx)
}
var re *saga.RunError
switch {
case errors.As(err, &re):
	// re.Status is compensated or needs_attention, re.Cause says why
case err != nil:
	// infrastructure error, lease lost, cancelled: calling Run again with the same key continues the run
	// (not for ErrIdemKeyConflict or ErrEngineStopped, see chapter 4)
default:
	// done: out is the output
}
```

### 5. Try it

The program connects to `SAGA_DSN`, by default the docker compose database (`postgres://saga:saga@localhost:5432/saga?sslmode=disable`).

```bash
go run ./examples/quickstart              # transfer done: debit ..., credit ...
go run ./examples/quickstart -fail        # run ... ended compensated: step quickstart.credit: credit refused
go run ./examples/quickstart -key demo-1  # done
go run ./examples/quickstart -key demo-1 -fail   # same key: the stored result, nothing runs again
```

Look at what the engine recorded:

```sql
SELECT id, workflow, status, error FROM saga.run ORDER BY created_at DESC LIMIT 5;
SELECT run_id, seq, name, status, attempt, output, comp_data FROM saga.step ORDER BY run_id, seq;
SELECT * FROM ledger.entry;   -- the compensated run left no entry
```

## Where to go next

- The key and what a module must do with it: [chapter 6](06-modules.md). This is the part to get right: the engine guarantees at-least-once execution; exactly-once effects come from the module.
- Waiting without holding resources (retries, timers, human approval): [chapter 5](05-waiting.md).
- Before going to production: [chapter 9](09-production.md).
