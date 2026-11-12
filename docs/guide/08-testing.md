# 8. Testing

Sagas are best tested against a real Postgres: the guarantees live in SQL (locks, fencing, unique keys), and an in-memory fake would test something else. A test database per test keeps tests independent and fast enough.

## A Postgres per test binary, a database per test

This repository's `internal/testdb` is a good template to copy:

- one container per test binary (testcontainers, `postgres:16-alpine`), or an existing server from an environment variable (`SAGA_TEST_DSN`);
- `New(t)` creates a fresh database, returns a pool, and drops the database when the test ends;
- `fsync=off` in the container for speed (do not benchmark durability there).

```go
func setup(t *testing.T) (*pgxpool.Pool, *saga.Engine) {
	t.Helper()
	pool := testdb.New(t)
	if err := saga.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	eng, err := saga.New(pool, saga.Options{
		LeaseTTL:         time.Second,      // fast takeovers in crash tests
		RecoveryInterval: 200 * time.Millisecond,
		Logger:           slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil { // needed for retries, Sleep, Await, recovery
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	return pool, eng
}
```

## The registry is global

Step and workflow names are process-wide and defining one twice panics. Two consequences:

- Production definitions live at package level and are shared by all tests: fine, tests just use them.
- Steps and workflows **defined inside tests** need unique names, including under `go test -count=N` (each repetition defines again). Use a counter, as `testdb.UniqueName(t, "wf")` does.

## What to test

### Happy path and idempotency

```go
out, err := CompleteOrder.Run(ctx, eng, "k1", in)
// assert out, assert module state
again, err := CompleteOrder.Run(ctx, eng, "k1", in)   // same key: stored result, no new effect
```

### Compensation

Make a later step fail (a fake module, a flag, an input the real module refuses) and check that earlier effects are undone and the run is `compensated`:

```go
_, err := CompleteOrder.Run(ctx, eng, "k2", inThatPaymentDeclines)
var re *saga.RunError
if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
	t.Fatalf("err = %v, want compensated", err)
}
// stock is back, no order exists
```

### Retries

Make the module fail transiently N times (the modulith's payment service has `FailNext(n)`), run, and wait for the outcome. Retries park the run, so use the handle and give it time (backoffs round up to about 5 s):

```go
_, err := wf.Run(ctx, eng, key, in)
if errors.Is(err, saga.ErrRunWaiting) {
	h, _ := wf.Lookup(ctx, eng, key)
	waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	out, err = h.Wait(waitCtx)
}
```

### Crash and resume

Interrupt a run mid-step and resume it with the same key:

```go
// 1. A step that blocks until the test lets it go, and counts executions by key.
// 2. Run in a goroutine with a cancellable ctx; cancel it while the step blocks.
// 3. Run again with the same key on the same or another engine (after LeaseTTL,
//    or immediately after eng.Stop, which releases leases).
// 4. Assert: the step ran again with the SAME key, completed steps did not
//    re-run, the final outcome is correct, the module applied the effect once.
```

`Engine.Stop` with an already-expired context simulates a shutdown: runs are cancelled with `saga.ErrShutdown` and their leases released, so a second engine (or the same key) takes over at once. The repository's `test/chaos` goes further with real child processes killed with SIGKILL or frozen with SIGSTOP.

### Determinism

Replay is where non-determinism shows. A cheap check: run the workflow, then force a resume (crash test above) and assert it reaches the same outcome. Reviews should reject `time.Now`, random numbers, map iteration and I/O between steps (chapter 3).

### Signals and timers

```go
h, _ := wf.Start(ctx, eng, key, in)
// wait until it is parked on Await
for st, _ := h.Status(ctx); st != saga.StatusWaiting; st, _ = h.Status(ctx) {
	time.Sleep(50 * time.Millisecond)
}
_ = eng.Signal(ctx, h.ID(), "manager-approval", Approval{OK: true})
out, err := h.Wait(ctx)
```

For `Sleep` and `Await` timeouts, keep durations short in tests and allow about 5 s of scheduler latency.

## Running

```bash
go test -race ./...                 # Docker required for testcontainers
SAGA_TEST_DSN=postgres://... go test ./...   # reuse a running Postgres (each test still gets its own database)
go test -race -count=10 ./...       # flakiness check before merging
```

Avoid fixed sleeps to order events between goroutines; wait for a condition (a status, a counter) with a deadline instead.
