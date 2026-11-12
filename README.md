# saga

Saga orchestrator for Go modular monoliths running as several instances.
Postgres is the source of truth: every step outcome is checkpointed, runs are
owned through a fenced lease kept alive by a batched heartbeat, and an
interrupted run is resumed (or compensated) by whichever instance picks it up.
River (in Postgres) drives recovery, retries, timers and signals; domain events
go through a transactional outbox to BullMQ.

Status: milestones M0 to M8 of [the plan](docs/PLAN.md) are implemented. See
[TASKS.md](TASKS.md) for the checklist and the known limitations.

## Usage

```go
var ReserveStock = saga.DefineStep("inventory.reserve",
	func(ctx context.Context, k saga.Key, in ReserveIn) (string, string, error) {
		// k is stable across re-executions: the module deduplicates on it.
		id, err := inventory.Reserve(ctx, string(k), in.SKU, in.Qty)
		return id, id, err
	},
	func(ctx context.Context, k saga.Key, reservationID string) error {
		return inventory.Release(ctx, string(k)) // idempotent, works when uncertain
	},
	saga.WithRetry(saga.Exponential(5, time.Second)))

var CompleteOrder = saga.DefineWorkflow("order.complete", 1,
	func(c saga.Ctx, in CompleteIn) (Order, error) {
		if err := saga.Lock(c, "cart:"+in.CartID, 5*time.Minute); err != nil {
			return Order{}, err
		}
		if _, err := ReserveStock.Run(c, ReserveIn{SKU: in.SKU, Qty: in.Qty}); err != nil {
			return Order{}, err // compensation runs automatically
		}
		return CreateOrder.Run(c, OrderIn{CartID: in.CartID})
	})

func main() {
	pool, _ := pgxpool.New(ctx, dsn)
	_ = saga.Migrate(ctx, pool)
	eng, _ := saga.New(pool, saga.Options{})
	_ = eng.Start(ctx) // recovery, retries, timers, signals
	defer eng.Stop(shutdownCtx)

	order, err := CompleteOrder.Run(ctx, eng, "checkout:"+requestID, in)
	if errors.Is(err, saga.ErrRunWaiting) { // parked (retry backoff, Sleep, Await)
		h, _ := CompleteOrder.Lookup(ctx, eng, "checkout:"+requestID)
		order, err = h.Wait(ctx)
	}
}
```

## Features

- Inline execution with per-step checkpoints and replay; `Workflow.Start` to enqueue instead.
- Leases with fencing, batched heartbeat, leader-only recovery scan, graceful shutdown that hands runs over.
- Durable retries (`WithRetry`, `Permanent`), `Sleep`, `Await` / `Engine.Signal` / `Engine.SignalKey`, step and run timeouts.
- Compensation in reverse order with durable retries, `needs_attention` and `Engine.RetryCompensation`.
- Business locks (`saga.Lock`) and the `idem` package for exactly-once module effects.
- `eventbus`: staging + outbox + relay to BullMQ, released only when a run is done.
- `Observer` for metrics, `RunHook` for transactional extensions, retention (`Options.Retention`).

## Documentation

- **User guide**: [English](docs/guide/README.md) | [Tiếng Việt](docs/huong-dan/README.md): getting started, steps, workflows, runs, waiting, modules, events, testing, production, troubleshooting.
- [Concepts](docs/concepts.md): runs, steps, keys, replay, compensation, parking.
- [Guarantees and failure modes](docs/guarantees.md): what holds under crashes, partitions and redeploys.
- [Operations](docs/operations.md): lifecycle, pool sizing, PgBouncer, timing, event bus, retention.
- [Patterns](docs/patterns.md): long-running external work, payload size.
- [Quickstart](examples/quickstart/main.go): the two-step transfer of the guide's first chapter.
- [Example modulith](examples/modulith): three isolated modules, a checkout flow, HTTP API, multiple instances.

## Development

```bash
make test        # Docker required: tests start Postgres 16 and Redis 7 via testcontainers
make test-chaos  # kills and freezes real child processes
make lint
SAGA_TEST_DSN=postgres://saga:saga@localhost:5432/saga?sslmode=disable make test  # reuse docker compose
go test -run '^$' -bench . -benchtime 2000x -count 5 .  # benchmarks (see docs/benchmarks.md)
```
