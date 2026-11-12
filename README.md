# saga

Saga orchestrator for Go modular monoliths running as several instances.
Postgres is the source of truth: every step outcome is checkpointed, runs are
owned through a fenced lease, and an interrupted run is resumed (or
compensated) by whichever instance picks it up.

Status: milestone M1 of the plan (synchronous core). Not yet implemented:
lease heartbeat and automatic recovery (M2), retry/sleep/signal via River (M3),
business locks and the `idem` helper (M4), event bus with outbox to BullMQ (M5),
example modulith (M6). See [TASKS.md](TASKS.md) for the detailed checklist.

## Usage

```go
var ReserveStock = saga.DefineStep("inventory.reserve",
	func(ctx context.Context, k saga.Key, in ReserveIn) (Reservation, string, error) {
		// k is stable across re-executions: pass it to the module as idempotency key.
		r, err := inventory.Reserve(ctx, string(k), in.Items)
		return r, r.ID, err
	},
	func(ctx context.Context, k saga.Key, reservationID string) error {
		if saga.CompensationUncertain(ctx) {
			return inventory.ReleaseByKey(ctx, string(k)) // outcome unknown: look it up by key
		}
		return inventory.Release(ctx, reservationID)
	})

var CompleteOrder = saga.DefineWorkflow("order.complete", 1,
	func(c saga.Ctx, in CompleteIn) (Order, error) {
		res, err := ReserveStock.Run(c, ReserveIn{Items: in.Items})
		if err != nil {
			return Order{}, err // compensation runs automatically
		}
		return CreateOrder.Run(c, OrderIn{Reservation: res.ID})
	})

func main() {
	pool, _ := pgxpool.New(ctx, dsn)
	_ = saga.Migrate(ctx, pool)
	eng := saga.New(pool, saga.Options{})
	order, err := CompleteOrder.Run(ctx, eng, "cart:"+cartID, in)
}
```

## Rules

- Define steps and workflows at package level; names are stable identifiers.
- Steps run at least once; modules must deduplicate on `saga.Key`.
- Compensators must be idempotent.
- Code between steps must be deterministic (it is replayed on resume). Bump the
  workflow version when the step sequence changes.
- Call steps sequentially from the workflow goroutine.

## Development

```bash
make test        # Docker required: tests start Postgres via testcontainers
SAGA_TEST_DSN=postgres://saga:saga@localhost:5432/saga?sslmode=disable make test  # reuse docker compose
```
