# 7. Event bus

The `eventbus` package publishes domain events through a transactional outbox in Postgres to BullMQ (Redis). Its key property for sagas: events emitted inside a step are **held until the run is done**. A run that compensates publishes nothing, so other parts of the system never react to an order that was rolled back.

## Setup

```go
bus := eventbus.New(pool, eventbus.Options{
	Redis: bullmq.RedisConnectionOptions{URL: redisURL},
})
if err := eventbus.Migrate(ctx, pool); err != nil { ... }    // schema "eventbus"

// Subscribers: register the same set on every instance that relays.
err := bus.Subscribe(eventbus.Subscriber{
	ID:     "billing.invoice",      // stable: part of BullMQ job ids
	Event:  "order.created",
	Handle: billing.OnOrderCreated, // nil on instances that only relay
})

eng, err := saga.New(pool, saga.Options{Hooks: []saga.RunHook{bus}}) // releases events when runs end
if err := bus.Start(ctx); err != nil { ... }                          // relay, workers, retention
defer bus.Stop(shutdownCtx)
```

Redis must run with `maxmemory-policy noeviction` and AOF (`docker-compose.yml` does).

## Emitting

Emit from the module, in the same transaction as the effect, with the **step's context**:

```go
func (s *Service) Create(ctx context.Context, key string, o Order) (Order, error) {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		created, err := idem.Do(ctx, tx, requests, key, func(ctx context.Context) (Order, error) {
			o.ID = key
			if _, err := tx.Exec(ctx, `INSERT INTO orders.order ...`); err != nil {
				return Order{}, err
			}
			ev, err := eventbus.NewEvent("order.created", OrderCreated{ID: o.ID})
			if err != nil {
				return Order{}, err
			}
			return o, s.bus.EmitTx(ctx, tx, ev)
		})
		o = created
		return err
	})
	return o, err
}
```

- With a step's `ctx` (`saga.RunIDFrom(ctx)` finds the run), the event is **staged**. It moves to the outbox in the transaction that marks the run `done`, and is dropped if the run is compensated.
- With a context that is not a step's (an HTTP handler, a background job), it goes straight to the outbox and is delivered once `tx` commits.
- Do not emit from the workflow body: it re-executes on every resume and would stage the event again. Do not use `context.Background()` inside a step: that bypasses staging.
- Emitting inside `idem.Do` makes the event once-per-key too.

## Handling

```go
func OnOrderCreated(ctx context.Context, ev eventbus.Event) error {
	var p OrderCreated
	if err := ev.Decode(&p); err != nil {
		return eventbus.Unrecoverable(err) // bad payload: do not retry
	}
	return invoices.CreateOnce(ctx, ev.ID, p.ID) // deduplicate by ev.ID
}
```

Delivery is **at least once** and **unordered**:

- deduplicate by `Event.ID` (e.g. with `idem.Do` keyed by it);
- re-read current state instead of trusting the order of events;
- a returned error retries with the subscriber's backoff (`Attempts` default 5, `Backoff` default 1 s doubling); `eventbus.Unrecoverable(err)` fails the job at once. Failed jobs stay 7 days in BullMQ's failed set (the dead letter queue).

Each subscriber gets its own BullMQ queue by default (`Queue` to group them; then every instance with a handler for one needs handlers for all).

## The needs_attention event

When a run is parked as `needs_attention`, the bus emits `eventbus.NeedsAttentionEvent` (`"saga.needs_attention"`) with an `eventbus.NeedsAttention` payload (`RunID`, `Workflow`, `Version`, `Error`) in the same transaction. Subscribe to it to alert on-call:

```go
_ = bus.Subscribe(eventbus.Subscriber{
	ID:    "ops.alert",
	Event: eventbus.NeedsAttentionEvent,
	Handle: func(ctx context.Context, ev eventbus.Event) error {
		var na eventbus.NeedsAttention
		if err := ev.Decode(&na); err != nil {
			return eventbus.Unrecoverable(err)
		}
		return pager.Notify(ctx, ev.ID, fmt.Sprintf("saga %s (%s) needs attention: %s", na.RunID, na.Workflow, na.Error))
	},
})
```

## Operating it

See [Operations: Event bus](../operations.md#event-bus) for Redis durability, relay behaviour, retiring subscribers and retention.
