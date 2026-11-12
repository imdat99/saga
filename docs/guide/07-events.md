# 7. Event bus

The `eventbus` package publishes domain events through a transactional outbox in Postgres to BullMQ (Redis), and delivers each event at least once to **every service that handles it**. Its key property for sagas: events emitted inside a step are **held until the run is done**. A run that compensates publishes nothing, so other services never react to an order that was rolled back.

## Two roles

| Role | Created with | Needs | Does |
| --- | --- | --- | --- |
| **Producer** | `eventbus.NewProducer(pool, opts)` | Postgres (the outbox) and Redis | `EmitTx` in the module's transaction, `RunEnded` (saga hook), the relay outbox → queues, retention |
| **Consumer** | `eventbus.NewConsumer(group, opts)` | **Redis only** | Runs the handlers of a **consumer group** |

A **consumer group** is usually one service (one codebase): `billing`, `mail`, `analytics`. All instances of a group share one BullMQ queue named after the group, and each event job runs every handler the group has for that event. Different groups each get their own copy of every event they handle.

```
            Postgres                         Redis
 module tx ─► outbox ─► relay (any producer) ─┬─► queue "shop"    ─► shop instances    (audit, mail handlers)
                                              ├─► queue "billing" ─► billing instances (invoice handler)
                                              └─► (no group for an event: dropped)
```

Routing lives in Redis: a consumer registers the events of its group when it starts, and every relay reads that registry. A service that only reacts to events needs **no access** to the producers' database, and adding a consumer service never requires redeploying the producers.

## Setup in a producer service

```go
events := eventbus.NewProducer(pool, eventbus.ProducerOptions{
	Redis: bullmq.RedisConnectionOptions{URL: redisURL},
})
if err := eventbus.Migrate(ctx, pool); err != nil { ... }               // schema "eventbus"
eng, err := saga.New(pool, saga.Options{Hooks: []saga.RunHook{events}}) // releases staged events
if err := events.Start(ctx); err != nil { ... }                         // relay + retention
defer events.Stop(shutdownCtx)
```

Every instance may run the relay: passes use `SKIP LOCKED`, so instances split the outbox.

## Setup in a consumer service

A service that only consumes imports `github.com/imdat99/saga/eventbus/consume`: it depends on BullMQ only (no saga engine, no Postgres driver). `eventbus.NewConsumer` and the other consumer names in `eventbus` are aliases of it, for services that also produce.

```go
billing, err := consume.New("billing", consume.Options{
	Redis:       bullmq.RedisConnectionOptions{URL: redisURL},
	Concurrency: 20,              // jobs in parallel per instance (default 10)
	Attempts:    8,               // deliveries before the job fails (default 5)
	Backoff:     2 * time.Second, // first retry delay, doubled (default 1s)
})
_ = billing.Subscribe(consume.Subscriber{ID: "invoice", Event: "order.created", Handle: invoices.OnOrderCreated})
_ = billing.Subscribe(consume.Subscriber{ID: "ledger", Event: "order.created", Handle: ledger.OnOrderCreated})
if err := billing.Start(ctx); err != nil { ... } // registers the group's events, starts the worker
defer billing.Stop(shutdownCtx)
```

A service that both emits and handles events creates both (the example modulith does). Producers and consumers of one bus must use the same Redis and `Prefix`; with Redis Cluster the prefix needs a hash tag, e.g. `{bull}`, as BullMQ requires.

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
			return o, s.events.EmitTx(ctx, tx, ev)
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
- a returned error retries the delivery to the group with its backoff, **running again only the handlers that failed**: the ones that succeeded are recorded in the job;
- `eventbus.Unrecoverable(err)` gives up on that handler for that event at once; the other handlers still run and retry normally, and the job then ends in BullMQ's failed set (the dead letter queue, kept 7 days);
- a panicking handler counts as a failed delivery (retried), it does not crash the process.

## Deploying consumers

- **All instances of a group run the same handlers.** During a rolling deploy, an old instance that takes a job for an event only the new code handles puts it back in the queue (without spending an attempt) for an instance that has the handler, for up to `HandOffWindow` (30 minutes) after the event was relayed; the delay between hand-offs grows with the job's age (a tenth of it, 0.5 to 30 seconds), so a forgotten route does not load Redis.
- **Routes are a union.** Removing a handler from the code does not unroute its event (an old instance may still handle it). Once no instance handles it any more, remove the route: `eventbus.Forget(ctx, eventbus.RegistryOptions{Redis: ...}, "billing", "order.created")`.
- **Retiring a whole service:** `eventbus.Unregister(ctx, eventbus.RegistryOptions{Redis: ...}, "billing")`, then drain or delete its queue. Stopping its instances is not enough: its queue keeps filling while it is registered.
- **A new group** receives the events relayed after its first `Start`, not the ones before. On a first deploy, before **any** group has registered, producers hold the events in the outbox instead of dropping them.
- **If Redis loses the registry** (restart without persistence), relays notice (the registry's epoch changed) and keep routing to the groups they knew for `RegistryGrace` (2 minutes), while running consumers register again within `RegistryCheck` (30 seconds). A relay started after the loss, or a group with no running consumer, is not covered: after Redis lost its data, start one consumer of every group, then the producers ([Operations](../operations.md#event-bus)).
- A group that is down keeps receiving jobs in its queue (registration is durable) and processes them when it comes back: watch the queue size in Redis.

## The needs_attention event

When a run is parked as `needs_attention`, the producer emits `eventbus.NeedsAttentionEvent` (`"saga.needs_attention"`) with an `eventbus.NeedsAttention` payload (`RunID`, `Workflow`, `Version`, `Error`) in the same transaction. Handle it in an ops group to alert on-call:

```go
ops, _ := eventbus.NewConsumer("ops", eventbus.ConsumerOptions{Redis: redisOpts})
_ = ops.Subscribe(eventbus.Subscriber{
	ID:    "page",
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

See [Operations: Event bus](../operations.md#event-bus) for Redis durability, relay behaviour and retention.
