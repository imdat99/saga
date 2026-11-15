# 12. Integrating an event bus

Chapter 7 explains why events must be staged in steps and released by a run-end hook. This chapter is the complete integration: the engine's contract, the tables, the hook, the relay to your broker (Kafka, NATS, RabbitMQ, Redis...), consumers, operations and tests. All the code is in [examples/outbox](../../examples/outbox), with tests that run against Postgres in CI; copy it into your application and adapt it.

## What the engine guarantees

The integration rests on three public extension points: `saga.RunHook`, `saga.RunInfo` and `saga.RunIDFrom`.

**`RunHook.RunEnded(ctx, tx, run)`**

- It is called **inside the transaction that records a terminal status** (`done`, `compensated`, `needs_attention`), under the run's lease and with the run's row locked. Whatever it writes commits atomically with the status, or not at all.
- It is called once per committed terminal transition. If it returns an error (or the instance dies before the commit), the transition rolls back, the hook's writes with it, and the run is resumed later and reaches the hook again: a retried hook always starts from a clean state.
- A run can reach it **twice**: `needs_attention` first, then `compensated` after an operator calls `Engine.RetryCompensation`. A `needs_attention` run never becomes `done`.
- Hooks run in the order of `Options.Hooks`. `RunInfo` gives the run's `ID`, `Workflow`, `Version`, `Status` and, for a compensated or needs_attention run, the `Error` that caused it.

**`RunIDFrom(ctx)`: where it reports a run**

| Code running in | `RunIDFrom` | Events written there |
| --- | --- | --- |
| A step function (and the module code it calls with its `ctx`) | the run's id | staged: released if the run is done, dropped if it is compensated |
| A compensator | **none** | published directly, even though the run is being compensated: right for "order cancelled", "payment refunded" |
| A child workflow's steps | the **child's** id | follow the child's outcome, not the parent's |
| Anything outside a run (HTTP handler, signal sender, consumer) | none | published directly |

Never emit from the workflow function's body (outside a step): it is re-executed on every resume.

**The database.** The hook writes with the engine's transaction, so the staging table and the outbox must live **in the same Postgres database as the saga schema**, and the modules must write staged events in that database too (their own schemas in it are fine). A module that owns a separate database cannot stage atomically with its business change: give it its own outbox and publish from there, outside the saga's guarantee.

## The life of an event

| The step's module transaction... | Run ends `done` | Run ends `compensated` | Run parks `needs_attention` |
| --- | --- | --- | --- |
| commits the business change and the staged event | event moved to the outbox | event deleted | event kept until an operator decides (then dropped by `compensated`) |
| is retried or re-executed (same key) | the deterministic event id makes the second insert a no-op | same | same |
| rolls back | no event | no event | no event |

## Schema

```sql
CREATE SCHEMA IF NOT EXISTS app;

-- Events emitted inside a saga step, waiting for the run's outcome.
CREATE TABLE IF NOT EXISTS app.event_staging (
	seq        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	id         text NOT NULL UNIQUE,     -- deterministic: derived from the step key
	run_id     uuid NOT NULL,             -- saga.run.id
	name       text NOT NULL,
	key        text,                     -- partition/ordering key for the broker (e.g. order id)
	payload    jsonb NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS event_staging_run_idx ON app.event_staging (run_id);

-- Events to publish, in order.
CREATE TABLE IF NOT EXISTS app.outbox (
	seq        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	id         text NOT NULL UNIQUE,
	name       text NOT NULL,
	key        text,
	payload    jsonb NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	sent_at    timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unsent_idx ON app.outbox (seq) WHERE sent_at IS NULL;
```

Put it in your migrations. The index on `event_staging.run_id` matters: the hook filters on it at the end of every run.

## Emitting

```go
type Event struct {
	ID      string // unique and deterministic, e.g. string(k) + ":order.placed"
	Name    string
	Key     string // optional broker key (ordering, partitioning)
	Payload any
}

// Emit writes ev in the module's transaction: staged when called from a saga
// step, straight to the outbox otherwise.
func Emit(ctx context.Context, tx pgx.Tx, ev Event) error {
	payload, err := json.Marshal(ev.Payload)
	if err != nil {
		return fmt.Errorf("encode %s: %w", ev.Name, err)
	}
	var key *string
	if ev.Key != "" {
		key = &ev.Key
	}
	if runID, inRun := saga.RunIDFrom(ctx); inRun {
		_, err = tx.Exec(ctx, `INSERT INTO app.event_staging (id, run_id, name, key, payload)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`, ev.ID, runID, ev.Name, key, payload)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO app.outbox (id, name, key, payload)
			VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`, ev.ID, ev.Name, key, payload)
	}
	return err
}
```

A module calls it in the same transaction as its business change, with the `ctx` the step passed it:

```go
func (m *Orders) Place(ctx context.Context, k saga.Key, o Order) error {
	return pgx.BeginFunc(ctx, m.pool, func(tx pgx.Tx) error {
		if err := insertOrder(ctx, tx, o); err != nil {
			return err
		}
		return outbox.Emit(ctx, tx, outbox.Event{
			ID: string(k) + ":order.placed", Name: "order.placed", Key: o.ID,
			Payload: OrderPlaced{OrderID: o.ID},
		})
	})
}
```

Event ids:

- **From a step:** derive the id from the step key (`string(k) + ":" + name`, plus an index if the step emits several of that name). The key is the same on every re-execution, so a step that runs twice stages the event once.
- **From a compensator:** the compensator receives the same key; use a different suffix (`string(k) + ":order.cancelled"`).
- **Outside a run:** use your own idempotency key (the request's), or a random id if the caller does not retry.

Keep payloads small (ids and the few fields consumers need), like step outputs.

## The hook

```go
type Hook struct{}

func (Hook) RunEnded(ctx context.Context, tx pgx.Tx, run saga.RunInfo) error {
	switch run.Status {
	case saga.StatusDone:
		_, err := tx.Exec(ctx, `
			WITH moved AS (DELETE FROM app.event_staging WHERE run_id = $1 RETURNING seq, id, name, key, payload)
			INSERT INTO app.outbox (id, name, key, payload)
			SELECT id, name, key, payload FROM moved ORDER BY seq
			ON CONFLICT (id) DO NOTHING`, run.ID)
		return err
	case saga.StatusCompensated:
		_, err := tx.Exec(ctx, `DELETE FROM app.event_staging WHERE run_id = $1`, run.ID)
		return err
	case saga.StatusNeedsAttention:
		// Staged events wait for the operator (RetryCompensation ends the run
		// compensated, which drops them). Alert now.
		payload, err := json.Marshal(map[string]string{"run_id": run.ID, "workflow": run.Workflow, "error": run.Error})
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO app.outbox (id, name, key, payload)
			VALUES (gen_random_uuid()::text, 'saga.needs_attention', $1, $2)`, run.ID, payload)
		return err
	}
	return nil
}

eng, err := saga.New(pool, saga.Options{Hooks: []saga.RunHook{outbox.Hook{}}})
```

- `ORDER BY seq` keeps the run's events in the order its steps emitted them.
- The random id of the alert is safe: if the transition rolls back, the alert row goes with it.
- **Cost:** without hooks, ending a run is one fenced statement (one round trip). With any hook, it becomes a transaction (begin, lease check, lock release, status, commit) plus the hook's statements: a few more round trips at the end of **every** run, on the run's row lock. Keep hooks to indexed statements, no network calls, no slow queries.
- A hook that keeps failing (for example, its tables are not migrated yet) blocks every run at its end: they are resumed and retried forever, never parked. Migrate before deploying the hook, and watch for errors wrapping `saga: run hook`.

## The relay

The relay reads the outbox in order, publishes to the broker, and marks the rows sent, in one transaction:

```go
type Message struct {
	Seq     int64
	ID      string
	Name    string
	Key     *string
	Payload []byte
}

// Publisher sends messages to the broker. It returns only when the broker
// acknowledged all of them (Kafka acks=all, JetStream ack, RabbitMQ confirms).
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

func RelayOnce(ctx context.Context, pool *pgxpool.Pool, pub Publisher, limit int) (int, error) {
	n := 0
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT seq, id, name, key, payload FROM app.outbox
			WHERE sent_at IS NULL ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return err
		}
		msgs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Message])
		if err != nil || len(msgs) == 0 {
			return err
		}
		if err := pub.Publish(ctx, msgs); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		seqs := make([]int64, len(msgs))
		for i, m := range msgs {
			seqs[i] = m.Seq
		}
		if _, err := tx.Exec(ctx, `UPDATE app.outbox SET sent_at = now() WHERE seq = ANY($1)`, seqs); err != nil {
			return err
		}
		n = len(msgs)
		return nil
	})
	return n, err
}
```

Run it in a loop on every instance (`outbox.Relay`: again at once after a full batch, after an interval otherwise). Properties:

- **At least once.** A crash or error after `Publish` and before the commit publishes the batch again. Consumers deduplicate by event id (below), and some brokers can too.
- **Several instances.** `FOR UPDATE SKIP LOCKED` hands each relay a different batch: nothing is published twice in normal operation, and throughput scales with instances.
- **Order.** One relay publishes in `seq` order. Several relays publish batches in parallel, so two events of the same key can overtake each other. If consumers need per-key order, run a single relay (for example, the holder of a `pg_try_advisory_lock` on a dedicated connection), or relay by key partition (`WHERE (hashtext(key) & 2147483647) % N = i`; rows without a key go to one of them, e.g. `key IS NULL AND i = 0`).
- **Locks while publishing.** The batch stays locked while the broker call runs: bound it with a timeout on `ctx`, and keep batches modest (100 to 500). A slow broker slows the relay; it does not lose events.
- **A poison event** (rejected by the broker every time) blocks its batch forever. Detect it (age of the oldest unsent row) and move it aside by hand, or add an attempt counter and a dead-letter table if this can happen with your broker.
- **Latency.** Polling adds up to the interval (for example 200 ms to 1 s). For lower latency, `NOTIFY` in the hook and `LISTEN` in the relay, keeping the poll as a fallback.

## Broker notes

The `Publisher` is the only broker-specific code. What each one needs for the guarantees above:

| Broker | Publish acknowledged when | Key | Duplicate handling |
| --- | --- | --- | --- |
| Kafka | `acks=all` (idempotent producer), after flushing the batch | message key = `Key` (per-partition order) | consumers deduplicate on the event id (put it in a header) |
| NATS JetStream | each publish is acknowledged by the stream | subject per event name, `Key` in the subject or a header | `Nats-Msg-Id` = event id: the server drops duplicates within its window |
| RabbitMQ | publisher confirms | routing key | `message_id` = event id; consumers deduplicate |
| Redis Streams / lists | the command returns (with AOF `appendfsync always` to survive a Redis crash) | stream per key or per name | consumers deduplicate |

Changing broker later replaces the `Publisher` only; staging, hook, outbox and relay stay.

## Consumers

Delivery is at least once and, across keys, unordered. A consumer:

- deduplicates on the event id, in the same transaction as its effect, for example with `idem` (chapter 6):

```go
err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
	_, err := idem.Do(ctx, tx, processed, eventID, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, invoices.Create(ctx, tx, orderID)
	})
	return err
})
```

- re-reads current state instead of trusting event order;
- may start a saga itself: use the event id (or a key derived from it) as the run's idempotency key, and a duplicate delivery joins the same run.

## Operating it

- **Retention.** Delete sent rows periodically, in batches (`outbox.PurgeSent(ctx, pool, 7*24*time.Hour, 5000)`). Staging rows are deleted by the hook; only `needs_attention` runs keep theirs, until they are compensated.
- **Monitoring.** Alert on the age of the oldest unsent row (relay or broker down) and on staging rows of runs that ended (a hook bug):

```sql
SELECT now() - min(created_at) FROM app.outbox WHERE sent_at IS NULL;

SELECT count(*) FROM app.event_staging s JOIN saga.run r ON r.id = s.run_id
WHERE r.status IN ('done', 'compensated');
```

- **Vacuum.** Both tables churn (insert, update or delete per event): lower their `autovacuum_vacuum_scale_factor` (e.g. 0.01), like `saga.step` (chapter 9).
- **Postgres load.** Per event: one staging insert in the module transaction, one move in the hook, one update and one delete later in the relay and purge.

## Testing the integration

The tests of [examples/outbox](../../examples/outbox/outbox_test.go) are the ones worth keeping in your application:

- a run that ends `done` puts its events in the outbox, and the staging table is empty;
- a run that ends `compensated` publishes nothing from its steps, but publishes what its compensators emit;
- a re-executed step stages its event once;
- `needs_attention` keeps the staged events and emits the alert; `RetryCompensation` then drops them;
- a failing `Publisher` leaves the events unsent; two relays at once publish every event exactly once.

## Checklist

- [ ] Staging and outbox in the saga's database; index on `event_staging.run_id`.
- [ ] Events written in the module's transaction, with the step's `ctx`, deterministic ids.
- [ ] Nothing emitted from a workflow body; compensators emit their own events on purpose.
- [ ] The hook registered on every instance's engine; tables migrated before it is deployed.
- [ ] A relay on every instance (or a single one if per-key order matters), `Publisher` waiting for broker acknowledgements.
- [ ] Consumers deduplicate on the event id.
- [ ] Retention of sent rows; alerts on outbox age and leftover staging rows.
