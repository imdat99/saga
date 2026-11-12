# 6. Writing modules with `idem`

The engine runs each step **at least once**. A step may be re-executed after a crash, a lease takeover, a timed-out attempt or a retry, always with the same `saga.Key`. Exactly-once effects are the module's job: it must recognise a key it has already processed and return the first result. The `idem` package does this inside the module's own Postgres transaction.

## Setup

Each module owns a `processed_requests` table in its own schema:

```go
var requests = idem.MustNew("inventory")          // table inventory.processed_requests

func (s *Service) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS inventory`); err != nil {
		return err
	}
	return requests.CreateTable(ctx, s.pool)       // or put the same DDL in your migrations
}
```

## Doing an effect once: `idem.Do`

```go
func (s *Service) Reserve(ctx context.Context, key, sku string, qty int) (string, error) {
	var id string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		id, err = idem.Do(ctx, tx, requests, key, func(ctx context.Context) (string, error) {
			tag, err := tx.Exec(ctx, `
				UPDATE inventory.stock SET available = available - $2
				WHERE sku = $1 AND available >= $2`, sku, qty)
			if err != nil {
				return "", err
			}
			if tag.RowsAffected() == 0 {
				return "", ErrOutOfStock
			}
			_, err = tx.Exec(ctx, `INSERT INTO inventory.reservation (key, sku, qty) VALUES ($1, $2, $3)`, key, sku, qty)
			return key, err
		})
		return err
	})
	return id, err
}
```

How it works:

- `Do` claims the key in `tx`, then runs `fn` in a savepoint. The claim, the effect and the stored result commit or roll back **together**.
- A second call with the same key returns the stored result without running `fn`.
- A concurrent call with the same key blocks until the first transaction ends, then returns its result (or runs `fn` itself if the first one rolled back).
- If `fn` fails, the claim is undone and the key stays free for the next attempt.
- The result is stored as JSON: keep it small.

Under `REPEATABLE READ` or `SERIALIZABLE`, a concurrent duplicate fails with a serialization error (SQLSTATE 40001) instead of waiting: retry the transaction.

Effects outside Postgres (an HTTP call to a payment provider) cannot share the transaction. Pass the key as the provider's idempotency key (Stripe's `Idempotency-Key`, a bank's end-to-end id); most providers deduplicate on it.

## Compensating: `idem.Abort`

A compensator may be called for a step whose outcome is unknown (`saga.CompensationUncertain(ctx)`), and an old attempt of the step may even still be running on a paused instance. `Abort` settles both cases:

```go
func (s *Service) Release(ctx context.Context, key string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, processed, err := idem.Abort[string](ctx, tx, requests, key)
		if err != nil || !processed {
			return err   // nothing was done, and nothing can be done any more
		}
		// processed: undo it (idempotently: the compensator itself can repeat)
		...
	})
}
```

- If the request was processed, `Abort` returns its stored result and `true`: undo it.
- If not, it writes a **tombstone**: a late `Do` with that key (an attempt that outlived its lease) fails with `idem.ErrAborted` and never takes effect.
- If an attempt holds the key in an open transaction, `Abort` waits for it to finish first.

Make the undo itself idempotent (e.g. `UPDATE ... SET released = true WHERE key = $1 AND NOT released`), because the compensator can run again after a crash.

## Reading and purging

```go
res, ok, err := idem.Lookup[string](ctx, pool, requests, key)   // committed result, does not wait
n, err := idem.Purge(ctx, pool, requests, 90*24*time.Hour, 10000) // delete old records
```

Keep the purge age **longer** than the saga's run retention and longest possible resume or retry delay: once a record is purged, a re-executed step with that key would act again. Purging also deletes tombstones.

## Module boundaries

The example modulith shows a strict layout:

- each module has its own schema and a `NOLOGIN` role, and runs its transactions with `SET LOCAL ROLE`;
- modules never import each other; only the `flows` package knows several modules and defines the steps and workflows;
- steps are thin adapters: they call one module function with the key and translate its errors (`saga.Permanent` for business refusals).

## Checklist for a module function called by a step

- [ ] Takes the key and deduplicates on it (`idem.Do`, or the provider's idempotency key).
- [ ] Its compensating function uses `idem.Abort` (or an equivalent lookup by key) and is idempotent.
- [ ] Returns distinct errors for business refusals, so the step can mark them `saga.Permanent`.
- [ ] Emits events, if any, in the same transaction, with the step's `ctx` (chapter 7).
