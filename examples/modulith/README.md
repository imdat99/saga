# Example modulith

Three isolated modules and one saga:

- `internal/modules/inventory`: stock and reservations, idempotent by step key (`idem.Do`), uncertain releases fenced with `idem.Abort`.
- `internal/modules/payment`: a fake card provider, idempotent by request key, that can decline or fail transiently.
- `internal/modules/order`: orders, emitting `order.created` in the same transaction (staged until the saga is done).
- `internal/flows`: `order.complete` = lock the cart, reserve stock, authorize payment (retried), create the order.
- `internal/api`: HTTP API; `internal/app`: wiring; `cmd/server`: one instance.

Each module has its own Postgres schema and role and only touches its tables (`SET LOCAL ROLE` per transaction). This is a guard rail against mistakes, not a security boundary: module code could `RESET ROLE`. Modules never import each other (depguard; add a rule to `.golangci.yml` for a new module); only `flows` knows several of them.

Steps are registered once per process; the flows call the modules bound by the most recent `app.New`. A server runs one App per process. The e2e test runs two Apps in one process, so they share the second App's modules (and pool): it exercises two engines, not two module sets.

`/debug/vars` (expvar: saga metrics, memstats, command line) is served on the API port for the demo; put it on an internal port in production. Retention purges runs after 30 days, which is also how long an `Idempotency-Key` is deduplicated; schedule `idem.Purge` of the modules' `processed_requests` with a longer age.

## Run two instances

```bash
make up   # Postgres 16 and Redis 7 from docker-compose.yml
PORT=8081 INSTANCE_ID=a go run ./examples/modulith/cmd/server &
PORT=8082 INSTANCE_ID=b go run ./examples/modulith/cmd/server &

psql postgres://saga:saga@localhost:5432/saga -c "INSERT INTO inventory.stock VALUES ('sku-1', 10)"

curl -si -X POST localhost:8081/orders -H 'Idempotency-Key: req-1' \
  -d '{"cart_id":"c1","sku":"sku-1","qty":2,"amount":500}'
# 201 {"order_id":..., "payment_key":...}; repeat it on :8082, same answer

curl -si -X POST localhost:8082/orders -H 'Idempotency-Key: req-2' \
  -d '{"cart_id":"c2","sku":"sku-1","qty":2,"amount":99999999}'
# 422: payment declined, reservation released

curl -s localhost:8081/debug/vars | jq .saga   # metrics from the saga Observer
```

Kill one instance with `kill -9` in the middle of traffic: the other resumes its runs after the lease TTL (30s by default).

Responses: `201` done, `400` invalid request, `422` compensated (business failure), `202` with `Location: /runs/{id}` when the run continues in the background (retry backoff, or still running after 30s), `500` needs attention (`POST /runs/{id}/retry-compensation` after fixing the cause). A key reused with a different body returns the first request's outcome.

Checkouts run inline in the request's instance, at most `api.MaxInline` (256) at a time; beyond that they are enqueued (`Workflow.Start`, bounded by the engine's `MaxWorkers`) and answered with 202. On SIGTERM the server stops accepting requests (5s for waiting handlers), then drains and hands over runs (15s + `ShutdownGrace` 5s), within a 30s termination grace period.
