package eventbus_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/saga/eventbus"
)

// Benchmarks against the testcontainers Postgres and Redis:
//
//	go test -run '^$' -bench . -benchtime 300x -count 5 ./eventbus

const benchBatch = 200

func noop(context.Context, eventbus.Event) error { return nil }

// Two services (groups) handle the event: two jobs per event, as two
// subscribers made before M9.
func BenchmarkRelay(b *testing.B) {
	tb := newTestBus(b)
	benchRelayOn(b, tb,
		tb.consumer("billing", eventbus.Subscriber{ID: "invoice", Event: "order.created", Handle: noop}),
		tb.consumer("mail", eventbus.Subscriber{ID: "confirm", Event: "order.created", Handle: noop}))
}

// One service with two handlers of the event: one job per event (before M9:
// one per handler).
func BenchmarkRelayOneGroupTwoHandlers(b *testing.B) {
	tb := newTestBus(b)
	benchRelayOn(b, tb, tb.consumer("shop",
		eventbus.Subscriber{ID: "invoice", Event: "order.created", Handle: noop},
		eventbus.Subscriber{ID: "confirm", Event: "order.created", Handle: noop}))
}

func benchRelayOn(b *testing.B, tb *bus, consumers ...*eventbus.Consumer) {
	b.Helper()
	ctx := context.Background()
	for _, c := range consumers {
		if err := c.Start(ctx); err != nil { // registers the group's routes
			b.Fatal(err)
		}
		_ = c.Stop(ctx)
	}
	p := tb.producer()
	pass, closeRelay, err := p.BenchRelay(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer closeRelay()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		if _, err := tb.pool.Exec(ctx, `INSERT INTO eventbus.outbox (id, name, payload)
			SELECT gen_random_uuid(), 'order.created', '{"order_id":"o-123","amount":4200}' FROM generate_series(1, $1)`, benchBatch); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		n, err := pass(ctx)
		if err != nil || n != benchBatch {
			b.Fatalf("relayed %d, %v", n, err)
		}
	}
}

// One event emitted in its own transaction (begin, insert, commit).
func BenchmarkEmitTx(b *testing.B) {
	tb := newTestBus(b)
	p := tb.producer()
	ctx := context.Background()
	ev, _ := eventbus.NewEvent("order.created", map[string]any{"order_id": "o-123", "amount": 4200})
	b.ReportAllocs()
	for b.Loop() {
		if err := pgx.BeginFunc(ctx, tb.pool, func(tx pgx.Tx) error { return p.EmitTx(ctx, tx, ev) }); err != nil {
			b.Fatal(err)
		}
	}
}
