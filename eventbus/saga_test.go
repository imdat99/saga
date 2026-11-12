package eventbus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/eventbus"
	"github.com/imdat99/saga/internal/testdb"
)

type order struct{ ID int }

// orderSaga creates an order in a module transaction that emits
// "order.created" (staged, since it runs inside a step), then charges.
func orderSaga(t *testing.T, pool *pgxpool.Pool, bus *eventbus.Producer, chargeErr error, refundErr error) *saga.Workflow[order, order] {
	t.Helper()
	create := saga.DefineStep(testdb.UniqueName(t, "order.create"),
		func(ctx context.Context, k saga.Key, o order) (order, int, error) {
			err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				ev, err := eventbus.NewEvent("order.created", o)
				if err != nil {
					return err
				}
				return bus.EmitTx(ctx, tx, ev)
			})
			return o, o.ID, err
		},
		func(ctx context.Context, k saga.Key, id int) error { return refundErr },
		saga.WithCompensatePolicy(saga.RetryPolicy{MaxAttempts: 1}))
	charge := saga.DefineStep(testdb.UniqueName(t, "charge"),
		func(ctx context.Context, k saga.Key, o order) (order, string, error) { return o, "", chargeErr }, nil)
	return saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, o order) (order, error) {
		o, err := create.Run(c, o)
		if err != nil {
			return order{}, err
		}
		return charge.Run(c, o)
	})
}

func sagaSetup(t *testing.T) (*pgxpool.Pool, *eventbus.Producer, *saga.Engine, *recorder) {
	t.Helper()
	b := newTestBus(t)
	if err := saga.Migrate(context.Background(), b.pool); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	start(t, b.consumer("shop",
		eventbus.Subscriber{ID: "billing.invoice", Event: "order.created", Handle: rec.handler("billing")},
		eventbus.Subscriber{ID: "mail.confirm", Event: "order.created", Handle: rec.handler("mail")}))
	start(t, b.consumer("ops", eventbus.Subscriber{ID: "page", Event: eventbus.NeedsAttentionEvent, Handle: rec.handler("ops")}))
	p := b.producer()
	start(t, p)
	eng, err := saga.New(b.pool, saga.Options{Hooks: []saga.RunHook{p}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	return b.pool, p, eng, rec
}

func count(t *testing.T, pool *pgxpool.Pool, q string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// G: a compensated saga delivers none of its staged events; a done saga
// delivers each exactly once per subscriber.
func TestSagaEventsDeliveredOnlyWhenDone(t *testing.T) {
	t.Parallel()
	pool, bus, eng, rec := sagaSetup(t)

	failed := orderSaga(t, pool, bus, errors.New("card declined"), nil)
	var re *saga.RunError
	if _, err := failed.Run(context.Background(), eng, "o1", order{ID: 1}); !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v, want compensated", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM eventbus.staging`) + count(t, pool, `SELECT count(*) FROM eventbus.outbox`); n != 0 {
		t.Fatalf("compensated run left %d events", n)
	}

	ok := orderSaga(t, pool, bus, nil, nil)
	if _, err := ok.Run(context.Background(), eng, "o2", order{ID: 2}); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, `SELECT count(*) FROM eventbus.staging`); n != 0 {
		t.Fatalf("done run left %d staged events", n)
	}
	waitFor(t, "2 deliveries", func() bool { return rec.total.Load() >= 2 })
	time.Sleep(300 * time.Millisecond)
	counts := rec.counts()
	if len(counts) != 2 {
		t.Fatalf("deliveries = %v, want one event to billing and mail", counts)
	}
	for k, n := range counts {
		if n != 1 {
			t.Fatalf("%s delivered %d times", k, n)
		}
	}
}

func TestNeedsAttentionEmitsEvent(t *testing.T) {
	t.Parallel()
	pool, bus, eng, rec := sagaSetup(t)
	stuck := orderSaga(t, pool, bus, errors.New("declined"), errors.New("refund API down"))
	var re *saga.RunError
	if _, err := stuck.Run(context.Background(), eng, "o1", order{ID: 1}); !errors.As(err, &re) || re.Status != saga.StatusNeedsAttention {
		t.Fatalf("err = %v, want needs_attention", err)
	}
	waitFor(t, "ops page", func() bool { return rec.total.Load() >= 1 })
	time.Sleep(200 * time.Millisecond)
	counts := rec.counts()
	if len(counts) != 1 {
		t.Fatalf("deliveries = %v, want only the needs_attention page", counts)
	}
	for k := range counts {
		if k[:4] != "ops/" {
			t.Fatalf("unexpected delivery %s", k)
		}
	}
}
