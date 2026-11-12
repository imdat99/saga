package eventbus_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga/eventbus"
	"github.com/imdat99/saga/internal/testdb"
	"github.com/imdat99/saga/internal/testredis"
)

var prefixes atomic.Int64

// recorder counts deliveries per subscriber and event id.
type recorder struct {
	mu    sync.Mutex
	seen  map[string]int
	total atomic.Int32
}

func newRecorder() *recorder { return &recorder{seen: map[string]int{}} }

func (r *recorder) handler(sub string) eventbus.Handler {
	return func(ctx context.Context, ev eventbus.Event) error {
		r.mu.Lock()
		r.seen[sub+"/"+ev.ID]++
		r.mu.Unlock()
		r.total.Add(1)
		return nil
	}
}

func (r *recorder) counts() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.seen)
}

func newBus(t *testing.T) (*pgxpool.Pool, *eventbus.Bus) {
	t.Helper()
	pool := testdb.New(t)
	if err := eventbus.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	bus := eventbus.New(pool, eventbus.Options{
		Redis:         bullmq.RedisConnectionOptions{URL: testredis.URL(t)},
		Prefix:        fmt.Sprintf("t%d_%d", time.Now().UnixNano(), prefixes.Add(1)),
		RelayInterval: 50 * time.Millisecond,
		Logger:        slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	return pool, bus
}

func start(t *testing.T, bus *eventbus.Bus) {
	t.Helper()
	if err := bus.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = bus.Stop(ctx)
	})
}

func emit(t *testing.T, pool *pgxpool.Pool, bus *eventbus.Bus, n int) []string {
	t.Helper()
	var ids []string
	err := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
		for i := range n {
			ev, err := eventbus.NewEvent("order.created", map[string]int{"order": i})
			if err != nil {
				return err
			}
			evs := []eventbus.Event{ev}
			if err := bus.EmitTx(context.Background(), tx, evs...); err != nil {
				return err
			}
			ids = append(ids, evs[0].ID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func unsent(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM eventbus.outbox WHERE sent_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestOutboxEventsReachEverySubscriberOnce(t *testing.T) {
	t.Parallel()
	pool, bus := newBus(t)
	rec := newRecorder()
	for _, id := range []string{"billing.invoice", "mail.confirm"} {
		if err := bus.Subscribe(eventbus.Subscriber{ID: id, Event: "order.created", Handle: rec.handler(id)}); err != nil {
			t.Fatal(err)
		}
	}
	start(t, bus)
	ids := emit(t, pool, bus, 3)

	waitFor(t, "6 deliveries", func() bool { return rec.total.Load() >= 6 })
	time.Sleep(200 * time.Millisecond) // a duplicate would show up by now
	counts := rec.counts()
	for _, id := range ids {
		for _, sub := range []string{"billing.invoice", "mail.confirm"} {
			if counts[sub+"/"+id] != 1 {
				t.Fatalf("%s got event %s %d times, want 1 (all: %v)", sub, id, counts[sub+"/"+id], counts)
			}
		}
	}
	if n := unsent(t, pool); n != 0 {
		t.Fatalf("unsent rows = %d", n)
	}
}

// H: the relay dies between AddBulk and marking the rows sent. The rows are
// pushed again with the same job ids; BullMQ drops the duplicates.
func TestRelayCrashAfterPushDeliversOnce(t *testing.T) {
	t.Parallel()
	pool, bus := newBus(t)
	rec := newRecorder()
	if err := bus.Subscribe(eventbus.Subscriber{ID: "billing.invoice", Event: "order.created", Handle: rec.handler("billing")}); err != nil {
		t.Fatal(err)
	}
	var pushes atomic.Int32
	bus.SetAfterPush(func() error {
		if pushes.Add(1) == 1 {
			return errors.New("relay killed before marking rows sent")
		}
		return nil
	})
	ids := emit(t, pool, bus, 5) // before Start: the first pass sees all 5
	start(t, bus)

	waitFor(t, "rows marked sent", func() bool { return unsent(t, pool) == 0 })
	waitFor(t, "5 deliveries", func() bool { return rec.total.Load() >= 5 })
	time.Sleep(300 * time.Millisecond)
	if pushes.Load() < 2 {
		t.Fatalf("pushes = %d, want the batch pushed twice", pushes.Load())
	}
	counts := rec.counts()
	for _, id := range ids {
		if counts["billing/"+id] != 1 {
			t.Fatalf("event %s delivered %d times, want 1", id, counts["billing/"+id])
		}
	}
}

func TestSubscribeValidation(t *testing.T) {
	t.Parallel()
	_, bus := newBus(t)
	for _, bad := range []eventbus.Subscriber{
		{ID: "a:b", Event: "x"},
		{ID: "ok", Event: ""},
		{ID: "ok", Event: "x", Queue: "q:1"},
	} {
		if err := bus.Subscribe(bad); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
	if err := bus.Subscribe(eventbus.Subscriber{ID: "dup", Event: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Subscribe(eventbus.Subscriber{ID: "dup", Event: "x"}); err == nil {
		t.Fatal("duplicate subscriber accepted")
	}
}
