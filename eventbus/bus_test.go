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

// bus is one test's event bus namespace: a database and a Redis prefix,
// shared by the producers and consumers the test creates.
type bus struct {
	t      testing.TB
	pool   *pgxpool.Pool
	redis  bullmq.RedisConnectionOptions
	prefix string
}

func newTestBus(tb testing.TB) *bus {
	tb.Helper()
	pool := testdb.New(tb)
	if err := eventbus.Migrate(context.Background(), pool); err != nil {
		tb.Fatal(err)
	}
	return &bus{
		t: tb, pool: pool,
		redis:  bullmq.RedisConnectionOptions{URL: testredis.URL(tb)},
		prefix: fmt.Sprintf("t%d_%d", time.Now().UnixNano(), prefixes.Add(1)),
	}
}

func (b *bus) logger() *slog.Logger {
	if tt, ok := b.t.(*testing.T); ok {
		return slog.New(slog.NewTextHandler(tt.Output(), &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	return slog.New(slog.DiscardHandler)
}

func (b *bus) producer() *eventbus.Producer {
	return eventbus.NewProducer(b.pool, eventbus.ProducerOptions{
		Redis: b.redis, Prefix: b.prefix, RelayInterval: 50 * time.Millisecond, Logger: b.logger(),
	})
}

// consumer returns a consumer of group with fast retries.
func (b *bus) consumer(group string, subs ...eventbus.Subscriber) *eventbus.Consumer {
	b.t.Helper()
	return b.consumerWith(group, func(*eventbus.ConsumerOptions) {}, subs...)
}

func (b *bus) consumerWith(group string, set func(*eventbus.ConsumerOptions), subs ...eventbus.Subscriber) *eventbus.Consumer {
	b.t.Helper()
	opts := eventbus.ConsumerOptions{
		Redis: b.redis, Prefix: b.prefix, Backoff: 20 * time.Millisecond, Logger: b.logger(),
	}
	set(&opts)
	c, err := eventbus.NewConsumer(group, opts)
	if err != nil {
		b.t.Fatal(err)
	}
	for _, s := range subs {
		if err := c.Subscribe(s); err != nil {
			b.t.Fatal(err)
		}
	}
	return c
}

type startStopper interface {
	Start(context.Context) error
	Stop(context.Context) error
}

func start(tb testing.TB, s startStopper) {
	tb.Helper()
	if err := s.Start(context.Background()); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})
}

func emit(tb testing.TB, b *bus, p *eventbus.Producer, n int) []string {
	tb.Helper()
	var ids []string
	err := pgx.BeginFunc(context.Background(), b.pool, func(tx pgx.Tx) error {
		for i := range n {
			ev, err := eventbus.NewEvent("order.created", map[string]int{"order": i})
			if err != nil {
				return err
			}
			evs := []eventbus.Event{ev}
			if err := p.EmitTx(context.Background(), tx, evs...); err != nil {
				return err
			}
			ids = append(ids, evs[0].ID)
		}
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return ids
}

func waitFor(tb testing.TB, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func unsent(tb testing.TB, pool *pgxpool.Pool) int {
	tb.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM eventbus.outbox WHERE sent_at IS NULL`).Scan(&n); err != nil {
		tb.Fatal(err)
	}
	return n
}

// expectOnce fails unless every (subscriber, event) pair was delivered once.
func expectOnce(tb testing.TB, rec *recorder, subs []string, ids []string) {
	tb.Helper()
	waitFor(tb, fmt.Sprint(len(subs)*len(ids), " deliveries"), func() bool { return int(rec.total.Load()) >= len(subs)*len(ids) })
	time.Sleep(300 * time.Millisecond) // a duplicate would show up by now
	counts := rec.counts()
	for _, id := range ids {
		for _, sub := range subs {
			if counts[sub+"/"+id] != 1 {
				tb.Fatalf("%s got event %s %d times, want 1 (%d deliveries in all)", sub, id, counts[sub+"/"+id], rec.total.Load())
			}
		}
	}
}

// One group with two handlers of the same event: one job per event, both
// handlers run once.
func TestOutboxEventsReachEverySubscriberOnce(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	start(t, b.consumer("shop",
		eventbus.Subscriber{ID: "billing.invoice", Event: "order.created", Handle: rec.handler("billing.invoice")},
		eventbus.Subscriber{ID: "mail.confirm", Event: "order.created", Handle: rec.handler("mail.confirm")}))
	p := b.producer()
	start(t, p)
	ids := emit(t, b, p, 3)
	expectOnce(t, rec, []string{"billing.invoice", "mail.confirm"}, ids)
	if n := unsent(t, b.pool); n != 0 {
		t.Fatalf("unsent rows = %d", n)
	}
}

// The bug M9 fixes: two services (groups) handle the same event, two
// producer instances relay. Before, a relay only pushed to the subscribers of
// its own process: deliveries were lost.
func TestSubscriberInAnotherService(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	// Service "shop" emits and handles its own audit; service "billing" only
	// consumes (a consumer has no database access at all).
	start(t, b.consumer("shop", eventbus.Subscriber{ID: "audit", Event: "order.created", Handle: rec.handler("audit")}))
	start(t, b.consumer("billing", eventbus.Subscriber{ID: "invoice", Event: "order.created", Handle: rec.handler("invoice")}))
	p1, p2 := b.producer(), b.producer() // two relaying instances of the shop
	start(t, p1)
	start(t, p2)
	ids := emit(t, b, p1, 20)
	expectOnce(t, rec, []string{"audit", "invoice"}, ids)
}

// Before any group ever registered (first deploy), events are held, not
// dropped; once a group registered, the routing applies: an event no group
// handles is marked sent to nobody.
func TestEventsHeldUntilFirstRegistration(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	p := b.producer()
	start(t, p)
	early := emit(t, b, p, 2)
	time.Sleep(300 * time.Millisecond) // several relay passes
	if n := unsent(t, b.pool); n != 2 {
		t.Fatalf("unsent = %d before any registration, want 2 (held)", n)
	}

	rec := newRecorder()
	start(t, b.consumer("late", eventbus.Subscriber{ID: "h", Event: "order.created", Handle: rec.handler("h")}))
	later := emit(t, b, p, 3)
	expectOnce(t, rec, []string{"h"}, append(early, later...))

	// An event nobody handles, now that a registry exists: sent to nobody.
	if err := pgx.BeginFunc(context.Background(), b.pool, func(tx pgx.Tx) error {
		ev, _ := eventbus.NewEvent("nobody.cares", 1)
		return p.EmitTx(context.Background(), tx, ev)
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "unrouted event marked sent", func() bool { return unsent(t, b.pool) == 0 })
}

// The registry is lost (Redis restarted without persistence): producers hold
// events while it is empty, the running consumers register again on their
// own, and nothing is lost.
func TestRegistryLossRecovers(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	start(t, b.consumerWith("svc", func(o *eventbus.ConsumerOptions) { o.RegistryCheck = 100 * time.Millisecond },
		eventbus.Subscriber{ID: "h", Event: "order.created", Handle: rec.handler("h")}))
	p := b.producer()
	start(t, p)
	first := emit(t, b, p, 2)
	expectOnce(t, rec, []string{"h"}, first)

	conn, err := bullmq.NewRedisConnection(context.Background(), b.redis)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Client().Del(context.Background(),
		b.prefix+":eventbus:routes", b.prefix+":eventbus:groups", b.prefix+":eventbus:version", b.prefix+":eventbus:epoch").Err(); err != nil {
		t.Fatal(err)
	}
	second := emit(t, b, p, 3)
	expectOnce(t, rec, []string{"h"}, append(first, second...))
}

// H: the relay dies between pushing and marking the rows sent. The rows are
// pushed again with the same job ids; BullMQ drops the duplicates.
func TestRelayCrashAfterPushDeliversOnce(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	start(t, b.consumer("billing", eventbus.Subscriber{ID: "invoice", Event: "order.created", Handle: rec.handler("billing")}))
	p := b.producer()
	var pushes atomic.Int32
	p.SetAfterPush(func() error {
		if pushes.Add(1) == 1 {
			return errors.New("relay killed before marking rows sent")
		}
		return nil
	})
	ids := emit(t, b, p, 5) // before Start: the first pass sees all 5
	start(t, p)

	waitFor(t, "rows marked sent", func() bool { return unsent(t, b.pool) == 0 })
	expectOnce(t, rec, []string{"billing"}, ids)
	if pushes.Load() < 2 {
		t.Fatalf("pushes = %d, want the batch pushed twice", pushes.Load())
	}
}

// A retry runs only the handlers that failed: the ones that succeeded are
// recorded in the job.
func TestRetryRunsOnlyFailedHandlers(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	var flakyCalls atomic.Int32
	flaky := func(ctx context.Context, ev eventbus.Event) error {
		if flakyCalls.Add(1) <= 2 {
			return errors.New("temporarily down")
		}
		return rec.handler("flaky")(ctx, ev)
	}
	start(t, b.consumer("svc",
		eventbus.Subscriber{ID: "steady", Event: "order.created", Handle: rec.handler("steady")},
		eventbus.Subscriber{ID: "flaky", Event: "order.created", Handle: flaky}))
	p := b.producer()
	start(t, p)
	ids := emit(t, b, p, 1)
	expectOnce(t, rec, []string{"steady", "flaky"}, ids)
	if n := flakyCalls.Load(); n != 3 {
		t.Fatalf("flaky handler ran %d times, want 3", n)
	}
}

// A handler that gives up (Unrecoverable) is not retried, and does not make
// the other handlers of the event run again.
func TestUnrecoverableHandlerIsNotRetried(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	var badCalls, flakyCalls atomic.Int32
	start(t, b.consumer("svc",
		eventbus.Subscriber{ID: "bad", Event: "order.created", Handle: func(ctx context.Context, ev eventbus.Event) error {
			badCalls.Add(1)
			return eventbus.Unrecoverable(errors.New("cannot ever handle this"))
		}},
		eventbus.Subscriber{ID: "flaky", Event: "order.created", Handle: func(ctx context.Context, ev eventbus.Event) error {
			if flakyCalls.Add(1) == 1 {
				return errors.New("blip")
			}
			return rec.handler("flaky")(ctx, ev)
		}}))
	p := b.producer()
	start(t, p)
	ids := emit(t, b, p, 1)
	expectOnce(t, rec, []string{"flaky"}, ids)
	if n := badCalls.Load(); n != 1 {
		t.Fatalf("unrecoverable handler ran %d times, want 1", n)
	}
	// The handler that gave up sends the job to the failed set (the dead
	// letter queue), even though the retry that completed it succeeded.
	waitFor(t, "job in the failed set", func() bool { return b.failedJobs("svc") == 1 })
}

// failedJobs counts the jobs in a group's failed set.
func (b *bus) failedJobs(group string) int64 {
	b.t.Helper()
	ctx := context.Background()
	q, err := bullmq.NewQueue(ctx, group, &bullmq.QueueOptions{Connection: b.redis, Prefix: b.prefix})
	if err != nil {
		b.t.Fatal(err)
	}
	defer q.Close()
	n, err := q.GetFailedCount(ctx)
	if err != nil {
		b.t.Fatal(err)
	}
	return n
}

// A handler that panics, among several, fails its delivery like an error
// (retried) instead of crashing the process.
func TestHandlerPanicIsAnError(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	var calls atomic.Int32
	start(t, b.consumer("svc",
		eventbus.Subscriber{ID: "steady", Event: "order.created", Handle: rec.handler("steady")},
		eventbus.Subscriber{ID: "boom", Event: "order.created", Handle: func(ctx context.Context, ev eventbus.Event) error {
			if calls.Add(1) == 1 {
				panic("nil map write")
			}
			return rec.handler("boom")(ctx, ev)
		}}))
	p := b.producer()
	start(t, p)
	ids := emit(t, b, p, 1)
	expectOnce(t, rec, []string{"steady", "boom"}, ids)
}

// Rolling deploy: an old instance of the group lacks the handler of an event
// that newer code handles. While only the old instance runs, it hands every
// job off; with a single attempt per job, every event still reaches the new
// instance once it starts: handing a job off spends no attempt.
func TestRollingDeployMissingHandlerRetries(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	oneAttempt := func(o *eventbus.ConsumerOptions) { o.Attempts = 1 }
	subsV2 := func() []eventbus.Subscriber {
		return []eventbus.Subscriber{
			{ID: "legacy", Event: "other.event", Handle: rec.handler("legacy")},
			{ID: "fresh", Event: "order.created", Handle: rec.handler("fresh")},
		}
	}
	// A v2 instance registered the new route, then went away (rollout in
	// progress); the old v1 instance runs alone.
	v2 := b.consumerWith("svc", oneAttempt, subsV2()...)
	if err := v2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := v2.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	start(t, b.consumerWith("svc", oneAttempt, eventbus.Subscriber{ID: "legacy", Event: "other.event", Handle: rec.handler("legacy")}))
	p := b.producer()
	start(t, p)
	ids := emit(t, b, p, 10)
	waitFor(t, "events relayed", func() bool { return unsent(t, b.pool) == 0 })
	time.Sleep(time.Second) // the v1 instance takes and hands off the jobs
	if n := rec.total.Load(); n != 0 {
		t.Fatalf("%d deliveries without the handler", n)
	}
	start(t, b.consumerWith("svc", oneAttempt, subsV2()...))
	expectOnce(t, rec, []string{"fresh"}, ids)
}

// Unregister stops the routing to a group; Forget drops one of its events.
func TestUnregisterAndForget(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	c := b.consumer("gone", eventbus.Subscriber{ID: "h", Event: "order.created", Handle: rec.handler("h")})
	start(t, c)
	p := b.producer()
	start(t, p)
	ids := emit(t, b, p, 1)
	expectOnce(t, rec, []string{"h"}, ids)

	ctx := context.Background()
	if err := eventbus.Unregister(ctx, eventbus.RegistryOptions{Redis: b.redis, Prefix: b.prefix}, "gone"); err != nil {
		t.Fatal(err)
	}
	emit(t, b, p, 2)
	waitFor(t, "events relayed", func() bool { return unsent(t, b.pool) == 0 })
	time.Sleep(300 * time.Millisecond)
	if n := rec.total.Load(); n != 1 {
		t.Fatalf("deliveries = %d after Unregister, want 1", n)
	}

	// Registering again (a restart) routes it again; Forget removes the route.
	if err := c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	c2 := b.consumer("gone", eventbus.Subscriber{ID: "h", Event: "order.created", Handle: rec.handler("h")})
	start(t, c2)
	ids = emit(t, b, p, 1)
	waitFor(t, "delivery after re-registration", func() bool { return rec.total.Load() == 2 })
	if err := eventbus.Forget(ctx, eventbus.RegistryOptions{Redis: b.redis, Prefix: b.prefix}, "gone", "order.created"); err != nil {
		t.Fatal(err)
	}
	emit(t, b, p, 2)
	waitFor(t, "events relayed", func() bool { return unsent(t, b.pool) == 0 })
	time.Sleep(300 * time.Millisecond)
	if n := rec.total.Load(); n != 2 {
		t.Fatalf("deliveries = %d after Forget, want 2 (ids %v)", n, ids)
	}
}

func TestValidation(t *testing.T) {
	t.Parallel()
	if _, err := eventbus.NewConsumer("a|b", eventbus.ConsumerOptions{}); err == nil {
		t.Fatal("group name with '|' accepted")
	}
	c, err := eventbus.NewConsumer("ok", eventbus.ConsumerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h := func(context.Context, eventbus.Event) error { return nil }
	for _, bad := range []eventbus.Subscriber{
		{ID: "a:b", Event: "x", Handle: h},
		{ID: "ok", Event: "", Handle: h},
		{ID: "ok", Event: "x"},
	} {
		if err := c.Subscribe(bad); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
	if err := c.Subscribe(eventbus.Subscriber{ID: "dup", Event: "x", Handle: h}); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe(eventbus.Subscriber{ID: "dup", Event: "y", Handle: h}); err == nil {
		t.Fatal("duplicate subscriber accepted")
	}
	empty, _ := eventbus.NewConsumer("empty", eventbus.ConsumerOptions{})
	if err := empty.Start(context.Background()); err == nil {
		t.Fatal("consumer without subscribers started")
	}
}

// EmitTx with several events in one call (the array path).
func TestEmitSeveralEvents(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	start(t, b.consumer("svc", eventbus.Subscriber{ID: "h", Event: "order.created", Handle: rec.handler("h")}))
	p := b.producer()
	start(t, p)
	evs := make([]eventbus.Event, 4)
	for i := range evs {
		evs[i], _ = eventbus.NewEvent("order.created", map[string]any{"i": i, "html": "<b>&</b>"})
	}
	evs[3].Payload = nil // stored as JSON null
	if err := pgx.BeginFunc(context.Background(), b.pool, func(tx pgx.Tx) error {
		return p.EmitTx(context.Background(), tx, evs...)
	}); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(evs))
	for i, ev := range evs {
		ids[i] = ev.ID
	}
	expectOnce(t, rec, []string{"h"}, ids)
}

// The handler gets the event as emitted: id, name, payload, database time.
func TestDeliveredEventContent(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	got := make(chan eventbus.Event, 1)
	start(t, b.consumer("svc", eventbus.Subscriber{
		ID: "h", Event: "order.created",
		Handle: func(ctx context.Context, ev eventbus.Event) error { got <- ev; return nil },
	}))
	p := b.producer()
	start(t, p)
	ev, _ := eventbus.NewEvent("order.created", map[string]any{"order_id": "o-1", "amount": 4200, "note": "<b>&</b>"})
	before := time.Now().Add(-time.Minute)
	if err := pgx.BeginFunc(context.Background(), b.pool, func(tx pgx.Tx) error {
		return p.EmitTx(context.Background(), tx, ev)
	}); err != nil {
		t.Fatal(err)
	}
	var id string
	_ = b.pool.QueryRow(context.Background(), `SELECT id::text FROM eventbus.outbox`).Scan(&id)
	select {
	case d := <-got:
		var payload struct {
			OrderID string `json:"order_id"`
			Amount  int    `json:"amount"`
			Note    string `json:"note"`
		}
		if err := d.Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if d.ID != id || d.Name != "order.created" || payload.OrderID != "o-1" || payload.Amount != 4200 ||
			payload.Note != "<b>&</b>" || d.At.Before(before) || d.At.After(time.Now().Add(time.Minute)) {
			t.Fatalf("delivered %+v (payload %+v), want id %s", d, payload, id)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no delivery")
	}
}

// Emitting and relaying work whatever the pool's query mode: raw values are
// binary in the default mode and text in the simple protocol; payloads must
// not be sent as bytea (simple and exec modes, used behind PgBouncer).
func TestQueryModes(t *testing.T) {
	t.Parallel()
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec, pgx.QueryExecModeCacheDescribe} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			b := newTestBus(t)
			cfg, err := pgxpool.ParseConfig(testdb.DSN(b.pool))
			if err != nil {
				t.Fatal(err)
			}
			cfg.ConnConfig.DefaultQueryExecMode = mode
			pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			rec := newRecorder()
			start(t, b.consumer("svc", eventbus.Subscriber{ID: "h", Event: "order.created", Handle: rec.handler("h")}))
			p := eventbus.NewProducer(pool, eventbus.ProducerOptions{Redis: b.redis, Prefix: b.prefix, RelayInterval: 50 * time.Millisecond})
			start(t, p)
			var ids []string
			if err := pgx.BeginFunc(context.Background(), pool, func(tx pgx.Tx) error {
				one, _ := eventbus.NewEvent("order.created", map[string]any{"one": true})
				single := []eventbus.Event{one}
				if err := p.EmitTx(context.Background(), tx, single...); err != nil { // single-event path
					return err
				}
				two := make([]eventbus.Event, 2)
				two[0], _ = eventbus.NewEvent("order.created", map[string]any{"html": "<b>&</b>"})
				two[1] = eventbus.Event{Name: "order.created"}                     // null payload
				if err := p.EmitTx(context.Background(), tx, two...); err != nil { // array path
					return err
				}
				ids = []string{single[0].ID, two[0].ID, two[1].ID}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			expectOnce(t, rec, []string{"h"}, ids)
			if n := unsent(t, b.pool); n != 0 {
				t.Fatalf("unsent = %d", n)
			}
		})
	}
}

// Registry lost with two groups: one registers again at once, the other
// only much later. The relay keeps routing to both (the routes it knew stay
// for RegistryGrace), so the slow group loses nothing.
func TestRegistryLossWithTwoGroups(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	start(t, b.consumerWith("fast", func(o *eventbus.ConsumerOptions) { o.RegistryCheck = 100 * time.Millisecond },
		eventbus.Subscriber{ID: "f", Event: "order.created", Handle: rec.handler("f")}))
	start(t, b.consumerWith("slow", func(o *eventbus.ConsumerOptions) { o.RegistryCheck = time.Hour },
		eventbus.Subscriber{ID: "s", Event: "order.created", Handle: rec.handler("s")}))
	p := b.producer()
	start(t, p)
	first := emit(t, b, p, 2)
	expectOnce(t, rec, []string{"f", "s"}, first)

	ctx := context.Background()
	conn, err := bullmq.NewRedisConnection(ctx, b.redis)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	keys := []string{b.prefix + ":eventbus:routes", b.prefix + ":eventbus:groups", b.prefix + ":eventbus:version", b.prefix + ":eventbus:epoch"}
	if err := conn.Client().Del(ctx, keys...).Err(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "fast group registered again", func() bool {
		ok, _ := conn.Client().HExists(ctx, b.prefix+":eventbus:groups", "fast").Result()
		return ok
	})
	if ok, _ := conn.Client().HExists(ctx, b.prefix+":eventbus:groups", "slow").Result(); ok {
		t.Fatal("slow group registered again already: the test does not exercise the grace")
	}
	second := emit(t, b, p, 3)
	expectOnce(t, rec, []string{"f", "s"}, append(first, second...))
}
