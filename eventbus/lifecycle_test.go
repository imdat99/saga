package eventbus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/eventbus"
)

// Stop must not wait on a relay pass that itself waits on the bus.
func TestStopDuringRelayPass(t *testing.T) {
	t.Parallel()
	pool, bus := newBus(t)
	rec := newRecorder()
	if err := bus.Subscribe(eventbus.Subscriber{ID: "billing.invoice", Event: "order.created", Handle: rec.handler("b")}); err != nil {
		t.Fatal(err)
	}
	inPass := make(chan struct{})
	release := make(chan struct{})
	first := true
	bus.SetAfterPush(func() error {
		if first {
			first = false
			close(inPass)
			<-release
		}
		return nil
	})
	emit(t, pool, bus, 1)
	if err := bus.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-inPass
	stopped := make(chan error, 1)
	go func() { stopped <- bus.Stop(context.Background()) }()
	time.Sleep(100 * time.Millisecond) // Stop is now waiting for the pass
	close(release)
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Stop deadlocked with an in-flight relay pass")
	}
}

func TestStartRejectsPartlyHandledQueue(t *testing.T) {
	t.Parallel()
	_, bus := newBus(t)
	rec := newRecorder()
	for _, s := range []eventbus.Subscriber{
		{ID: "a", Event: "x", Queue: "shared", Handle: rec.handler("a")},
		{ID: "b", Event: "x", Queue: "shared"},
	} {
		if err := bus.Subscribe(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := bus.Start(context.Background()); err == nil {
		_ = bus.Stop(context.Background())
		t.Fatal("Start accepted a queue whose workers cannot handle every job")
	}
}

// Retention keeps the staged events of runs still going, however old.
func TestRetentionKeepsStagingOfLiveRuns(t *testing.T) {
	t.Parallel()
	pool, bus := newBus(t)
	ctx := context.Background()
	if err := saga.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO saga.run (id, workflow, version, idem_key, status, input) VALUES
			('00000000-0000-0000-0000-000000000001', 'w', 1, 'live', 'waiting', '{}'),
			('00000000-0000-0000-0000-000000000002', 'w', 1, 'ended', 'compensated', '{}');
		INSERT INTO eventbus.staging (id, run_id, name, payload, created_at) VALUES
			(gen_random_uuid(), '00000000-0000-0000-0000-000000000001', 'e', '{}', now() - interval '30 days'),
			(gen_random_uuid(), '00000000-0000-0000-0000-000000000002', 'e', '{}', now() - interval '30 days');
		INSERT INTO eventbus.outbox (id, name, payload, created_at, sent_at) VALUES
			(gen_random_uuid(), 'e', '{}', now() - interval '30 days', now() - interval '30 days'),
			(gen_random_uuid(), 'e', '{}', now() - interval '30 days', NULL);`); err != nil {
		t.Fatal(err)
	}
	if err := bus.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, `SELECT count(*) FROM eventbus.staging`); n != 1 {
		t.Fatalf("staging rows = %d, want 1 (the live run's)", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM eventbus.outbox`); n != 1 {
		t.Fatalf("outbox rows = %d, want 1 (the unsent one)", n)
	}
}

// A job being processed while Stop runs must not deadlock it (the processor
// used to take the bus mutex that Stop held).
func TestStopWhileHandlerRuns(t *testing.T) {
	t.Parallel()
	pool, bus := newBus(t)
	inHandler := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	if err := bus.Subscribe(eventbus.Subscriber{
		ID: "slow", Event: "order.created",
		Handle: func(ctx context.Context, ev eventbus.Event) error {
			once.Do(func() { close(inHandler) })
			<-release
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	emit(t, pool, bus, 1)
	<-inHandler
	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopped <- bus.Stop(ctx)
	}()
	time.Sleep(100 * time.Millisecond)
	close(release)
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Stop deadlocked with a handler in progress")
	}
}
