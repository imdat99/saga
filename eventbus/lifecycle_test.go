package eventbus_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/eventbus"
)

// Stop must not wait on a relay pass that itself waits.
func TestStopDuringRelayPass(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	rec := newRecorder()
	start(t, b.consumer("billing", eventbus.Subscriber{ID: "invoice", Event: "order.created", Handle: rec.handler("b")}))
	p := b.producer()
	inPass := make(chan struct{})
	release := make(chan struct{})
	first := true
	p.SetAfterPush(func() error {
		if first {
			first = false
			close(inPass)
			<-release
		}
		return nil
	})
	emit(t, b, p, 1)
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-inPass
	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop(context.Background()) }()
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

// Start twice, Subscribe after Start.
func TestStartTwice(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	p := b.producer()
	start(t, p)
	if err := p.Start(context.Background()); err == nil {
		t.Fatal("producer started twice")
	}
	c := b.consumer("svc", eventbus.Subscriber{ID: "h", Event: "x", Handle: newRecorder().handler("h")})
	start(t, c)
	if err := c.Start(context.Background()); err == nil {
		t.Fatal("consumer started twice")
	}
	if err := c.Subscribe(eventbus.Subscriber{ID: "late", Event: "x", Handle: newRecorder().handler("late")}); err == nil {
		t.Fatal("Subscribe accepted after Start")
	}
}

// Retention keeps the staged events of runs still going, however old.
func TestRetentionKeepsStagingOfLiveRuns(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	p := b.producer()
	ctx := context.Background()
	if err := saga.Migrate(ctx, b.pool); err != nil {
		t.Fatal(err)
	}
	if _, err := b.pool.Exec(ctx, `
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
	if err := p.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, b.pool, `SELECT count(*) FROM eventbus.staging`); n != 1 {
		t.Fatalf("staging rows = %d, want 1 (the live run's)", n)
	}
	if n := count(t, b.pool, `SELECT count(*) FROM eventbus.outbox`); n != 1 {
		t.Fatalf("outbox rows = %d, want 1 (the unsent one)", n)
	}
}

// A job being processed while Stop runs must not deadlock it.
func TestStopWhileHandlerRuns(t *testing.T) {
	t.Parallel()
	b := newTestBus(t)
	inHandler := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	c := b.consumer("slow", eventbus.Subscriber{
		ID: "slow", Event: "order.created",
		Handle: func(ctx context.Context, ev eventbus.Event) error {
			once.Do(func() { close(inHandler) })
			<-release
			return nil
		},
	})
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	p := b.producer()
	start(t, p)
	emit(t, b, p, 1)
	<-inHandler
	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopped <- c.Stop(ctx)
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
