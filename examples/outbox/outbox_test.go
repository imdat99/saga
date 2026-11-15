package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/examples/outbox"
	"github.com/imdat99/saga/internal/testdb"
)

func newDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	if err := saga.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, outbox.Schema); err != nil {
		t.Fatal(err)
	}
	return pool
}

type flags struct {
	failSecond bool
	failComp   atomic.Bool
	inRunStep  atomic.Bool
	inRunComp  atomic.Bool
	emitTwice  bool
}

func define(t *testing.T, pool *pgxpool.Pool, f *flags) *saga.Workflow[string, string] {
	t.Helper()
	place := saga.DefineStep(testdb.UniqueName(t, "place"),
		func(ctx context.Context, k saga.Key, orderID string) (string, string, error) {
			_, in := saga.RunIDFrom(ctx)
			f.inRunStep.Store(in)
			err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				n := 1
				if f.emitTwice {
					n = 2 // a re-executed step: same key, same event id
				}
				for range n {
					if err := outbox.Emit(ctx, tx, outbox.Event{ID: string(k) + ":order.placed", Name: "order.placed", Key: orderID, Payload: map[string]string{"order_id": orderID}}); err != nil {
						return err
					}
				}
				return nil
			})
			return orderID, orderID, err
		},
		func(ctx context.Context, k saga.Key, orderID string) error {
			_, in := saga.RunIDFrom(ctx)
			f.inRunComp.Store(in)
			if f.failComp.Load() {
				return saga.Permanent(errors.New("refund provider down"))
			}
			return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				return outbox.Emit(ctx, tx, outbox.Event{ID: string(k) + ":order.cancelled", Name: "order.cancelled", Key: orderID, Payload: map[string]string{"order_id": orderID}})
			})
		})
	second := saga.DefineStep(testdb.UniqueName(t, "second"),
		func(ctx context.Context, k saga.Key, orderID string) (string, struct{}, error) {
			if f.failSecond {
				return "", struct{}{}, saga.Permanent(errors.New("out of stock"))
			}
			return orderID, struct{}{}, nil
		}, nil)
	return saga.DefineWorkflow(testdb.UniqueName(t, "doc.order"), 1, func(c saga.Ctx, orderID string) (string, error) {
		if _, err := place.Run(c, orderID); err != nil {
			return "", err
		}
		return second.Run(c, orderID)
	})
}

func tableNames(t *testing.T, pool *pgxpool.Pool, table string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT name FROM app."+table+" ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func engine(t *testing.T, pool *pgxpool.Pool) *saga.Engine {
	t.Helper()
	eng, err := saga.New(pool, saga.Options{Hooks: []saga.RunHook{outbox.Hook{}}})
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

func TestDoneReleases(t *testing.T) {
	pool := newDB(t)
	f := &flags{emitTwice: true}
	wf := define(t, pool, f)
	if _, err := wf.Run(context.Background(), engine(t, pool), testdb.UniqueName(t, "k"), "o1"); err != nil {
		t.Fatal(err)
	}
	if !f.inRunStep.Load() {
		t.Error("RunIDFrom false inside a step")
	}
	if got := tableNames(t, pool, "outbox"); fmt.Sprint(got) != "[order.placed]" {
		t.Errorf("outbox %v", got)
	}
	if got := tableNames(t, pool, "event_staging"); len(got) != 0 {
		t.Errorf("staging %v", got)
	}
}

func TestCompensatedDrops(t *testing.T) {
	pool := newDB(t)
	f := &flags{failSecond: true}
	wf := define(t, pool, f)
	_, err := wf.Run(context.Background(), engine(t, pool), testdb.UniqueName(t, "k"), "o2")
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err %v", err)
	}
	if f.inRunComp.Load() {
		t.Error("RunIDFrom true inside a compensator")
	}
	// order.placed dropped; order.cancelled (from the compensator) published.
	if got := tableNames(t, pool, "outbox"); fmt.Sprint(got) != "[order.cancelled]" {
		t.Errorf("outbox %v", got)
	}
	if got := tableNames(t, pool, "event_staging"); len(got) != 0 {
		t.Errorf("staging %v", got)
	}
}

func TestNeedsAttentionThenRetry(t *testing.T) {
	pool := newDB(t)
	f := &flags{failSecond: true}
	f.failComp.Store(true)
	wf := define(t, pool, f)
	eng := engine(t, pool)
	ctx := t.Context()
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		sctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = eng.Stop(sctx)
	}()
	_, err := wf.Run(ctx, eng, testdb.UniqueName(t, "k"), "o3")
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusNeedsAttention {
		t.Fatalf("err %v", err)
	}
	if got := tableNames(t, pool, "outbox"); fmt.Sprint(got) != "[saga.needs_attention]" {
		t.Errorf("outbox %v", got)
	}
	if got := tableNames(t, pool, "event_staging"); fmt.Sprint(got) != "[order.placed]" {
		t.Errorf("staging kept? %v", got)
	}
	// The monitoring queries of chapter 12 run, and see no leftover staging
	// (the run is needs_attention, not ended).
	var age *time.Duration
	if err := pool.QueryRow(ctx, `SELECT now() - min(created_at) FROM app.outbox WHERE sent_at IS NULL`).Scan(&age); err != nil || age == nil {
		t.Errorf("oldest unsent: %v %v", age, err)
	}
	var leftover int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM app.event_staging s JOIN saga.run r ON r.id = s.run_id
		WHERE r.status IN ('done', 'compensated')`).Scan(&leftover); err != nil || leftover != 0 {
		t.Errorf("leftover staging: %d %v", leftover, err)
	}
	f.failComp.Store(false)
	if err := eng.RetryCompensation(ctx, re.RunID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var st string
		_ = pool.QueryRow(ctx, `SELECT status FROM saga.run WHERE id = $1`, re.RunID).Scan(&st)
		if st == "compensated" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status %s", st)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := tableNames(t, pool, "outbox"); fmt.Sprint(got) != "[saga.needs_attention order.cancelled]" {
		t.Errorf("outbox %v", got)
	}
	if got := tableNames(t, pool, "event_staging"); len(got) != 0 {
		t.Errorf("staging %v", got)
	}
}

type fakePub struct {
	mu   sync.Mutex
	got  []string
	fail atomic.Bool
}

func (p *fakePub) Publish(ctx context.Context, msgs []outbox.Message) error {
	if p.fail.Load() {
		return errors.New("broker down")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range msgs {
		p.got = append(p.got, m.ID)
	}
	return nil
}

func TestRelay(t *testing.T) {
	pool := newDB(t)
	ctx := context.Background()
	for i := range 250 {
		if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			return outbox.Emit(ctx, tx, outbox.Event{ID: fmt.Sprintf("e%03d", i), Name: "x", Payload: i})
		}); err != nil {
			t.Fatal(err)
		}
	}
	pub := &fakePub{}
	pub.fail.Store(true)
	if _, err := outbox.RelayOnce(ctx, pool, pub, 100); err == nil {
		t.Fatal("no error from a failing broker")
	}
	pub.fail.Store(false)
	// Two relays at once: SKIP LOCKED splits the work, nothing twice.
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for {
				n, err := outbox.RelayOnce(ctx, pool, pub, 50)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		})
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, id := range pub.got {
		if seen[id] {
			t.Errorf("published twice: %s", id)
		}
		seen[id] = true
	}
	if len(seen) != 250 {
		t.Errorf("published %d", len(seen))
	}
	var unsent int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM app.outbox WHERE sent_at IS NULL`).Scan(&unsent)
	if unsent != 0 {
		t.Errorf("unsent %d", unsent)
	}
	if n, err := outbox.PurgeSent(ctx, pool, 0, 1000); err != nil || n != 250 {
		t.Errorf("purge %d %v", n, err)
	}
}
