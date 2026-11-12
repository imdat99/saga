package idem_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga/idem"
	"github.com/imdat99/saga/internal/testdb"
)

func setup(t *testing.T) (*pgxpool.Pool, *idem.Table) {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE SCHEMA inventory; CREATE TABLE inventory.stock (sku text PRIMARY KEY, reserved int NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO inventory.stock VALUES ('sku-1', 0)`); err != nil {
		t.Fatal(err)
	}
	tbl := idem.MustNew("inventory")
	if err := tbl.CreateTable(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool, tbl
}

// reserve is a module operation made idempotent with Do.
func reserve(ctx context.Context, pool *pgxpool.Pool, tbl *idem.Table, key string, fail bool, calls *atomic.Int32) (int, error) {
	var out int
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var err error
		out, err = idem.Do(ctx, tx, tbl, key, func(ctx context.Context) (int, error) {
			calls.Add(1)
			var n int
			if err := tx.QueryRow(ctx, `UPDATE inventory.stock SET reserved = reserved + 1 WHERE sku = 'sku-1' RETURNING reserved`).Scan(&n); err != nil {
				return 0, err
			}
			if fail {
				return 0, errors.New("boom")
			}
			return n, nil
		})
		return err
	})
	return out, err
}

func reserved(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT reserved FROM inventory.stock`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestDoAppliesOncePerKey(t *testing.T) {
	pool, tbl := setup(t)
	ctx := context.Background()
	var calls atomic.Int32
	first, err := reserve(ctx, pool, tbl, "run:1:reserve", false, &calls)
	if err != nil {
		t.Fatal(err)
	}
	again, err := reserve(ctx, pool, tbl, "run:1:reserve", false, &calls)
	if err != nil || again != first {
		t.Fatalf("replay = %d, %v; want %d", again, err, first)
	}
	if calls.Load() != 1 || reserved(t, pool) != 1 {
		t.Fatalf("calls=%d reserved=%d, want 1 and 1", calls.Load(), reserved(t, pool))
	}
	v, found, err := idem.Lookup[int](ctx, pool, tbl, "run:1:reserve")
	if err != nil || !found || v != first {
		t.Fatalf("lookup = %d, %v, %v", v, found, err)
	}
	if _, found, _ := idem.Lookup[int](ctx, pool, tbl, "other"); found {
		t.Fatal("unknown key found")
	}
}

func TestDoFailureStoresNothing(t *testing.T) {
	pool, tbl := setup(t)
	ctx := context.Background()
	var calls atomic.Int32
	if _, err := reserve(ctx, pool, tbl, "k", true, &calls); err == nil {
		t.Fatal("want error")
	}
	if _, err := reserve(ctx, pool, tbl, "k", false, &calls); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || reserved(t, pool) != 1 {
		t.Fatalf("calls=%d reserved=%d, want 2 and 1", calls.Load(), reserved(t, pool))
	}
}

func TestDoConcurrentSameKey(t *testing.T) {
	pool, tbl := setup(t)
	ctx := context.Background()
	var calls atomic.Int32
	var wg sync.WaitGroup
	results := make([]int, 8)
	for i := range results {
		wg.Go(func() {
			v, err := reserve(ctx, pool, tbl, "same", false, &calls)
			if err != nil {
				t.Error(err)
			}
			results[i] = v
		})
	}
	wg.Wait()
	if calls.Load() != 1 || reserved(t, pool) != 1 {
		t.Fatalf("calls=%d reserved=%d, want 1 and 1", calls.Load(), reserved(t, pool))
	}
	for _, v := range results {
		if v != results[0] {
			t.Fatalf("results differ: %v", results)
		}
	}
}

func TestPurgeAndValidation(t *testing.T) {
	pool, tbl := setup(t)
	ctx := context.Background()
	var calls atomic.Int32
	if _, err := reserve(ctx, pool, tbl, "old", false, &calls); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE inventory.processed_requests SET created_at = now() - interval '2 days'`); err != nil {
		t.Fatal(err)
	}
	n, err := idem.Purge(ctx, pool, tbl, 24*time.Hour, 100)
	if err != nil || n != 1 {
		t.Fatalf("purge = %d, %v", n, err)
	}
	if _, err := idem.New(`x"; DROP TABLE y; --`); err == nil {
		t.Fatal("invalid schema accepted")
	}
}

// A caller that commits after fn failed must not leave the key claimed.
func TestDoFailureThenCommitLeavesKeyFree(t *testing.T) {
	pool, tbl := setup(t)
	ctx := context.Background()
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, err := idem.Do(ctx, tx, tbl, "k", func(ctx context.Context) (int, error) {
			if _, err := tx.Exec(ctx, `UPDATE inventory.stock SET reserved = reserved + 1`); err != nil {
				return 0, err
			}
			return 0, errors.New("boom")
		})
		if err == nil {
			t.Fatal("want error")
		}
		return nil // commits anyway
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, _ := idem.Lookup[int](ctx, pool, tbl, "k"); found {
		t.Fatal("failed request left a claim")
	}
	if reserved(t, pool) != 0 {
		t.Fatal("effect of the failed fn was committed")
	}
}

func TestAbortFencesOffLateAttempt(t *testing.T) {
	pool, tbl := setup(t)
	ctx := context.Background()
	var calls atomic.Int32

	// Compensator first: the request never ran, and now never will.
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, processed, err := idem.Abort[int](ctx, tx, tbl, "late")
		if processed {
			t.Fatal("unprocessed request reported processed")
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reserve(ctx, pool, tbl, "late", false, &calls); !errors.Is(err, idem.ErrAborted) {
		t.Fatalf("late attempt err = %v, want ErrAborted", err)
	}
	if calls.Load() != 0 || reserved(t, pool) != 0 {
		t.Fatal("aborted request took effect")
	}

	// Request first: Abort reports it processed, with its result.
	first, err := reserve(ctx, pool, tbl, "done", false, &calls)
	if err != nil {
		t.Fatal(err)
	}
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		v, processed, err := idem.Abort[int](ctx, tx, tbl, "done")
		if !processed || v != first {
			t.Fatalf("abort of processed request = %d, %v", v, processed)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Abort waits for an attempt still in flight, then reports it processed.
func TestAbortWaitsForInFlightAttempt(t *testing.T) {
	pool, tbl := setup(t)
	ctx := context.Background()
	claimed := make(chan struct{})
	finish := make(chan struct{})
	attempt := make(chan error, 1)
	go func() {
		attempt <- pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			_, err := idem.Do(ctx, tx, tbl, "slow", func(ctx context.Context) (int, error) {
				close(claimed)
				<-finish // the attempt is still running when the compensator starts
				return 7, nil
			})
			return err
		})
	}()
	<-claimed
	aborted := make(chan error, 1)
	var processed bool
	var v int
	go func() {
		aborted <- pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			var err error
			v, processed, err = idem.Abort[int](ctx, tx, tbl, "slow")
			return err
		})
	}()
	select {
	case err := <-aborted:
		t.Fatalf("Abort returned before the attempt ended: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(finish)
	if err := <-attempt; err != nil {
		t.Fatal(err)
	}
	if err := <-aborted; err != nil {
		t.Fatal(err)
	}
	if !processed || v != 7 {
		t.Fatalf("abort = %d, %v; want the committed result", v, processed)
	}
}
