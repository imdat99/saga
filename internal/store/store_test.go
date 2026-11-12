package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/store"
	"github.com/imdat99/saga/internal/testdb"
)

// The async start checkpoint must turn synchronous_commit off for its own
// transaction only.
func TestStartStepFencedAsyncScope(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := saga.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	const runID = "00000000-0000-0000-0000-0000000000aa"
	if _, err := pool.Exec(ctx, `
		INSERT INTO saga.run (id, workflow, version, idem_key, status, input, owner_id, lease_until, lease_version)
		VALUES ($1, 'w', 1, 'k', 'running', '{}', 'me', now() + interval '1 hour', 1)`, runID); err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	setting := func(q interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	) string {
		var v string
		if err := q.QueryRow(ctx, `SHOW synchronous_commit`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}

	// The production statement, inside an explicit transaction so the
	// setting can be observed before commit.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var attempt int
	var now time.Time
	if err := tx.QueryRow(ctx, store.StartStepFencedSQL, runID, 99, "probe", "me", 1, true).Scan(&attempt, &now); err != nil {
		t.Fatal(err)
	}
	if v := setting(tx); v != "off" {
		t.Fatalf("inside the transaction: synchronous_commit = %s, want off", v)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if v := setting(conn); v != "on" {
		t.Fatalf("after commit: synchronous_commit = %s, want on (local to the transaction)", v)
	}

	// The real statement works in both modes and still fences.
	st := store.New(pool)
	for i, async := range []bool{false, true} {
		attempt, _, err := st.StartStepFenced(ctx, store.Lease{RunID: runID, Owner: "me", Version: 1}, i+1, "s", async)
		if err != nil || attempt != 1 {
			t.Fatalf("async=%v: attempt=%d err=%v", async, attempt, err)
		}
	}
	if _, _, err := st.StartStepFenced(ctx, store.Lease{RunID: runID, Owner: "me", Version: 2}, 3, "s", true); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale lease err = %v, want ErrLeaseLost", err)
	}
}

// Normal flows must not mark steps uncertain: a first attempt, and a re-run
// of a step whose previous attempt failed (retrying). Only a re-run of a
// step left "started" is.
func TestUncertainOnlyAfterUnknownAttempt(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := saga.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	const runID = "00000000-0000-0000-0000-0000000000bb"
	if _, err := pool.Exec(ctx, `
		INSERT INTO saga.run (id, workflow, version, idem_key, status, input, owner_id, lease_until, lease_version)
		VALUES ($1, 'w', 1, 'k', 'running', '{}', 'me', now() + interval '1 hour', 1)`, runID); err != nil {
		t.Fatal(err)
	}
	st := store.New(pool)
	l := store.Lease{RunID: runID, Owner: "me", Version: 1}
	uncertain := func(seq int) bool {
		var u bool
		if err := pool.QueryRow(ctx, `SELECT uncertain FROM saga.step WHERE run_id = $1 AND seq = $2`, runID, seq).Scan(&u); err != nil {
			t.Fatal(err)
		}
		return u
	}
	start := func(seq int) {
		if _, _, err := st.StartStepFenced(ctx, l, seq, "s", false); err != nil {
			t.Fatal(err)
		}
	}
	start(1) // first attempt
	if uncertain(1) {
		t.Fatal("first attempt marked uncertain")
	}
	if err := st.FinishStepFenced(ctx, l, 1, "s", store.Outcome{Status: store.StepRetrying, Error: "boom"}); err != nil {
		t.Fatal(err)
	}
	start(1) // re-run after a failed attempt
	if uncertain(1) {
		t.Fatal("re-run of a retrying step marked uncertain")
	}
	start(1) // re-run of a step left started: outcome unknown
	if !uncertain(1) {
		t.Fatal("re-run of a started step not marked uncertain")
	}
}
