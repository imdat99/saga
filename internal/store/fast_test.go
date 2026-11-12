package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/store"
	"github.com/imdat99/saga/internal/testdb"
)

// poolWithMode opens a second pool on the database of pool, in mode, with at
// most maxConns connections.
func poolWithMode(t *testing.T, pool *pgxpool.Pool, mode pgx.QueryExecMode, maxConns int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testdb.DSN(pool))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = mode
	cfg.MaxConns = maxConns
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

// The hot-path statements behave the same on the fast path (pgconn, default
// mode) and on the pgx path (other modes, e.g. for PgBouncer).
func TestHotPathStatementsBothPaths(t *testing.T) {
	base := testdb.New(t)
	if err := saga.Migrate(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	for i, mode := range []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement, pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			st := store.New(poolWithMode(t, base, mode, 2))
			id := func(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-0000000000%d%d", i, n) }
			checkInsertRun(t, st, mode.String(), id)
			checkStepCheckpoints(t, st, base, id(1))
		})
	}
}

func checkInsertRun(t *testing.T, st *store.Store, name string, id func(int) string) {
	t.Helper()
	ctx := context.Background()
	r := store.Run{ID: id(1), Workflow: "w", Version: 3, IdemKey: "k-" + name, Input: []byte(`{"n":1}`)}
	ok, deadline, err := st.InsertRun(ctx, r, "me", time.Minute, 0)
	if err != nil || !ok || deadline != nil {
		t.Fatalf("insert: ok=%v deadline=%v err=%v", ok, deadline, err)
	}
	dup := r
	dup.ID = id(2)
	if ok, _, err := st.InsertRun(ctx, dup, "me", time.Minute, 0); err != nil || ok {
		t.Fatalf("duplicate idem key: ok=%v err=%v", ok, err)
	}
	timed := store.Run{ID: id(3), Workflow: "w", Version: 1, IdemKey: "d-" + name, Input: []byte(`1`)}
	before := time.Now().Add(-time.Second)
	ok, deadline, err = st.InsertRun(ctx, timed, "me", time.Minute, time.Hour)
	if err != nil || !ok || deadline == nil ||
		deadline.Before(before.Add(time.Hour)) || deadline.After(before.Add(time.Hour+time.Minute)) {
		t.Fatalf("insert with timeout: ok=%v deadline=%v err=%v", ok, deadline, err)
	}
	got, err := st.GetRun(ctx, r.ID)
	if err != nil || got.Workflow != "w" || got.Version != 3 || string(got.Input) != `{"n": 1}` || got.OwnerID != "me" {
		t.Fatalf("stored run = %+v, %v", got, err)
	}
}

func checkStepCheckpoints(t *testing.T, st *store.Store, base *pgxpool.Pool, runID string) {
	t.Helper()
	ctx := context.Background()
	l := store.Lease{RunID: runID, Owner: "me", Version: 1}
	attempt, now, err := st.StartStepFenced(ctx, l, 1, "s1", false)
	if err != nil || attempt != 1 || time.Since(now).Abs() > time.Minute {
		t.Fatalf("start: attempt=%d now=%v err=%v", attempt, now, err)
	}
	if attempt, _, err := st.StartStepFenced(ctx, l, 1, "s1", true); err != nil || attempt != 2 {
		t.Fatalf("restart: attempt=%d err=%v", attempt, err)
	}
	if err := st.FinishStepFenced(ctx, l, 1, "s1", store.Outcome{Status: store.StepDone, Output: []byte(`{"x":"é"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishStepFenced(ctx, l, 2, "s2", store.Outcome{
		Status: store.StepFailed, Error: "it's 'quoted'", ErrorKind: "permanent", Uncertain: true,
	}); err != nil {
		t.Fatal(err)
	}
	checkRecordedSteps(t, st, base, runID)
	checkStaleLeases(t, st, runID)
}

func checkRecordedSteps(t *testing.T, st *store.Store, base *pgxpool.Pool, runID string) {
	t.Helper()
	ctx := context.Background()
	steps, err := st.ListSteps(ctx, runID)
	if err != nil || len(steps) != 2 {
		t.Fatalf("steps = %+v, %v", steps, err)
	}
	s1, s2 := steps[0], steps[1]
	if s1.Status != store.StepDone || string(s1.Output) != `{"x": "é"}` || s1.CompData != nil ||
		s1.Error != "" || s1.ErrorKind != "" || !s1.Uncertain || s1.Attempt != 2 {
		t.Errorf("step 1 = %+v", s1)
	}
	if s2.Status != store.StepFailed || s2.Error != "it's 'quoted'" || s2.ErrorKind != "permanent" ||
		!s2.Uncertain || s2.Output != nil {
		t.Errorf("step 2 = %+v", s2)
	}
	var nullErr bool
	if err := base.QueryRow(ctx, `SELECT error IS NULL AND error_kind IS NULL FROM saga.step WHERE run_id = $1 AND seq = 1`,
		runID).Scan(&nullErr); err != nil || !nullErr {
		t.Errorf("empty error stored as NULL: %v, %v", nullErr, err)
	}
}

func checkStaleLeases(t *testing.T, st *store.Store, runID string) {
	t.Helper()
	ctx := context.Background()
	stale := store.Lease{RunID: runID, Owner: "me", Version: 2}
	if _, _, err := st.StartStepFenced(ctx, stale, 3, "s3", false); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("start with stale lease: %v", err)
	}
	if err := st.FinishStepFenced(ctx, stale, 1, "s1", store.Outcome{Status: store.StepFailed}); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("finish with stale lease: %v", err)
	}
	other := store.Lease{RunID: runID, Owner: "you", Version: 1}
	if err := st.FinishStepFenced(ctx, other, 1, "s1", store.Outcome{Status: store.StepFailed}); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("finish with other owner: %v", err)
	}
}

// A prepared statement dropped behind pgx's back (here DEALLOCATE ALL; in
// practice a pooler or a schema change) fails one call, which closes the
// connection, and the next call prepares it again.
func TestFastPathRecoversFromStaleStatement(t *testing.T) {
	base := testdb.New(t)
	ctx := context.Background()
	if err := saga.Migrate(ctx, base); err != nil {
		t.Fatal(err)
	}
	pool := poolWithMode(t, base, pgx.QueryExecModeCacheStatement, 1)
	st := store.New(pool)
	const runID = "00000000-0000-0000-0000-0000000000cc"
	if ok, _, err := st.InsertRun(ctx, store.Run{ID: runID, Workflow: "w", Version: 1, IdemKey: "k", Input: []byte(`{}`)},
		"me", time.Minute, 0); err != nil || !ok {
		t.Fatalf("insert: %v %v", ok, err)
	}
	l := store.Lease{RunID: runID, Owner: "me", Version: 1}
	if _, _, err := st.StartStepFenced(ctx, l, 1, "s", false); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DEALLOCATE ALL`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.StartStepFenced(ctx, l, 2, "s", false); err == nil {
		t.Fatal("call on the deallocated statement succeeded; the test does not exercise recovery")
	}
	if _, _, err := st.StartStepFenced(ctx, l, 2, "s", false); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
}

// A statement cancelled while it waits (here on the run row lock) returns an
// error; the connection is dropped and the next call works.
func TestFastPathCancelMidStatement(t *testing.T) {
	base := testdb.New(t)
	ctx := context.Background()
	if err := saga.Migrate(ctx, base); err != nil {
		t.Fatal(err)
	}
	pool := poolWithMode(t, base, pgx.QueryExecModeCacheStatement, 1)
	st := store.New(pool)
	const runID = "00000000-0000-0000-0000-0000000000dd"
	if ok, _, err := st.InsertRun(ctx, store.Run{ID: runID, Workflow: "w", Version: 1, IdemKey: "k", Input: []byte(`{}`)},
		"me", time.Minute, 0); err != nil || !ok {
		t.Fatalf("insert: %v %v", ok, err)
	}
	l := store.Lease{RunID: runID, Owner: "me", Version: 1}

	blocker, err := base.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blocker.Exec(ctx, `SELECT 1 FROM saga.run WHERE id = $1 FOR UPDATE`, runID); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, _, err = st.StartStepFenced(cctx, l, 1, "s", false)
	cancel()
	if err == nil {
		t.Fatal("blocked statement returned without error")
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// The cancelled statement may still commit on the server once the lock
	// is free (Postgres does not notice the closed socket while it waits),
	// as with the regular pgx path: attempt is 1 or 2.
	if attempt, _, err := st.StartStepFenced(ctx, l, 1, "s", false); err != nil || attempt < 1 || attempt > 2 {
		t.Fatalf("after cancellation: attempt=%d err=%v", attempt, err)
	}
}

// Concurrent callers share the pooled parameter buffers and connections.
func TestFastPathConcurrent(t *testing.T) {
	base := testdb.New(t)
	ctx := context.Background()
	if err := saga.Migrate(ctx, base); err != nil {
		t.Fatal(err)
	}
	st := store.New(poolWithMode(t, base, pgx.QueryExecModeCacheStatement, 4))
	const runs, steps = 16, 20
	var wg sync.WaitGroup
	for i := range runs {
		wg.Go(func() {
			runID := fmt.Sprintf("00000000-0000-0000-0001-%012d", i)
			if ok, _, err := st.InsertRun(ctx, store.Run{ID: runID, Workflow: "w", Version: 1, IdemKey: runID, Input: []byte(`{}`)},
				"me", time.Minute, 0); err != nil || !ok {
				t.Errorf("insert %d: %v %v", i, ok, err)
				return
			}
			l := store.Lease{RunID: runID, Owner: "me", Version: 1}
			for seq := 1; seq <= steps; seq++ {
				name := fmt.Sprintf("s%d-%d", i, seq)
				if _, _, err := st.StartStepFenced(ctx, l, seq, name, seq%2 == 0); err != nil {
					t.Errorf("start: %v", err)
					return
				}
				out := fmt.Appendf(nil, `{"run":%d,"seq":%d}`, i, seq)
				if err := st.FinishStepFenced(ctx, l, seq, name, store.Outcome{Status: store.StepDone, Output: out}); err != nil {
					t.Errorf("finish: %v", err)
					return
				}
			}
			got, err := st.ListSteps(ctx, runID)
			if err != nil || len(got) != steps {
				t.Errorf("run %d: %d steps, %v", i, len(got), err)
				return
			}
			for _, s := range got {
				if want := fmt.Sprintf("s%d-%d", i, s.Seq); s.Name != want ||
					string(s.Output) != fmt.Sprintf(`{"run": %d, "seq": %d}`, i, s.Seq) {
					t.Errorf("run %d step %d = %s %s", i, s.Seq, s.Name, s.Output)
				}
			}
		})
	}
	wg.Wait()
}

// FinishRunFenced records a terminal status like Fenced + SetRunStatus: lease
// released, locks dropped for done and compensated only, output and error
// kept when not given.
func TestFinishRunFencedBothPaths(t *testing.T) {
	base := testdb.New(t)
	if err := saga.Migrate(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	for mi, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeExec} {
		st := store.New(poolWithMode(t, base, mode, 2))
		for si, c := range []finishCase{
			{store.RunDone, []byte(`{"ok":true}`), "", false},
			{store.RunCompensated, nil, "boom", false},
			{store.RunNeedsAttention, nil, "", true},
		} {
			checkFinishRun(t, st, base, fmt.Sprintf("00000000-0000-0000-0002-%06d%06d", mi, si), c)
		}
		if err := st.FinishRunFenced(context.Background(), store.Lease{}, store.RunRunning, nil, ""); err == nil {
			t.Error("non-terminal status accepted")
		}
	}
}

type finishCase struct {
	status    string
	out       []byte
	errMsg    string
	keepLocks bool
}

func checkFinishRun(t *testing.T, st *store.Store, base *pgxpool.Pool, runID string, c finishCase) {
	t.Helper()
	ctx := context.Background()
	if ok, _, err := st.InsertRun(ctx, store.Run{ID: runID, Workflow: "w", Version: 1, IdemKey: runID, Input: []byte(`{}`)},
		"me", time.Minute, 0); err != nil || !ok {
		t.Fatalf("insert: %v %v", ok, err)
	}
	if _, err := base.Exec(ctx, `UPDATE saga.run SET error = 'earlier', output = '1' WHERE id = $1`, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := base.Exec(ctx, `INSERT INTO saga.lock (key, run_id, until, ttl_us) VALUES ($1, $2, now() + interval '1 hour', 1)`,
		"lock-"+runID, runID); err != nil {
		t.Fatal(err)
	}
	live := store.Lease{RunID: runID, Owner: "me", Version: 1}
	if err := st.FinishRunFenced(ctx, store.Lease{RunID: runID, Owner: "me", Version: 2}, c.status, c.out, c.errMsg); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("%s stale lease: %v", c.status, err)
	}
	if err := st.FinishRunFenced(ctx, live, c.status, c.out, c.errMsg); err != nil {
		t.Fatalf("%s: %v", c.status, err)
	}
	r, err := st.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	wantOut, wantErr := `1`, "earlier"
	if c.out != nil {
		wantOut = `{"ok": true}`
	}
	if c.errMsg != "" {
		wantErr = c.errMsg
	}
	if r.Status != c.status || r.OwnerID != "" || string(r.Output) != wantOut || r.Error != wantErr {
		t.Errorf("%s: run = %+v", c.status, r)
	}
	var locks int
	if err := base.QueryRow(ctx, `SELECT count(*) FROM saga.lock WHERE run_id = $1`, runID).Scan(&locks); err != nil {
		t.Fatal(err)
	}
	if (locks == 1) != c.keepLocks {
		t.Errorf("%s: %d locks left", c.status, locks)
	}
	if err := st.FinishRunFenced(ctx, live, c.status, nil, ""); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("second finish: %v, want ErrLeaseLost (owner released)", err)
	}
}

// blockRun holds the run row lock in another transaction until the returned
// func is called, so fenced statements on runID wait.
func blockRun(t *testing.T, pool *pgxpool.Pool, runID string) func() {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM saga.run WHERE id = $1 FOR UPDATE`, runID); err != nil {
		t.Fatal(err)
	}
	return func() { _ = tx.Rollback(ctx) }
}

// The per-execution Interrupt stops a statement in flight when it fires,
// refuses new statements afterwards (or once a covered context is done, even
// without Fire), and leaves the pool usable; a narrower context (own
// deadline) keeps pgconn's watch.
func TestInterrupt(t *testing.T) {
	base := testdb.New(t)
	ctx := context.Background()
	if err := saga.Migrate(ctx, base); err != nil {
		t.Fatal(err)
	}
	pool := poolWithMode(t, base, pgx.QueryExecModeCacheStatement, 1)
	st := store.New(pool)
	var none *store.Interrupt // nil-safe
	none.Cover(ctx)
	none.Fire()
	if none.Covers() {
		t.Fatal("nil interrupt covers")
	}

	const runID = "00000000-0000-0000-0000-0000000000ee"
	if ok, _, err := st.InsertRun(ctx, store.Run{ID: runID, Workflow: "w", Version: 1, IdemKey: "k", Input: []byte(`{}`)},
		"me", time.Minute, 0); err != nil || !ok {
		t.Fatalf("insert: %v %v", ok, err)
	}

	// Narrower context: its own deadline still applies (pgconn's watch).
	runCtx, cancelRun := context.WithCancel(ctx)
	in := &store.Interrupt{}
	in.Cover(runCtx)
	l := store.Lease{RunID: runID, Owner: "me", Version: 1, Interrupt: in}
	unblock := blockRun(t, base, runID)
	short, cancelShort := context.WithTimeout(runCtx, 150*time.Millisecond)
	_, _, err := st.StartStepFenced(short, l, 1, "s", false)
	cancelShort()
	unblock()
	if err == nil {
		t.Fatal("statement under a narrower expired context succeeded")
	}
	if _, _, err := st.StartStepFenced(runCtx, l, 2, "s", false); err != nil {
		t.Fatalf("watched statement after the narrow timeout: %v", err)
	}

	// A covered context cancelled without Fire (a path that forgot it):
	// no statement starts.
	gone, cancelGone := context.WithCancel(ctx)
	inGone := &store.Interrupt{}
	inGone.Cover(gone)
	cancelGone()
	lGone := store.Lease{RunID: runID, Owner: "me", Version: 1, Interrupt: inGone}
	if _, _, err := st.StartStepFenced(gone, lGone, 9, "s", false); !errors.Is(err, context.Canceled) {
		t.Fatalf("statement under a done context: %v", err)
	}

	// Execution cancelled (context, then Fire) while the statement waits.
	unblock = blockRun(t, base, runID)
	time.AfterFunc(150*time.Millisecond, func() {
		cancelRun()
		in.Fire()
	})
	start := time.Now()
	_, _, err = st.StartStepFenced(runCtx, l, 3, "s", false)
	unblock()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled statement: %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("cancellation took %v", d)
	}
	// No statement starts once the context is done.
	if err := st.FinishStepFenced(runCtx, l, 2, "s", store.Outcome{Status: store.StepDone}); !errors.Is(err, context.Canceled) {
		t.Fatalf("statement after cancellation: %v", err)
	}

	// The pool (one connection) is still usable, with no stale deadline.
	l2 := store.Lease{RunID: runID, Owner: "me", Version: 1}
	if err := st.FinishStepFenced(ctx, l2, 2, "s", store.Outcome{Status: store.StepDone}); err != nil {
		t.Fatalf("after interrupt: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, _, err := st.StartStepFenced(ctx, l2, 4, "s", false); err != nil {
		t.Fatalf("after interrupt, later: %v", err)
	}
}
