package saga_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/testdb"
)

type recordingObserver struct {
	mu    sync.Mutex
	steps []saga.StepAttempt
	runs  []saga.RunInfo
}

func (o *recordingObserver) StepAttempt(a saga.StepAttempt) {
	o.mu.Lock()
	o.steps = append(o.steps, a)
	o.mu.Unlock()
}

func (o *recordingObserver) RunEnded(r saga.RunInfo) {
	o.mu.Lock()
	o.runs = append(o.runs, r)
	o.mu.Unlock()
}

func TestObserverSeesStepsAndRuns(t *testing.T) {
	t.Parallel()
	obs := &recordingObserver{}
	_, eng := setup(t, saga.Options{Observer: obs})
	l := newLedger()
	a := addStep(t, l, "a", 1, nil)
	b := addStep(t, l, "b", 1, func() error { return errors.New("no") })
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		x, err = b.Run(c, x)
		return out{Total: x}, err
	})
	_, _ = wf.Run(context.Background(), eng, "k1", in{N: 1})
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.steps) != 2 || obs.steps[0].Outcome != "done" || obs.steps[1].Outcome != "failed" || obs.steps[1].Err == nil {
		t.Fatalf("steps = %+v", obs.steps)
	}
	if len(obs.runs) != 1 || obs.runs[0].Status != saga.StatusCompensated {
		t.Fatalf("runs = %+v", obs.runs)
	}
}

func TestPurgeRunsKeepsRecentAndNeedsAttention(t *testing.T) {
	t.Parallel()
	pool, eng := setup(t, saga.Options{})
	a := addStep(t, newLedger(), "a", 1, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		return out{Total: x}, err
	})
	ctx := context.Background()
	for _, k := range []string{"old", "recent"} {
		if _, err := wf.Run(ctx, eng, k, in{N: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `
		UPDATE saga.run SET updated_at = now() - interval '2 days' WHERE idem_key = 'old';
		INSERT INTO saga.run (id, workflow, version, idem_key, status, input, updated_at)
		VALUES (gen_random_uuid(), 'w', 1, 'stuck', 'needs_attention', '{}', now() - interval '2 days')`); err != nil {
		t.Fatal(err)
	}
	n, err := eng.PurgeRuns(ctx, 24*time.Hour, 100)
	if err != nil || n != 1 {
		t.Fatalf("purged = %d, %v; want 1", n, err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM saga.step`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 1 {
		t.Fatalf("steps left = %d, want 1 (cascade)", left)
	}
	if _, err := eng.PurgeRuns(ctx, 0, 1); err == nil {
		t.Fatal("PurgeRuns accepted age 0")
	}
}
