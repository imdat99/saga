package saga_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/testdb"
)

// The engine works whatever the pool's query mode: the default mode uses the
// pgconn fast path for hot-path statements, the others (typical behind
// PgBouncer) the regular pgx path.
func TestQueryModes(t *testing.T) {
	for name, mode := range map[string]pgx.QueryExecMode{
		"cache_statement": pgx.QueryExecModeCacheStatement,
		"cache_describe":  pgx.QueryExecModeCacheDescribe,
		"exec":            pgx.QueryExecModeExec,
		"simple":          pgx.QueryExecModeSimpleProtocol,
	} {
		t.Run(name, func(t *testing.T) {
			base := testdb.New(t)
			if err := saga.Migrate(context.Background(), base); err != nil {
				t.Fatal(err)
			}
			cfg, err := pgxpool.ParseConfig(testdb.DSN(base))
			if err != nil {
				t.Fatal(err)
			}
			cfg.ConnConfig.DefaultQueryExecMode = mode
			pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			eng := newEngine(t, pool, saga.Options{})

			l := newLedger()
			a := addStep(t, l, "a", 1, nil)
			b := addStep(t, l, "b", 10, nil)
			boom := errors.New("boom")
			c := addStep(t, l, "c", 100, func() error { return saga.Permanent(boom) })
			wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(cx saga.Ctx, v in) (out, error) {
				x, err := a.Run(cx, v.N)
				if err != nil {
					return out{}, err
				}
				y, err := b.Run(cx, x)
				if err != nil || v.N > 0 {
					return out{Total: y}, err
				}
				_, err = c.Run(cx, y)
				return out{}, err
			})

			got, err := wf.Run(context.Background(), eng, "ok", in{N: 5})
			if err != nil || got.Total != 16 {
				t.Fatalf("run = %+v, %v", got, err)
			}
			if got, err := wf.Run(context.Background(), eng, "ok", in{N: 7}); err != nil || got.Total != 16 {
				t.Fatalf("joined run = %+v, %v", got, err)
			}

			_, err = wf.Run(context.Background(), eng, "fail", in{N: 0})
			var re *saga.RunError
			if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
				t.Fatalf("failing run: %v", err)
			}
			want := []string{"do:a", "do:b", "do:a", "do:b", "undo:b", "undo:a"}
			if got := l.snapshot(); !slices.Equal(got, want) {
				t.Fatalf("effects = %v, want %v", got, want)
			}
		})
	}
}
