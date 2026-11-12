// Command longrunning shows a workflow that lives for hours or days across
// processes: it waits for a person (Await + Signal), for an external system
// that calls back (a step that starts the work + Await on the step key +
// SignalKey), and for a fixed delay (Sleep). While it waits, it holds no
// goroutine, connection or lease: it is a row in Postgres that any worker
// resumes.
//
// See README.md in this directory. In short:
//
//	make up                                              # Postgres from docker-compose.yml
//	go run ./examples/longrunning worker                 # terminal 1: resumes runs, fake renderer
//	go run ./examples/longrunning start -doc doc-1       # terminal 2: prints the run id, exits
//	go run ./examples/longrunning approve -run <id>      # any time later, from any process
//	go run ./examples/longrunning status -run <id>
//	go run ./examples/longrunning demo [-reject]         # everything in one process
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/idem"
)

// PublishIn is the workflow input. The waiting times are part of it: code
// between steps must be deterministic, so durations must not come from flags
// or configuration that may differ between the process that starts a run and
// the one that resumes it days later.
type PublishIn struct {
	DocID           string        `json:"doc_id"`
	Title           string        `json:"title"`
	ApprovalTimeout time.Duration `json:"approval_timeout"` // how long a reviewer has
	RenderTimeout   time.Duration `json:"render_timeout"`   // how long the renderer has
	Embargo         time.Duration `json:"embargo"`          // delay between approval and publication
}

// PublishOut is the workflow output.
type PublishOut struct {
	DocID      string `json:"doc_id"`
	URL        string `json:"url"`
	ApprovedBy string `json:"approved_by"`
}

// Approval is the payload of the "approval" signal.
type Approval struct {
	OK   bool   `json:"ok"`
	By   string `json:"by"`
	Note string `json:"note,omitempty"`
}

// Rendered is what the renderer reports through SignalKey.
type Rendered struct {
	URL string `json:"url"`
}

// approvalSignal is the name the workflow awaits and approve sends.
const approvalSignal = "approval"

// publisher is the "documents" module: it owns schema longrun and makes its
// effects exactly-once per saga.Key with idem.
type publisher struct {
	pool     *pgxpool.Pool
	requests *idem.Table
}

func (p *publisher) migrate(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS longrun;
		-- created_by is the saga.Key of the step that created the row: the
		-- compensator finds the row by key, even when the step's outcome is
		-- uncertain and it has no compensation data.
		CREATE TABLE IF NOT EXISTS longrun.document (
			doc_id     text PRIMARY KEY,
			title      text NOT NULL,
			status     text NOT NULL CHECK (status IN ('draft', 'published', 'discarded')),
			url        text,
			created_by text NOT NULL UNIQUE
		);
		-- The external renderer's queue. ref is the saga.Key of the step that
		-- submitted the job: the only thing the renderer needs to answer.
		CREATE TABLE IF NOT EXISTS longrun.render_job (
			ref        text PRIMARY KEY,
			doc_id     text NOT NULL,
			status     text NOT NULL CHECK (status IN ('queued', 'done', 'cancelled')),
			created_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return err
	}
	return p.requests.CreateTable(ctx, p.pool)
}

// once runs fn exactly once per key, in a transaction.
func (p *publisher) once(ctx context.Context, key saga.Key, fn func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		_, err := idem.Do(ctx, tx, p.requests, key, func(ctx context.Context) (struct{}, error) {
			return struct{}{}, fn(tx)
		})
		return err
	})
}

// undo runs fn if the request of key was processed, and fences it off
// otherwise (see idem.Abort): it works when the step's outcome is uncertain.
func (p *publisher) undo(ctx context.Context, key saga.Key, fn func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		_, processed, err := idem.Abort[struct{}](ctx, tx, p.requests, key)
		if err != nil || !processed {
			return err
		}
		return fn(tx)
	})
}

// define registers the steps and the workflow (once per process).
func define(p *publisher) *saga.Workflow[PublishIn, PublishOut] {
	createDraft := saga.DefineStep("longrun.create_draft",
		func(ctx context.Context, k saga.Key, in PublishIn) (string, string, error) {
			err := p.once(ctx, k, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `
					INSERT INTO longrun.document (doc_id, title, status, created_by) VALUES ($1, $2, 'draft', $3)`,
					in.DocID, in.Title, string(k))
				return err
			})
			return in.DocID, in.DocID, err
		},
		func(ctx context.Context, k saga.Key, _ string) error {
			return p.undo(ctx, k, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `UPDATE longrun.document SET status = 'discarded' WHERE created_by = $1`, string(k))
				return err
			})
		},
		saga.WithRetry(saga.Exponential(5, time.Second)))

	// startRender submits the job and returns its reference (the step key).
	// The workflow then awaits a signal named after that reference.
	startRender := saga.DefineStep("longrun.start_render",
		func(ctx context.Context, k saga.Key, docID string) (saga.Key, string, error) {
			err := p.once(ctx, k, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `INSERT INTO longrun.render_job (ref, doc_id, status) VALUES ($1, $2, 'queued')`,
					string(k), docID)
				return err
			})
			return k, docID, err
		},
		func(ctx context.Context, k saga.Key, _ string) error {
			return p.undo(ctx, k, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `UPDATE longrun.render_job SET status = 'cancelled' WHERE ref = $1 AND status = 'queued'`,
					string(k))
				return err
			})
		},
		saga.WithRetry(saga.Exponential(5, time.Second)))

	publish := saga.DefineStep("longrun.publish",
		func(ctx context.Context, k saga.Key, out PublishOut) (PublishOut, struct{}, error) {
			err := p.once(ctx, k, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `UPDATE longrun.document SET status = 'published', url = $2 WHERE doc_id = $1`,
					out.DocID, out.URL)
				return err
			})
			return out, struct{}{}, err
		},
		nil, // last step: nothing after it can fail
		saga.WithRetry(saga.Exponential(5, time.Second)))

	return saga.DefineWorkflow("longrun.publish_document", 1,
		func(c saga.Ctx, in PublishIn) (PublishOut, error) {
			if _, err := createDraft.Run(c, in); err != nil {
				return PublishOut{}, err
			}

			// 1. Wait for a person, for up to ApprovalTimeout. Days, if need be:
			// the run is parked in Postgres meanwhile. A signal sent before the
			// run gets here is kept and consumed at once.
			approval, err := saga.Await[Approval](c, approvalSignal, in.ApprovalTimeout)
			if err != nil {
				return PublishOut{}, err // ErrAwaitTimeout: compensate; ErrRunWaiting: return as is
			}
			if !approval.OK {
				return PublishOut{}, fmt.Errorf("rejected by %s: %s", approval.By, approval.Note)
			}

			// 2. Wait for an external system that calls back. The step's key is
			// the job reference; the renderer answers with SignalKey(ref, ...).
			ref, err := startRender.Run(c, in.DocID)
			if err != nil {
				return PublishOut{}, err
			}
			rendered, err := saga.Await[Rendered](c, string(ref), in.RenderTimeout)
			if err != nil {
				return PublishOut{}, err // timeout: the render job is cancelled by compensation
			}

			// 3. Wait a fixed time (durable timer, database time).
			if err := saga.Sleep(c, in.Embargo); err != nil {
				return PublishOut{}, err
			}

			return publish.Run(c, PublishOut{DocID: in.DocID, URL: rendered.URL, ApprovedBy: approval.By})
		},
		// Whatever happens, give up (and compensate) after a week.
		saga.WithRunTimeout(7*24*time.Hour))
}

// renderer simulates an external system: it takes queued jobs older than
// delay and reports each result with SignalKey, knowing only the reference.
// It signals before marking the job done: a crash in between re-signals,
// which the engine ignores (the first signal wins).
func renderer(ctx context.Context, pool *pgxpool.Pool, eng *saga.Engine, delay time.Duration, log *slog.Logger) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rows, err := pool.Query(ctx, `
			SELECT ref, doc_id FROM longrun.render_job
			WHERE status = 'queued' AND created_at < now() - $1::bigint * interval '1 millisecond'
			ORDER BY created_at LIMIT 10`, delay.Milliseconds())
		if err != nil {
			continue
		}
		type job struct{ ref, docID string }
		jobs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (job, error) {
			var j job
			return j, r.Scan(&j.ref, &j.docID)
		})
		if err != nil {
			continue
		}
		for _, j := range jobs {
			url := "https://cdn.example.com/" + j.docID + ".pdf"
			if err := eng.SignalKey(ctx, saga.Key(j.ref), Rendered{URL: url}); err != nil {
				log.Warn("renderer: signal failed", "ref", j.ref, "err", err)
				continue
			}
			_, _ = pool.Exec(ctx, `UPDATE longrun.render_job SET status = 'done' WHERE ref = $1 AND status = 'queued'`, j.ref)
			log.Info("renderer: job done, result signalled", "doc", j.docID)
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1], os.Args[2:])
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: longrunning <command> [flags]
  worker   [-render-delay 5s]                    resume runs, run the fake renderer (until Ctrl-C)
  start    -doc ID [-title T] [-approval 10m] [-render 5m] [-embargo 10s]
  approve  -run ID [-by NAME] [-reject] [-note TEXT]
  status   -run ID
  demo     [-reject]                             worker + start + approve in one process`)
	os.Exit(2)
}

// app is what every command needs.
type app struct {
	pool    *pgxpool.Pool
	eng     *saga.Engine
	publish *saga.Workflow[PublishIn, PublishOut]
	log     *slog.Logger
}

func open(ctx context.Context) (*app, error) {
	dsn := os.Getenv("SAGA_DSN")
	if dsn == "" {
		dsn = "postgres://saga:saga@localhost:5432/saga?sslmode=disable" //nolint:gosec // docker-compose development default
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := saga.Migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	p := &publisher{pool: pool, requests: idem.MustNew("longrun")}
	if err := p.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate longrun: %w", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	eng, err := saga.New(pool, saga.Options{Logger: log})
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &app{pool: pool, eng: eng, publish: define(p), log: log}, nil
}

func (a *app) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = a.eng.Stop(ctx)
	a.pool.Close()
}

func run(ctx context.Context, cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var (
		docID       = fs.String("doc", "", "document id (also the idempotency key)")
		title       = fs.String("title", "Quarterly report", "document title")
		approvalTTL = fs.Duration("approval", 10*time.Minute, "approval timeout")
		renderTTL   = fs.Duration("render", 5*time.Minute, "render timeout")
		embargo     = fs.Duration("embargo", 10*time.Second, "delay between approval and publication")
		runID       = fs.String("run", "", "run id")
		by          = fs.String("by", "alice", "reviewer")
		reject      = fs.Bool("reject", false, "reject instead of approving")
		note        = fs.String("note", "", "review note")
		renderDelay = fs.Duration("render-delay", 5*time.Second, "how long the fake renderer takes")
	)
	_ = fs.Parse(args)

	a, err := open(ctx)
	if err != nil {
		return err
	}
	defer a.close()

	switch cmd {
	case "worker":
		// Start makes this process resume parked runs (timers, signals,
		// retries) and recover runs abandoned by crashed processes.
		if err := a.eng.Start(ctx); err != nil {
			return err
		}
		a.log.Info("worker running; Ctrl-C to stop")
		renderer(ctx, a.pool, a.eng, *renderDelay, a.log)
		return nil

	case "start":
		if *docID == "" {
			return errors.New("-doc is required")
		}
		// Start enqueues the run and returns at once; a worker executes it.
		h, err := a.publish.Start(ctx, a.eng, "publish:"+*docID, PublishIn{
			DocID: *docID, Title: *title,
			ApprovalTimeout: *approvalTTL, RenderTimeout: *renderTTL, Embargo: *embargo,
		})
		if err != nil {
			return err
		}
		fmt.Println(h.ID())
		return nil

	case "approve":
		if *runID == "" {
			return errors.New("-run is required")
		}
		// Any process can signal, days later; no worker needs to be the one
		// that started the run. Sending twice is harmless: the first wins.
		return a.eng.Signal(ctx, *runID, approvalSignal, Approval{OK: !*reject, By: *by, Note: *note})

	case "status":
		if *runID == "" {
			return errors.New("-run is required")
		}
		return a.printStatus(ctx, *runID)

	case "demo":
		return a.demo(ctx, *reject, *renderDelay)
	}
	usage()
	return nil
}

func (a *app) printStatus(ctx context.Context, runID string) error {
	h := a.publish.Handle(a.eng, runID)
	st, err := h.Status(ctx)
	if err != nil {
		return err
	}
	fmt.Println("status:", st)
	rows, err := a.pool.Query(ctx, `SELECT seq, name, status FROM saga.step WHERE run_id = $1 ORDER BY seq`, runID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name, status string
		if err := rows.Scan(&seq, &name, &status); err != nil {
			return err
		}
		fmt.Printf("  %d %-34s %s\n", seq, name, status)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	switch st {
	case saga.StatusDone, saga.StatusCompensated, saga.StatusNeedsAttention:
		out, err := h.Wait(ctx)
		fmt.Printf("outcome: %+v %v\n", out, err)
	default:
	}
	return nil
}

// demo runs a worker, starts a run, approves (or rejects) it two seconds
// later from "another caller", and waits for the outcome.
func (a *app) demo(ctx context.Context, reject bool, renderDelay time.Duration) error {
	if err := a.eng.Start(ctx); err != nil {
		return err
	}
	rctx, stopRenderer := context.WithCancel(ctx)
	defer stopRenderer()
	go renderer(rctx, a.pool, a.eng, renderDelay, a.log)

	docID := fmt.Sprintf("doc-%d", time.Now().UnixNano())
	h, err := a.publish.Start(ctx, a.eng, "publish:"+docID, PublishIn{
		DocID: docID, Title: "Demo",
		ApprovalTimeout: time.Minute, RenderTimeout: time.Minute, Embargo: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	fmt.Println("run", h.ID(), "started; waiting for approval")

	time.Sleep(2 * time.Second)
	if err := a.eng.Signal(ctx, h.ID(), approvalSignal, Approval{OK: !reject, By: "alice", Note: "demo"}); err != nil {
		return err
	}
	fmt.Println("approval sent (ok =", !reject, ")")

	wctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	out, err := h.Wait(wctx)
	var re *saga.RunError
	switch {
	case errors.As(err, &re):
		fmt.Printf("run ended %s: %s\n", re.Status, re.Cause)
	case err != nil:
		return err
	default:
		fmt.Printf("published %s at %s (approved by %s)\n", out.DocID, out.URL, out.ApprovedBy)
	}
	return a.printStatus(ctx, h.ID())
}
