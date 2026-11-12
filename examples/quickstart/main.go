// Command quickstart is the program of docs/guide/01-getting-started.md: a
// two-step transfer saga whose second step fails on demand, so that the first
// one is compensated.
//
//	make up   # Postgres from docker-compose.yml
//	go run ./examples/quickstart            # transfer done
//	go run ./examples/quickstart -fail      # credit fails, debit compensated
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/idem"
)

// TransferIn is the workflow input. Inputs, outputs and compensation data are
// stored as JSON: keep them small (IDs, amounts), not whole entities.
type TransferIn struct {
	From, To string
	Amount   int64
}

// TransferOut is the workflow output.
type TransferOut struct {
	DebitID, CreditID string
}

// ledger is a toy "module": it owns the schema "ledger" and makes its effects
// exactly-once per saga.Key with idem.Do.
type ledger struct {
	pool       *pgxpool.Pool
	requests   *idem.Table
	failCredit bool
}

func (l *ledger) migrate(ctx context.Context) error {
	if _, err := l.pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS ledger;
		CREATE TABLE IF NOT EXISTS ledger.entry (
			key     text PRIMARY KEY,
			account text   NOT NULL,
			amount  bigint NOT NULL
		)`); err != nil {
		return err
	}
	return l.requests.CreateTable(ctx, l.pool)
}

// post writes one entry, once per key: a retried or re-executed step gets the
// first entry back.
func (l *ledger) post(ctx context.Context, key saga.Key, account string, amount int64) (string, error) {
	var id string
	err := pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		var err error
		id, err = idem.Do(ctx, tx, l.requests, key, func(ctx context.Context) (string, error) {
			_, err := tx.Exec(ctx, `INSERT INTO ledger.entry (key, account, amount) VALUES ($1, $2, $3)`,
				string(key), account, amount)
			return string(key), err
		})
		return err
	})
	return id, err
}

// reverse undoes the entry of key, if it was made. It works when the outcome
// of the step is uncertain too: idem.Abort tells whether the effect happened,
// and fences off an attempt still running elsewhere.
func (l *ledger) reverse(ctx context.Context, key saga.Key) error {
	return pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		_, processed, err := idem.Abort[string](ctx, tx, l.requests, key)
		if err != nil || !processed {
			return err
		}
		_, err = tx.Exec(ctx, `DELETE FROM ledger.entry WHERE key = $1`, string(key))
		return err
	})
}

var errCreditRefused = errors.New("credit refused")

// define registers the steps and the workflow. In an application this happens
// at package level, once: names are global and must stay stable across
// deploys.
func define(l *ledger) *saga.Workflow[TransferIn, TransferOut] {
	debit := saga.DefineStep("quickstart.debit",
		func(ctx context.Context, k saga.Key, in TransferIn) (string, string, error) {
			id, err := l.post(ctx, k, in.From, -in.Amount)
			return id, id, err // output, compensation data, error
		},
		func(ctx context.Context, k saga.Key, _ string) error { return l.reverse(ctx, k) },
		saga.WithRetry(saga.Exponential(3, time.Second)))

	credit := saga.DefineStep("quickstart.credit",
		func(ctx context.Context, k saga.Key, in TransferIn) (string, string, error) {
			if l.failCredit {
				return "", "", saga.Permanent(errCreditRefused) // no retry
			}
			id, err := l.post(ctx, k, in.To, in.Amount)
			return id, id, err
		},
		func(ctx context.Context, k saga.Key, _ string) error { return l.reverse(ctx, k) })

	return saga.DefineWorkflow("quickstart.transfer", 1,
		func(c saga.Ctx, in TransferIn) (TransferOut, error) {
			debitID, err := debit.Run(c, in)
			if err != nil {
				return TransferOut{}, err
			}
			creditID, err := credit.Run(c, in)
			if err != nil {
				return TransferOut{}, err // the engine compensates debit
			}
			return TransferOut{DebitID: debitID, CreditID: creditID}, nil
		})
}

func main() {
	fail := flag.Bool("fail", false, "make the credit step fail permanently")
	key := flag.String("key", "", "idempotency key (default: a new one per execution)")
	flag.Parse()
	if err := run(*fail, *key); err != nil {
		log.Fatal(err)
	}
}

func run(fail bool, key string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := os.Getenv("SAGA_DSN")
	if dsn == "" {
		dsn = "postgres://saga:saga@localhost:5432/saga?sslmode=disable" //nolint:gosec // docker-compose development default
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := saga.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	l := &ledger{pool: pool, requests: idem.MustNew("ledger"), failCredit: fail}
	if err := l.migrate(ctx); err != nil {
		return fmt.Errorf("migrate ledger: %w", err)
	}
	transfer := define(l)

	eng, err := saga.New(pool, saga.Options{})
	if err != nil {
		return err
	}
	if err := eng.Start(ctx); err != nil { // recovery, retries, timers, signals
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = eng.Stop(sctx)
	}()

	if key == "" {
		key = fmt.Sprintf("transfer-%d", time.Now().UnixNano())
	}
	out, err := transfer.Run(ctx, eng, key, TransferIn{From: "alice", To: "bob", Amount: 100})
	if errors.Is(err, saga.ErrRunWaiting) || errors.Is(err, saga.ErrRunInProgress) {
		// Parked for a retry backoff, or executing elsewhere: wait for it.
		h, lerr := transfer.Lookup(ctx, eng, key)
		if lerr != nil {
			return lerr
		}
		out, err = h.Wait(ctx)
	}
	var re *saga.RunError
	switch {
	case errors.As(err, &re):
		fmt.Printf("run %s ended %s: %s\n", re.RunID, re.Status, re.Cause)
		return nil
	case err != nil:
		return err
	}
	fmt.Printf("transfer done: debit %s, credit %s\n", out.DebitID, out.CreditID)
	return nil
}
