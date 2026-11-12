// Command multiworkflow shows several workflows working together: a parent
// order workflow fans out one child shipment workflow per warehouse, the
// children run in parallel (each is its own run), and the two sides talk with
// signals in both directions:
//
//	multi.order (parent)                        multi.shipment (child, one run per warehouse)
//	 ├─ start_shipment ×N  ───Start───────────► pick (reversible)
//	 ├─ Await "ready:<wh>" ×N ◄──signal──────── signal parent "ready:<wh>"
//	 ├─ charge (reversible)                      Await "decision"
//	 ├─ signal "decision" ×N  ────────────────► dispatch (irreversible, only after commit)
//	 └─ Await "shipped:<wh>" ×N ◄──signal────── signal parent "shipped:<wh>"
//
// It is a two-phase protocol: children only do reversible work until the
// parent decides; any failure before the decision (a child out of stock, the
// charge declined, a timeout) makes the parent compensate, and compensating a
// start_shipment step tells that child to abort, so it compensates its own
// steps. See README.md.
//
//	make up
//	go run ./examples/multiworkflow demo                       # all warehouses ship
//	go run ./examples/multiworkflow demo -out-of-stock wh-b    # one child fails: everything undone
//	go run ./examples/multiworkflow demo -decline              # charge fails after all are ready
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/idem"
)

// --- Workflow inputs, outputs and signal payloads -------------------------

// Line is one order line, served by one warehouse.
type Line struct {
	Warehouse string `json:"warehouse"`
	SKU       string `json:"sku"`
	Qty       int    `json:"qty"`
}

// OrderIn is the parent's input. Timeouts are part of it so that the
// workflow stays deterministic on replay.
type OrderIn struct {
	OrderID         string        `json:"order_id"`
	Amount          int64         `json:"amount"`
	Lines           []Line        `json:"lines"`
	ReadyTimeout    time.Duration `json:"ready_timeout"`    // children must be ready by then
	DecisionTimeout time.Duration `json:"decision_timeout"` // children give up waiting for the decision
	ShippedTimeout  time.Duration `json:"shipped_timeout"`
}

// OrderOut is the parent's output.
type OrderOut struct {
	OrderID  string            `json:"order_id"`
	Tracking map[string]string `json:"tracking"` // warehouse -> tracking number
}

// ShipmentIn is a child's input: everything it needs, including where to
// report (the parent's run id).
type ShipmentIn struct {
	ParentRunID     string        `json:"parent_run_id"`
	OrderID         string        `json:"order_id"`
	Warehouse       string        `json:"warehouse"`
	Lines           []Line        `json:"lines"`
	DecisionTimeout time.Duration `json:"decision_timeout"`
}

// Ready is what a child reports once its reversible work is done (or failed).
type Ready struct {
	OK     bool   `json:"ok"`
	Reason string `json:"reason,omitempty"`
}

// Decision is the parent's verdict, sent to each child.
type Decision struct {
	Commit bool   `json:"commit"`
	Reason string `json:"reason,omitempty"`
}

// Shipped is what a child reports after dispatch.
type Shipped struct {
	Tracking string `json:"tracking"`
}

// SignalIn is the input of the shared "send a signal" step.
type SignalIn struct {
	RunID   string          `json:"run_id"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
}

// Signal names.
const decisionSignal = "decision"

func readySignal(wh string) string   { return "ready:" + wh }
func shippedSignal(wh string) string { return "shipped:" + wh }

var (
	errOutOfStock = errors.New("out of stock")
	errDeclined   = errors.New("payment declined")
)

// --- Module: warehouses and payments (exactly-once per saga.Key) --------

type store struct {
	pool     *pgxpool.Pool
	requests *idem.Table
	decline  bool
}

func (s *store) migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS multi;
		CREATE TABLE IF NOT EXISTS multi.stock (
			warehouse text NOT NULL,
			sku       text NOT NULL,
			available int  NOT NULL CHECK (available >= 0),
			PRIMARY KEY (warehouse, sku)
		);
		-- Rows are keyed by the saga.Key of the step that created them, so
		-- compensators find them by key even when the outcome is uncertain.
		CREATE TABLE IF NOT EXISTS multi.pick (
			key       text PRIMARY KEY,
			warehouse text NOT NULL,
			sku       text NOT NULL,
			qty       int  NOT NULL,
			released  boolean NOT NULL DEFAULT false
		);
		CREATE TABLE IF NOT EXISTS multi.payment (
			key      text PRIMARY KEY,
			order_id text   NOT NULL,
			amount   bigint NOT NULL,
			refunded boolean NOT NULL DEFAULT false
		);
		CREATE TABLE IF NOT EXISTS multi.dispatch (
			key       text PRIMARY KEY,
			order_id  text NOT NULL,
			warehouse text NOT NULL,
			tracking  text NOT NULL
		)`); err != nil {
		return err
	}
	return s.requests.CreateTable(ctx, s.pool)
}

func (s *store) restock(ctx context.Context, wh, sku string, qty int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO multi.stock (warehouse, sku, available) VALUES ($1, $2, $3)
		ON CONFLICT (warehouse, sku) DO UPDATE SET available = excluded.available`, wh, sku, qty)
	return err
}

// pick takes the lines of one warehouse out of stock, once per key.
func (s *store) pick(ctx context.Context, k saga.Key, lines []Line) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := idem.Do(ctx, tx, s.requests, k, func(ctx context.Context) (struct{}, error) {
			for i, l := range lines {
				tag, err := tx.Exec(ctx, `
					UPDATE multi.stock SET available = available - $3
					WHERE warehouse = $1 AND sku = $2 AND available >= $3`, l.Warehouse, l.SKU, l.Qty)
				if err != nil {
					return struct{}{}, err
				}
				if tag.RowsAffected() == 0 {
					return struct{}{}, fmt.Errorf("%w: %s at %s", errOutOfStock, l.SKU, l.Warehouse)
				}
				if _, err := tx.Exec(ctx, `INSERT INTO multi.pick (key, warehouse, sku, qty) VALUES ($1, $2, $3, $4)`,
					fmt.Sprintf("%s#%d", k, i), l.Warehouse, l.SKU, l.Qty); err != nil {
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		})
		return err
	})
}

// unpick puts picked items back, if the pick happened (idem.Abort fences
// off a late attempt otherwise).
func (s *store) unpick(ctx context.Context, k saga.Key) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, processed, err := idem.Abort[struct{}](ctx, tx, s.requests, k)
		if err != nil || !processed {
			return err
		}
		_, err = tx.Exec(ctx, `
			WITH released AS (
				UPDATE multi.pick SET released = true
				WHERE key LIKE $1 || '#%' AND NOT released
				RETURNING warehouse, sku, qty)
			UPDATE multi.stock st SET available = st.available + r.qty
			FROM released r WHERE st.warehouse = r.warehouse AND st.sku = r.sku`, string(k))
		return err
	})
}

func (s *store) charge(ctx context.Context, k saga.Key, orderID string, amount int64) error {
	if s.decline {
		return errDeclined
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := idem.Do(ctx, tx, s.requests, k, func(ctx context.Context) (struct{}, error) {
			_, err := tx.Exec(ctx, `INSERT INTO multi.payment (key, order_id, amount) VALUES ($1, $2, $3)`,
				string(k), orderID, amount)
			return struct{}{}, err
		})
		return err
	})
}

func (s *store) refund(ctx context.Context, k saga.Key) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, processed, err := idem.Abort[struct{}](ctx, tx, s.requests, k)
		if err != nil || !processed {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE multi.payment SET refunded = true WHERE key = $1`, string(k))
		return err
	})
}

// dispatch hands the parcel to the carrier: irreversible, once per key.
func (s *store) dispatch(ctx context.Context, k saga.Key, orderID, wh string) (string, error) {
	var tracking string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		tracking, err = idem.Do(ctx, tx, s.requests, k, func(ctx context.Context) (string, error) {
			t := "TRK-" + orderID + "-" + wh
			_, err := tx.Exec(ctx, `INSERT INTO multi.dispatch (key, order_id, warehouse, tracking) VALUES ($1, $2, $3, $4)`,
				string(k), orderID, wh, t)
			return t, err
		})
		return err
	})
	return tracking, err
}

// --- Steps and workflows ---------------------------------------------------

type flows struct {
	order    *saga.Workflow[OrderIn, OrderOut]
	shipment *saga.Workflow[ShipmentIn, Shipped]

	sendSignal    *saga.Step[SignalIn, struct{}, struct{}]
	pick          *saga.Step[ShipmentIn, struct{}, struct{}]
	dispatch      *saga.Step[ShipmentIn, string, struct{}]
	startShipment *saga.Step[ShipmentIn, string, struct{}]
	charge        *saga.Step[OrderIn, struct{}, struct{}]
}

// signalRun sends a signal to another run through the sendSignal step.
func (f *flows) signalRun(c saga.Ctx, runID, name string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = f.sendSignal.Run(c, SignalIn{RunID: runID, Name: name, Payload: raw})
	return err
}

// define registers the steps and both workflows. Steps reach the engine
// through eng, set once it exists (steps only run after that).
func define(s *store, eng **saga.Engine) *flows {
	f := &flows{}

	// sendSignal is shared by both workflows: signalling another run is a
	// side effect, so it is a step (recorded, not repeated on replay). It is
	// idempotent anyway: the first signal of a name wins.
	f.sendSignal = saga.DefineStep("multi.signal",
		func(ctx context.Context, _ saga.Key, in SignalIn) (struct{}, struct{}, error) {
			return struct{}{}, struct{}{}, (*eng).Signal(ctx, in.RunID, in.Name, in.Payload)
		}, nil,
		saga.WithRetry(saga.Exponential(5, time.Second)))

	// --- child: one shipment ---

	f.pick = saga.DefineStep("multi.pick",
		func(ctx context.Context, k saga.Key, in ShipmentIn) (struct{}, struct{}, error) {
			err := s.pick(ctx, k, in.Lines)
			if errors.Is(err, errOutOfStock) {
				err = saga.Permanent(err)
			}
			return struct{}{}, struct{}{}, err
		},
		func(ctx context.Context, k saga.Key, _ struct{}) error { return s.unpick(ctx, k) },
		saga.WithRetry(saga.Exponential(5, time.Second)))

	f.dispatch = saga.DefineStep("multi.dispatch",
		func(ctx context.Context, k saga.Key, in ShipmentIn) (string, struct{}, error) {
			t, err := s.dispatch(ctx, k, in.OrderID, in.Warehouse)
			return t, struct{}{}, err
		}, nil, // irreversible: it only runs after the parent committed
		saga.WithRetry(saga.Exponential(10, time.Second)))

	f.shipment = saga.DefineWorkflow("multi.shipment", 1, f.shipmentFlow,
		saga.WithRunTimeout(24*time.Hour))

	// --- parent: the order ---

	// startShipment starts one child. The child's idempotency key is this
	// step's key: a re-executed step joins the same child. Its compensator
	// tells the child to abort (if it was started at all); the child then
	// compensates its own steps.
	f.startShipment = saga.DefineStep("multi.start_shipment",
		func(ctx context.Context, k saga.Key, in ShipmentIn) (string, struct{}, error) {
			h, err := f.shipment.Start(ctx, *eng, string(k), in)
			if err != nil {
				return "", struct{}{}, err
			}
			return h.ID(), struct{}{}, nil
		},
		func(ctx context.Context, k saga.Key, _ struct{}) error {
			h, err := f.shipment.Lookup(ctx, *eng, string(k))
			if errors.Is(err, saga.ErrRunNotFound) {
				return nil // never started
			}
			if err != nil {
				return err
			}
			// Ignored if the child already got a decision (first signal wins).
			return (*eng).Signal(ctx, h.ID(), decisionSignal, Decision{Reason: "order failed"})
		},
		saga.WithRetry(saga.Exponential(5, time.Second)))

	f.charge = saga.DefineStep("multi.charge",
		func(ctx context.Context, k saga.Key, in OrderIn) (struct{}, struct{}, error) {
			err := s.charge(ctx, k, in.OrderID, in.Amount)
			if errors.Is(err, errDeclined) {
				err = saga.Permanent(err)
			}
			return struct{}{}, struct{}{}, err
		},
		func(ctx context.Context, k saga.Key, _ struct{}) error { return s.refund(ctx, k) },
		saga.WithRetry(saga.Exponential(5, time.Second)))

	f.order = saga.DefineWorkflow("multi.order", 1, f.orderFlow,
		saga.WithRunTimeout(24*time.Hour))

	return f
}

// shipmentFlow is the child workflow: one warehouse's part of an order.
func (f *flows) shipmentFlow(c saga.Ctx, in ShipmentIn) (Shipped, error) {
	if _, err := f.pick.Run(c, in); err != nil {
		// Tell the parent only about a recorded failure of the step
		// (*saga.StepError: same on replay). Parking (ErrRunWaiting),
		// lease loss and shutdown are not failures: return them as is.
		if _, ok := errors.AsType[*saga.StepError](err); ok {
			if serr := f.signalRun(c, in.ParentRunID, readySignal(in.Warehouse), Ready{Reason: err.Error()}); serr != nil {
				return Shipped{}, serr
			}
		}
		return Shipped{}, err
	}
	if err := f.signalRun(c, in.ParentRunID, readySignal(in.Warehouse), Ready{OK: true}); err != nil {
		return Shipped{}, err
	}

	// Phase two: wait for the parent's verdict. A child left without
	// one (parent stuck) gives up and puts the stock back.
	d, err := saga.Await[Decision](c, decisionSignal, in.DecisionTimeout)
	if err != nil {
		return Shipped{}, err
	}
	if !d.Commit {
		return Shipped{}, fmt.Errorf("aborted by the order: %s", d.Reason)
	}

	tracking, err := f.dispatch.Run(c, in)
	if err != nil {
		return Shipped{}, err
	}
	out := Shipped{Tracking: tracking}
	return out, f.signalRun(c, in.ParentRunID, shippedSignal(in.Warehouse), out)
}

// orderFlow is the parent workflow.
func (f *flows) orderFlow(c saga.Ctx, in OrderIn) (OrderOut, error) {
	// Group lines by warehouse, in a deterministic order.
	byWH := map[string][]Line{}
	for _, l := range in.Lines {
		byWH[l.Warehouse] = append(byWH[l.Warehouse], l)
	}
	warehouses := make([]string, 0, len(byWH))
	for wh := range byWH {
		warehouses = append(warehouses, wh)
	}
	slices.Sort(warehouses) // never iterate a map between steps

	// Fan out: the children run in parallel, each as its own run.
	children := map[string]string{} // warehouse -> child run id
	for _, wh := range warehouses {
		id, err := f.startShipment.Run(c, ShipmentIn{
			ParentRunID: c.RunID(), OrderID: in.OrderID, Warehouse: wh,
			Lines: byWH[wh], DecisionTimeout: in.DecisionTimeout,
		})
		if err != nil {
			return OrderOut{}, err
		}
		children[wh] = id
	}

	// Fan in, phase one: every child must be ready. Signals that
	// arrive before the corresponding Await are kept.
	for _, wh := range warehouses {
		r, err := saga.Await[Ready](c, readySignal(wh), in.ReadyTimeout)
		if err != nil {
			return OrderOut{}, err // timeout: compensate (children are aborted)
		}
		if !r.OK {
			return OrderOut{}, fmt.Errorf("warehouse %s: %s", wh, r.Reason)
		}
	}

	// Last reversible step of the parent.
	if _, err := f.charge.Run(c, in); err != nil {
		return OrderOut{}, err
	}

	// The point of no return: commit every child.
	for _, wh := range warehouses {
		if err := f.signalRun(c, children[wh], decisionSignal, Decision{Commit: true}); err != nil {
			return OrderOut{}, err
		}
	}

	// Fan in, phase two: collect the tracking numbers.
	out := OrderOut{OrderID: in.OrderID, Tracking: map[string]string{}}
	for _, wh := range warehouses {
		sh, err := saga.Await[Shipped](c, shippedSignal(wh), in.ShippedTimeout)
		if err != nil {
			return OrderOut{}, err
		}
		out.Tracking[wh] = sh.Tracking
	}
	return out, nil
}

// --- Demo ------------------------------------------------------------------

func main() {
	if len(os.Args) < 2 || os.Args[1] != "demo" {
		fmt.Fprintln(os.Stderr, "usage: multiworkflow demo [-out-of-stock WAREHOUSE] [-decline]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := demo(ctx, os.Args[2:])
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func demo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	outOfStock := fs.String("out-of-stock", "", "warehouse with no stock (wh-a, wh-b or wh-c)")
	decline := fs.Bool("decline", false, "decline the payment after every warehouse is ready")
	_ = fs.Parse(args)

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
		return err
	}
	s := &store{pool: pool, requests: idem.MustNew("multi"), decline: *decline}
	if err := s.migrate(ctx); err != nil {
		return err
	}

	var eng *saga.Engine
	f := define(s, &eng)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if eng, err = saga.New(pool, saga.Options{Logger: log}); err != nil {
		return err
	}
	if err := eng.Start(ctx); err != nil { // children run from the queue
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = eng.Stop(sctx)
	}()

	orderID := fmt.Sprintf("o%d", time.Now().UnixNano()%1e9)
	sku := "sku-" + orderID // fresh stock rows per demo
	lines := []Line{
		{Warehouse: "wh-a", SKU: sku, Qty: 2},
		{Warehouse: "wh-b", SKU: sku, Qty: 1},
		{Warehouse: "wh-c", SKU: sku, Qty: 3},
	}
	for _, l := range lines {
		qty := 10
		if l.Warehouse == *outOfStock {
			qty = 0
		}
		if err := s.restock(ctx, l.Warehouse, l.SKU, qty); err != nil {
			return err
		}
	}

	in := OrderIn{
		OrderID: orderID, Amount: 4200, Lines: lines,
		ReadyTimeout: time.Minute, DecisionTimeout: 3 * time.Minute, ShippedTimeout: 2 * time.Minute,
	}
	fmt.Println("order", orderID, "started")
	out, err := f.order.Run(ctx, eng, "order:"+orderID, in)
	if errors.Is(err, saga.ErrRunWaiting) {
		// The parent parked in an Await: wait for it like any caller would.
		h, lerr := f.order.Lookup(ctx, eng, "order:"+orderID)
		if lerr != nil {
			return lerr
		}
		wctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		defer cancel()
		out, err = h.Wait(wctx)
	}
	var re *saga.RunError
	switch {
	case errors.As(err, &re):
		fmt.Printf("order %s: %s\n", re.Status, re.Cause)
	case err != nil:
		return err
	default:
		fmt.Printf("order done, tracking: %v\n", out.Tracking)
	}

	// Children finish compensating asynchronously after an abort: give them
	// a moment, then show every run of this order and the stock.
	return report(ctx, pool, orderID, sku)
}

func report(ctx context.Context, pool *pgxpool.Pool, orderID, sku string) error {
	var parentID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM saga.run WHERE idem_key = $1`, "order:"+orderID).Scan(&parentID); err != nil {
		return err
	}
	// Children's keys are the parent's start_shipment step keys.
	deadline := time.Now().Add(time.Minute)
	for {
		var open int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM saga.run
			WHERE idem_key LIKE $1 || ':%' AND status NOT IN ('done', 'compensated', 'needs_attention')`,
			parentID).Scan(&open); err != nil {
			return err
		}
		if open == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	rows, err := pool.Query(ctx, `
		SELECT workflow, status, coalesce(input->>'warehouse', ''), coalesce(error, '')
		FROM saga.run WHERE id = $1 OR idem_key LIKE $1 || ':%'
		ORDER BY workflow, input->>'warehouse'`, parentID)
	if err != nil {
		return err
	}
	fmt.Println("runs:")
	if err := forEach(rows, func(r pgx.CollectableRow) error {
		var wf, st, wh, cause string
		if err := r.Scan(&wf, &st, &wh, &cause); err != nil {
			return err
		}
		fmt.Printf("  %-14s %-5s %-12s %s\n", wf, wh, st, cause)
		return nil
	}); err != nil {
		return err
	}
	rows, err = pool.Query(ctx, `SELECT warehouse, available FROM multi.stock WHERE sku = $1 ORDER BY warehouse`, sku)
	if err != nil {
		return err
	}
	fmt.Println("stock left (started with 10, or 0 for an out-of-stock warehouse):")
	if err := forEach(rows, func(r pgx.CollectableRow) error {
		var wh string
		var n int
		if err := r.Scan(&wh, &n); err != nil {
			return err
		}
		fmt.Printf("  %s %d\n", wh, n)
		return nil
	}); err != nil {
		return err
	}
	var charged, refunded bool
	_ = pool.QueryRow(ctx, `SELECT true, refunded FROM multi.payment WHERE order_id = $1`, orderID).Scan(&charged, &refunded)
	fmt.Printf("payment: charged=%v refunded=%v\n", charged, refunded)
	return nil
}

func forEach(rows pgx.Rows, fn func(pgx.CollectableRow) error) error {
	_, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (struct{}, error) { return struct{}{}, fn(r) })
	return err
}
