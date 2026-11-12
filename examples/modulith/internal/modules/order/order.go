// Package order records orders and announces them with an order.created
// event, emitted in the same transaction as the order row.
package order

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga/eventbus"
	"github.com/imdat99/saga/examples/modulith/internal/platform/pgrole"
	"github.com/imdat99/saga/idem"
)

const role = "order_svc"

// CreatedEvent is emitted when an order is created.
const CreatedEvent = "order.created"

var requests = idem.MustNew("orders")

// Order is an order row.
type Order struct {
	ID         string `json:"id"`
	CartID     string `json:"cart_id"`
	SKU        string `json:"sku"`
	Qty        int    `json:"qty"`
	Amount     int64  `json:"amount"`
	PaymentKey string `json:"payment_key"`
	Status     string `json:"status"`
}

// Created is the payload of CreatedEvent: IDs only, subscribers re-read.
type Created struct {
	OrderID string `json:"order_id"`
	CartID  string `json:"cart_id"`
}

// Emitter is the part of the event bus the module needs.
type Emitter interface {
	EmitTx(ctx context.Context, tx pgx.Tx, events ...eventbus.Event) error
}

// Service is the order module.
type Service struct {
	pool *pgxpool.Pool
	bus  Emitter
}

// New returns the module.
func New(pool *pgxpool.Pool, bus Emitter) *Service { return &Service{pool: pool, bus: bus} }

// Migrate creates the module's schema, tables and role. The role may also
// write events (staging/outbox) since the module emits in its own tx.
func (s *Service) Migrate(ctx context.Context) error {
	if err := pgrole.EnsureRole(ctx, s.pool, "orders", role, `
		CREATE TABLE IF NOT EXISTS orders.order (
			id          text   PRIMARY KEY,
			cart_id     text   NOT NULL,
			sku         text   NOT NULL,
			qty         int    NOT NULL,
			amount      bigint NOT NULL,
			payment_key text   NOT NULL,
			status      text   NOT NULL CHECK (status IN ('placed', 'cancelled'))
		);
		CREATE TABLE IF NOT EXISTS orders.processed_requests (
			key        text PRIMARY KEY,
			result     jsonb,
			aborted    boolean NOT NULL DEFAULT false,
			created_at timestamptz NOT NULL DEFAULT now()
		);
		CREATE INDEX IF NOT EXISTS processed_requests_created_idx ON orders.processed_requests (created_at);
		GRANT USAGE ON SCHEMA eventbus TO `+role+`;
		GRANT INSERT ON eventbus.staging, eventbus.outbox TO `+role+`;`); err != nil {
		return err
	}
	return nil
}

// Create places an order under key (its id), once per key, and emits
// CreatedEvent in the same transaction.
func (s *Service) Create(ctx context.Context, key string, o Order) (Order, error) {
	var out Order
	err := pgrole.InRoleTx(ctx, s.pool, role, func(tx pgx.Tx) error {
		var err error
		out, err = idem.Do(ctx, tx, requests, key, func(ctx context.Context) (Order, error) {
			o.ID, o.Status = key, "placed"
			if _, err := tx.Exec(ctx, `
				INSERT INTO orders.order (id, cart_id, sku, qty, amount, payment_key, status)
				VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				o.ID, o.CartID, o.SKU, o.Qty, o.Amount, o.PaymentKey, o.Status); err != nil {
				return Order{}, err
			}
			ev, err := eventbus.NewEvent(CreatedEvent, Created{OrderID: o.ID, CartID: o.CartID})
			if err != nil {
				return Order{}, err
			}
			return o, s.bus.EmitTx(ctx, tx, ev)
		})
		return err
	})
	return out, err
}

// Cancel marks an order cancelled; a missing order is fine (idempotent).
func (s *Service) Cancel(ctx context.Context, key string) error {
	return pgrole.InRoleTx(ctx, s.pool, role, func(tx pgx.Tx) error {
		if _, _, err := idem.Abort[Order](ctx, tx, requests, key); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE orders.order SET status = 'cancelled' WHERE id = $1`, key)
		return err
	})
}

// Get loads an order.
func (s *Service) Get(ctx context.Context, id string) (Order, error) {
	var o Order
	err := pgrole.InRoleTx(ctx, s.pool, role, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			SELECT id, cart_id, sku, qty, amount, payment_key, status FROM orders.order WHERE id = $1`, id).
			Scan(&o.ID, &o.CartID, &o.SKU, &o.Qty, &o.Amount, &o.PaymentKey, &o.Status)
	})
	return o, err
}
