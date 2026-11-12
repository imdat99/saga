// Package inventory reserves stock. It owns the inventory schema and runs as
// the inventory_svc role; other modules reach it only through saga steps.
package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga/examples/modulith/internal/platform/pgrole"
	"github.com/imdat99/saga/idem"
)

const role = "inventory_svc"

// ErrOutOfStock means the reservation cannot be satisfied; retrying will not
// help until stock is added.
var ErrOutOfStock = errors.New("inventory: out of stock")

var requests = idem.MustNew("inventory")

// Service is the inventory module.
type Service struct{ pool *pgxpool.Pool }

// New returns the module.
func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

// Migrate creates the module's schema, tables and role.
func (s *Service) Migrate(ctx context.Context) error {
	return pgrole.EnsureRole(ctx, s.pool, "inventory", role, `
		CREATE TABLE IF NOT EXISTS inventory.stock (
			sku       text PRIMARY KEY,
			available int  NOT NULL CHECK (available >= 0)
		);
		CREATE TABLE IF NOT EXISTS inventory.reservation (
			key      text PRIMARY KEY,
			sku      text NOT NULL REFERENCES inventory.stock (sku),
			qty      int  NOT NULL,
			released boolean NOT NULL DEFAULT false
		);
		CREATE TABLE IF NOT EXISTS inventory.processed_requests (
			key        text PRIMARY KEY,
			result     jsonb,
			aborted    boolean NOT NULL DEFAULT false,
			created_at timestamptz NOT NULL DEFAULT now()
		);
		CREATE INDEX IF NOT EXISTS processed_requests_created_idx ON inventory.processed_requests (created_at);`)
}

// Restock sets the available quantity of sku (admin and tests).
func (s *Service) Restock(ctx context.Context, sku string, qty int) error {
	return pgrole.InRoleTx(ctx, s.pool, role, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO inventory.stock (sku, available) VALUES ($1, $2)
			ON CONFLICT (sku) DO UPDATE SET available = excluded.available`, sku, qty)
		return err
	})
}

// Available returns the available quantity of sku.
func (s *Service) Available(ctx context.Context, sku string) (int, error) {
	var n int
	err := pgrole.InRoleTx(ctx, s.pool, role, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT available FROM inventory.stock WHERE sku = $1`, sku).Scan(&n)
	})
	return n, err
}

// Reserve takes qty of sku under key, once per key. It returns the
// reservation key.
func (s *Service) Reserve(ctx context.Context, key, sku string, qty int) (string, error) {
	var out string
	err := pgrole.InRoleTx(ctx, s.pool, role, func(tx pgx.Tx) error {
		var err error
		out, err = idem.Do(ctx, tx, requests, key, func(ctx context.Context) (string, error) {
			tag, err := tx.Exec(ctx, `
				UPDATE inventory.stock SET available = available - $2
				WHERE sku = $1 AND available >= $2`, sku, qty)
			if err != nil {
				return "", err
			}
			if tag.RowsAffected() == 0 {
				return "", fmt.Errorf("%w: %s x%d", ErrOutOfStock, sku, qty)
			}
			_, err = tx.Exec(ctx, `INSERT INTO inventory.reservation (key, sku, qty) VALUES ($1, $2, $3)`, key, sku, qty)
			return key, err
		})
		return err
	})
	return out, err
}

// Release gives back the stock of a reservation. It is idempotent, and safe
// for a reservation that never happened: the key is fenced off so a late
// Reserve attempt cannot take stock afterwards.
func (s *Service) Release(ctx context.Context, key string) error {
	return pgrole.InRoleTx(ctx, s.pool, role, func(tx pgx.Tx) error {
		_, processed, err := idem.Abort[string](ctx, tx, requests, key)
		if err != nil || !processed {
			return err
		}
		var sku string
		var qty int
		err = tx.QueryRow(ctx, `
			UPDATE inventory.reservation SET released = true
			WHERE key = $1 AND NOT released
			RETURNING sku, qty`, key).Scan(&sku, &qty)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // already released
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE inventory.stock SET available = available + $2 WHERE sku = $1`, sku, qty)
		return err
	})
}
