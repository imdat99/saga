// Package payment authorizes payments with a fake provider. The provider is
// idempotent by request key, like real ones (Stripe's Idempotency-Key), and
// can be told to fail transiently to exercise saga retries.
package payment

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga/examples/modulith/internal/platform/pgrole"
)

const role = "payment_svc"

// ErrDeclined means the provider refused the payment for good.
var ErrDeclined = errors.New("payment: declined")

// ErrUnavailable is a transient provider failure.
var ErrUnavailable = errors.New("payment: provider unavailable")

// Service is the payment module.
type Service struct {
	pool *pgxpool.Pool
	// Limit declines authorizations above it.
	Limit int64
	// failNext makes the next n authorizations fail with ErrUnavailable.
	failNext atomic.Int32
}

// New returns the module.
func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool, Limit: 1_000_000} }

// FailNext makes the next n Authorize calls fail transiently (tests, demos).
func (s *Service) FailNext(n int32) { s.failNext.Store(n) }

// takeFailure consumes one forced failure, if any are left.
func (s *Service) takeFailure() bool {
	for {
		n := s.failNext.Load()
		if n <= 0 {
			return false
		}
		if s.failNext.CompareAndSwap(n, n-1) {
			return true
		}
	}
}

// Migrate creates the module's schema, tables and role.
func (s *Service) Migrate(ctx context.Context) error {
	return pgrole.EnsureRole(ctx, s.pool, "payment", role, `
		CREATE TABLE IF NOT EXISTS payment.authorization (
			key    text   PRIMARY KEY,
			amount bigint NOT NULL,
			status text   NOT NULL CHECK (status IN ('authorized', 'voided'))
		);`)
}

// Authorize holds amount on the customer's card, once per key: a retried
// request returns the first authorization.
func (s *Service) Authorize(ctx context.Context, key string, amount int64) (string, error) {
	if s.takeFailure() {
		return "", ErrUnavailable
	}
	if amount > s.Limit {
		return "", fmt.Errorf("%w: amount %d over limit", ErrDeclined, amount)
	}
	err := pgrole.InRoleTx(ctx, s.pool, role, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO payment.authorization (key, amount, status) VALUES ($1, $2, 'authorized')
			ON CONFLICT (key) DO NOTHING`, key, amount)
		return err
	})
	return key, err
}

// Void releases an authorization. Voiding an unknown key records a voided
// authorization, so a late Authorize with that key changes nothing.
func (s *Service) Void(ctx context.Context, key string) error {
	return pgrole.InRoleTx(ctx, s.pool, role, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO payment.authorization (key, amount, status) VALUES ($1, 0, 'voided')
			ON CONFLICT (key) DO UPDATE SET status = 'voided'`, key)
		return err
	})
}

// Status returns the status of an authorization ("" if unknown).
func (s *Service) Status(ctx context.Context, key string) (string, error) {
	var st string
	err := pgrole.InRoleTx(ctx, s.pool, role, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `SELECT status FROM payment.authorization WHERE key = $1`, key).Scan(&st)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	})
	return st, err
}
