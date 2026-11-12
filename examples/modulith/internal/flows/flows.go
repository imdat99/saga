// Package flows wires the modules into sagas. It is the only package that
// knows several modules; modules never import each other.
package flows

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/examples/modulith/internal/modules/inventory"
	"github.com/imdat99/saga/examples/modulith/internal/modules/order"
	"github.com/imdat99/saga/examples/modulith/internal/modules/payment"
)

// CompleteOrderInput is what the client sends.
type CompleteOrderInput struct {
	CartID string `json:"cart_id"`
	SKU    string `json:"sku"`
	Qty    int    `json:"qty"`
	Amount int64  `json:"amount"`
}

// CompleteOrderResult is the outcome of a completed order.
type CompleteOrderResult struct {
	OrderID    string `json:"order_id"`
	PaymentKey string `json:"payment_key"`
}

// Flows holds the registered workflows.
type Flows struct {
	CompleteOrder *saga.Workflow[CompleteOrderInput, CompleteOrderResult]
}

// CartLockTTL bounds how long a parked or dead checkout keeps a cart locked.
const CartLockTTL = 5 * time.Minute

// Modules are the module services the steps call.
type Modules struct {
	Inventory *inventory.Service
	Payment   *payment.Service
	Orders    *order.Service
}

var (
	once    sync.Once
	defined *Flows
	bound   atomic.Pointer[Modules]
)

// Define registers the steps and workflows on first call (step and workflow
// names are process-global) and binds m as the modules they call; a later
// call only rebinds (tests running several apps in one process).
func Define(m Modules) *Flows {
	bound.Store(&m)
	once.Do(func() { defined = define() })
	return defined
}

func define() *Flows {
	inv := func() *inventory.Service { return bound.Load().Inventory }
	pay := func() *payment.Service { return bound.Load().Payment }
	ord := func() *order.Service { return bound.Load().Orders }

	reserve := saga.DefineStep("inventory.reserve",
		func(ctx context.Context, k saga.Key, in CompleteOrderInput) (string, string, error) {
			id, err := inv().Reserve(ctx, string(k), in.SKU, in.Qty)
			if errors.Is(err, inventory.ErrOutOfStock) {
				err = saga.Permanent(err)
			}
			return id, id, err
		},
		// The reservation is keyed by the step key, so the compensator works
		// from the key alone, uncertain or not.
		func(ctx context.Context, k saga.Key, _ string) error { return inv().Release(ctx, string(k)) })

	authorize := saga.DefineStep("payment.authorize",
		func(ctx context.Context, k saga.Key, amount int64) (string, string, error) {
			ref, err := pay().Authorize(ctx, string(k), amount)
			if errors.Is(err, payment.ErrDeclined) {
				err = saga.Permanent(err)
			}
			return ref, ref, err
		},
		func(ctx context.Context, k saga.Key, _ string) error { return pay().Void(ctx, string(k)) },
		saga.WithRetry(saga.Exponential(5, time.Second)),
		saga.WithTimeout(10*time.Second))

	create := saga.DefineStep("order.create",
		func(ctx context.Context, k saga.Key, o order.Order) (string, string, error) {
			created, err := ord().Create(ctx, string(k), o)
			return created.ID, created.ID, err
		},
		func(ctx context.Context, k saga.Key, _ string) error { return ord().Cancel(ctx, string(k)) })

	complete := saga.DefineWorkflow("order.complete", 1,
		func(c saga.Ctx, in CompleteOrderInput) (CompleteOrderResult, error) {
			if err := saga.Lock(c, "cart:"+in.CartID, CartLockTTL); err != nil {
				return CompleteOrderResult{}, err
			}
			if _, err := reserve.Run(c, in); err != nil {
				return CompleteOrderResult{}, err
			}
			payRef, err := authorize.Run(c, in.Amount)
			if err != nil {
				return CompleteOrderResult{}, err
			}
			orderID, err := create.Run(c, order.Order{
				CartID: in.CartID, SKU: in.SKU, Qty: in.Qty, Amount: in.Amount, PaymentKey: payRef,
			})
			if err != nil {
				return CompleteOrderResult{}, err
			}
			return CompleteOrderResult{OrderID: orderID, PaymentKey: payRef}, nil
		},
		saga.WithRunTimeout(time.Hour))

	return &Flows{CompleteOrder: complete}
}
