// Package app is the composition root of the example modulith: it wires the
// pool, the saga engine, the event bus, the modules, the flows and the API
// by hand (constructor injection; the graph is small).
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/imdat99/bullmq"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/eventbus"
	"github.com/imdat99/saga/examples/modulith/internal/api"
	"github.com/imdat99/saga/examples/modulith/internal/flows"
	"github.com/imdat99/saga/examples/modulith/internal/modules/inventory"
	"github.com/imdat99/saga/examples/modulith/internal/modules/order"
	"github.com/imdat99/saga/examples/modulith/internal/modules/payment"
)

// Config is read from the environment by cmd/server.
type Config struct {
	DatabaseURL string
	RedisURL    string
	// RedisPrefix isolates BullMQ keys (default "bull").
	RedisPrefix string
	InstanceID  string
	LeaseTTL    time.Duration
	Logger      *slog.Logger
	// OnOrderCreated, if set, also receives order.created deliveries.
	OnOrderCreated eventbus.Handler
	// Observer receives engine metrics (cmd/server uses expvar).
	Observer saga.Observer
}

// App is one running instance.
type App struct {
	Pool      *pgxpool.Pool
	Engine    *saga.Engine
	Events    *eventbus.Producer // emits (modules) and relays to the queues
	Consumer  *eventbus.Consumer // this service's handlers (group "modulith")
	Inventory *inventory.Service
	Payment   *payment.Service
	Orders    *order.Service
	Flows     *flows.Flows
	API       *api.Server
	log       *slog.Logger
}

// New connects, migrates and wires an instance. Steps are process-global: the
// flows call the modules of the most recent App, which is fine for instances
// sharing a database (tests); a server runs one App per process.
func New(ctx context.Context, cfg Config) (*App, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	a := &App{Pool: pool, log: cfg.Logger}
	if err := a.wire(ctx, cfg); err != nil {
		pool.Close()
		return nil, err
	}
	return a, nil
}

func (a *App) wire(ctx context.Context, cfg Config) error {
	if err := saga.Migrate(ctx, a.Pool); err != nil {
		return err
	}
	if err := eventbus.Migrate(ctx, a.Pool); err != nil {
		return err
	}
	redis := bullmq.RedisConnectionOptions{URL: cfg.RedisURL}
	a.Events = eventbus.NewProducer(a.Pool, eventbus.ProducerOptions{
		Redis: redis, Prefix: cfg.RedisPrefix, Logger: cfg.Logger,
	})
	// One consumer group for the whole modulith (one codebase); another
	// service would create its own group with NewConsumer and Redis only.
	consumer, err := eventbus.NewConsumer("modulith", eventbus.ConsumerOptions{
		Redis: redis, Prefix: cfg.RedisPrefix, Logger: cfg.Logger,
	})
	if err != nil {
		return err
	}
	a.Consumer = consumer
	a.Inventory = inventory.New(a.Pool)
	a.Payment = payment.New(a.Pool)
	a.Orders = order.New(a.Pool, a.Events)
	for _, m := range []interface{ Migrate(context.Context) error }{a.Inventory, a.Payment, a.Orders} {
		if err := m.Migrate(ctx); err != nil {
			return err
		}
	}

	if err := a.Consumer.Subscribe(eventbus.Subscriber{
		ID: "mail.order-confirmation", Event: order.CreatedEvent,
		Handle: func(ctx context.Context, ev eventbus.Event) error {
			var c order.Created
			if err := ev.Decode(&c); err != nil {
				return eventbus.Unrecoverable(err)
			}
			a.log.Info("order confirmation sent", "order", c.OrderID, "event", ev.ID)
			if cfg.OnOrderCreated != nil {
				return cfg.OnOrderCreated(ctx, ev)
			}
			return nil
		},
	}); err != nil {
		return err
	}
	if err := a.Consumer.Subscribe(eventbus.Subscriber{
		ID: "ops.needs-attention", Event: eventbus.NeedsAttentionEvent,
		Handle: func(ctx context.Context, ev eventbus.Event) error {
			a.log.Error("saga needs attention", "event", string(ev.Payload))
			return nil
		},
	}); err != nil {
		return err
	}

	eng, err := saga.New(a.Pool, saga.Options{
		InstanceID: cfg.InstanceID,
		LeaseTTL:   cfg.LeaseTTL,
		Hooks:      []saga.RunHook{a.Events},
		Observer:   cfg.Observer,
		Retention:  30 * 24 * time.Hour,
		Logger:     cfg.Logger,
	})
	if err != nil {
		return err
	}
	a.Engine = eng
	a.Flows = flows.Define(flows.Modules{Inventory: a.Inventory, Payment: a.Payment, Orders: a.Orders})
	a.API = api.New(eng, a.Flows, a.Inventory, cfg.Logger)
	return nil
}

// Start starts the saga engine, the event relay and the event handlers.
func (a *App) Start(ctx context.Context) error {
	if err := a.Engine.Start(ctx); err != nil {
		return err
	}
	if err := a.Consumer.Start(ctx); err != nil {
		return err
	}
	return a.Events.Start(ctx)
}

// Stop drains the engine (runs left over are released for recovery), stops
// the bus and closes the pool.
func (a *App) Stop(ctx context.Context) error {
	err := errors.Join(a.Engine.Stop(ctx), a.Events.Stop(ctx), a.Consumer.Stop(ctx))
	a.Pool.Close()
	return err
}

// Handler returns the HTTP routes.
func (a *App) Handler() http.Handler { return a.API.Handler() }
