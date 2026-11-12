// Command server runs one instance of the example modulith. Start several
// with different PORT and INSTANCE_ID against the same Postgres and Redis.
package main

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/imdat99/saga/examples/modulith/internal/app"
	"github.com/imdat99/saga/examples/modulith/internal/platform/metrics"
)

// routes adds /debug/vars (expvar: saga metrics, memstats) to the API.
func routes(api http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /debug/vars", expvar.Handler())
	mux.Handle("/", api)
	return mux
}

// Shutdown budgets, see run.
const (
	httpBudget = 5 * time.Second
	stopBudget = 15 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.New(ctx, app.Config{
		DatabaseURL: env("DATABASE_URL", "postgres://saga:saga@localhost:5432/saga?sslmode=disable"),
		RedisURL:    env("REDIS_URL", "redis://localhost:6379"),
		InstanceID:  os.Getenv("INSTANCE_ID"),
		Logger:      log,
		Observer:    metrics.New("saga"),
	})
	if err != nil {
		return err
	}
	defer func() {
		// Engine and bus get their own budget after the HTTP server.
		stopCtx, cancel := context.WithTimeout(context.Background(), stopBudget)
		defer cancel()
		if err := a.Stop(stopCtx); err != nil {
			log.Warn("stop", "err", err)
		}
	}()
	if err := a.Start(ctx); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              ":" + env("PORT", "8080"),
		Handler:           routes(a.Handler()),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("listening", "addr", srv.Addr)

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http: %w", err)
		}
	case <-ctx.Done():
	}

	// Handlers waiting for their run are cut off after httpBudget (the runs
	// keep going); runs still going are drained, then handed over, by a.Stop
	// in the deferred call. Total stays under a 30s termination grace period:
	// httpBudget + stopBudget + Options.ShutdownGrace (5s).
	shutdown, cancel := context.WithTimeout(context.Background(), httpBudget)
	defer cancel()
	if err := srv.Shutdown(shutdown); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}
