// Package testdb starts one Postgres container per test binary and hands each
// test its own freshly created database, so tests can run in parallel.
package testdb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	once    sync.Once
	baseDSN string
	initErr error
	counter atomic.Int64
)

// DSNEnv overrides the container: point it at an existing server (the user
// must be allowed to CREATE DATABASE).
const DSNEnv = "SAGA_TEST_DSN"

func start() {
	if dsn := os.Getenv(DSNEnv); dsn != "" {
		baseDSN = dsn
		return
	}
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("saga"),
		tcpostgres.WithUsername("saga"),
		tcpostgres.WithPassword("saga"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		initErr = fmt.Errorf("start postgres container: %w", err)
		return
	}
	// The container is reaped by testcontainers' Ryuk when the binary exits.
	baseDSN, initErr = c.ConnectionString(ctx, "sslmode=disable")
}

// New returns a pool on a new empty database dropped when the test ends.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	once.Do(start)
	if initErr != nil {
		t.Fatalf("testdb: %v", initErr)
	}
	ctx := context.Background()

	name := fmt.Sprintf("t_%d_%d", os.Getpid(), counter.Add(1))
	admin, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("testdb: connect: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("testdb: create database: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(baseDSN)
	if err != nil {
		t.Fatalf("testdb: parse dsn: %v", err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("testdb: pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		conn, err := pgx.Connect(ctx, baseDSN)
		if err != nil {
			return
		}
		defer conn.Close(ctx)
		_, _ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	return pool
}

var names atomic.Int64

// UniqueName returns a registry-safe name for steps and workflows defined
// inside a test. The saga registry is global and rejects duplicates, and
// -count=N re-runs a test in the same process, hence the counter.
func UniqueName(t testing.TB, suffix string) string {
	base := strings.NewReplacer("/", ".", " ", "_").Replace(t.Name())
	return fmt.Sprintf("%s.%s.%d", base, suffix, names.Add(1))
}
