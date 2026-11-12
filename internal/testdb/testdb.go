// Package testdb starts one Postgres container per test binary and hands each
// test its own freshly created database, so tests can run in parallel.
package testdb

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
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
	errInit error
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
		errInit = fmt.Errorf("start postgres container: %w", err)
		return
	}
	// The container is reaped by testcontainers' Ryuk when the binary exits.
	baseDSN, errInit = c.ConnectionString(ctx, "sslmode=disable")
}

// New returns a pool on a new empty database dropped when the test ends.
func New(tb testing.TB) *pgxpool.Pool {
	tb.Helper()
	once.Do(start)
	if errInit != nil {
		tb.Fatalf("testdb: %v", errInit)
	}
	ctx := context.Background()

	name := fmt.Sprintf("t_%d_%d", os.Getpid(), counter.Add(1))
	admin, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		tb.Fatalf("testdb: connect: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		tb.Fatalf("testdb: create database: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(baseDSN)
	if err != nil {
		tb.Fatalf("testdb: parse dsn: %v", err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		tb.Fatalf("testdb: pool: %v", err)
	}

	tb.Cleanup(func() {
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
func UniqueName(tb testing.TB, suffix string) string {
	tb.Helper()
	base := strings.NewReplacer("/", ".", " ", "_").Replace(tb.Name())
	return fmt.Sprintf("%s.%s.%d", base, suffix, names.Add(1))
}

// DSN returns a connection string for the database of pool, for handing to a
// child process.
func DSN(pool *pgxpool.Pool) string {
	c := pool.Config().ConnConfig
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.User, c.Password),
		Host:     net.JoinHostPort(c.Host, strconv.Itoa(int(c.Port))),
		Path:     "/" + c.Database,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}
