// Package testdb hands each test its own freshly created database on a shared
// Postgres server, so tests can run in parallel (also across packages: names
// include the process id).
//
// The server is the docker compose one by default (make up); SAGA_TEST_DSN
// points elsewhere. Testcontainers is deliberately not used: Go modules have
// no test-only dependencies, so it would sit in the go.mod every user of the
// library downloads and resolves.
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
)

var (
	once    sync.Once
	baseDSN string
	errInit error
	counter atomic.Int64
)

// DSNEnv points the tests at a Postgres server; the user must be allowed to
// CREATE DATABASE. Unset, DefaultDSN (docker compose) is used.
const DSNEnv = "SAGA_TEST_DSN"

// DefaultDSN is the Postgres of the repository's docker-compose.yml.
const DefaultDSN = "postgres://saga:saga@localhost:5432/saga?sslmode=disable" //nolint:gosec // local docker compose credentials, test only

func start() {
	baseDSN = os.Getenv(DSNEnv)
	if baseDSN == "" {
		baseDSN = DefaultDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		errInit = fmt.Errorf("connect to %s (start it with `make up`, or set %s): %w", redact(baseDSN), DSNEnv, err)
		return
	}
	_ = conn.Close(ctx)
}

// redact hides the password of dsn for error messages.
func redact(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "the test database"
	}
	return u.Redacted()
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
