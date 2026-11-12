// Package testredis starts one Redis container per test binary. Tests isolate
// themselves with a unique BullMQ prefix instead of separate databases.
package testredis

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// URLEnv overrides the container: point it at an existing Redis.
const URLEnv = "SAGA_TEST_REDIS_URL"

var (
	once    sync.Once
	url     string
	errInit error
)

func start() {
	if u := os.Getenv(URLEnv); u != "" {
		url = u
		return
	}
	ctx := context.Background()
	// As in production: never evict queue keys.
	c, err := tcredis.Run(ctx, "redis:7-alpine",
		testcontainers.WithCmd("redis-server", "--maxmemory-policy", "noeviction"))
	if err != nil {
		errInit = fmt.Errorf("start redis container: %w", err)
		return
	}
	// The container is reaped by testcontainers' Ryuk when the binary exits.
	url, errInit = c.ConnectionString(ctx)
}

// URL returns the Redis URL shared by the tests of this binary.
func URL(tb testing.TB) string {
	tb.Helper()
	once.Do(start)
	if errInit != nil {
		tb.Fatalf("testredis: %v", errInit)
	}
	return url
}
