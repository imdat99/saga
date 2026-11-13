package consume

import (
	"context"
	"fmt"

	"github.com/imdat99/bullmq"

	"github.com/imdat99/saga/eventbus/internal/registry"
)

// RegistryOptions locates the routing registry, for Unregister and Forget.
type RegistryOptions struct {
	Redis  bullmq.RedisConnectionOptions
	Prefix string // default "bull", as for producers and consumers
}

func withRegistry(ctx context.Context, o RegistryOptions, fn func(*bullmq.RedisConnection, string) error) error {
	if o.Prefix == "" {
		o.Prefix = registry.DefaultPrefix
	}
	conn, err := bullmq.NewRedisConnection(ctx, o.Redis)
	if err != nil {
		return fmt.Errorf("eventbus: redis: %w", err)
	}
	defer conn.Close()
	return fn(conn, o.Prefix)
}

// Unregister removes a consumer group: relays stop pushing jobs to it. Use it
// when retiring a group for good (stopping its consumers is not enough: its
// queue keeps filling while it is registered, and a running consumer would
// register it again). Jobs already in its queue stay there; drain or
// obliterate the queue separately.
func Unregister(ctx context.Context, o RegistryOptions, group string) error {
	return withRegistry(ctx, o, func(c *bullmq.RedisConnection, prefix string) error {
		return registry.Unregister(ctx, c.Client(), prefix, group)
	})
}

// Forget removes one event from a group's routes, once no instance of the
// group handles it any more (routes are a union: a consumer starting without
// the handler does not remove it, and a running one that still has it adds
// it back). Until then, jobs for it keep being handed between instances.
func Forget(ctx context.Context, o RegistryOptions, group, event string) error {
	return withRegistry(ctx, o, func(c *bullmq.RedisConnection, prefix string) error {
		return registry.Forget(ctx, c.Client(), prefix, group, event)
	})
}
