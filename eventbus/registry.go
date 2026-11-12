package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/imdat99/bullmq"
	"github.com/redis/go-redis/v9"
)

// The routing registry lives in Redis, next to the queues, so that every
// relay pushes every event to every interested consumer group, whichever
// process or service the group runs in.
//
//	<prefix>:eventbus:routes   hash  field "<group>|<event>" -> "1"
//	<prefix>:eventbus:groups   hash  field "<group>" -> {"attempts":5,"backoff_ms":1000}
//	<prefix>:eventbus:version  counter, bumped on every change
//
// Routes are only ever added by consumers (the union of what the instances of
// a group handle, so a rolling deploy never drops an event a newer instance
// handles); removing one is explicit (Forget, Unregister). Relays reload the
// registry only when the version moved: one GET per relay pass otherwise.
//
// Registration is durable (no TTL): while a consumer group is down its queue
// keeps receiving jobs, processed when it comes back. An event is delivered to
// the groups registered when it is relayed.

func routesKey(prefix string) string  { return prefix + ":eventbus:routes" }
func groupsKey(prefix string) string  { return prefix + ":eventbus:groups" }
func versionKey(prefix string) string { return prefix + ":eventbus:version" }

// routeField joins a group and an event name; group names cannot contain '|'.
func routeField(group, event string) string { return group + "|" + event }

// policy is a group's delivery settings, as stored in the registry.
type policy struct {
	Attempts  int   `json:"attempts"`
	BackoffMS int64 `json:"backoff_ms"`
}

// register adds the group's routes and sets its policy, atomically.
func register(ctx context.Context, c redis.UniversalClient, prefix, group string, events []string, p policy) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	fields := make([]any, 0, 2*len(events))
	for _, ev := range events {
		fields = append(fields, routeField(group, ev), "1")
	}
	_, err = c.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, groupsKey(prefix), group, raw)
		if len(fields) > 0 {
			pipe.HSet(ctx, routesKey(prefix), fields...)
		}
		pipe.Incr(ctx, versionKey(prefix))
		return nil
	})
	if err != nil {
		return fmt.Errorf("eventbus: register group %s: %w", group, err)
	}
	return nil
}

// route is where a relay pushes the jobs of one group: everything needed per
// job is computed once per registry load.
type route struct {
	group    string
	suffix   string // "." + group, appended to the event id for the job id
	attempts int
	backoff  *bullmq.BackoffStrategy
}

// registry is a loaded routing table: event name -> interested groups.
type registry struct {
	version int64
	byEvent map[string][]*route
}

// readVersion returns the registry version (0 if never written).
func readVersion(ctx context.Context, c redis.UniversalClient, prefix string) (int64, error) {
	v, err := c.Get(ctx, versionKey(prefix)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("eventbus: read registry version: %w", err)
	}
	return v, nil
}

// loadRegistry reads the whole routing table and its version.
func loadRegistry(ctx context.Context, c redis.UniversalClient, prefix string) (*registry, error) {
	var ver *redis.StringCmd
	var groups, routes *redis.MapStringStringCmd
	if _, err := c.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		ver = pipe.Get(ctx, versionKey(prefix))
		groups = pipe.HGetAll(ctx, groupsKey(prefix))
		routes = pipe.HGetAll(ctx, routesKey(prefix))
		return nil
	}); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("eventbus: read registry: %w", err)
	}
	reg := &registry{byEvent: map[string][]*route{}}
	if v, err := ver.Result(); err == nil {
		if reg.version, err = strconv.ParseInt(v, 10, 64); err != nil {
			return nil, fmt.Errorf("eventbus: registry version %q: %w", v, err)
		}
	}
	byGroup := map[string]*route{}
	for g, raw := range groups.Val() {
		var p policy
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, fmt.Errorf("eventbus: group %s: %w", g, err)
		}
		byGroup[g] = &route{
			group: g, suffix: "." + g, attempts: max(p.Attempts, 1),
			backoff: bullmq.ExponentialBackoff(max(time.Duration(p.BackoffMS)*time.Millisecond, time.Millisecond)),
		}
	}
	for field := range routes.Val() {
		g, ev, ok := strings.Cut(field, "|")
		r := byGroup[g]
		if !ok || r == nil {
			continue // a route without its group (unregistered meanwhile)
		}
		reg.byEvent[ev] = append(reg.byEvent[ev], r)
	}
	return reg, nil
}

// RegistryOptions locates a routing registry, for the operations below.
type RegistryOptions struct {
	Redis  bullmq.RedisConnectionOptions
	Prefix string // default "bull", as for producers and consumers
}

func withRegistry(ctx context.Context, o RegistryOptions, fn func(redis.UniversalClient, string) error) error {
	if o.Prefix == "" {
		o.Prefix = defaultPrefix
	}
	conn, err := bullmq.NewRedisConnection(ctx, o.Redis)
	if err != nil {
		return fmt.Errorf("eventbus: redis: %w", err)
	}
	defer conn.Close()
	return fn(conn.Client(), o.Prefix)
}

// Unregister removes a consumer group: relays stop pushing jobs to it. Use it
// when retiring a group for good (stopping its consumers is not enough: its
// queue keeps filling while it is registered). Jobs already in its queue stay
// there; drain or obliterate the queue separately.
func Unregister(ctx context.Context, o RegistryOptions, group string) error {
	return withRegistry(ctx, o, func(c redis.UniversalClient, prefix string) error {
		var fields []string
		iter := c.HScan(ctx, routesKey(prefix), 0, group+"|*", 1000).Iterator()
		for iter.Next(ctx) {
			if f := iter.Val(); strings.HasPrefix(f, group+"|") {
				fields = append(fields, f)
			}
		}
		if err := iter.Err(); err != nil {
			return fmt.Errorf("eventbus: unregister %s: %w", group, err)
		}
		_, err := c.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HDel(ctx, groupsKey(prefix), group)
			if len(fields) > 0 {
				pipe.HDel(ctx, routesKey(prefix), fields...)
			}
			pipe.Incr(ctx, versionKey(prefix))
			return nil
		})
		if err != nil {
			return fmt.Errorf("eventbus: unregister %s: %w", group, err)
		}
		return nil
	})
}

// Forget removes one event from a group's routes, after every instance of
// the group stopped handling it (routes are a union, so a consumer starting
// without the handler does not remove it). Until then, jobs for it keep
// failing on instances without a handler.
func Forget(ctx context.Context, o RegistryOptions, group, event string) error {
	return withRegistry(ctx, o, func(c redis.UniversalClient, prefix string) error {
		_, err := c.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HDel(ctx, routesKey(prefix), routeField(group, event))
			pipe.Incr(ctx, versionKey(prefix))
			return nil
		})
		if err != nil {
			return fmt.Errorf("eventbus: forget %s for %s: %w", event, group, err)
		}
		return nil
	})
}
