// Package registry is the routing table of the event bus, in Redis: which
// consumer groups handle which events, with each group's delivery policy.
// Consumers write it (eventbus/consume), relays read it (eventbus). It
// depends on bullmq and go-redis only, so that consumer-only services do not
// link the producer side (Postgres, saga, River).
//
//	<prefix>:eventbus:routes   hash  field "<group>|<event>" -> "1"
//	<prefix>:eventbus:groups   hash  field "<group>" -> {"attempts":5,"backoff_ms":1000}
//	<prefix>:eventbus:version  counter, bumped on every change
//	<prefix>:eventbus:epoch    random id, set by the first registration
//
// The epoch tells a relay that the registry was lost (Redis restarted
// without persistence, keys deleted) rather than changed: it is created again
// by the next registration. Relays then keep routing to the groups they knew
// for a grace period, while the running consumers register again.
//
// Routes are only ever added by consumers (the union of what the instances of
// a group handle, so a rolling deploy never drops an event a newer instance
// handles); removing one is explicit (Forget, Unregister). Relays reload the
// table only when the version moved. In Redis Cluster, the prefix needs a
// hash tag (e.g. "{bull}"), as BullMQ itself requires: the three keys are
// written in one MULTI.
package registry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/imdat99/bullmq"
	"github.com/redis/go-redis/v9"
)

// DefaultPrefix is the BullMQ key prefix used when none is configured.
const DefaultPrefix = "bull"

// NameRE validates group names and subscriber IDs: they end up in Redis keys
// and job ids, where ':' and '|' have a meaning, and in HSCAN patterns.
var NameRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

func routesKey(prefix string) string  { return prefix + ":eventbus:routes" }
func groupsKey(prefix string) string  { return prefix + ":eventbus:groups" }
func versionKey(prefix string) string { return prefix + ":eventbus:version" }
func epochKey(prefix string) string   { return prefix + ":eventbus:epoch" }

// newEpoch returns a random registry epoch.
func newEpoch() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func routeField(group, event string) string { return group + "|" + event }

// Policy is a group's delivery settings.
type Policy struct {
	Attempts  int   `json:"attempts"`
	BackoffMS int64 `json:"backoff_ms"`
}

// Register adds the group's routes and sets its policy, atomically.
func Register(ctx context.Context, c redis.UniversalClient, prefix, group string, events []string, p Policy) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	fields := make([]any, 0, 2*len(events))
	for _, ev := range events {
		fields = append(fields, routeField(group, ev), "1")
	}
	_, err = c.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.SetNX(ctx, epochKey(prefix), newEpoch(), 0)
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

// Registered reports whether the group and every one of its routes are in
// the table (a wiped or partly lost registry needs Register again).
func Registered(ctx context.Context, c redis.UniversalClient, prefix, group string, events []string) (bool, error) {
	var hasGroup *redis.BoolCmd
	var routes *redis.SliceCmd
	fields := make([]string, len(events))
	for i, ev := range events {
		fields[i] = routeField(group, ev)
	}
	if _, err := c.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		hasGroup = pipe.HExists(ctx, groupsKey(prefix), group)
		routes = pipe.HMGet(ctx, routesKey(prefix), fields...)
		return nil
	}); err != nil {
		return false, fmt.Errorf("eventbus: check group %s: %w", group, err)
	}
	if !hasGroup.Val() {
		return false, nil
	}
	for _, v := range routes.Val() {
		if v == nil {
			return false, nil
		}
	}
	return true, nil
}

// Route is where a relay pushes the jobs of one group: everything needed per
// job is computed once per load.
type Route struct {
	Group    string
	Suffix   string // "." + Group, appended to the event id for the job id
	Attempts int
	Backoff  *bullmq.BackoffStrategy
}

// Table is a loaded routing table.
type Table struct {
	// Version is the registry version (0: nothing registered, or lost).
	Version int64
	// Epoch identifies the registry's life ("" when nothing is registered).
	Epoch   string
	ByEvent map[string][]*Route
	Groups  map[string]*Route
}

// State is what a relay checks on every pass.
type State struct {
	Version int64
	Epoch   string
}

// ReadState returns the registry version and epoch, in one round trip.
func ReadState(ctx context.Context, c redis.UniversalClient, prefix string) (State, error) {
	vals, err := c.MGet(ctx, versionKey(prefix), epochKey(prefix)).Result()
	if err != nil {
		return State{}, fmt.Errorf("eventbus: read registry state: %w", err)
	}
	var st State
	if v, ok := vals[0].(string); ok {
		if st.Version, err = strconv.ParseInt(v, 10, 64); err != nil {
			return State{}, fmt.Errorf("eventbus: registry version %q: %w", v, err)
		}
	}
	st.Epoch, _ = vals[1].(string)
	return st, nil
}

// Union returns the routes of a and b (a's policy wins for a group in both).
func Union(a, b *Table) *Table {
	u := &Table{Version: a.Version, Epoch: a.Epoch, ByEvent: map[string][]*Route{}, Groups: map[string]*Route{}}
	maps.Copy(u.Groups, b.Groups)
	maps.Copy(u.Groups, a.Groups) // a's policy wins
	for _, t := range []*Table{a, b} {
		for ev, routes := range t.ByEvent {
			for _, r := range routes {
				if !containsGroup(u.ByEvent[ev], r.Group) {
					u.ByEvent[ev] = append(u.ByEvent[ev], u.Groups[r.Group])
				}
			}
		}
	}
	return u
}

func containsGroup(rs []*Route, group string) bool {
	for _, r := range rs {
		if r.Group == group {
			return true
		}
	}
	return false
}

// Load reads the whole table and its version (read first: a change in
// between shows as a newer version on the next check).
func Load(ctx context.Context, c redis.UniversalClient, prefix string) (*Table, error) {
	var ver, epoch *redis.StringCmd
	var groups, routes *redis.MapStringStringCmd
	if _, err := c.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		ver = pipe.Get(ctx, versionKey(prefix))
		epoch = pipe.Get(ctx, epochKey(prefix))
		groups = pipe.HGetAll(ctx, groupsKey(prefix))
		routes = pipe.HGetAll(ctx, routesKey(prefix))
		return nil
	}); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("eventbus: read registry: %w", err)
	}
	t := &Table{ByEvent: map[string][]*Route{}, Groups: map[string]*Route{}}
	if v, err := ver.Result(); err == nil {
		if t.Version, err = strconv.ParseInt(v, 10, 64); err != nil {
			return nil, fmt.Errorf("eventbus: registry version %q: %w", v, err)
		}
	}
	t.Epoch = epoch.Val()
	for g, raw := range groups.Val() {
		var p Policy
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, fmt.Errorf("eventbus: group %s: %w", g, err)
		}
		t.Groups[g] = &Route{
			Group: g, Suffix: "." + g, Attempts: max(p.Attempts, 1),
			Backoff: bullmq.ExponentialBackoff(max(time.Duration(p.BackoffMS)*time.Millisecond, time.Millisecond)),
		}
	}
	for field := range routes.Val() {
		g, ev, ok := strings.Cut(field, "|")
		r := t.Groups[g]
		if !ok || r == nil {
			continue // a route without its group (unregistered meanwhile)
		}
		t.ByEvent[ev] = append(t.ByEvent[ev], r)
	}
	return t, nil
}

// Unregister removes a group and its routes.
func Unregister(ctx context.Context, c redis.UniversalClient, prefix, group string) error {
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
}

// Forget removes one route.
func Forget(ctx context.Context, c redis.UniversalClient, prefix, group, event string) error {
	_, err := c.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HDel(ctx, routesKey(prefix), routeField(group, event))
		pipe.Incr(ctx, versionKey(prefix))
		return nil
	})
	if err != nil {
		return fmt.Errorf("eventbus: forget %s for %s: %w", event, group, err)
	}
	return nil
}
