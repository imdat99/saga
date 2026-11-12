package consume

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/imdat99/bullmq"

	"github.com/imdat99/saga/eventbus/internal/registry"
)

// ErrStarted means Start was called twice, or Subscribe after Start.
var ErrStarted = errors.New("eventbus: already started")

// Options configures a Consumer.
type Options struct {
	// Redis is the BullMQ connection.
	Redis bullmq.RedisConnectionOptions
	// Prefix is the BullMQ key prefix (default "bull"), the producers' one.
	// In Redis Cluster it needs a hash tag, e.g. "{bull}".
	Prefix string
	// Concurrency is the number of event jobs one instance processes in
	// parallel (default 10).
	Concurrency int
	// Attempts is the number of deliveries of an event to the group before
	// its job fails (default 5). Handlers that succeeded are not run again.
	Attempts int
	// Backoff is the first retry delay, doubled each time (default 1s).
	Backoff time.Duration
	// HandOffWindow is how long a job for an event this instance has no
	// handler for (rolling deploy) is handed back to the queue for another
	// instance without spending attempts (default 30m). Past it, the job's
	// attempts count and it ends in the failed set.
	HandOffWindow time.Duration
	// RegistryCheck is how often a running consumer checks that its routes
	// are still registered, and registers them again if not (Redis restarted
	// without persistence, keys deleted). Default 30s.
	RegistryCheck time.Duration
	Logger        *slog.Logger
}

func (o *Options) setDefaults() {
	if o.Prefix == "" {
		o.Prefix = registry.DefaultPrefix
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 10
	}
	if o.Attempts <= 0 {
		o.Attempts = 5
	}
	if o.Backoff <= 0 {
		o.Backoff = time.Second
	}
	if o.HandOffWindow <= 0 {
		o.HandOffWindow = 30 * time.Minute
	}
	if o.RegistryCheck <= 0 {
		o.RegistryCheck = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Consumer is one instance of a consumer group: the handlers of a service.
// Its instances share the group's queue (named after the group), so every
// instance of a group must run the same handlers (one codebase); during a
// rolling deploy, an instance that lacks a handler for an event routed to the
// group hands the job back for an instance that has it.
type Consumer struct {
	group string
	opts  Options
	log   *slog.Logger

	life       sync.Mutex
	mu         sync.Mutex
	subs       map[string]Subscriber // by ID
	run        *state
	isStarting bool
}

// New returns a consumer of group (letters, digits, '.', '_', '-').
func New(group string, opts Options) (*Consumer, error) {
	if !registry.NameRE.MatchString(group) {
		return nil, fmt.Errorf("eventbus: invalid group name %q", group)
	}
	opts.setDefaults()
	return &Consumer{
		group: group, opts: opts,
		log:  opts.Logger.With("component", "eventbus.consumer", "group", group),
		subs: map[string]Subscriber{},
	}, nil
}

// Group returns the consumer group name.
func (c *Consumer) Group() string { return c.group }

// Subscribe registers a handler. Call it before Start.
func (c *Consumer) Subscribe(s Subscriber) error {
	if !registry.NameRE.MatchString(s.ID) {
		return fmt.Errorf("eventbus: invalid subscriber id %q", s.ID)
	}
	if s.Event == "" {
		return fmt.Errorf("eventbus: subscriber %s has no event", s.ID)
	}
	if s.Handle == nil {
		return fmt.Errorf("eventbus: subscriber %s has no handler", s.ID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.run != nil || c.isStarting {
		return ErrStarted
	}
	if _, dup := c.subs[s.ID]; dup {
		return fmt.Errorf("eventbus: subscriber %s registered twice", s.ID)
	}
	c.subs[s.ID] = s
	return nil
}

// state is what Start builds; handlers is immutable once the worker runs.
type state struct {
	group    string
	opts     *Options
	log      *slog.Logger
	conn     *bullmq.RedisConnection
	worker   *bullmq.Worker
	handlers map[string][]Subscriber // by event name, in ID order
	events   []string                // sorted keys of handlers
	cancel   context.CancelFunc
	checker  chan struct{} // closed when the registry check loop exits
}

// Start registers the group's routes (so that every relay pushes the
// group's events from now on) and starts the worker and the registry check.
func (c *Consumer) Start(ctx context.Context) error {
	c.life.Lock()
	defer c.life.Unlock()
	c.mu.Lock()
	if c.run != nil || c.isStarting {
		c.mu.Unlock()
		return ErrStarted
	}
	c.isStarting = true // freezes Subscribe
	subs := maps.Clone(c.subs)
	c.mu.Unlock()
	var started *state
	defer func() {
		c.mu.Lock()
		c.isStarting = false
		c.run = started
		c.mu.Unlock()
	}()
	if len(subs) == 0 {
		return fmt.Errorf("eventbus: group %s has no subscriber", c.group)
	}
	s := c.newState(subs)
	conn, err := bullmq.NewRedisConnection(ctx, c.opts.Redis)
	if err != nil {
		return fmt.Errorf("eventbus: redis: %w", err)
	}
	s.conn = conn
	if err := s.register(ctx); err != nil {
		_ = conn.Close()
		return err
	}
	w, err := bullmq.NewWorkerWithConnection(ctx, c.group, s.process, conn, &bullmq.WorkerOptions{
		Prefix:      c.opts.Prefix,
		Concurrency: c.opts.Concurrency,
	})
	if err != nil {
		// The routes stay registered: the group's queue keeps the events
		// until an instance starts (registration is durable by design).
		_ = conn.Close()
		return fmt.Errorf("eventbus: worker %s: %w", c.group, err)
	}
	s.worker = w
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel
	s.checker = make(chan struct{})
	go s.checkLoop(loopCtx)
	started = s
	return nil
}

func (c *Consumer) newState(subs map[string]Subscriber) *state {
	s := &state{group: c.group, opts: &c.opts, log: c.log, handlers: map[string][]Subscriber{}}
	for _, id := range slices.Sorted(maps.Keys(subs)) {
		sub := subs[id]
		s.handlers[sub.Event] = append(s.handlers[sub.Event], sub)
	}
	s.events = slices.Sorted(maps.Keys(s.handlers))
	return s
}

func (s *state) register(ctx context.Context) error {
	return registry.Register(ctx, s.conn.Client(), s.opts.Prefix, s.group, s.events,
		registry.Policy{Attempts: s.opts.Attempts, BackoffMS: s.opts.Backoff.Milliseconds()})
}

// checkLoop registers the group again when its routes went missing from
// Redis, so that relays do not drop its events for the rest of its life.
func (s *state) checkLoop(ctx context.Context) {
	defer close(s.checker)
	t := time.NewTicker(s.opts.RegistryCheck)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		ok, err := registry.Registered(ctx, s.conn.Client(), s.opts.Prefix, s.group, s.events)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Warn("check routes in the registry", "err", err)
			}
			continue
		}
		if ok {
			continue
		}
		s.log.Warn("routes missing from the registry, registering again")
		if err := s.register(ctx); err != nil && ctx.Err() == nil {
			s.log.Warn("register routes again", "err", err)
		}
	}
}

// Stop closes the worker (waiting for jobs in progress until ctx is done)
// and the Redis connection. The group stays registered: its queue keeps
// receiving events until it starts again (see Unregister).
func (c *Consumer) Stop(ctx context.Context) error {
	c.life.Lock()
	defer c.life.Unlock()
	c.mu.Lock()
	s := c.run
	c.mu.Unlock()
	if s == nil {
		return nil
	}
	s.cancel()
	<-s.checker
	timeout := time.Duration(math.MaxInt64) // no deadline: wait for the jobs
	if d, ok := ctx.Deadline(); ok {
		timeout = max(time.Until(d), 0)
	}
	err := s.worker.Close(timeout)
	_ = s.conn.Close()
	c.mu.Lock()
	c.run = nil
	c.mu.Unlock()
	return errors.Join(err, ctx.Err())
}

// delivery is a job's data: the event, plus the subscribers of the group
// already done with it in an earlier attempt (absent on the first one):
// those that succeeded and those that gave up (Unrecoverable).
type delivery struct {
	Event
	Done  []string `json:"done,omitempty"`
	Given []string `json:"given,omitempty"`
}

// process runs the group's handlers of the job's event.
func (s *state) process(ctx context.Context, job *bullmq.Job) (any, error) {
	subs := s.handlers[job.Name]
	if len(subs) == 0 {
		return nil, s.handOff(ctx, job)
	}
	var d delivery
	if err := json.Unmarshal(job.Data, &d); err != nil {
		return nil, bullmq.NewUnrecoverableError(fmt.Sprintf("eventbus: bad event payload: %v", err))
	}
	if len(subs) == 1 && len(d.Done) == 0 {
		// One handler: nothing to record, its error is the job's (BullMQ
		// turns a panic into a failure of the attempt).
		return nil, subs[0].Handle(ctx, d.Event)
	}
	return nil, fanOut(ctx, job, &d, subs)
}

// fanOut runs the pending handlers of d concurrently (the last one in the
// calling goroutine) and settles the job: retry the transient failures only
// (recording the others in the job first), fail the job if a handler gave
// up, complete it otherwise.
func fanOut(ctx context.Context, job *bullmq.Job, d *delivery, subs []Subscriber) error {
	pending := make([]Subscriber, 0, len(subs))
	for _, sub := range subs {
		if !slices.Contains(d.Done, sub.ID) {
			pending = append(pending, sub)
		}
	}
	errs := make([]error, len(pending))
	var wg sync.WaitGroup
	for i := range len(pending) - 1 {
		wg.Go(func() { errs[i] = call(ctx, pending[i], d.Event) })
	}
	if n := len(pending); n > 0 {
		errs[n-1] = call(ctx, pending[n-1], d.Event)
	}
	wg.Wait()

	var transient []error
	before := len(d.Done)
	for i, err := range errs {
		switch {
		case err == nil:
			d.Done = append(d.Done, pending[i].ID)
		case bullmq.IsUnrecoverable(err):
			d.Done = append(d.Done, pending[i].ID) // not retried
			d.Given = append(d.Given, pending[i].ID+": "+err.Error())
		default:
			transient = append(transient, fmt.Errorf("%s: %w", pending[i].ID, err))
		}
	}
	if len(transient) > 0 {
		err := errors.Join(transient...)
		if len(d.Done) > before {
			if uErr := job.UpdateData(ctx, d); uErr != nil {
				// Without the record a retry runs the finished handlers
				// again: allowed (at least once), just wasteful.
				err = errors.Join(err, fmt.Errorf("eventbus: record finished subscribers: %w", uErr))
			}
		}
		if len(d.Given) > 0 {
			// Text only: wrapping an unrecoverable error would make BullMQ
			// fail the whole job, and the transient failures would never be
			// retried.
			err = fmt.Errorf("%w (given up: %v)", err, d.Given)
		}
		return err
	}
	if len(d.Given) > 0 {
		// Handlers that gave up, in this attempt or an earlier one: the job
		// ends in the failed set, the dead letter queue.
		return bullmq.NewUnrecoverableError(fmt.Sprintf("eventbus: given up: %v", d.Given))
	}
	return nil
}

// call runs one handler, turning a panic into an error: handlers other than
// the processor's own goroutine are not covered by BullMQ's recovery.
func call(ctx context.Context, sub Subscriber, ev Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("eventbus: handler %s panicked: %v", sub.ID, r)
		}
	}()
	return sub.Handle(ctx, ev)
}

// Hand-off delays grow with the job's age: a tenth of it, between 500ms and
// 30s. A young job (rollout in progress, instances with the handler already
// running) comes back quickly; an old one (a handler removed without Forget,
// nobody will take it) cycles through Redis at most every 30s instead of
// every half-second for the whole HandOffWindow. (Growing with the hand-off
// count instead made jobs wait up to 30s during a rollout, after a few
// unlucky picks by old instances.)
const (
	handOffMin = 500 * time.Millisecond
	handOffMax = 30 * time.Second
	// handOffWarnAge is the job age past which a hand-off is logged: beyond
	// a rollout, it means a route without handler.
	handOffWarnAge = 5 * time.Minute
)

// handOffDelay is the delay before a job of the given age is available again.
func handOffDelay(age time.Duration) time.Duration {
	return min(max(age/10, handOffMin), handOffMax)
}

// handOff deals with an event routed to the group (some instance of it
// handles the event) that this instance has no handler for: a rolling deploy.
// Within HandOffWindow of the job's creation, the job goes back to the queue
// after a short delay without spending an attempt (a plain error would: an
// old instance taking every retry would fail the job), so that an instance
// with the handler takes it.
func (s *state) handOff(ctx context.Context, job *bullmq.Job) error {
	err := fmt.Errorf("eventbus: group %s has no handler for %q on this instance", s.group, job.Name)
	age := time.Since(time.UnixMilli(job.Timestamp))
	if age > s.opts.HandOffWindow {
		return err
	}
	if age > handOffWarnAge {
		s.log.Warn("event handed off for long: no instance of the group handles it (Forget missing?)",
			"event", job.Name, "job", job.ID, "age", age)
	}
	if mErr := job.MoveToDelayed(ctx, time.Now().Add(handOffDelay(age))); mErr != nil {
		return errors.Join(err, mErr)
	}
	return bullmq.ErrDelayed
}
