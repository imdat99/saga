package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/imdat99/bullmq"
)

// ConsumerOptions configures a Consumer.
type ConsumerOptions struct {
	// Redis is the BullMQ connection.
	Redis bullmq.RedisConnectionOptions
	// Prefix is the BullMQ key prefix (default "bull"), the producers' one.
	Prefix string
	// Concurrency is the number of event jobs one instance processes in
	// parallel (default 10).
	Concurrency int
	// Attempts is the number of deliveries of an event to the group before
	// its job fails (default 5). Handlers that succeeded are not run again.
	Attempts int
	// Backoff is the first retry delay, doubled each time (default 1s).
	Backoff time.Duration
	Logger  *slog.Logger
}

func (o *ConsumerOptions) setDefaults() {
	if o.Prefix == "" {
		o.Prefix = defaultPrefix
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
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Consumer is one instance of a consumer group: the handlers of a service.
// Its instances share the group's queue (named after the group), so every
// instance of a group must run the same handlers (one codebase); during a
// rolling deploy, an instance that lacks a handler for an event routed to the
// group fails that job retryably, for an instance that has it.
//
// A consumer needs Redis only: a service that just reacts to events needs no
// access to the producers' database.
type Consumer struct {
	group string
	opts  ConsumerOptions
	log   *slog.Logger

	life       sync.Mutex
	mu         sync.Mutex
	subs       map[string]Subscriber // by ID
	run        *consumerState
	isStarting bool
}

// NewConsumer returns a consumer of group (letters, digits, '.', '_', '-').
func NewConsumer(group string, opts ConsumerOptions) (*Consumer, error) {
	if !nameRE.MatchString(group) {
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
	if !nameRE.MatchString(s.ID) {
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

// consumerState is what Start builds; immutable once the worker runs.
type consumerState struct {
	group    string
	conn     *bullmq.RedisConnection
	worker   *bullmq.Worker
	handlers map[string][]Subscriber // by event name
}

// Start registers the group's routes in the registry (so that every relay
// pushes the group's events from now on) and starts the worker.
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
	var st *consumerState
	defer func() {
		c.mu.Lock()
		c.isStarting = false
		c.run = st
		c.mu.Unlock()
	}()
	if len(subs) == 0 {
		return fmt.Errorf("eventbus: group %s has no subscriber", c.group)
	}

	s := &consumerState{group: c.group, handlers: map[string][]Subscriber{}}
	ids := slices.Sorted(maps.Keys(subs)) // deterministic handler order
	for _, id := range ids {
		sub := subs[id]
		s.handlers[sub.Event] = append(s.handlers[sub.Event], sub)
	}
	conn, err := bullmq.NewRedisConnection(ctx, c.opts.Redis)
	if err != nil {
		return fmt.Errorf("eventbus: redis: %w", err)
	}
	s.conn = conn
	events := slices.Sorted(maps.Keys(s.handlers))
	if err := register(ctx, conn.Client(), c.opts.Prefix, c.group, events,
		policy{Attempts: c.opts.Attempts, BackoffMS: c.opts.Backoff.Milliseconds()}); err != nil {
		_ = conn.Close()
		return err
	}
	w, err := bullmq.NewWorkerWithConnection(ctx, c.group, s.process, conn, &bullmq.WorkerOptions{
		Prefix:      c.opts.Prefix,
		Concurrency: c.opts.Concurrency,
	})
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("eventbus: worker %s: %w", c.group, err)
	}
	s.worker = w
	st = s
	return nil
}

// Stop closes the worker (waiting for jobs in progress until ctx is done)
// and the Redis connection. The group stays registered: its queue keeps
// receiving events until it starts again (see Unregister).
func (c *Consumer) Stop(ctx context.Context) error {
	c.life.Lock()
	defer c.life.Unlock()
	c.mu.Lock()
	st := c.run
	c.mu.Unlock()
	if st == nil {
		return nil
	}
	timeout := time.Second
	if d, ok := ctx.Deadline(); ok {
		timeout = max(time.Until(d), 0)
	}
	err := st.worker.Close(timeout)
	_ = st.conn.Close()
	c.mu.Lock()
	c.run = nil
	c.mu.Unlock()
	return errors.Join(err, ctx.Err())
}

// delivery is a job's data: the event, plus the subscribers that already
// handled it in an earlier attempt (absent on the first one).
type delivery struct {
	Event
	Done []string `json:"done,omitempty"`
}

// process runs the group's handlers of the job's event. Handlers that
// succeed (or give up with Unrecoverable) are recorded in the job before a
// retry, so the retry runs only the ones that failed transiently.
func (s *consumerState) process(ctx context.Context, job *bullmq.Job) (any, error) {
	subs := s.handlers[job.Name]
	if len(subs) == 0 {
		return nil, s.handOff(ctx, job)
	}
	var d delivery
	if err := json.Unmarshal(job.Data, &d); err != nil {
		return nil, bullmq.NewUnrecoverableError(fmt.Sprintf("eventbus: bad event payload: %v", err))
	}
	if len(subs) == 1 && len(d.Done) == 0 {
		// One handler: nothing to record, its error is the job's.
		return nil, subs[0].Handle(ctx, d.Event)
	}
	return nil, s.fanOut(ctx, job, &d, subs)
}

// fanOut runs the pending handlers of d concurrently.
func (s *consumerState) fanOut(ctx context.Context, job *bullmq.Job, d *delivery, subs []Subscriber) error {
	pending := make([]Subscriber, 0, len(subs))
	for _, sub := range subs {
		if !slices.Contains(d.Done, sub.ID) {
			pending = append(pending, sub)
		}
	}
	errs := make([]error, len(pending))
	var wg sync.WaitGroup
	for i, sub := range pending {
		wg.Go(func() { errs[i] = sub.Handle(ctx, d.Event) })
	}
	wg.Wait()

	var transient, given []error
	before := len(d.Done)
	for i, err := range errs {
		switch {
		case err == nil:
			d.Done = append(d.Done, pending[i].ID)
		case bullmq.IsUnrecoverable(err):
			d.Done = append(d.Done, pending[i].ID) // given up: not retried
			given = append(given, fmt.Errorf("%s: %w", pending[i].ID, err))
		default:
			transient = append(transient, fmt.Errorf("%s: %w", pending[i].ID, err))
		}
	}
	if len(transient) > 0 {
		if len(d.Done) > before {
			if err := job.UpdateData(ctx, d); err != nil {
				// Without the record a retry runs the succeeded handlers
				// again: allowed (at least once), just wasteful.
				transient = append(transient, fmt.Errorf("eventbus: record handled subscribers: %w", err))
			}
		}
		err := errors.Join(transient...)
		if len(given) > 0 {
			// Text only: wrapping an unrecoverable error would make BullMQ
			// fail the whole job, and the transient failures would never
			// be retried.
			err = fmt.Errorf("%w (given up: %s)", err, errors.Join(given...).Error())
		}
		return err
	}
	if len(given) > 0 {
		return bullmq.NewUnrecoverableError(errors.Join(given...).Error())
	}
	return nil
}

// Handing off a job this instance has no handler for.
const (
	handOffDelay = 500 * time.Millisecond
	// maxHandOffs bounds the free hand-offs of one job: past it (no
	// instance of the group handles the event any more, Forget was not
	// called), its attempts count and it ends in the failed set.
	maxHandOffs = 100
)

// handOff deals with an event routed to the group (some instance of it
// handles the event) that this instance has no handler for: a rolling deploy.
// The job goes back to the queue after a short delay without spending an
// attempt (a plain error would: an old instance taking every retry would fail
// the job), so that an instance with the handler takes it.
func (s *consumerState) handOff(ctx context.Context, job *bullmq.Job) error {
	err := fmt.Errorf("eventbus: group %s has no handler for %q on this instance", s.group, job.Name)
	if job.AttemptsStarted > maxHandOffs {
		return err
	}
	if mErr := job.MoveToDelayed(ctx, time.Now().Add(handOffDelay)); mErr != nil {
		return errors.Join(err, mErr)
	}
	return bullmq.ErrDelayed
}
