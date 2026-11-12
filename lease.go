package saga

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/imdat99/saga/internal/store"
)

// activeRun is a run this instance executes and keeps leased.
type activeRun struct {
	version int64
	cancel  context.CancelCauseFunc
	ctx     context.Context //nolint:containedctx // only read for Err(), to skip interrupted runs
	// renewed is the local (monotonic) time just before the last successful
	// renewal or acquisition was sent. The lease is surely ours until
	// renewed+ttl; past renewed+ttl-interval we stop trusting it.
	renewed time.Time
	// intr stops the checkpoint statement in flight when ctx is cancelled
	// (see store.Interrupt): every cancellation of ctx goes through
	// interrupt, or through the caller's context whose AfterFunc fires it.
	intr       store.Interrupt
	stopParent func() bool // the AfterFunc on the caller's context, if any
}

// interrupt cancels the execution with cause and stops its statement in
// flight. The context is cancelled first, so the interrupted statement
// reports the cancellation.
func (r *activeRun) interrupt(cause error) {
	r.cancel(cause)
	r.intr.Fire()
}

// heartbeat renews the leases of all runs executing in this instance with one
// batched UPDATE per tick, instead of one goroutine and one query per run.
// The goroutine runs only while there are active runs. It uses its own small
// pool so that a pool saturated by step checkpoints cannot starve renewals.
type heartbeat struct {
	st       *store.Store
	owner    string
	ttl      time.Duration
	interval time.Duration
	log      *slog.Logger

	mu         sync.Mutex
	runs       map[string]*activeRun
	isRunning  bool
	isStopping bool
	quit       chan struct{}
	loopDone   chan struct{}
}

func newHeartbeat(st *store.Store, owner string, ttl time.Duration, log *slog.Logger) *heartbeat {
	return &heartbeat{
		st: st, owner: owner, ttl: ttl, interval: ttl / 3, log: log,
		runs: map[string]*activeRun{},
		quit: make(chan struct{}),
	}
}

// track registers a leased run and returns a context cancelled (with cause
// ErrLeaseLost) when its lease cannot be renewed, and the entry to pass to
// untrack when the execution ends. acquired is the local time taken just
// before the lease was acquired. Once the heartbeat is stopping, the context
// is returned already cancelled with ErrShutdown.
//
// The entry's Interrupt covers the returned context; the caller may cover
// contexts derived from it that are cancelled only with it (Interrupt.Cover).
func (h *heartbeat) track(
	ctx context.Context,
	runID string,
	version int64,
	acquired time.Time,
) (context.Context, *activeRun) {
	rctx, cancel := context.WithCancelCause(ctx)
	r := &activeRun{version: version, cancel: cancel, ctx: rctx, renewed: acquired}
	h.mu.Lock()
	if h.isStopping {
		h.mu.Unlock()
		cancel(ErrShutdown)
		return rctx, r // not registered, not covered: statements fail on ctx
	}
	r.intr.Cover(rctx)
	h.runs[runID] = r
	if !h.isRunning {
		h.isRunning = true
		h.loopDone = make(chan struct{})
		go h.loop(h.quit, h.loopDone)
	}
	h.mu.Unlock()
	if ctx.Done() != nil {
		// The caller's cancellation also stops the statement in flight.
		// rctx is cancelled here with the caller's cause before Fire: the
		// callback may run before propagation reaches rctx (always for a
		// non-stdlib context), and the interrupted statement must report
		// that cause. Registered outside h.mu: a caller's context type may
		// run its own AfterFunc code. Only for cancellable callers (three
		// allocations).
		r.stopParent = context.AfterFunc(ctx, func() { r.interrupt(context.Cause(ctx)) })
	}
	return rctx, r
}

// untrack ends the execution tracked as r.
func (h *heartbeat) untrack(runID string, r *activeRun) {
	if r.stopParent != nil {
		r.stopParent()
	}
	h.mu.Lock()
	if cur, ok := h.runs[runID]; ok && cur == r {
		delete(h.runs, runID)
	}
	h.mu.Unlock()
	r.cancel(nil)
}

// interrupter returns the Interrupt of a registered execution, nil otherwise.
func (r *activeRun) interrupter() *store.Interrupt {
	if r.intr.Covers() {
		return &r.intr
	}
	return nil
}

// cancelAll interrupts every active run with ErrShutdown, and makes runs
// tracked from now on start cancelled.
func (h *heartbeat) cancelAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.isStopping = true
	for _, r := range h.runs {
		r.interrupt(ErrShutdown)
	}
}

// stop ends the renewal loop and waits for it. Runs still tracked are left to
// lapse: their leases expire and recovery takes over.
func (h *heartbeat) stop() {
	h.mu.Lock()
	if h.isStopping && h.quit == nil {
		h.mu.Unlock()
		return
	}
	h.isStopping = true
	close(h.quit)
	h.quit = nil
	done := h.loopDone
	running := h.isRunning
	h.mu.Unlock()
	if running {
		<-done
	}
}

// loop gets quit from track, read under the lock: stop() may nil the field
// before the goroutine is scheduled.
func (h *heartbeat) loop(quit <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-quit:
			h.mu.Lock()
			h.isRunning = false
			h.mu.Unlock()
			return
		case <-t.C:
		}
		if !h.tick() {
			return
		}
	}
}

// tick renews all live leases once. It returns false (and marks the loop
// stopped) when there is nothing left to renew.
func (h *heartbeat) tick() bool {
	h.mu.Lock()
	if len(h.runs) == 0 {
		h.isRunning = false
		// Go maps never shrink: drop the buckets grown during a burst of
		// concurrent runs instead of keeping them for the engine's lifetime.
		h.runs = map[string]*activeRun{}
		h.mu.Unlock()
		return false
	}
	ids := make([]string, 0, len(h.runs))
	versions := make([]int64, 0, len(h.runs))
	for id, r := range h.runs {
		// An interrupted run is about to return; let its lease lapse so that
		// recovery can take over even if its step ignores cancellation.
		if r.ctx.Err() != nil {
			continue
		}
		ids = append(ids, id)
		versions = append(versions, r.version)
	}
	h.mu.Unlock()
	if len(ids) == 0 {
		return true
	}

	sent := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), h.interval)
	renewed, err := h.st.RenewLeases(ctx, h.owner, ids, versions, h.ttl)
	cancel()

	h.mu.Lock()
	defer h.mu.Unlock()
	if err != nil {
		h.log.Warn("lease renewal failed", "runs", len(ids), "err", err)
	}
	for i, id := range ids {
		r, ok := h.runs[id]
		if !ok || r.version != versions[i] {
			continue
		}
		switch {
		case err == nil && renewed[id]:
			r.renewed = sent
		case err == nil:
			// The row moved on: taken over, released, finished or parked.
			r.interrupt(ErrLeaseLost)
		case time.Since(r.renewed) >= h.ttl-h.interval:
			// The database has been unreachable for most of a TTL: another
			// instance may own the run before the next tick.
			r.interrupt(ErrLeaseLost)
		}
	}
	return true
}
