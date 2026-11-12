package saga

import (
	"bytes"
	"context"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/imdat99/saga/internal/store"
)

// Key is the idempotency key of one step invocation: "<runID>:<seq>:<step>".
// A step re-executed after a crash gets the same key, so the module it calls
// can recognise the retry and return the first result instead of acting twice.
type Key string

// RunID returns the id of the run the key belongs to.
func (k Key) RunID() string {
	id, _, _ := strings.Cut(string(k), ":")
	return id
}

// Ctx is the context a workflow function receives. Steps must be called with
// it, one after another (not from concurrent goroutines): their position in
// the call sequence is what replay matches against the recorded history.
type Ctx interface {
	context.Context
	RunID() string
	sealed()
}

type runCtxKey struct{}

type runCtx struct {
	context.Context
	eng   *Engine
	run   store.Run
	lease store.Lease
	steps map[int]store.Step // history, by seq
	seq   int
	fatal error // lease lost, DB failure or non-determinism: stop, do not compensate
	// jbuf is scratch for encoding step results (see appendJSON).
	jbuf bytes.Buffer
}

// jsonV1 makes encoding/json/v2 produce exactly what encoding/json.Marshal
// does (the encoding persisted so far).
var jsonV1 = json.DefaultOptionsV1()

// appendJSON encodes v after the content of c.jbuf and returns its bytes,
// valid until c.jbuf is reset. Unlike json.Marshal it does not copy the
// result out of the encoder's buffer: one allocation less per step. Earlier
// slices stay valid when the buffer grows (it moves, the old bytes are kept).
func (c *runCtx) appendJSON(v any) (json.RawMessage, error) {
	start := c.jbuf.Len()
	if err := jsonv2.MarshalWrite(&c.jbuf, v, jsonV1); err != nil {
		return nil, err
	}
	b := c.jbuf.Bytes()
	return b[start:len(b):len(b)], nil
}

func (c *runCtx) RunID() string { return c.run.ID }
func (c *runCtx) sealed()       {}

func (c *runCtx) Value(k any) any {
	if k == (runCtxKey{}) {
		return c
	}
	return c.Context.Value(k)
}

func (c *runCtx) nextSeq() int {
	c.seq++
	return c.seq
}

// errSuspended is the fatal error of a run that parked itself.
var errSuspended = fmt.Errorf("%w (parked)", ErrRunWaiting)

// suspend stops the workflow function after the run was parked.
func (c *runCtx) suspend() error { return c.setFatal(errSuspended) }

func (c *runCtx) nonDeterministic(seq int, recorded, now string) error {
	return c.setFatal(fmt.Errorf("%w: position %d recorded %q, now %q", ErrNonDeterministic, seq, recorded, now))
}

// Error kinds persisted with a failed step, so replay rebuilds an error that
// still matches the engine's sentinels and Permanent.
const (
	kindPermanent    = "permanent"
	kindStepTimeout  = "step_timeout"
	kindRunTimeout   = "run_timeout"
	kindAwaitTimeout = "await_timeout"
)

var kindSentinels = map[string]error{
	kindLockHeld:     ErrLockHeld,
	kindLockLost:     ErrLockLost,
	kindStepTimeout:  ErrStepTimeout,
	kindRunTimeout:   ErrRunTimeout,
	kindAwaitTimeout: ErrAwaitTimeout,
}

// errorKind classifies err for persistence.
func errorKind(err error) string {
	switch {
	case IsPermanent(err):
		return kindPermanent
	case errors.Is(err, ErrRunTimeout):
		return kindRunTimeout
	case errors.Is(err, ErrAwaitTimeout):
		return kindAwaitTimeout
	case errors.Is(err, ErrStepTimeout):
		return kindStepTimeout
	}
	return ""
}

// replayedError is a step failure rebuilt from history: same message, and
// errors.Is still matches the engine sentinel it wrapped. Errors defined by
// the application come back with their message only.
type replayedError struct {
	msg      string
	sentinel error
}

func (e *replayedError) Error() string { return e.msg }
func (e *replayedError) Unwrap() error { return e.sentinel }

// recordedError rebuilds the error of a step that failed in an earlier
// execution.
func recordedError(msg, kind string) error {
	switch kind {
	case kindPermanent:
		return Permanent(errors.New(msg))
	case kindLockHeld:
		return Permanent(&replayedError{msg: msg, sentinel: ErrLockHeld})
	}
	if s, ok := kindSentinels[kind]; ok {
		return &replayedError{msg: msg, sentinel: s}
	}
	return errors.New(msg)
}

// failedOutcome is the row of a step that failed with cause. A step whose
// attempt timed out may have taken effect: it stays uncertain, so it is still
// compensated.
func failedOutcome(status string, cause error) store.Outcome {
	return store.Outcome{
		Status: status, Error: cause.Error(), ErrorKind: errorKind(cause),
		Uncertain: errors.Is(cause, ErrStepTimeout),
	}
}

// reservedPrefix marks the engine's own pseudo-steps (Sleep, Await, Now, Rand).
const reservedPrefix = "saga."

func isReserved(name string) bool { return strings.HasPrefix(name, reservedPrefix) }

// fail records err as fatal. If the run context was cancelled, the
// cancellation cause (ErrLeaseLost, ErrShutdown, the caller's error) wins over
// the error it provoked, such as a failed query.
func (c *runCtx) fail(err error) error {
	return c.setFatal(interruption(c, err))
}

// interruption returns the cancellation cause of ctx in place of err when ctx
// is done, except for ErrLeaseLost which is already the most precise answer.
func interruption(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil && !errors.Is(err, ErrLeaseLost) {
		return context.Cause(ctx)
	}
	return err
}

func (c *runCtx) setFatal(err error) error {
	if c.fatal == nil {
		c.fatal = err
	}
	return c.fatal
}

// stepKey builds the key in a stack buffer: one allocation (the string)
// instead of the three of fmt.Sprintf, on every step.
func stepKey(runID string, seq int, name string) Key {
	var buf [128]byte
	b := append(buf[:0], runID...)
	b = append(b, ':')
	b = strconv.AppendInt(b, int64(seq), 10)
	b = append(b, ':')
	b = append(b, name...)
	return Key(b)
}

// RunIDFrom returns the id of the saga run ctx belongs to, if any. Modules use
// it (e.g. the event bus) to tell whether they are called from a saga step.
func RunIDFrom(ctx context.Context) (string, bool) {
	c, ok := ctx.Value(runCtxKey{}).(*runCtx)
	if !ok || c == nil {
		return "", false
	}
	return c.run.ID, true
}

type uncertainKey struct{}

// CompensationUncertain reports whether a compensator is undoing a step whose
// outcome is not fully known: the process died after the step started and
// before its result was recorded, a retry was pending, or one of its attempts
// timed out (and may have taken effect). The compensation data is the zero
// value when no attempt recorded success; otherwise it is the data of the
// successful attempt. Either way the compensator must look up what (if
// anything) was done under its Key, e.g. with idem.Abort.
//
// A step defined without a compensator records no compensation data. If a
// later deploy adds one, it sees the steps recorded before as uncertain, with
// the zero value as data.
func CompensationUncertain(ctx context.Context) bool {
	v, _ := ctx.Value(uncertainKey{}).(bool)
	return v
}
