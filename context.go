package saga

import (
	"context"
	"fmt"

	"github.com/imdat99/saga/internal/store"
)

// Key is the idempotency key of one step invocation: "<runID>:<seq>:<step>".
// A step re-executed after a crash gets the same key, so the module it calls
// can recognise the retry and return the first result instead of acting twice.
type Key string

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

func (c *runCtx) setFatal(err error) error {
	if c.fatal == nil {
		c.fatal = err
	}
	return c.fatal
}

func stepKey(runID string, seq int, name string) Key {
	return Key(fmt.Sprintf("%s:%d:%s", runID, seq, name))
}

// RunIDFrom returns the id of the saga run ctx belongs to, if any. Modules use
// it (e.g. the event bus) to tell whether they are called from a saga step.
func RunIDFrom(ctx context.Context) (string, bool) {
	c, ok := ctx.Value(runCtxKey{}).(*runCtx)
	if !ok {
		return "", false
	}
	return c.run.ID, true
}

type uncertainKey struct{}

// CompensationUncertain reports whether a compensator is undoing a step whose
// outcome is unknown: the process died after the step started and before its
// result was recorded. The compensation data is then the zero value, and the
// compensator must look up what (if anything) was done under its Key.
func CompensationUncertain(ctx context.Context) bool {
	v, _ := ctx.Value(uncertainKey{}).(bool)
	return v
}
