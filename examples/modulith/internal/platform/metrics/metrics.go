// Package metrics turns saga engine observations into expvar counters, served
// at /debug/vars. A real deployment would feed Prometheus or OpenTelemetry the
// same way.
package metrics

import (
	"expvar"
	"time"

	"github.com/imdat99/saga"
)

// Observer implements saga.Observer with expvar maps.
type Observer struct {
	runs      *expvar.Map // runs ended, by status
	steps     *expvar.Map // step attempts, by outcome
	stepNanos *expvar.Map // cumulated step attempt time, by step
	stepCount *expvar.Map // step attempts, by step
}

// New registers the counters under prefix (expvar names are process-global:
// call once per prefix).
func New(prefix string) *Observer {
	return &Observer{
		runs:      expvar.NewMap(prefix + ".runs_ended"),
		steps:     expvar.NewMap(prefix + ".step_attempts"),
		stepNanos: expvar.NewMap(prefix + ".step_nanos"),
		stepCount: expvar.NewMap(prefix + ".step_count"),
	}
}

// StepAttempt implements saga.Observer.
func (o *Observer) StepAttempt(a saga.StepAttempt) {
	o.steps.Add(a.Outcome, 1)
	o.stepNanos.Add(a.Step, int64(a.Duration/time.Nanosecond))
	o.stepCount.Add(a.Step, 1)
}

// RunEnded implements saga.Observer.
func (o *Observer) RunEnded(r saga.RunInfo) { o.runs.Add(string(r.Status), 1) }

var _ saga.Observer = (*Observer)(nil)
