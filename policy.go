package saga

import (
	"errors"
	"time"
)

// RetryPolicy decides how often and how fast a failed step (or compensation)
// is tried again. Retries are durable: the run is parked with its lease
// released and a River job resumes it after the backoff, on any instance.
//
// River promotes scheduled jobs about every 5 seconds, so delays shorter than
// that are rounded up in practice.
type RetryPolicy struct {
	// MaxAttempts counts all attempts, the first one included. 1 disables
	// retries.
	MaxAttempts int
	// InitialBackoff is the delay before the second attempt.
	InitialBackoff time.Duration
	// MaxBackoff caps the delay. Zero means no cap.
	MaxBackoff time.Duration
	// Multiplier grows the delay after each attempt. Values below 1 (and NaN)
	// mean 2.
	Multiplier float64
}

// Exponential returns a policy of attempts tries, starting at initial and
// doubling, capped at 10 minutes.
func Exponential(attempts int, initial time.Duration) RetryPolicy {
	return RetryPolicy{MaxAttempts: attempts, InitialBackoff: initial, MaxBackoff: 10 * time.Minute, Multiplier: 2}
}

// noRetry is the default step policy: a failed step fails the workflow.
var noRetry = RetryPolicy{MaxAttempts: 1}

// defaultCompensatePolicy applies to compensators without their own policy.
var defaultCompensatePolicy = Exponential(10, time.Second)

// maxBackoff caps policies without MaxBackoff, so the float to Duration
// conversion cannot overflow.
const maxBackoff = 24 * time.Hour

// backoff returns the delay after the given failed attempt (1-based).
func (p RetryPolicy) backoff(attempt int) time.Duration {
	m := p.Multiplier
	if !(m >= 1) { // also catches NaN
		m = 2
	}
	limit := float64(p.MaxBackoff)
	if p.MaxBackoff <= 0 {
		limit = float64(maxBackoff)
	}
	d := float64(max(p.InitialBackoff, 0))
	for range attempt - 1 {
		d *= m
		if d >= limit {
			break
		}
	}
	// Converting a float beyond the int64 range is undefined: clamp first.
	return time.Duration(min(d, limit))
}

// exhausted reports whether attempt (1-based, just failed) was the last one.
func (p RetryPolicy) exhausted(attempt int) bool {
	return attempt >= max(p.MaxAttempts, 1)
}

// permanentError marks an error that retrying cannot fix.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as not retryable: the step fails at once, whatever its
// RetryPolicy. Use it for validation errors, declined payments and the like.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// StepOption configures a step.
type StepOption func(*stepConfig)

type stepConfig struct {
	retry      RetryPolicy
	compensate RetryPolicy
	timeout    time.Duration
}

// WithRetry sets the retry policy of the step's invocation.
func WithRetry(p RetryPolicy) StepOption { return func(c *stepConfig) { c.retry = p } }

// WithCompensatePolicy sets the retry policy of the step's compensator
// (default: 10 attempts, exponential from 1s). Once exhausted, the run is
// parked as needs_attention until Engine.RetryCompensation.
func WithCompensatePolicy(p RetryPolicy) StepOption {
	return func(c *stepConfig) { c.compensate = p }
}

// WithTimeout bounds one attempt of the step: its context is cancelled after
// d and the attempt counts as a transient failure.
func WithTimeout(d time.Duration) StepOption { return func(c *stepConfig) { c.timeout = d } }
