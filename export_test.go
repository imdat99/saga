package saga

import "context"

// RecoverExpired runs one recovery scan (white-box tests).
func (e *Engine) RecoverExpired(ctx context.Context) (int, error) { return e.recoverExpired(ctx) }

// Resume runs the resume_run job body for runID (white-box tests).
func (e *Engine) Resume(ctx context.Context, runID string) error { return e.resume(ctx, runID) }
