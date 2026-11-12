package consume

import (
	"context"

	"github.com/imdat99/bullmq"
)

// BenchProcess returns the job processor of c without a worker (handlers
// from Subscribe), for benchmarks.
func (c *Consumer) BenchProcess() func(context.Context, *bullmq.Job) error {
	s := c.newState(c.subs)
	return func(ctx context.Context, j *bullmq.Job) error {
		_, err := s.process(ctx, j)
		return err
	}
}
