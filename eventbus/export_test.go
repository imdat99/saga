package eventbus

import (
	"context"

	"github.com/imdat99/bullmq"
)

// SetAfterPush installs a hook between pushing a batch and marking it sent.
func (p *Producer) SetAfterPush(fn func() error) { p.afterPush = fn }

// Cleanup runs one retention pass.
func (p *Producer) Cleanup(ctx context.Context) error { return p.cleanup(ctx) }

// BenchRelay prepares a relay without its loops and returns a pass function
// (benchmarks).
func (p *Producer) BenchRelay(ctx context.Context) (func(context.Context) (int, error), func(), error) {
	st, err := p.prepare(ctx)
	if err != nil {
		return nil, nil, err
	}
	return func(ctx context.Context) (int, error) { return p.relayOnce(ctx, st) }, st.close, nil
}

// BenchProcess returns the job processor of a consumer without a worker
// (benchmarks): handlers from Subscribe.
func (c *Consumer) BenchProcess() func(context.Context, *bullmq.Job) error {
	s := &consumerState{group: c.group, handlers: map[string][]Subscriber{}}
	for _, sub := range c.subs {
		s.handlers[sub.Event] = append(s.handlers[sub.Event], sub)
	}
	return func(ctx context.Context, j *bullmq.Job) error {
		_, err := s.process(ctx, j)
		return err
	}
}
