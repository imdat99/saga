package eventbus

import "context"

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
