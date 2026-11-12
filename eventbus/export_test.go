package eventbus

import "context"

// SetAfterPush installs a hook run between AddBulk and marking rows sent
// (crash simulation). Call before Start.
func (b *Bus) SetAfterPush(fn func() error) { b.afterPush = fn }

// Cleanup runs one retention pass.
func (b *Bus) Cleanup(ctx context.Context) error { return b.cleanup(ctx) }
