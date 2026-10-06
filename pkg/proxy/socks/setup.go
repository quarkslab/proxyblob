package proxy

import "context"

// A setup operation belongs to both its handler and its logical stream. Stop
// its watcher when setup returns, even if the stream remains open afterwards.
func socketSetupContext(parent context.Context, closed <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	select {
	case <-closed:
		cancel()
	default:
	}
	go func() {
		select {
		case <-closed:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
