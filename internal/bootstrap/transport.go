package bootstrap

import "github.com/atsika/aznet"

// DefaultWriteBuffer bounds the bytes aznet may queue for upload per session.
// ProxyBlob's writer orders records (controls first, streams in turn), but
// that order only holds for records it still owns: anything handed to aznet
// waits in arrival order. A smaller allowance keeps small requests from
// queueing behind bulk data; a larger one lets aznet upload bigger chunks.
const DefaultWriteBuffer = 1 << 20

// DefaultWriteChunks further caps the allowance at that many transport chunks.
// aznet uploads one chunk at a time, so the wait behind queued bulk data grows
// with chunks, not bytes: 1 MiB is a fraction of one 4 MiB Blob chunk but about
// 21 sequential Queue messages. Five chunks leave Blob and Table at 1 MiB and
// bring Queue to about 240 KiB, where its throughput is unchanged.
const DefaultWriteChunks = 5

// TransportOptions returns the aznet options both ends apply to a session.
func TransportOptions() []aznet.Option {
	return []aznet.Option{aznet.WithBufferLimits(aznet.BufferLimits{Write: DefaultWriteBuffer, WriteChunks: DefaultWriteChunks})}
}
