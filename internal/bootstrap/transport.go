package bootstrap

import (
	"os"
	"strconv"

	"proxyblob/internal/diag"

	"github.com/atsika/aznet"
)

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
// PROXYBLOB_WRITE_BUFFER overrides the write allowance in bytes and
// PROXYBLOB_WRITE_CHUNKS its chunk cap (0 disables the cap).
func TransportOptions() ([]aznet.Option, error) {
	size := DefaultWriteBuffer
	if value, ok := os.LookupEnv("PROXYBLOB_WRITE_BUFFER"); ok {
		n, err := strconv.Atoi(value)
		if err != nil || n < 64<<10 {
			return nil, diag.ErrInvalidFlowConfig
		}
		size = n
	}
	chunks := DefaultWriteChunks
	if value, ok := os.LookupEnv("PROXYBLOB_WRITE_CHUNKS"); ok {
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			return nil, diag.ErrInvalidFlowConfig
		}
		chunks = n
	}
	return []aznet.Option{aznet.WithBufferLimits(aznet.BufferLimits{Write: size, WriteChunks: chunks})}, nil
}
