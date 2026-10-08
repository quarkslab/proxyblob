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

// TransportOptions returns the aznet options both ends apply to a session.
// PROXYBLOB_WRITE_BUFFER overrides the write allowance in bytes.
func TransportOptions() ([]aznet.Option, error) {
	size := DefaultWriteBuffer
	if value, ok := os.LookupEnv("PROXYBLOB_WRITE_BUFFER"); ok {
		n, err := strconv.Atoi(value)
		if err != nil || n < 64<<10 {
			return nil, diag.ErrInvalidFlowConfig
		}
		size = n
	}
	return []aznet.Option{aznet.WithBufferLimits(aznet.BufferLimits{Write: size})}, nil
}
