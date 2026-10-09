package bootstrap

import (
	"errors"
	"testing"

	"proxyblob/internal/diag"
)

// aznet keeps its limits private, so this checks the option is supplied and
// invalid sizes are refused; end-to-end runs observe the effect.
func TestTransportOptionsWriteBuffer(t *testing.T) {
	for _, value := range []string{"", "4194304", "65536"} {
		if value != "" {
			t.Setenv("PROXYBLOB_WRITE_BUFFER", value)
		}
		if opts, err := TransportOptions(); err != nil || len(opts) != 1 {
			t.Fatalf("%q: %d options, %v", value, len(opts), err)
		}
	}
	for _, bad := range []string{"0", "1024", "nonsense"} {
		t.Setenv("PROXYBLOB_WRITE_BUFFER", bad)
		if _, err := TransportOptions(); !errors.Is(err, diag.ErrInvalidFlowConfig) {
			t.Fatalf("%q accepted: %v", bad, err)
		}
	}
}

func TestTransportOptionsWriteChunks(t *testing.T) {
	for _, value := range []string{"", "0", "5", "64"} {
		if value != "" {
			t.Setenv("PROXYBLOB_WRITE_CHUNKS", value)
		}
		if opts, err := TransportOptions(); err != nil || len(opts) != 1 {
			t.Fatalf("%q: %d options, %v", value, len(opts), err)
		}
	}
	for _, bad := range []string{"-1", "nonsense"} {
		t.Setenv("PROXYBLOB_WRITE_CHUNKS", bad)
		if _, err := TransportOptions(); !errors.Is(err, diag.ErrInvalidFlowConfig) {
			t.Fatalf("%q accepted: %v", bad, err)
		}
	}
}
