//go:build js

package agent

import (
	"io"
	"testing"

	"proxyblob/internal/agent/netenv/netenvtest"
	"proxyblob/internal/diag"
	"proxyblob/internal/relay"
)

func TestJSBINDReturnsUnsupportedWithoutHostAllocation(t *testing.T) {
	// The host has only the existing socket API; BIND must not call even that API.
	state := netenvtest.Host(t, "pending")
	stream := agentStream(t)
	if err := relay.WriteRequest(stream, relay.Bind, []byte{1, 127, 0, 0, 1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	code, _, err := relay.ReadReply(stream)
	if err != nil || code != diag.ErrUnsupportedCommand {
		t.Fatalf("BIND unsupported reply %d %v", code, err)
	}
	if rest, err := io.ReadAll(stream); err != nil || len(rest) != 0 {
		t.Fatalf("after reply %x %v", rest, err)
	}
	counts := state.Call("counts")
	if counts.Get("callbacks").Int() != 0 || counts.Get("disposed").Int() != 0 {
		t.Fatalf("BIND allocated host operation: %v", counts)
	}
}
