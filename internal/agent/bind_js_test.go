//go:build js

package agent

import (
	"bytes"
	"io"
	"testing"
)

func TestJSBINDReturnsUnsupportedWithoutHostAllocation(t *testing.T) {
	// The host has only the existing socket API; BIND must not call even that API.
	state := testJSHost(t, "pending")
	_, c := udpTestControl(t)
	control := c.ProtocolConn()
	if _, err := control.Write([]byte{5, 2, 0, 1, 127, 0, 0, 1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(control)
	if err != nil || !bytes.Equal(got, []byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("BIND unsupported reply %x %v", got, err)
	}
	counts := state.Call("counts")
	if counts.Get("callbacks").Int() != 0 || counts.Get("disposed").Int() != 0 {
		t.Fatalf("BIND allocated host operation: %v", counts)
	}
}
