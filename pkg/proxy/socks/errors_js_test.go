//go:build js && wasm

package proxy

import (
	"context"
	"errors"
	"proxyblob/pkg/protocol"
	"testing"
)

func TestJSSetupFailureDoesNotCarryHostMessage(t *testing.T) {
	testJSHost(t, "error") // host reports the text "setup failure"
	conn, err := dialTCPContext(context.Background(), "127.0.0.1:9")
	if conn != nil || !errors.Is(err, protocol.Error(protocol.ErrTransportError)) || err.Error() != "22" {
		t.Fatalf("unsanitized host failure: %v", err)
	}
}
