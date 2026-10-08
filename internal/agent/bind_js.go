//go:build js

package agent

import (
	"proxyblob/internal/diag"
	"proxyblob/internal/mux"
)

// The JS host offers no TCP listener, so BIND is not supported in WASM.
func (a *Agent) bind(stream *mux.ProtocolConn, _ []byte) {
	a.fail(stream, diag.ErrUnsupportedCommand)
}
