//go:build js

package agent

import (
	"proxyblob/internal/diag"
	"proxyblob/internal/mux"
)

func (h *SocksHandler) handleBind(c *mux.Connection, _ []byte) byte {
	h.SendError(c, diag.ErrUnsupportedCommand)
	return diag.ErrUnsupportedCommand
}
