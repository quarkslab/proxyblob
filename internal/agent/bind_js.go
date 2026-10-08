//go:build js

package agent

import "proxyblob/internal/mux"

func (h *SocksHandler) handleBind(c *mux.Connection, _ []byte) byte {
	h.SendError(c, mux.ErrUnsupportedCommand)
	return mux.ErrUnsupportedCommand
}
