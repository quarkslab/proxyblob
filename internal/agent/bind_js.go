//go:build js

package agent

import "proxyblob/pkg/protocol"

func (h *SocksHandler) handleBind(c *protocol.Connection, _ []byte) byte {
	h.SendError(c, protocol.ErrUnsupportedCommand)
	return protocol.ErrUnsupportedCommand
}
