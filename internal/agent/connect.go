package agent

import (
	"net"
	"proxyblob/internal/agent/netenv"
	"proxyblob/internal/socks5"

	"proxyblob/internal/mux"
)

// handleConnect processes the SOCKS5 CONNECT command.
// It establishes a TCP connection to the requested target and
// sets up bidirectional data transfer between client and target.
//
// The CONNECT command format is:
//
//	+-----+-----+-----+------+----------+----------+
//	| VER | CMD | RSV | ATYP | DST.ADDR | DST.PORT |
//	+-----+-----+-----+------+----------+----------+
//	|  1  |  1  |  1  |  1   | Variable |    2     |
//
// Returns an error code indicating success or specific failure reason.
func (h *SocksHandler) handleConnect(conn *mux.Connection, cmdData []byte) byte {
	if len(cmdData) < 4 {
		// Send malformed request response
		response := []byte{socks5.Version5, socks5.GeneralFailure, 0x00, socks5.IPv4, 0, 0, 0, 0, 0, 0}
		h.SendData(conn.ID, response)
		return mux.ErrAddressNotSupported
	}

	// Parse target address
	target, errCode := socks5.ParseAddress(cmdData[3:])
	if errCode != mux.ErrNone {
		h.SendError(conn, errCode)
		return errCode
	}

	// Establish TCP connection to target (uses Bun bridge on WASM, native net on other platforms)
	setupCtx, cancelSetup := socketSetupContext(h.Ctx, conn.Closed)
	targetConn, err := netenv.DialTCPContext(setupCtx, target)
	cancelSetup()
	if err != nil {
		// Map network error to appropriate protocol error code
		errCode = mux.MapNetError(err)
		h.SendError(conn, errCode)
		return errCode
	}

	// Enable TCP_NODELAY to disable Nagle's algorithm for better TLS performance
	if tcpConn, ok := targetConn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	if !conn.AttachDestination(targetConn) {
		return mux.ErrConnectionClosed
	}

	if h.sendTCPReply(conn, socks5.Succeeded, targetConn.LocalAddr().(*net.TCPAddr)) != mux.ErrNone {
		return mux.ErrPacketSendFailed
	}

	// Start data transfer
	return h.handleTCPDataTransfer(conn, targetConn)
}

// handleTCPDataTransfer manages bidirectional data transfer for TCP connections.
// Uses io.Copy for efficient data transfer: io.Copy(dst, src)
//
// The transfer continues until either:
//   - The connection is closed by either end
//   - The context is canceled
//   - An error occurs
func (h *SocksHandler) handleTCPDataTransfer(conn *mux.Connection, tcpConn net.Conn) byte {
	err := mux.Forward(tcpConn, conn.ProtocolConn())
	if err != nil {
		code := mux.StreamErrorCode(err)
		h.SendClose(conn.ID, code)
		return code
	}
	h.SendClose(conn.ID, mux.ErrNone)
	return mux.ErrNone
}
