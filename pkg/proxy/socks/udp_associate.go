package proxy

import (
	"encoding/binary"
	"errors"
	"net"
	"os"
	"time"

	"proxyblob/pkg/protocol"
)

// handleUDPAssociate processes the SOCKS5 UDP ASSOCIATE command.
// It creates a single UDP relay socket, tells the client which port to use,
// then dispatches all relay logic to handleUDPPackets.
func (h *SocksHandler) handleUDPAssociate(conn *protocol.Connection) byte {
	setupCtx, cancelSetup := socketSetupContext(h.Ctx, conn.Closed)
	udpConn, err := listenUDPContext(setupCtx)
	cancelSetup()
	if err != nil {
		h.SendError(conn, protocol.ErrNetworkUnreachable)
		return protocol.ErrNetworkUnreachable
	}

	port := udpConn.LocalPort()

	// Send success response: VER REP RSV ATYP BND.ADDR(4) BND.PORT(2)
	response := []byte{
		Version5, Succeeded, 0, IPv4,
		0, 0, 0, 0,
		byte(port >> 8), byte(port & 0xff),
	}
	if errCode := h.SendData(conn.ID, response); errCode != protocol.ErrNone {
		udpConn.Close()
		return protocol.ErrPacketSendFailed
	}

	go h.handleUDPPackets(conn, udpConn)

	// Hold the control connection open; close the socket when context dies.
	select {
	case <-conn.Closed:
	case <-h.Ctx.Done():
		udpConn.Close()
	}
	return protocol.ErrNone
}

// handleUDPPackets relays UDP datagrams between the SOCKS client and internet targets.
//
// A single socket handles both directions:
//   - Packets from clientAddr → strip SOCKS5 header → forward to target
//   - Packets from a known target → wrap in SOCKS5 header → forward to clientAddr
func (h *SocksHandler) handleUDPPackets(conn *protocol.Connection, udpConn UDPRelayConn) {
	defer udpConn.Close()

	buf := make([]byte, 16*1024)
	var clientAddr *net.UDPAddr

	type targetInfo struct {
		addr       *net.UDPAddr
		lastActive time.Time
	}
	targets := make(map[string]*targetInfo)

	cleanup := time.NewTicker(30 * time.Second)
	defer cleanup.Stop()

	for {
		// Check for shutdown before blocking on I/O.
		select {
		case <-h.Ctx.Done():
			return
		case <-conn.Closed:
			return
		case <-cleanup.C:
			now := time.Now()
			for k, t := range targets {
				if now.Sub(t.lastActive) > time.Minute {
					delete(targets, k)
				}
			}
		default:
		}

		udpConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, addr, err := udpConn.ReadFrom(buf)
		if err != nil {
			if isUDPTimeout(err) {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			h.SendClose(conn.ID, protocol.ErrNetworkUnreachable)
			return
		}

		if n == 0 {
			continue
		}

		if clientAddr == nil {
			// First valid SOCKS5 UDP datagram (RSV=0x0000, FRAG=0) sets the client.
			if n > 3 && buf[0] == 0 && buf[1] == 0 && buf[2] == 0 {
				clientAddr = addr
			} else {
				continue
			}
		}

		if addr.IP.Equal(clientAddr.IP) && addr.Port == clientAddr.Port {
			// Client → target
			if n <= 3 {
				continue
			}
			targetAddr, headerLen, errCode := ExtractUDPHeader(buf[:n])
			if errCode != protocol.ErrNone {
				continue
			}
			targetUDPAddr, err := net.ResolveUDPAddr("udp", targetAddr)
			if err != nil {
				continue
			}
			targets[targetAddr] = &targetInfo{addr: targetUDPAddr, lastActive: time.Now()}
			udpConn.WriteTo(buf[headerLen:n], targetUDPAddr)
		} else {
			// Target → client: find the matching target entry and wrap with SOCKS5 header.
			for _, t := range targets {
				if !t.addr.IP.Equal(addr.IP) || t.addr.Port != addr.Port {
					continue
				}
				t.lastActive = time.Now()

				var header []byte
				if ip4 := addr.IP.To4(); ip4 != nil {
					header = append([]byte{0, 0, 0, IPv4}, ip4...)
				} else {
					header = append([]byte{0, 0, 0, IPv6}, addr.IP.To16()...)
				}
				var portBuf [2]byte
				binary.BigEndian.PutUint16(portBuf[:], uint16(addr.Port))
				header = append(header, portBuf[:]...)

				udpConn.WriteTo(append(header, buf[:n]...), clientAddr)
				break
			}
		}
	}
}

// isUDPTimeout reports whether err is a read-deadline / timeout error.
func isUDPTimeout(err error) bool {
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	return errors.Is(err, os.ErrDeadlineExceeded)
}
