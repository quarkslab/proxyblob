package proxy

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"proxyblob/pkg/protocol"
)

// The proxy owns the client endpoint. The agent only owns target-facing UDP.
func (h *SocksHandler) handleUDPAssociate(conn *protocol.Connection, request []byte) byte {
	if target, code := ParseAddress(request); code != protocol.ErrNone {
		h.SendError(conn, code)
		return code
	} else if _, _, err := net.SplitHostPort(target); err != nil {
		h.SendError(conn, protocol.ErrAddressNotSupported)
		return protocol.ErrAddressNotSupported
	}
	d := conn.EnableDatagrams()
	if d == nil {
		return protocol.ErrInvalidState
	}
	setupCtx, cancel := socketSetupContext(h.Ctx, conn.Closed)
	socket, err := listenUDPContext(setupCtx)
	cancel()
	if err != nil {
		h.SendError(conn, protocol.ErrNetworkUnreachable)
		return protocol.ErrNetworkUnreachable
	}
	defer socket.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-done:
		case <-conn.Closed:
		case <-h.Ctx.Done():
		}
		socket.Close()
	}()
	// A control FIN ends this association, including while setup is in flight.
	go func() { io.Copy(io.Discard, conn.ProtocolConn()); h.SendClose(conn.ID, protocol.ErrNone) }()
	address, err := d.Request(request)
	if err != nil {
		return protocol.ErrConnectionClosed
	}
	if h.SendData(conn.ID, append([]byte{Version5, address[0], 0}, address[1:]...)) != protocol.ErrNone {
		return protocol.ErrPacketSendFailed
	}
	if address[0] != Succeeded {
		return protocol.ErrNetworkUnreachable
	}
	err = h.relayAgentUDP(conn, d, socket)
	if err != nil && !errors.Is(err, net.ErrClosed) {
		log.Warn().Err(err).Msg("Agent UDP relay stopped")
	}
	h.SendClose(conn.ID, protocol.ErrNone)
	return protocol.ErrNone
}

func (h *SocksHandler) relayAgentUDP(c *protocol.Connection, d *protocol.Datagrams, socket UDPRelayConn) (result error) {
	// Keys are resolved IP:port pairs, never unbounded client-provided domain
	// strings. Evict idle entries on admission; drop new destinations at capacity.
	var mu sync.Mutex
	targets := make(map[string]time.Time)
	finished := make(chan error, 1)
	go func() {
		buf := make([]byte, 65535)
		for {
			n, from, err := socket.ReadFrom(buf)
			if err != nil {
				finished <- err
				c.Close()
				return
			}
			mu.Lock()
			last, known := targets[from.String()]
			if known && time.Since(last) <= time.Minute {
				targets[from.String()] = time.Now()
			} else {
				known = false
			}
			mu.Unlock()
			if !known {
				continue
			}
			packet := append([]byte{0, 0, 0}, UDPAddress(from)...)
			packet = append(packet, buf[:n]...)
			if len(packet) > protocol.MaxDatagramSize {
				continue
			} // complete SOCKS packet must fit the client UDP payload
			if err = d.Send(packet); err != nil && !errors.Is(err, protocol.ErrDatagramDropped) {
				finished <- err
				c.Close()
				return
			}
		}
	}()
	defer func() {
		socket.Close()
		if readErr := <-finished; readErr != nil && !errors.Is(readErr, net.ErrClosed) {
			result = readErr
		}
	}()
	for {
		packet, err := d.Receive()
		if err != nil {
			return err
		}
		target, header, code := ExtractUDPHeader(packet)
		if code != protocol.ErrNone {
			continue
		}
		ctx, cancel := socketSetupContext(h.Ctx, c.Closed)
		addr, err := resolveUDPContext(ctx, target)
		cancel()
		if err != nil {
			log.Debug().Err(err).Msg("UDP destination resolution failed")
			continue
		}
		mu.Lock()
		now := time.Now()
		for key, last := range targets {
			if now.Sub(last) > time.Minute {
				delete(targets, key)
			}
		}
		_, exists := targets[addr.String()]
		if !exists && len(targets) >= h.udpDestinations {
			mu.Unlock()
			continue
		}
		targets[addr.String()] = now
		mu.Unlock()
		if err = socket.WriteTo(packet[header:], addr); err != nil {
			if errors.Is(err, io.ErrShortWrite) {
				continue
			} // host refused whole datagram at its finite send limit
			return err
		}
	}
}
