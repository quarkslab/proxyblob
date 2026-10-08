package agent

import (
	"errors"
	"io"
	"net"
	"proxyblob/internal/agent/netenv"
	"proxyblob/internal/socks5"
	"sync"
	"time"

	"proxyblob/internal/mux"
)

// The proxy owns the client endpoint. The agent only owns target-facing UDP.
func (h *SocksHandler) handleUDPAssociate(conn *mux.Connection, request []byte) byte {
	if target, code := socks5.ParseAddress(request); code != mux.ErrNone {
		return h.failUDPAssociate(conn, code)
	} else if _, _, err := net.SplitHostPort(target); err != nil {
		return h.failUDPAssociate(conn, mux.ErrAddressNotSupported)
	}
	d := conn.EnableDatagrams()
	if d == nil {
		return mux.ErrInvalidState
	}
	// A control FIN ends this association, including while setup is in flight.
	go func() { io.Copy(io.Discard, conn.ProtocolConn()); h.SendClose(conn.ID, mux.ErrNone) }()
	setupCtx, cancel := socketSetupContext(h.Ctx, conn.Closed)
	socket, err := netenv.ListenUDPContext(setupCtx)
	cancel()
	if err != nil {
		return h.failUDPAssociate(conn, mux.ErrNetworkUnreachable)
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

	address, err := d.Request(request)
	if err != nil {
		return mux.ErrConnectionClosed
	}
	if h.SendData(conn.ID, append([]byte{socks5.Version5, address[0], 0}, address[1:]...)) != mux.ErrNone {
		return mux.ErrPacketSendFailed
	}
	if address[0] != socks5.Succeeded {
		h.SendClose(conn.ID, mux.ErrNone)
		return mux.ErrNone
	}
	err = h.relayAgentUDP(conn, d, socket)
	if err != nil && !errors.Is(err, net.ErrClosed) {
		h.ReportError(conn.ID, mux.ErrorCode(err))
	}
	h.SendClose(conn.ID, mux.ErrNone)
	return mux.ErrNone
}

func (h *SocksHandler) relayAgentUDP(c *mux.Connection, d *mux.Datagrams, socket netenv.UDPConn) (result error) {
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
			packet := append([]byte{0, 0, 0}, socks5.UDPAddress(from)...)
			packet = append(packet, buf[:n]...)
			if len(packet) > mux.MaxDatagramSize {
				continue
			} // complete SOCKS packet must fit the client UDP payload
			if err = d.Send(packet); err != nil && !errors.Is(err, mux.ErrDatagramDropped) {
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
		target, header, code := socks5.ExtractUDPHeader(packet)
		if code != mux.ErrNone {
			continue
		}
		ctx, cancel := socketSetupContext(h.Ctx, c.Closed)
		addr, err := netenv.ResolveUDPContext(ctx, target)
		cancel()
		if err != nil {
			// Resolution failure drops this datagram; avoid per-packet diagnostics.
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

// A failure reply is application data: close gracefully so the proxy drains it
// before closing TCP. A nonzero tunnel CLOSE would discard that accepted reply.
func (h *SocksHandler) failUDPAssociate(c *mux.Connection, code byte) byte {
	h.SendError(c, code)
	h.SendClose(c.ID, mux.ErrNone)
	return mux.ErrNone
}
