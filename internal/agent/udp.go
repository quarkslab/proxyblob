package agent

import (
	"errors"
	"io"
	"net"
	"proxyblob/internal/agent/netenv"
	"proxyblob/internal/diag"
	"proxyblob/internal/relay"
	"sync"
	"time"

	"proxyblob/internal/mux"
)

// udp opens a target-facing UDP socket and relays datagrams until the proxy
// ends the association (stream EOF or close). The proxy owns the client socket.
func (a *Agent) udp(stream *mux.ProtocolConn) {
	d := stream.EnableDatagrams()
	if d == nil {
		stream.CloseWithCode(diag.ErrInvalidState)
		return
	}
	// The proxy's EOF ends this association, including while setup is in flight.
	go func() { io.Copy(io.Discard, stream); stream.CloseWithCode(diag.ErrNone) }()
	setupCtx, cancel := socketSetupContext(a.Ctx, stream.Done())
	socket, err := netenv.ListenUDPContext(setupCtx)
	cancel()
	if err != nil {
		a.fail(stream, diag.ErrNetworkUnreachable)
		return
	}
	defer socket.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-done:
		case <-stream.Done():
		case <-a.Ctx.Done():
		}
		socket.Close()
	}()
	if relay.WriteReply(stream, diag.ErrNone, nil) != nil {
		return
	}
	err = a.relayUDP(stream, d, socket)
	if err != nil && !errors.Is(err, net.ErrClosed) {
		a.ReportError(stream.ID(), diag.ErrorCode(err))
	}
	stream.CloseWithCode(diag.ErrNone)
}

func (a *Agent) relayUDP(stream *mux.ProtocolConn, d *mux.Datagrams, socket netenv.UDPConn) (result error) {
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
				stream.CloseWithCode(diag.ErrNone)
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
			packet := relay.Datagram(from, buf[:n])
			if len(packet) > mux.MaxDatagramSize {
				continue
			} // the whole datagram must fit the client's UDP payload
			if err = d.Send(packet); err != nil && !errors.Is(err, diag.ErrDatagramDropped) {
				finished <- err
				stream.CloseWithCode(diag.ErrNone)
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
		target, header, code := relay.ParseDatagram(packet)
		if code != diag.ErrNone {
			continue
		}
		ctx, cancel := socketSetupContext(a.Ctx, stream.Done())
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
		if !exists && len(targets) >= a.udpDestinations {
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
