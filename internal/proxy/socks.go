package proxy

import (
	"bufio"
	"errors"
	"io"
	"net"
	"slices"
	"sync/atomic"
	"time"

	"proxyblob/internal/diag"
	"proxyblob/internal/mux"
	"proxyblob/internal/relay"
	"proxyblob/internal/socks5"
)

// serveSOCKS negotiates SOCKS5 with client and serves its request. It owns the
// reservation: every path either opens it or releases it.
func (s *ProxyServer) serveSOCKS(client net.Conn, r *mux.Reservation) {
	cmd, addr, ok := negotiate(client)
	if !ok {
		r.Release()
		return
	}
	switch cmd {
	case socks5.Connect, socks5.Bind:
		s.serveTCP(client, r, cmd, addr)
	case socks5.UDPAssociate:
		s.serveUDP(client, r, addr)
	}
}

// negotiate runs the NoAuth greeting and reads one request (RFC 1928). It
// replies to a malformed or unsupported request itself; ok reports a request
// to serve. A client that disconnects mid-request gets no reply.
func negotiate(client net.Conn) (cmd byte, addr []byte, ok bool) {
	var greeting [2]byte
	if _, err := io.ReadFull(client, greeting[:]); err != nil || greeting[0] != socks5.Version5 || greeting[1] == 0 {
		return 0, nil, false
	}
	methods := make([]byte, greeting[1])
	if _, err := io.ReadFull(client, methods); err != nil {
		return 0, nil, false
	}
	if !slices.Contains(methods, socks5.NoAuth) {
		client.Write([]byte{socks5.Version5, socks5.NoAcceptableMethods})
		return 0, nil, false
	}
	if _, err := client.Write([]byte{socks5.Version5, socks5.NoAuth}); err != nil {
		return 0, nil, false
	}
	var header [3]byte // VER CMD RSV
	if _, err := io.ReadFull(client, header[:]); err != nil {
		return 0, nil, false
	}
	// Read the whole request before judging it, so a rejection leaves no unread
	// bytes behind (closing over them would reset the connection and lose the reply).
	addr, err := socks5.ReadAddress(client)
	if err != nil {
		if diag.ErrorCode(err) == diag.ErrAddressNotSupported {
			reject(client, socks5.AddressTypeNotSupported)
		}
		return 0, nil, false
	}
	if header[0] != socks5.Version5 || header[2] != 0 {
		reject(client, socks5.GeneralFailure)
		return 0, nil, false
	}
	switch header[1] {
	case socks5.Connect, socks5.Bind, socks5.UDPAssociate:
		return header[1], addr, true
	}
	reject(client, socks5.CommandNotSupported)
	return 0, nil, false
}

// rejectDrain bounds how long a rejected client may keep sending.
const rejectDrain = time.Second

// reject sends a failure reply, then half-closes and briefly drains the client
// so the reply is delivered rather than lost to a reset.
func reject(client net.Conn, rep byte) {
	if _, err := client.Write(socks5.Reply(rep, nil)); err != nil {
		return
	}
	if cw, ok := client.(interface{ CloseWrite() error }); ok && cw.CloseWrite() == nil {
		client.SetReadDeadline(time.Now().Add(rejectDrain))
		io.Copy(io.Discard, io.LimitReader(client, 64<<10))
	}
}

// replyCode maps an agent's relay reply code to a SOCKS5 reply code.
func replyCode(code byte) byte {
	switch code {
	case diag.ErrNone:
		return socks5.Succeeded
	case diag.ErrNetworkUnreachable:
		return socks5.NetworkUnreachable
	case diag.ErrHostUnreachable:
		return socks5.HostUnreachable
	case diag.ErrConnectionRefused:
		return socks5.ConnectionRefused
	case diag.ErrTTLExpired:
		return socks5.TTLExpired
	case diag.ErrUnsupportedCommand:
		return socks5.CommandNotSupported
	case diag.ErrAddressNotSupported:
		return socks5.AddressTypeNotSupported
	}
	return socks5.GeneralFailure
}

// serveTCP relays CONNECT or BIND: one stream, one request, the agent's
// replies (two for BIND), then both directions until EOF.
func (s *ProxyServer) serveTCP(client net.Conn, r *mux.Reservation, cmd byte, addr []byte) {
	if _, code := socks5.ParseAddress(addr); code != diag.ErrNone {
		reject(client, replyCode(code))
		r.Release()
		return
	}
	stream, err := r.Open(s.Ctx)
	if err != nil {
		return
	}
	if relay.WriteRequest(stream, cmd, addr) != nil {
		stream.CloseWithCode(diag.ErrConnectionClosed)
		return
	}
	// The client may disconnect while the agent works (BIND can wait minutes).
	// Its EOF must end the request, and any early payload must be kept.
	stopWatch := watchClient(client, func() { stream.CloseWrite() })
	replies := 1
	if cmd == socks5.Bind {
		replies = 2
	}
	for range replies {
		code, bound, err := relay.ReadReply(stream)
		if err != nil {
			stopWatch()
			stream.CloseWithCode(diag.ErrNone)
			return
		}
		if _, err := client.Write(socks5.Reply(replyCode(code), bound)); err != nil || code != diag.ErrNone {
			stopWatch()
			stream.CloseWithCode(diag.ErrNone)
			return
		}
	}
	err = mux.Forward(stream, stopWatch())
	if err != nil {
		s.ReportError(stream.ID(), diag.StreamErrorCode(err))
		return
	}
	stream.CloseWithCode(diag.ErrNone)
}

// watchClient calls closed once the client reaches EOF or fails while the proxy
// waits on the agent, without consuming early payload. The returned stop ends
// the watch and yields a connection that replays any buffered bytes.
func watchClient(client net.Conn, closed func()) (stop func() net.Conn) {
	buffered := bufio.NewReader(client)
	var stopping atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := buffered.Peek(1); err != nil && !stopping.Load() {
			closed()
		}
	}()
	return func() net.Conn {
		stopping.Store(true)
		// Unblock the peek; a timeout error is consumed by it, never replayed.
		client.SetReadDeadline(time.Now())
		<-done
		client.SetReadDeadline(time.Time{})
		return &bufferedConn{Conn: client, reader: buffered}
	}
}

// bufferedConn reads through a buffer that may hold early client bytes, while
// keeping TCP half-close.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func (c *bufferedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}
