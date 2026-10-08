// Package proxy is the operator-side SOCKS5 front-end for one agent. It
// negotiates SOCKS5 with each local client, then carries the request to the
// agent as one relay request on its own tunnel stream.
package proxy

import (
	"context"
	"errors"
	"net"
	"sync"

	"proxyblob/internal/mux"

	"github.com/rs/zerolog/log"
)

// ProxyServer serves SOCKS5 clients over one agent session.
type ProxyServer struct {
	*mux.Session

	// listener accepts incoming TCP connections; lifecycleMu guards replacement.
	listener    net.Listener
	lifecycleMu sync.Mutex
	stopOnce    sync.Once
}

// NewProxyServer creates a proxy server with the default flow configuration.
func NewProxyServer(ctx context.Context, conn net.Conn) *ProxyServer {
	server, err := NewProxyServerWithConfig(ctx, conn, mux.DefaultFlowConfig())
	if err != nil {
		panic(err)
	}
	return server
}

// NewProxyServerWithConfig creates a proxy server over the agent session conn.
func NewProxyServerWithConfig(ctx context.Context, conn net.Conn, cfg mux.FlowConfig) (*ProxyServer, error) {
	session, err := mux.NewSession(ctx, conn, cfg)
	if err != nil {
		return nil, err
	}
	session.OnError = protocolErrorReporter(log.Logger)
	return &ProxyServer{Session: session}, nil
}

// Start begins listening for SOCKS clients on address. A local bind failure
// leaves the agent session alive.
func (s *ProxyServer) Start(address string) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.Ctx.Err() != nil || s.listener != nil {
		return
	}
	var err error
	s.listener, err = net.Listen("tcp", address)
	if err != nil {
		log.Error().Err(err).Str("addr", address).Msg("Failed to listen on address")
		return
	}
	s.StartReceiving()
	go s.acceptLoop(s.listener)
}

// ListenerAddr returns the current local SOCKS address, if started.
func (s *ProxyServer) ListenerAddr() net.Addr {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// StopListening stops local SOCKS service while retaining the agent session.
func (s *ProxyServer) StopListening() {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
	// Keep entries until each handler sends its CLOSE after any queued NEW.
	// Removing here would make SendClose silently skip peer notification.
	s.Connections.Range(func(_, value any) bool {
		value.(*mux.Connection).Close()
		return true
	})
}

// Stop aborts pending I/O and every client, and stops listening.
func (s *ProxyServer) Stop() {
	s.stopOnce.Do(func() {
		s.lifecycleMu.Lock()
		defer s.lifecycleMu.Unlock()
		s.Session.Stop()
		if s.listener != nil {
			s.listener.Close()
			s.listener = nil
		}
	})
}

// acceptLoop serves clients until the listener or the session closes.
func (s *ProxyServer) acceptLoop(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.Ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return // session or local listener shutdown
			}
			if _, ok := err.(net.Error); ok {
				continue // retry temporary network errors
			}
			return
		}
		go s.handleConnection(listener, conn)
	}
}

// handleConnection reserves a tunnel stream before negotiating, so a client
// beyond the session's capacity is refused before any SOCKS reply, then
// negotiates SOCKS5 locally and serves the request.
func (s *ProxyServer) handleConnection(listener net.Listener, client net.Conn) {
	defer client.Close()
	// Disable Nagle's algorithm for better TLS performance.
	if tcp, ok := client.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	s.lifecycleMu.Lock()
	if s.listener != listener || s.Ctx.Err() != nil {
		s.lifecycleMu.Unlock()
		return
	}
	reservation, err := s.Reserve()
	if err != nil {
		s.lifecycleMu.Unlock()
		return
	}
	// Closing the reservation (StopListening, Stop) also closes the client.
	reservation.AttachDestination(client)
	s.lifecycleMu.Unlock()
	s.serveSOCKS(client, reservation)
}
