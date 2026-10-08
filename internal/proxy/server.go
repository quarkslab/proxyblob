// Package proxy implements a SOCKS proxy server.
// It accepts client connections and forwards traffic through transport channels
// to remote agents. The server manages connection lifecycle and bidirectional
// data transfer.
package proxy

import (
	"context"
	"errors"
	"net"
	"proxyblob/internal/mux"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// ProxyServer implements a SOCKS proxy server that forwards traffic transparently.
// It accepts client connections and manages the protocol flow between clients and
// remote agents.
type ProxyServer struct {
	// BaseHandler provides common protocol functionality
	*mux.BaseHandler

	// listener accepts incoming TCP connections; lifecycleMu guards replacement.
	listener    net.Listener
	receiveOnce sync.Once
	lifecycleMu sync.Mutex
	stopOnce    sync.Once
}

// NewProxyServer creates a proxy server instance with the given connection.
// The connection is used for communication with remote agents.
func NewProxyServer(ctx context.Context, conn net.Conn) *ProxyServer {
	server, err := NewProxyServerWithConfig(ctx, conn, mux.DefaultFlowConfig())
	if err != nil {
		panic(err)
	}
	return server
}

func NewProxyServerWithConfig(ctx context.Context, conn net.Conn, cfg mux.FlowConfig) (*ProxyServer, error) {
	base, err := mux.NewBaseHandlerWithConfig(ctx, conn, cfg)
	if err != nil {
		return nil, err
	}
	server := &ProxyServer{BaseHandler: base}
	server.PacketHandler = server
	server.OnError = protocolErrorReporter(log.Logger)
	return server, nil
}

// Start begins listening for client connections on the specified address.
// It launches background goroutines for accepting connections and processing
// protocol messages. A local bind failure leaves the agent session alive.
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

// StartReceiving monitors the tunnel independently of the local SOCKS listener.
// Start may be called later without creating a second reader.
func (s *ProxyServer) StartReceiving() { s.receiveOnce.Do(func() { go s.ReceiveLoop() }) }

// Stop aborts pending I/O and terminates the proxy server by closing all active
// connections, canceling the handler's context, and stopping the listener.
func (s *ProxyServer) Stop() {
	s.stopOnce.Do(func() {
		s.lifecycleMu.Lock()
		defer s.lifecycleMu.Unlock()
		s.CloseAllConnections()
		s.Abort()
		if s.listener != nil {
			s.listener.Close()
			s.listener = nil
		}
	})
}

// OnNew handles new connection requests. The server is the only one initiating
// connections, so this always returns ErrUnexpectedPacket.
func (s *ProxyServer) OnNew(connectionID uuid.UUID, data []byte) byte {
	return mux.ErrUnexpectedPacket
}

// OnAck processes connection acknowledgments from agents.
// Returns an error code indicating success or failure.
func (s *ProxyServer) OnAck(connectionID uuid.UUID, data []byte) byte {
	value, ok := s.Connections.Load(connectionID)
	if !ok {
		return mux.ErrConnectionNotFound
	}
	conn := value.(*mux.Connection)

	// Check if connection already established (ProtocolConn should be nil for new connections)
	if conn.ProtocolConn() != nil {
		return mux.ErrInvalidState
	}

	// Create the virtual protocol connection (marks connection as established)
	if !conn.SetProtocolConn(mux.NewProtocolConn(s.Ctx, connectionID, s.BaseHandler)) {
		return mux.ErrConnectionClosed
	}
	conn.StartDelivery()

	return mux.ErrNone
}

// OnData processes data received from agents and forwards it to the client.
// Returns an error code indicating success or failure.
func (s *ProxyServer) OnData(connectionID uuid.UUID, data []byte) byte {
	value, ok := s.Connections.Load(connectionID)
	if !ok {
		return mux.ErrConnectionNotFound
	}
	conn := value.(*mux.Connection)

	// The connection is only ready to receive data once OnAck has created the
	// virtual protocol connection. Dropping the payload here would be silent
	// data loss, so report the unexpected state instead.
	if conn.ProtocolConn() == nil {
		return mux.ErrInvalidState
	}

	// Delivery uses reserved memory and never blocks shared dispatch. The
	// sender must pause before exhausting its negotiated receive credit.
	if !conn.Deliver(data) {
		return mux.ErrConnectionClosed
	}
	return mux.ErrNone
}

// OnClose handles connection termination from agents. It cleans up the
// connection state.
func (s *ProxyServer) OnClose(connectionID uuid.UUID, errorCode byte) byte {
	s.ReportError(connectionID, errorCode)
	return s.PeerClose(connectionID, errorCode)
}

// cleanupConnection closes all connection resources and removes from connection map.
// This helper reduces code duplication in handleConnection.
func (s *ProxyServer) cleanupConnection(connID uuid.UUID, clientConn net.Conn, proxyConn *mux.Connection) {
	clientConn.Close()
	if pc := proxyConn.ProtocolConn(); pc != nil {
		pc.Close()
	}
	proxyConn.Close()
	s.Connections.Delete(connID)
}

// acceptLoop accepts incoming TCP connections and spawns goroutines to handle
// each one. It continues until the context is canceled or a non-temporary
// error occurs.
func (s *ProxyServer) acceptLoop(listener net.Listener) {
	for {
		select {
		case <-s.Ctx.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				if s.Ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
					return // Exit quietly on session or local listener shutdown
				}

				if _, ok := err.(net.Error); ok {
					continue // Retry on temporary network errors
				}
				return
			}

			go s.handleConnection(listener, conn)
		}
	}
}

// AckTimeout bounds how long a new logical connection waits for the agent's
// acknowledgment before being abandoned.
//
// It covers connection setup only: no payload flows until the acknowledgment
// arrives, so the deadline is never refreshed by progress, and once the
// connection is established no further timeout applies. It therefore bounds
// round-trip latency under contention, not transfer duration.
//
// All logical connections share a single transport, so their acknowledgments
// serialize and the last connection opened waits behind every other one. The
// value must accommodate that queueing delay on a high-latency driver, not
// just one round trip. The cost of a generous bound is that an unreachable
// agent takes this long to report.
const AckTimeout = 120 * time.Second

// handleConnection processes a new client connection by:
//   - Generating a unique connection ID
//   - Initiating connection with remote agent
//   - Setting up bidirectional data forwarding
//   - Managing connection lifecycle and cleanup
func (s *ProxyServer) handleConnection(listener net.Listener, clientConn net.Conn) {
	defer clientConn.Close()

	// Enable TCP_NODELAY to disable Nagle's algorithm for better TLS performance
	if tcpConn, ok := clientConn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	connID := uuid.New()
	proxyConn := mux.NewConnection(connID, s.Ctx.Done())
	s.lifecycleMu.Lock()
	if s.listener != listener || s.Ctx.Err() != nil {
		s.lifecycleMu.Unlock()
		return
	}
	proxyConn.AttachDestination(clientConn)
	if err := s.RegisterConnection(proxyConn); err != nil {
		s.lifecycleMu.Unlock()
		proxyConn.Close()
		return
	}
	s.lifecycleMu.Unlock()

	// 1. Initiate connection with the agent
	errCode := s.SendNewConnection(connID)
	if errCode != mux.ErrNone {
		proxyConn.Close()
		return
	}

	// 2. Wait for the agent's acknowledgment. OnAck publishes the virtual
	// connection and closes Established() from the receive goroutine, so we block
	// on that signal rather than polling for the pointer.
	select {
	case <-s.Ctx.Done():
		s.SendClose(connID, mux.ErrHandlerStopped)
		s.Connections.Delete(connID)
		return
	case <-proxyConn.Closed:
		s.SendClose(connID, mux.ErrHandlerStopped)
		s.Connections.Delete(connID)
		return
	case <-time.After(AckTimeout):
		s.SendClose(connID, mux.ErrTransportTimeout)
		s.Connections.Delete(connID)
		return
	case <-proxyConn.Established():
		// Agent acknowledged connection.
	}

	// Forward owns both copy lifetimes and propagates directional EOF.
	err := mux.Forward(proxyConn.ProtocolConn(), clientConn)
	if err == nil {
		s.SendClose(connID, mux.ErrNone)
	}
	s.cleanupConnection(connID, clientConn, proxyConn)
	if err != nil {
		s.ReportError(connID, mux.StreamErrorCode(err))
	}
}
