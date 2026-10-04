// Package proxy implements a SOCKS proxy server.
// It accepts client connections and forwards traffic through transport channels
// to remote agents. The server manages connection lifecycle and bidirectional
// data transfer.
package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"proxyblob/pkg/protocol"
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
	*protocol.BaseHandler

	// Listener accepts incoming TCP connections
	Listener    net.Listener
	receiveOnce sync.Once
	lifecycleMu sync.Mutex
	stopOnce    sync.Once
}

// NewProxyServer creates a proxy server instance with the given connection.
// The connection is used for communication with remote agents.
func NewProxyServer(ctx context.Context, conn net.Conn) *ProxyServer {
	server := &ProxyServer{}
	server.BaseHandler = protocol.NewBaseHandler(ctx, conn)
	server.PacketHandler = server
	return server
}

// Start begins listening for client connections on the specified address.
// It launches background goroutines for accepting connections and processing
// protocol messages. If listening fails, the server is stopped.
func (s *ProxyServer) Start(address string) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.Ctx.Err() != nil || s.Listener != nil {
		return
	}
	var err error
	s.Listener, err = net.Listen("tcp", address)
	if err != nil {
		log.Error().Err(err).Str("addr", address).Msg("Failed to listen on address")
		s.Cancel()
		return
	}

	s.StartReceiving()
	go s.acceptLoop()
}

// StartReceiving monitors the tunnel independently of the local SOCKS listener.
// Start may be called later without creating a second reader.
func (s *ProxyServer) StartReceiving() { s.receiveOnce.Do(func() { go s.ReceiveLoop() }) }

// Stop gracefully terminates the proxy server by closing all active
// connections, canceling the handler's context, and stopping the listener.
func (s *ProxyServer) Stop() {
	s.stopOnce.Do(func() {
		s.lifecycleMu.Lock()
		defer s.lifecycleMu.Unlock()
		s.CloseAllConnections()
		s.Cancel()
		if s.Listener != nil {
			s.Listener.Close()
		}
	})
}

// OnNew handles new connection requests. The server is the only one initiating
// connections, so this always returns ErrUnexpectedPacket.
func (s *ProxyServer) OnNew(connectionID uuid.UUID, data []byte) byte {
	return protocol.ErrUnexpectedPacket
}

// OnAck processes connection acknowledgments from agents.
// Returns an error code indicating success or failure.
func (s *ProxyServer) OnAck(connectionID uuid.UUID, data []byte) byte {
	value, ok := s.Connections.Load(connectionID)
	if !ok {
		return protocol.ErrConnectionNotFound
	}
	conn := value.(*protocol.Connection)

	// Check if connection already established (ProtocolConn should be nil for new connections)
	if conn.ProtocolConn() != nil {
		return protocol.ErrInvalidState
	}

	// Create the virtual protocol connection (marks connection as established)
	conn.SetProtocolConn(protocol.NewProtocolConn(s.Ctx, connectionID, s.BaseHandler))
	conn.StartDelivery()
	conn.LastActivity = time.Now()
	return protocol.ErrNone
}

// OnData processes data received from agents and forwards it to the client.
// Returns an error code indicating success or failure.
func (s *ProxyServer) OnData(connectionID uuid.UUID, data []byte) byte {
	value, ok := s.Connections.Load(connectionID)
	if !ok {
		return protocol.ErrConnectionNotFound
	}
	conn := value.(*protocol.Connection)
	conn.LastActivity = time.Now()

	// The connection is only ready to receive data once OnAck has created the
	// virtual protocol connection. Dropping the payload here would be silent
	// data loss, so report the unexpected state instead.
	if conn.ProtocolConn() == nil {
		return protocol.ErrInvalidState
	}

	// Deliver blocks until the payload is accepted by the per-connection
	// goroutine, so back-pressure never discards data. A false return means the
	// connection is closed or the handler is shutting down.
	if !conn.Deliver(data) {
		return protocol.ErrConnectionClosed
	}
	return protocol.ErrNone
}

// OnClose handles connection termination from agents. It cleans up the
// connection state.
func (s *ProxyServer) OnClose(connectionID uuid.UUID, errorCode byte) byte {
	value, ok := s.Connections.Load(connectionID)
	if !ok {
		return protocol.ErrNone // Connection already removed, nothing to do
	}
	conn := value.(*protocol.Connection)
	conn.Close()
	s.Connections.Delete(connectionID)
	return protocol.ErrNone
}

// cleanupConnection closes all connection resources and removes from connection map.
// This helper reduces code duplication in handleConnection.
func (s *ProxyServer) cleanupConnection(connID uuid.UUID, clientConn net.Conn, proxyConn *protocol.Connection) {
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
func (s *ProxyServer) acceptLoop() {
	for {
		select {
		case <-s.Ctx.Done():
			return
		default:
			conn, err := s.Listener.Accept()
			if err != nil {
				if s.Ctx.Err() != nil {
					return // Exit quietly on shutdown
				}

				if _, ok := err.(net.Error); ok {
					continue // Retry on temporary network errors
				}
				return
			}

			go s.handleConnection(conn)
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
func (s *ProxyServer) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	// Enable TCP_NODELAY to disable Nagle's algorithm for better TLS performance
	if tcpConn, ok := clientConn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	connID := uuid.New()
	proxyConn := protocol.NewConnection(connID, s.Ctx.Done())
	s.Connections.Store(proxyConn.ID, proxyConn)

	// 1. Initiate connection with the agent
	errCode := s.SendNewConnection(connID)
	if errCode != protocol.ErrNone {
		s.Connections.Delete(connID)
		return
	}

	// 2. Wait for the agent's acknowledgment. OnAck publishes the virtual
	// connection and closes Established() from the receive goroutine, so we block
	// on that signal rather than polling for the pointer.
	select {
	case <-s.Ctx.Done():
		s.SendClose(connID, protocol.ErrHandlerStopped)
		s.Connections.Delete(connID)
		return
	case <-time.After(AckTimeout):
		s.SendClose(connID, protocol.ErrTransportTimeout)
		s.Connections.Delete(connID)
		return
	case <-proxyConn.Established():
		// Agent acknowledged connection.
	}

	// 3. Connection established, start bidirectional forwarding using io.Copy.
	// Load the virtual connection once; it is stable for the connection's life.
	protoConn := proxyConn.ProtocolConn()
	errCh := make(chan error, 2)

	// Client → Agent
	go func() {
		_, err := io.Copy(protoConn, clientConn)
		errCh <- err
	}()

	// Agent → Client
	go func() {
		_, err := io.Copy(clientConn, protoConn)
		errCh <- err
	}()

	// Wait for BOTH directions to complete before cleaning up
	var err1, err2 error
	for i := 0; i < 2; i++ {
		select {
		case <-s.Ctx.Done():
			s.cleanupConnection(connID, clientConn, proxyConn)
			return
		case <-proxyConn.Closed:
			s.cleanupConnection(connID, clientConn, proxyConn)
			return
		case err := <-errCh:
			if err1 == nil {
				err1 = err
			} else {
				err2 = err
			}
		}
	}

	// Both directions finished - NOW clean up
	s.cleanupConnection(connID, clientConn, proxyConn)

	// Log errors if any
	for _, err := range []error{err1, err2} {
		if err != nil && !errors.Is(err, io.EOF) {
			log.Debug().Err(err).Str("conn_id", connID.String()).Msg("Connection closed with error")
		}
	}
}
