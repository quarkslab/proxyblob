// Package proxy implements SOCKS5 proxy functionality.
// It implements SOCKS5 negotiation and forwarding following RFC 1928, supporting
// CONNECT, native BIND and UDP ASSOCIATE commands with NoAuth authentication.
package proxy

import (
	"context"
	"io"
	"net"
	"slices"
	"time"

	"proxyblob/pkg/protocol"

	"github.com/google/uuid"
)

// SocksHandler implements a SOCKS5 protocol handler.
// It processes authentication, commands, and data transfer between clients
// and remote targets. The handler is safe for concurrent use.
type SocksHandler struct {
	*protocol.BaseHandler
	udpDestinations int
	bindTimeout     time.Duration
}

// Option configures agent-side SOCKS setup limits.
type Option func(*SocksHandler) error

// WithBindTimeout bounds native BIND resolution, listener setup and peer wait.
// The default is two minutes; it does not limit the accepted TCP conversation.
func WithBindTimeout(d time.Duration) Option {
	return func(h *SocksHandler) error {
		if d <= 0 {
			return protocol.ErrInvalidBindTimeout
		}
		h.bindTimeout = d
		return nil
	}
}

// NewSocksHandler creates a SOCKS5 handler with the given connection.
// The connection is used for sending and receiving protocol messages.
func NewSocksHandler(ctx context.Context, conn net.Conn, options ...Option) *SocksHandler {
	handler, err := NewSocksHandlerWithConfig(ctx, conn, protocol.DefaultFlowConfig(), options...)
	if err != nil {
		panic(err)
	}
	return handler
}

func NewSocksHandlerWithConfig(ctx context.Context, conn net.Conn, cfg protocol.FlowConfig, options ...Option) (*SocksHandler, error) {
	base, err := protocol.NewBaseHandlerWithConfig(ctx, conn, cfg)
	if err != nil {
		return nil, err
	}
	handler := &SocksHandler{BaseHandler: base, udpDestinations: cfg.UDPDestinations, bindTimeout: 2 * time.Minute}
	for _, option := range options {
		if err := option(handler); err != nil {
			base.Abort()
			return nil, err
		}
	}
	handler.PacketHandler = handler
	return handler, nil
}

// Start begins processing SOCKS5 requests. The address parameter is ignored
// because BIND creates its listener per request.
func (h *SocksHandler) Start(address string) {
	go h.ReceiveLoop()
}

// Stop aborts the handler, closing all active connections
// and canceling the context.
func (h *SocksHandler) Stop() {
	h.CloseAllConnections()
	h.Abort()
}

// OnNew handles new connection requests by initializing it and
// starting the SOCKS5 protocol flow.
func (h *SocksHandler) OnNew(connectionID uuid.UUID, data []byte) byte {
	// Check if the connection already exists
	if _, ok := h.Connections.Load(connectionID); ok {
		return protocol.ErrConnectionExists
	}

	// Create new connection
	conn, err := h.AcceptConnection(connectionID, data)
	if err != nil {
		return protocol.ErrInvalidState
	}
	if h.Ctx.Err() != nil {
		conn.Close()
		h.Connections.Delete(conn.ID)
		return protocol.ErrHandlerStopped
	}

	// Create the virtual protocol connection
	if !conn.SetProtocolConn(protocol.NewProtocolConn(h.Ctx, connectionID, h.BaseHandler)) {
		return protocol.ErrConnectionClosed
	}
	conn.StartDelivery()

	// Send ACK and process in a goroutine so ReceiveLoop never blocks on aznet writes
	go func() {
		errCode := h.SendConnAck(connectionID)
		if errCode != protocol.ErrNone {
			h.SendClose(connectionID, protocol.ErrConnectionClosed)
			return
		}
		h.processConnection(conn)
	}()
	return protocol.ErrNone
}

// OnAck reports ErrUnexpectedPacket as the agent only accepts incoming
// connections and does not initiate them.
func (h *SocksHandler) OnAck(connectionID uuid.UUID, data []byte) byte {
	return protocol.ErrUnexpectedPacket
}

// OnData admits bytes into the stream reservation without blocking dispatch.
func (h *SocksHandler) OnData(connectionID uuid.UUID, data []byte) byte {
	value, ok := h.Connections.Load(connectionID)
	if !ok {
		return protocol.ErrConnectionNotFound
	}
	conn := value.(*protocol.Connection)

	// Without a virtual protocol connection there is no reader for the payload.
	// Dropping it would be silent data loss, so report the unexpected state.
	if conn.ProtocolConn() == nil {
		return protocol.ErrInvalidState
	}

	// Delivery uses reserved memory and never blocks shared dispatch. The
	// sender must pause before exhausting its negotiated receive credit.
	if !conn.Deliver(data) {
		return protocol.ErrConnectionClosed
	}
	return protocol.ErrNone
}

// OnClose cleans up resources associated with a connection.
// It is safe to call multiple times.
func (h *SocksHandler) OnClose(connectionID uuid.UUID, errorCode byte) byte {
	return h.PeerClose(connectionID, errorCode)
}

// processConnection handles the SOCKS5 protocol flow for a single connection.
// The flow consists of three phases:
//
//  1. Authentication method negotiation
//  2. Command processing (CONNECT, BIND, UDP ASSOCIATE)
//  3. Data transfer between client and target
func (h *SocksHandler) processConnection(conn *protocol.Connection) {
	// SOCKS protocol has 3 sequential phases
	errCode := h.handleAuthNegotiation(conn)
	if errCode != protocol.ErrNone {
		h.SendClose(conn.ID, protocol.ErrNone)
		return
	}

	errCode = h.handleCommand(conn)
	if errCode != protocol.ErrNone {
		h.SendClose(conn.ID, protocol.ErrNone)
		return
	}

	errCode = h.handleDataTransfer(conn)
	if errCode != protocol.ErrNone {
		h.SendClose(conn.ID, errCode)
		return
	}
}

// SendError sends a SOCKS5 error reply to the client.
// It maps internal error codes to SOCKS5 reply codes as defined in RFC 1928.
func (h *SocksHandler) SendError(conn *protocol.Connection, errCode byte) {
	// Default to general failure
	socksReplyCode := GeneralFailure

	// Map internal error codes to SOCKS reply codes
	switch errCode {
	case protocol.ErrNone:
		socksReplyCode = Succeeded
	case protocol.ErrNetworkUnreachable:
		socksReplyCode = NetworkUnreachable
	case protocol.ErrHostUnreachable:
		socksReplyCode = HostUnreachable
	case protocol.ErrConnectionRefused:
		socksReplyCode = ConnectionRefused
	case protocol.ErrTTLExpired:
		socksReplyCode = TTLExpired
	case protocol.ErrUnsupportedCommand:
		socksReplyCode = CommandNotSupported
	case protocol.ErrAddressNotSupported:
		socksReplyCode = AddressTypeNotSupported
	}

	// Build and send error response
	response := []byte{Version5, socksReplyCode, 0x00, IPv4, 0, 0, 0, 0, 0, 0}
	h.SendData(conn.ID, response)
}

// handleAuthNegotiation processes the client's authentication method selection.
// Currently only the NO AUTHENTICATION REQUIRED (0x00) method is supported.
func (h *SocksHandler) handleAuthNegotiation(conn *protocol.Connection) byte {
	// Read SOCKS5 auth packet: [version(1)][nmethods(1)][methods(nmethods)]
	// Use stack allocation for small fixed-size header
	var headerBuf [2]byte
	header := headerBuf[:]
	if _, err := io.ReadFull(conn.ProtocolConn(), header); err != nil {
		return protocol.ErrConnectionClosed
	}

	// Check version
	if header[0] != Version5 {
		return protocol.ErrInvalidSocksVersion
	}

	// Read methods
	nmethods := int(header[1])
	if nmethods == 0 {
		return protocol.ErrInvalidPacket
	}

	// Use stack buffer for typical case (most clients send 1-2 methods)
	// Max nmethods is 255, but allocate reasonable stack space
	var methodsBuf [4]byte
	var methods []byte
	if nmethods <= 4 {
		methods = methodsBuf[:nmethods]
	} else {
		methods = make([]byte, nmethods)
	}
	if _, err := io.ReadFull(conn.ProtocolConn(), methods); err != nil {
		return protocol.ErrConnectionClosed
	}

	// Currently we only support NoAuth (0x00)
	if !slices.Contains(methods, NoAuth) {
		h.SendData(conn.ID, []byte{Version5, NoAcceptableMethods})
		return protocol.ErrAuthFailed
	}

	// Send response
	response := []byte{Version5, NoAuth}
	errCode := h.SendData(conn.ID, response)
	if errCode != protocol.ErrNone {
		return protocol.ErrConnectionClosed
	}

	return protocol.ErrNone
}

// handleCommand processes SOCKS5 commands from the client.
// Supported commands are:
//
//   - CONNECT (0x01): Establish TCP/IP stream connection
//
//   - UDP ASSOCIATE (0x03): UDP relay
//
//   - BIND (0x02): agent-side TCP listener (native only)
func (h *SocksHandler) handleCommand(conn *protocol.Connection) byte {
	// Read SOCKS5 command header: [version(1)][cmd(1)][rsv(1)][atyp(1)]
	// Use stack allocation for fixed-size header
	var headerBuf [4]byte
	header := headerBuf[:]
	if _, err := io.ReadFull(conn.ProtocolConn(), header); err != nil {
		return protocol.ErrConnectionClosed
	}

	// Check SOCKS version
	if header[0] != Version5 {
		h.SendError(conn, protocol.ErrInvalidSocksVersion)
		return protocol.ErrInvalidSocksVersion
	}

	if header[2] != 0 {
		h.SendError(conn, protocol.ErrInvalidPacket)
		return protocol.ErrInvalidPacket
	}

	cmd := header[1]
	atyp := header[3]

	// Read address based on address type
	var addr []byte
	switch atyp {
	case IPv4:
		// Use stack allocation for IPv4 (6 bytes: 4 IP + 2 port)
		var addrBuf [6]byte
		addr = addrBuf[:]
	case IPv6:
		// Use stack allocation for IPv6 (18 bytes: 16 IP + 2 port)
		var addrBuf [18]byte
		addr = addrBuf[:]
	case Domain:
		// Read domain length first
		var lenBuf [1]byte
		if _, err := io.ReadFull(conn.ProtocolConn(), lenBuf[:]); err != nil {
			return protocol.ErrConnectionClosed
		}
		domainLen := int(lenBuf[0])
		// Domain name can be up to 255 bytes, use heap allocation
		addr = make([]byte, 1+domainLen+2) // length + domain + port
		addr[0] = lenBuf[0]
		if _, err := io.ReadFull(conn.ProtocolConn(), addr[1:]); err != nil {
			return protocol.ErrConnectionClosed
		}
	default:
		h.SendError(conn, protocol.ErrAddressNotSupported)
		return protocol.ErrAddressNotSupported
	}

	// Read the address and port
	if atyp != Domain {
		if _, err := io.ReadFull(conn.ProtocolConn(), addr); err != nil {
			return protocol.ErrConnectionClosed
		}
	}

	// Build complete command data for handlers
	cmdData := append(header, addr...)

	var errCode byte
	switch cmd {
	case Connect:
		errCode = h.handleConnect(conn, cmdData)
	case Bind:
		errCode = h.handleBind(conn, cmdData)
	case UDPAssociate:
		if header[2] != 0 {
			return h.failUDPAssociate(conn, protocol.ErrInvalidPacket)
		}
		errCode = h.handleUDPAssociate(conn, cmdData[3:])
	default:
		h.SendError(conn, protocol.ErrUnsupportedCommand)
		return protocol.ErrUnsupportedCommand
	}

	return errCode
}

// handleDataTransfer manages the flow of data between client and target.
// Each command handler implements its own data transfer mechanism.
func (h *SocksHandler) handleDataTransfer(conn *protocol.Connection) byte {
	// Each command handler takes care of data transfer
	// Just wait for connection to be closed
	<-conn.Closed
	return protocol.ErrNone
}
