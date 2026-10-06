// Package proxy implements a SOCKS proxy server.
package proxy

import (
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"proxyblob/pkg/protocol"
)

// ErrToString maps protocol error codes to human-readable messages.
// These messages are only used on the server side for logging and debugging.
var ErrToString = map[byte]string{
	// General errors
	protocol.ErrNone:            "no error",
	protocol.ErrInvalidCommand:  "invalid command",
	protocol.ErrContextCanceled: "context canceled",

	// Connection state errors
	protocol.ErrConnectionClosed:   "connection closed",
	protocol.ErrStreamNotConnected: "stream socket is no longer connected",
	protocol.ErrStreamReset:        "stream reset by peer",
	protocol.ErrStreamBrokenPipe:   "stream write failed: broken pipe",
	protocol.ErrStreamCanceled:     "stream canceled",
	protocol.ErrConnectionNotFound: "connection not found",
	protocol.ErrConnectionExists:   "connection already exists",
	protocol.ErrInvalidState:       "invalid connection state",
	protocol.ErrPacketSendFailed:   "failed to send packet",
	protocol.ErrHandlerStopped:     "handler stopped",
	protocol.ErrUnexpectedPacket:   "unexpected packet received",

	// Transport layer errors
	protocol.ErrTransportClosed:  "transport closed",
	protocol.ErrTransportTimeout: "transport timeout",
	protocol.ErrTransportError:   "general transport error",

	// SOCKS reply codes
	protocol.ErrInvalidSocksVersion: "invalid SOCKS version",
	protocol.ErrUnsupportedCommand:  "unsupported command",
	protocol.ErrHostUnreachable:     "host unreachable",
	protocol.ErrConnectionRefused:   "connection refused",
	protocol.ErrNetworkUnreachable:  "network unreachable",
	protocol.ErrAddressNotSupported: "address type not supported",
	protocol.ErrTTLExpired:          "TTL expired",
	protocol.ErrGeneralSocksFailure: "general SOCKS server failure",
	protocol.ErrAuthFailed:          "authentication failed",

	protocol.ErrBufferFull:               "receive buffer full",
	byte(protocol.ErrShortPacket):        "incomplete protocol packet",
	byte(protocol.ErrMalformedPacket):    "malformed protocol framing",
	byte(protocol.ErrUnsupportedVersion): "unsupported tunnel protocol version",
	byte(protocol.ErrFlowControl):        "invalid receive credit",
	byte(protocol.ErrCapacity):           "tunnel stream reservation exhausted",
	byte(protocol.ErrDatagramDropped):    "datagram dropped at finite queue limit",
	byte(protocol.ErrInvalidFlowConfig):  "invalid finite flow limits",
	byte(protocol.ErrInvalidBindTimeout): "BIND timeout must be positive",
	byte(protocol.ErrJSHostProtocol):     "JS socket host contract violation",
	byte(protocol.ErrJSHostUnsupported):  "JS socket host version 2 required",
	byte(protocol.ErrReceivePanic):       "protocol receive loop panic",
	byte(protocol.ErrWriteDrain):         "tunnel write drain failed",
	byte(protocol.ErrDeliveryDrain):      "tunnel delivery drain forced to abort",
	byte(protocol.ErrPeerDrain):          "peer close drain forced to abort",
	byte(protocol.ErrBootstrapNamespace): "incomplete bootstrap namespace",

	byte(protocol.ErrNoBindPeers):     "no BIND peer addresses",
	byte(protocol.ErrNoDNSAddresses):  "no DNS addresses",
	byte(protocol.ErrNoBindInterface): "no usable interface for wildcard BIND",

	// Protocol packet errors
	protocol.ErrInvalidPacket: "invalid protocol packet structure",
}

// ErrorDescription retains the code separately when a peer uses an unknown value.
func ErrorDescription(code byte) string {
	if text, ok := ErrToString[code]; ok {
		return text
	}
	return "unknown protocol error"
}

func protocolErrorReporter(logger zerolog.Logger) func(uuid.UUID, byte) {
	return func(id uuid.UUID, code byte) {
		if code == protocol.ErrNone {
			return
		}
		event := logger.Warn()
		if code == protocol.ErrStreamCanceled {
			event = logger.Debug()
		}
		event.Uint8("code", code).Str("conn_id", id.String()).Msg(ErrorDescription(code))
	}
}
