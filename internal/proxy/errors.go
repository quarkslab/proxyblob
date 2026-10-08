package proxy

import (
	"proxyblob/internal/mux"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// ErrToString maps protocol error codes to human-readable messages.
// These messages are only used on the server side for logging and debugging.
var ErrToString = map[byte]string{
	// General errors
	mux.ErrNone:            "no error",
	mux.ErrInvalidCommand:  "invalid command",
	mux.ErrContextCanceled: "context canceled",

	// Connection state errors
	mux.ErrConnectionClosed:   "connection closed",
	mux.ErrStreamNotConnected: "stream socket is no longer connected",
	mux.ErrStreamReset:        "stream reset by peer",
	mux.ErrStreamBrokenPipe:   "stream write failed: broken pipe",
	mux.ErrStreamCanceled:     "stream canceled",
	mux.ErrConnectionNotFound: "connection not found",
	mux.ErrConnectionExists:   "connection already exists",
	mux.ErrInvalidState:       "invalid connection state",
	mux.ErrPacketSendFailed:   "failed to send packet",
	mux.ErrHandlerStopped:     "handler stopped",
	mux.ErrUnexpectedPacket:   "unexpected packet received",

	// Transport layer errors
	mux.ErrTransportClosed:  "transport closed",
	mux.ErrTransportTimeout: "transport timeout",
	mux.ErrTransportError:   "general transport error",

	// SOCKS reply codes
	mux.ErrInvalidSocksVersion: "invalid SOCKS version",
	mux.ErrUnsupportedCommand:  "unsupported command",
	mux.ErrHostUnreachable:     "host unreachable",
	mux.ErrConnectionRefused:   "connection refused",
	mux.ErrNetworkUnreachable:  "network unreachable",
	mux.ErrAddressNotSupported: "address type not supported",
	mux.ErrTTLExpired:          "TTL expired",
	mux.ErrGeneralSocksFailure: "general SOCKS server failure",
	mux.ErrAuthFailed:          "authentication failed",

	mux.ErrBufferFull:               "receive buffer full",
	byte(mux.ErrShortPacket):        "incomplete protocol packet",
	byte(mux.ErrMalformedPacket):    "malformed protocol framing",
	byte(mux.ErrUnsupportedVersion): "unsupported tunnel protocol version",
	byte(mux.ErrFlowControl):        "invalid receive credit",
	byte(mux.ErrCapacity):           "tunnel stream reservation exhausted",
	byte(mux.ErrDatagramDropped):    "datagram dropped at finite queue limit",
	byte(mux.ErrInvalidFlowConfig):  "invalid finite flow limits",
	byte(mux.ErrInvalidBindTimeout): "BIND timeout must be positive",
	byte(mux.ErrJSHostProtocol):     "JS socket host contract violation",
	byte(mux.ErrJSHostUnsupported):  "JS socket host version 2 required",
	byte(mux.ErrReceivePanic):       "protocol receive loop panic",
	byte(mux.ErrWriteDrain):         "tunnel write drain failed",
	byte(mux.ErrDeliveryDrain):      "tunnel delivery drain forced to abort",
	byte(mux.ErrPeerDrain):          "peer close drain forced to abort",
	byte(mux.ErrBootstrapNamespace): "incomplete bootstrap namespace",

	byte(mux.ErrNoBindPeers):     "no BIND peer addresses",
	byte(mux.ErrNoDNSAddresses):  "no DNS addresses",
	byte(mux.ErrNoBindInterface): "no usable interface for wildcard BIND",

	// Protocol packet errors
	mux.ErrInvalidPacket: "invalid protocol packet structure",
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
		if code == mux.ErrNone {
			return
		}
		event := logger.Warn()
		if mux.IsStreamClosure(code) {
			event = logger.Debug()
		}
		event.Uint8("code", code).Str("conn_id", id.String()).Msg(ErrorDescription(code))
	}
}
