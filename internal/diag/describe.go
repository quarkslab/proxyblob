package diag

// descriptions maps diagnostic codes to text for the proxy's logs.
var descriptions = map[byte]string{
	// General errors
	ErrNone:            "no error",
	ErrInvalidCommand:  "invalid command",
	ErrContextCanceled: "context canceled",

	// Connection state errors
	ErrConnectionClosed:   "connection closed",
	ErrStreamNotConnected: "stream socket is no longer connected",
	ErrStreamReset:        "stream reset by peer",
	ErrStreamBrokenPipe:   "stream write failed: broken pipe",
	ErrStreamCanceled:     "stream canceled",
	ErrConnectionNotFound: "connection not found",
	ErrConnectionExists:   "connection already exists",
	ErrInvalidState:       "invalid connection state",
	ErrPacketSendFailed:   "failed to send packet",
	ErrHandlerStopped:     "handler stopped",
	ErrUnexpectedPacket:   "unexpected packet received",

	// Transport layer errors
	ErrTransportClosed:  "transport closed",
	ErrTransportTimeout: "transport timeout",
	ErrTransportError:   "general transport error",

	// SOCKS reply codes
	ErrInvalidSocksVersion: "invalid SOCKS version",
	ErrUnsupportedCommand:  "unsupported command",
	ErrHostUnreachable:     "host unreachable",
	ErrConnectionRefused:   "connection refused",
	ErrNetworkUnreachable:  "network unreachable",
	ErrAddressNotSupported: "address type not supported",
	ErrTTLExpired:          "TTL expired",
	ErrGeneralSocksFailure: "general SOCKS server failure",
	ErrAuthFailed:          "authentication failed",

	ErrBufferFull:               "receive buffer full",
	byte(ErrShortPacket):        "incomplete protocol packet",
	byte(ErrMalformedPacket):    "malformed protocol framing",
	byte(ErrUnsupportedVersion): "unsupported tunnel protocol version",
	byte(ErrFlowControl):        "invalid receive credit",
	byte(ErrCapacity):           "tunnel stream reservation exhausted",
	byte(ErrDatagramDropped):    "datagram dropped at finite queue limit",
	byte(ErrInvalidFlowConfig):  "invalid finite flow limits",
	byte(ErrInvalidBindTimeout): "BIND timeout must be positive",
	byte(ErrJSHostProtocol):     "JS socket host contract violation",
	byte(ErrJSHostUnsupported):  "JS socket host version 2 required",
	byte(ErrReceivePanic):       "protocol receive loop panic",
	byte(ErrWriteDrain):         "tunnel write drain failed",
	byte(ErrDeliveryDrain):      "tunnel delivery drain forced to abort",
	byte(ErrPeerDrain):          "peer close drain forced to abort",
	byte(ErrBootstrapNamespace): "incomplete bootstrap namespace",

	byte(ErrNoBindPeers):     "no BIND peer addresses",
	byte(ErrNoDNSAddresses):  "no DNS addresses",
	byte(ErrNoBindInterface): "no usable interface for wildcard BIND",

	// Protocol packet errors
	ErrInvalidPacket: "invalid protocol packet structure",
}

// Description retains the code separately when a peer uses an unknown value.
func Description(code byte) string {
	if text, ok := descriptions[code]; ok {
		return text
	}
	return "unknown protocol error"
}
