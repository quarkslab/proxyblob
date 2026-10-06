// Package proxy implements a SOCKS proxy server.
package proxy

import (
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

	// Protocol packet errors
	protocol.ErrInvalidPacket: "invalid protocol packet structure",
}