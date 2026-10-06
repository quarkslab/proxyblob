// Package protocol defines the communication protocol between proxy and agent.
package protocol

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"syscall"
)

// Protocol error codes for agent-server communication.
// Uses byte values to minimize binary size and network traffic.
const (
	// General errors (0-9)
	ErrNone            byte = 0 // Operation completed successfully
	ErrInvalidCommand  byte = 1 // Command type is not recognized
	ErrContextCanceled byte = 2 // Context canceled

	// Connection errors (10-19)
	ErrConnectionClosed   byte = 10 // Connection was terminated
	ErrConnectionNotFound byte = 11 // Connection ID does not exist
	ErrConnectionExists   byte = 12 // Connection ID already in use
	ErrInvalidState       byte = 13 // Connection in wrong state for operation
	ErrPacketSendFailed   byte = 14 // Packet transmission failed
	ErrHandlerStopped     byte = 15 // Protocol handler is not running
	ErrUnexpectedPacket   byte = 16 // Received unexpected packet type
	ErrBufferFull         byte = 17 // Per-connection delivery buffer is full

	// Transport errors (20-29)
	ErrTransportClosed  byte = 20 // Transport layer terminated
	ErrTransportTimeout byte = 21 // Transport operation timed out
	ErrTransportError   byte = 22 // Transport operation failed

	// SOCKS errors (30-39)
	ErrInvalidSocksVersion byte = 30 // Unsupported SOCKS protocol version
	ErrUnsupportedCommand  byte = 31 // SOCKS command not implemented
	ErrHostUnreachable     byte = 32 // Target host not accessible
	ErrConnectionRefused   byte = 33 // Target refused connection
	ErrNetworkUnreachable  byte = 34 // Network path not accessible
	ErrAddressNotSupported byte = 35 // Address format not supported
	ErrTTLExpired          byte = 36 // Time-to-live exceeded
	ErrGeneralSocksFailure byte = 37 // Unspecified SOCKS failure
	ErrAuthFailed          byte = 38 // Authentication rejected

	// Packet errors (40-49)
	ErrInvalidPacket byte = 40 // Malformed packet structure
)

// MapNetError converts a network error to an appropriate protocol error code.
// This consolidates error mapping logic used throughout the codebase.
func MapNetError(err error) byte {
	if err == nil {
		return ErrNone
	}

	// Check for timeout
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return ErrTTLExpired
	}

	// Check for net.OpError (most common network error type)
	if opErr, ok := err.(*net.OpError); ok {
		if errors.Is(opErr, net.ErrClosed) {
			return ErrTransportClosed
		}
		switch opErr.Op {
		case "dial":
			return ErrNetworkUnreachable
		case "read", "write":
			return ErrHostUnreachable
		case "listen":
			return ErrNetworkUnreachable
		}
	}

	// Preserve the empty-resolution failure without an agent-side explanation.
	if errors.Is(err, ErrNoDNSAddresses) {
		return ErrHostUnreachable
	}

	// Check for DNS errors
	if _, ok := err.(*net.DNSError); ok {
		return ErrHostUnreachable
	}

	// Check for closed connection
	if errors.Is(err, net.ErrClosed) {
		return ErrTransportClosed
	}

	// Default to connection refused
	return ErrConnectionRefused
}

// Error carries an application-owned diagnostic without an agent-side description.
// Existing byte wire codes are unchanged. Codes below 128 may be used by local
// diagnostics; adding one does not introduce a new packet or protocol version.
type Error byte

func (e Error) Error() string { return strconv.Itoa(int(e)) }

// Local diagnostic errors. These are not new wire messages.
const (
	ErrShortPacket        Error = 41
	ErrMalformedPacket    Error = 42
	ErrUnsupportedVersion Error = 43
	ErrFlowControl        Error = 44
	ErrCapacity           Error = 45
	ErrDatagramDropped    Error = 46
	ErrInvalidFlowConfig  Error = 47
	ErrInvalidBindTimeout Error = 48
	ErrJSHostProtocol     Error = 49
	ErrJSHostUnsupported  Error = 50
	ErrReceivePanic       Error = 51
	ErrWriteDrain         Error = 52
	ErrDeliveryDrain      Error = 53
	ErrPeerDrain          Error = 54
	ErrBootstrapNamespace Error = 55
	ErrNoBindPeers        Error = 56
	ErrNoDNSAddresses     Error = 57
	ErrNoBindInterface    Error = 58
)

// ErrorCode never formats an underlying error (which may contain credentials).
// It preserves our numeric diagnostics through wrappers and sanitizes other causes.
func ErrorCode(err error) byte {
	if err == nil {
		return ErrNone
	}
	var code Error
	if errors.As(err, &code) {
		return byte(code)
	}
	if errors.Is(err, context.Canceled) {
		return ErrContextCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return ErrTransportTimeout
	}
	return ErrTransportError
}

// ErrStreamCanceled identifies an explicit local shutdown, not a failed transfer.
// Older proxies retain this unknown numeric code as a warning.
const (
	ErrStreamCanceled     byte = 59
	ErrStreamReset        byte = 60
	ErrStreamBrokenPipe   byte = 61
	ErrStreamNotConnected byte = 62
)

// StreamErrorCode classifies every cause in a joined forwarding error. An
// expected close must never hide a reset, timeout, or another real failure.
func StreamErrorCode(err error) byte {
	if err == nil {
		return ErrNone
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		code := ErrNone
		for _, cause := range joined.Unwrap() {
			next := StreamErrorCode(cause)
			if next != ErrNone && !IsStreamClosure(next) {
				return next
			}
			if next != ErrNone && (code == ErrNone || code == ErrStreamCanceled) {
				code = next
			}
		}
		return code
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return StreamErrorCode(wrapped.Unwrap())
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, context.Canceled) {
		return ErrStreamCanceled
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return ErrStreamReset
	}
	if errors.Is(err, syscall.ENOTCONN) {
		return ErrStreamNotConnected
	}
	if errors.Is(err, syscall.EPIPE) {
		return ErrStreamBrokenPipe
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return ErrTransportTimeout
	}
	return ErrorCode(err)
}

// IsStreamClosure identifies socket lifecycle diagnostics normally kept at debug.
// These must not override a timeout, protocol error, or unknown failure.
func IsStreamClosure(code byte) bool {
	return code == ErrStreamCanceled || code == ErrStreamReset || code == ErrStreamBrokenPipe || code == ErrStreamNotConnected
}
