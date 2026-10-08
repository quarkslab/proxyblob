// Package netenv is the agent's network access: TCP dialing and UDP sockets,
// implemented with Go's net package natively and through the JS socket host
// when the agent runs as WebAssembly.
package netenv

import (
	"net"
	"time"
)

// UDPConn is a UDP socket that can send and receive datagrams to/from
// arbitrary addresses. Used by the SOCKS5 UDP ASSOCIATE handler.
// Implementations must be safe for concurrent use.
type UDPConn interface {
	// LocalPort returns the port this socket is bound to.
	LocalPort() int

	// ReadFrom reads a datagram into b, returning the sender's address.
	// Blocks until data arrives, the socket is closed, or the read deadline fires.
	// A short caller buffer returns io.ErrShortBuffer, never silent truncation.
	ReadFrom(b []byte) (int, *net.UDPAddr, error)

	// WriteTo sends a datagram to addr.
	WriteTo(b []byte, addr *net.UDPAddr) error

	// SetReadDeadline changes the deadline for pending and future ReadFrom calls.
	// A zero value disables the deadline.
	SetReadDeadline(t time.Time) error

	// Close closes the socket.
	Close() error
}
