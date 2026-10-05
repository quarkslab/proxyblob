package protocol

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ProtocolConn implements net.Conn by wrapping the protocol layer.
// This allows using standard io.Copy for data transfer, just like armon/go-socks5.
type ProtocolConn struct {
	readMu       sync.Mutex
	writeMu      sync.Mutex
	writeEnded   bool
	writeEndErr  error
	readErr      error
	eof          chan struct{}
	eofOnce      sync.Once
	id           uuid.UUID
	handler      *BaseHandler
	readBuffer   chan []byte // internal buffer for received data
	readData     []byte      // partial read buffer
	readOffset   int
	closed       chan struct{}
	closeOnce    sync.Once // guards SendClose (protocol-level close)
	shutdownOnce sync.Once // guards close(closed) (local shutdown)
	ctx          context.Context
}

// NewProtocolConn creates a virtual connection that uses the protocol layer.
func NewProtocolConn(ctx context.Context, id uuid.UUID, handler *BaseHandler) *ProtocolConn {
	return &ProtocolConn{
		id:         id,
		eof:        make(chan struct{}),
		handler:    handler,
		readBuffer: make(chan []byte, 1024), // Buffered channel for received data
		closed:     make(chan struct{}),
		ctx:        ctx,
	}
}

// Read implements net.Conn.Read - reads data received via protocol.
func (c *ProtocolConn) Read(b []byte) (n int, err error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if len(b) == 0 {
		return 0, nil
	}
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	case <-c.ctx.Done():
		return 0, c.ctx.Err()
	default:
	}
	// If we have buffered data, return it first
	if c.readOffset < len(c.readData) {
		n = copy(b, c.readData[c.readOffset:])
		c.readOffset += n
		if c.readOffset >= len(c.readData) {
			c.readData = nil
			c.readOffset = 0
		}
		return n, nil
	}

	// Graceful EOF is separate from abort and follows the final enqueue.
	var (
		data []byte
		ok   bool
	)
	select {
	case data, ok = <-c.readBuffer:
	default:
		select {
		case <-c.eof:
			// EOF is published only after the last enqueue. Recheck the queue.
			select {
			case data, ok = <-c.readBuffer:
			default:
				return 0, c.readErr
			}
		case <-c.closed:
			return 0, net.ErrClosed
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		case data, ok = <-c.readBuffer:
		}
	}
	if !ok {
		// readBuffer was closed.
		return 0, c.readErr
	}

	// Copy as much as possible to b
	n = copy(b, data)
	// If there's leftover data, store it
	if n < len(data) {
		c.readData = data
		c.readOffset = n
	}
	return n, nil
}

// Write implements net.Conn.Write - sends data via protocol.
func (c *ProtocolConn) Write(b []byte) (n int, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeEnded {
		return 0, io.ErrClosedPipe
	}
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	case <-c.ctx.Done():
		return 0, c.ctx.Err()
	default:
	}
	for len(b) > 0 {
		size := min(len(b), MaxPacketDataSize)
		if err := c.handler.sendData(c.id, b[:size], c.closed); err != nil {
			return n, err
		}
		n += size
		b = b[size:]
	}
	return n, nil
}

// CloseWrite sends directional EOF after every preceding Write completes.
// The opposite direction remains readable until its own EOF.
func (c *ProtocolConn) CloseWrite() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeEnded {
		return c.writeEndErr
	}
	c.writeEnded = true
	c.writeEndErr = c.handler.sendConfirmed(CmdEOF, c.id, nil, c.closed)
	return c.writeEndErr
}

func (c *ProtocolConn) finishRead(err error) { c.eofOnce.Do(func() { c.readErr = err; close(c.eof) }) }

// Shutdown closes the ProtocolConn's closed channel without sending a CmdClose packet.
// Called by Connection.Close() to unblock delivery goroutines and ProtocolConn.Read().
// Safe to call multiple times.
func (c *ProtocolConn) Shutdown() {
	c.shutdownOnce.Do(func() {
		close(c.closed)
	})
}

// Close implements net.Conn.Close.
// Sends a CmdClose packet and then shuts down locally.
func (c *ProtocolConn) Close() error {
	c.closeOnce.Do(func() {
		c.Shutdown()
		go c.handler.SendClose(c.id, ErrConnectionClosed)
	})
	c.Shutdown()
	return nil
}

// LocalAddr implements net.Conn.LocalAddr (returns dummy address).
func (c *ProtocolConn) LocalAddr() net.Addr {
	return &protocolAddr{network: "protocol", address: c.id.String()}
}

// RemoteAddr implements net.Conn.RemoteAddr (returns dummy address).
func (c *ProtocolConn) RemoteAddr() net.Addr {
	return &protocolAddr{network: "protocol", address: "remote"}
}

// SetDeadline implements net.Conn.SetDeadline (not implemented).
func (c *ProtocolConn) SetDeadline(t time.Time) error {
	return errors.ErrUnsupported
}

// SetReadDeadline implements net.Conn.SetReadDeadline (not implemented).
func (c *ProtocolConn) SetReadDeadline(t time.Time) error {
	return errors.ErrUnsupported
}

// SetWriteDeadline implements net.Conn.SetWriteDeadline (not implemented).
func (c *ProtocolConn) SetWriteDeadline(t time.Time) error {
	return errors.ErrUnsupported
}

// DeliverData is called by the protocol handler when data arrives for this connection.
func (c *ProtocolConn) DeliverData(data []byte) bool {
	select {
	case <-c.closed:
		return false
	case <-c.ctx.Done():
		return false
	default:
	}
	select {
	case <-c.closed:
		return false
	case <-c.ctx.Done():
		return false
	case c.readBuffer <- data:
		return true
	}
}

// protocolAddr implements net.Addr for protocol connections.
type protocolAddr struct {
	network string
	address string
}

func (a *protocolAddr) Network() string {
	return a.network
}

func (a *protocolAddr) String() string {
	return a.address
}
