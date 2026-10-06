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
	owner        *Connection
	id           uuid.UUID
	handler      *BaseHandler
	closed       chan struct{}
	closeOnce    sync.Once // guards SendClose (protocol-level close)
	shutdownOnce sync.Once // guards close(closed) (local shutdown)
	ctx          context.Context
}

// NewProtocolConn creates a virtual connection that uses the protocol layer.
func NewProtocolConn(ctx context.Context, id uuid.UUID, handler *BaseHandler) *ProtocolConn {
	return &ProtocolConn{
		id:      id,
		handler: handler,
		closed:  make(chan struct{}),
		ctx:     ctx,
	}
}

// Read releases receive credit only after copying bytes out of reserved memory.
func (c *ProtocolConn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if len(b) == 0 {
		return 0, nil
	}
	if c.owner == nil {
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		}
	}
	return c.owner.read(c.ctx, c.closed, b)
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
	return c.handler.sendBytes(c.id, b, c.closed)

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
	if c.owner != nil {
		c.owner.sendMu.Lock()
		defer c.owner.sendMu.Unlock()
		c.owner.sendEnded = true
	}
	c.writeEndErr = c.handler.sendConfirmed(CmdEOF, c.id, nil, c.closed)
	return c.writeEndErr
}

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
	return c.closeWithCode(ErrStreamCanceled)
}

func (c *ProtocolConn) closeWithCode(code byte) error {
	c.closeOnce.Do(func() {
		c.Shutdown()
		c.handler.SendClose(c.id, code)
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
