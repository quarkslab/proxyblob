package protocol

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
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

// copyFrom pipelines socket reads into reserved outbound capacity. Unlike
// Write, it can read the next frame before the preceding upload completes.
// Success still requires confirmation of every byte; Forward emits EOF only
// after this barrier. One scratch frame is the only unqueued payload storage.
func (c *ProtocolConn) copyFrom(src net.Conn) (n64 int64, err error) {
	defer func() {
		if err != nil {
			c.handler.writeErrMu.Lock()
			err = errors.Join(err, c.handler.writeErr)
			c.handler.writeErrMu.Unlock()
		}
	}()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeEnded {
		return 0, io.ErrClosedPipe
	}
	if c.owner == nil {
		return 0, net.ErrClosed
	}
	c.owner.sendMu.Lock()
	defer c.owner.sendMu.Unlock()
	finished := make(chan struct{})
	defer close(finished)
	// A failed upload must interrupt a socket Read even if the source goes idle.
	go func() {
		select {
		case <-finished:
			return
		case <-c.closed:
		case <-c.handler.Ctx.Done():
		}
		src.Close()
	}()
	var completed atomic.Int64
	var last <-chan error
	buf := make([]byte, min(c.handler.flow.DataFrame, c.handler.flow.StreamWindow))
	emptyReads := 0
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			emptyReads = 0
		} else if readErr == nil {
			emptyReads++
			if emptyReads >= 100 {
				readErr = io.ErrNoProgress
			}
		}
		for offset := 0; offset < n; {
			size, err := c.owner.acquireCredit(n-offset, c.closed)
			if err != nil {
				return completed.Load(), err
			}
			done, err := c.handler.enqueue(CmdData, c.id, buf[offset:offset+size], c.closed, true, &completed)
			if err != nil {
				return completed.Load(), err
			}
			last = done
			offset += size
		}
		if readErr != nil {
			if readErr != io.EOF {
				return completed.Load(), readErr
			}
			if last != nil {
				select {
				case err := <-last:
					if err != nil {
						return completed.Load(), err
					}
				case <-c.closed:
					return completed.Load(), net.ErrClosed
				case <-c.handler.Ctx.Done():
					return completed.Load(), c.handler.Ctx.Err()
				}
			}
			return completed.Load(), nil
		}
	}
}
