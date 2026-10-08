//go:build js

package netenv

import (
	"context"
	"errors"
	"io"
	"net"
	"proxyblob/internal/mux"
	"sync"
	"syscall/js"
	"time"
)

// At most one pull is outstanding. Both the callback and the retained Go
// buffer are bounded; consuming the chunk grants the next pull.
const jsTCPChunkBytes = 64 * 1024

var errJSHostProtocol = mux.ErrJSHostProtocol

type jsConn struct {
	writeMu    sync.Mutex
	life       jsLifetime
	mu         sync.Mutex
	socket     js.Value
	data       []byte
	requested  bool
	connected  bool
	eof        bool
	writeEnded bool
	err        error
	changed    chan struct{}
}

func DialTCP(target string) (net.Conn, error) { return DialTCPContext(context.Background(), target) }

func DialTCPContext(parent context.Context, target string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, err
	}
	if err := requireJSHost("TCPDial"); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := &jsConn{changed: make(chan struct{})}
	done := make(chan error, 1)
	complete := func(err error) {
		select {
		case done <- err:
		default:
		}
	}
	onConnect := js.FuncOf(func(_ js.Value, args []js.Value) any {
		c.mu.Lock()
		if c.err == nil && !c.connected {
			c.socket = args[0]
			c.connected = true
			complete(nil)
		}
		c.mu.Unlock()
		return nil
	})
	onData := js.FuncOf(func(_ js.Value, args []js.Value) any {
		c.mu.Lock()
		if c.err != nil {
			c.mu.Unlock()
			return nil
		}
		n := args[1].Length()
		if !c.connected || !c.requested || c.eof || n <= 0 || n > jsTCPChunkBytes {
			c.mu.Unlock()
			c.fail(errJSHostProtocol)
			complete(errJSHostProtocol)
			return nil
		}
		c.data = make([]byte, n)
		js.CopyBytesToGo(c.data, args[1])
		c.requested = false
		c.signal()
		c.mu.Unlock()
		return nil
	})
	onClose := js.FuncOf(func(_ js.Value, _ []js.Value) any {
		c.mu.Lock()
		connected := c.connected
		if connected && c.err == nil {
			c.eof = true
			c.signal()
		}
		c.mu.Unlock()
		if !connected {
			c.fail(net.ErrClosed)
			complete(net.ErrClosed)
		}
		return nil
	})
	onError := js.FuncOf(func(_ js.Value, args []js.Value) any {
		err := jsSocketError()
		c.fail(err)
		complete(err)
		return nil
	})
	onWritable := js.FuncOf(func(_ js.Value, _ []js.Value) any {
		c.mu.Lock()
		if c.err == nil {
			c.signal()
		}
		c.mu.Unlock()
		return nil
	})
	c.life.callbacks = []js.Func{onConnect, onData, onClose, onError, onWritable}
	// Host v2 never invokes a callback inline. The handle is installed before
	// any callback can run, so every terminal path can detach and release safely.
	c.life.handle = js.Global().Call("TCPDial", host, port, onConnect, onData, onClose, onError, onWritable)
	select {
	case err = <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err == nil {
		err = ctx.Err()
	}
	c.mu.Lock()
	if err == nil {
		err = c.err
	}
	c.mu.Unlock()
	if err != nil {
		c.fail(err)
		return nil, err
	}
	return c, nil
}

// signal runs with mu held. No goroutine or pipe owns a second copy of data.
func (c *jsConn) signal() { close(c.changed); c.changed = make(chan struct{}) }
func (c *jsConn) fail(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		c.data = nil
		c.signal()
	}
	c.mu.Unlock()
	c.life.dispose()
}
func (c *jsConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for {
		c.mu.Lock()
		if c.err != nil {
			err := c.err
			c.mu.Unlock()
			return 0, err
		}
		if len(c.data) > 0 {
			n := copy(b, c.data)
			c.data = c.data[n:]
			if len(c.data) == 0 {
				c.data = nil
			}
			c.mu.Unlock()
			return n, nil
		}
		if c.eof {
			c.mu.Unlock()
			return 0, io.EOF
		}
		if !c.requested {
			c.requested = true
			c.life.handle.Call("read", jsTCPChunkBytes)
		}
		changed := c.changed
		c.mu.Unlock()
		<-changed
	}
}
func (c *jsConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	written := 0
	for {
		c.mu.Lock()
		if c.err != nil {
			err := c.err
			c.mu.Unlock()
			return written, err
		}
		if c.writeEnded {
			c.mu.Unlock()
			return written, io.ErrClosedPipe
		}
		if written == len(b) {
			c.mu.Unlock()
			return written, nil
		}
		chunk := b[written:min(len(b), written+jsTCPChunkBytes)]
		data := js.Global().Get("Uint8Array").New(len(chunk))
		js.CopyBytesToJS(data, chunk)
		result := c.socket.Call("write", data)
		if result.Type() == js.TypeString && result.String() == "would-block" {
			changed := c.changed
			c.mu.Unlock()
			<-changed
			continue
		}
		c.mu.Unlock()
		if result.Type() != js.TypeNumber {
			return written, errors.ErrUnsupported
		}
		n := result.Int()
		if result.Float() != float64(n) || n < 0 || n > len(chunk) {
			return written, io.ErrShortWrite
		}
		written += n
		if n < len(chunk) {
			return written, io.ErrShortWrite
		}
	}
}
func (c *jsConn) Close() error { c.fail(net.ErrClosed); return nil }
func (c *jsConn) CloseWrite() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if c.writeEnded {
		return nil
	}
	c.writeEnded = true
	c.socket.Call("end")
	return nil
}
func (c *jsConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *jsConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *jsConn) SetDeadline(time.Time) error      { return errors.ErrUnsupported }
func (c *jsConn) SetReadDeadline(time.Time) error  { return errors.ErrUnsupported }
func (c *jsConn) SetWriteDeadline(time.Time) error { return errors.ErrUnsupported }
