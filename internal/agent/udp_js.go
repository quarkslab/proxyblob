//go:build js

package agent

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"syscall/js"
	"time"
)

const (
	jsUDPMaxPacket    = 65507
	jsUDPQueueBytes   = 256 * 1024
	jsUDPQueuePackets = 64
)

type jsUDPPacket struct {
	data []byte
	addr *net.UDPAddr
}
type jsUDPConn struct {
	life        jsLifetime
	mu          sync.Mutex
	socket      js.Value
	port        int
	bound       bool
	packets     []jsUDPPacket
	queuedBytes int
	err         error
	changed     chan struct{}
	dl          time.Time
}

func listenUDP() (UDPRelayConn, error) { return listenUDPContext(context.Background()) }
func listenUDPContext(parent context.Context) (UDPRelayConn, error) {
	if err := requireJSHost("UDPListen"); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := &jsUDPConn{changed: make(chan struct{})}
	done := make(chan error, 1)
	complete := func(err error) {
		select {
		case done <- err:
		default:
		}
	}
	onBind := js.FuncOf(func(_ js.Value, args []js.Value) any {
		c.mu.Lock()
		if c.err == nil && !c.bound {
			c.socket = args[0]
			c.port = args[1].Int()
			c.bound = true
			complete(nil)
		}
		c.mu.Unlock()
		return nil
	})
	onData := js.FuncOf(func(_ js.Value, args []js.Value) any {
		c.mu.Lock()
		defer c.mu.Unlock()
		n := args[1].Length()
		// UDP is lossy. Drop before allocating when either budget is exhausted.
		if c.err != nil || !c.bound || n > jsUDPMaxPacket || len(c.packets) >= jsUDPQueuePackets || n > jsUDPQueueBytes-c.queuedBytes {
			return nil
		}
		data := make([]byte, n)
		js.CopyBytesToGo(data, args[1])
		c.packets = append(c.packets, jsUDPPacket{data, &net.UDPAddr{IP: net.ParseIP(args[3].String()), Port: args[2].Int()}})
		c.queuedBytes += n
		c.signal()
		return nil
	})
	onError := js.FuncOf(func(_ js.Value, args []js.Value) any {
		err := jsSocketError()
		c.fail(err)
		complete(err)
		return nil
	})
	c.life.callbacks = []js.Func{onBind, onData, onError}
	c.life.handle = js.Global().Call("UDPListen", onBind, onData, onError)
	var err error
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
func (c *jsUDPConn) signal() { close(c.changed); c.changed = make(chan struct{}) }
func (c *jsUDPConn) fail(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		c.packets = nil
		c.queuedBytes = 0
		c.signal()
	}
	c.mu.Unlock()
	c.life.dispose()
}
func (c *jsUDPConn) LocalPort() int { return c.port }
func (c *jsUDPConn) ReadFrom(b []byte) (int, *net.UDPAddr, error) {
	for {
		c.mu.Lock()
		if c.err != nil {
			err := c.err
			c.mu.Unlock()
			return 0, nil, err
		}
		if !c.dl.IsZero() && !time.Now().Before(c.dl) {
			c.mu.Unlock()
			return 0, nil, &net.OpError{Op: "read", Net: "udp", Err: os.ErrDeadlineExceeded}
		}
		if len(c.packets) > 0 {
			pkt := c.packets[0]
			c.packets[0] = jsUDPPacket{}
			c.packets = c.packets[1:]
			c.queuedBytes -= len(pkt.data)
			c.mu.Unlock()
			n := copy(b, pkt.data)
			if n < len(pkt.data) {
				return n, pkt.addr, io.ErrShortBuffer
			}
			return n, pkt.addr, nil
		}
		changed, dl := c.changed, c.dl
		c.mu.Unlock()
		var timer *time.Timer
		var timeout <-chan time.Time
		if !dl.IsZero() {
			timer = time.NewTimer(time.Until(dl))
			timeout = timer.C
		}
		select {
		case <-changed:
		case <-timeout:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}
func (c *jsUDPConn) WriteTo(b []byte, addr *net.UDPAddr) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	if len(b) > jsUDPMaxPacket {
		return io.ErrShortWrite
	}
	data := js.Global().Get("Uint8Array").New(len(b))
	js.CopyBytesToJS(data, b)
	result := c.socket.Call("send", data, addr.Port, addr.IP.String())
	if result.Type() != js.TypeBoolean || !result.Bool() {
		return io.ErrShortWrite
	}
	return nil
}
func (c *jsUDPConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dl = t
	c.signal()
	return nil
}
func (c *jsUDPConn) Close() error { c.fail(net.ErrClosed); return nil }
