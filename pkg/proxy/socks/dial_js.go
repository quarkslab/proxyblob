//go:build js

package proxy

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall/js"
	"time"
)

// inboxCapacity bounds how many inbound chunks may sit between the JS onData
// callback and the goroutine that feeds them into the pipe. It is deliberately
// bounded: an unbounded queue would only trade unbounded goroutine growth for
// unbounded memory growth whenever the reader stalls.
const inboxCapacity = 256

// errInboxOverflow is handed to the reader when more than inboxCapacity chunks
// pile up. Quietly dropping a chunk would corrupt the stream exactly as badly
// as reordering it would, so an overflow is fatal for the connection instead.
var errInboxOverflow = errors.New("inbound queue overflow: reader too slow")

// jsChunk is a single inbound event. eof marks the end of the stream and rides
// through the same queue as the data, so the writer observes it in arrival
// order, after every chunk that preceded it.
type jsChunk struct {
	data []byte
	eof  bool
}

type jsConn struct {
	writeMu    sync.Mutex
	writeEnded bool
	socket     js.Value
	pr         *io.PipeReader
	pw         *io.PipeWriter
	inbox      chan jsChunk
	closed     chan struct{}
	downOnce   sync.Once // guards close(closed) + pw teardown
	closeOnce  sync.Once // guards the pr / JS socket teardown done by Close
}

func dialTCP(target string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	conn := &jsConn{
		pr:     pr,
		pw:     pw,
		inbox:  make(chan jsChunk, inboxCapacity),
		closed: make(chan struct{}),
	}

	// Exactly ONE writer goroutine for the whole lifetime of the connection.
	// It exits via shutdown/Close, so it cannot leak even if the dial fails.
	go conn.writeLoop()

	done := make(chan error, 1)

	onConnect := js.FuncOf(func(_ js.Value, args []js.Value) any {
		conn.socket = args[0]
		select {
		case done <- nil:
		default:
		}
		return nil
	})

	onData := js.FuncOf(func(_ js.Value, args []js.Value) any {
		jsData := args[1]
		data := make([]byte, jsData.Length())
		js.CopyBytesToGo(data, jsData)

		// The enqueue happens here, synchronously, inside the callback: the
		// ordering guarantee comes from the callback's own invocation order,
		// so it must NOT be deferred to a goroutine. enqueue never blocks, so
		// the JS event loop is not stalled either.
		conn.enqueue(jsChunk{data: data})
		return nil
	})

	onClose := js.FuncOf(func(_ js.Value, args []js.Value) any {
		// The EOF marker goes through the queue rather than closing the pipe
		// straight away, so chunks still in flight are flushed to the reader
		// before it sees io.EOF.
		conn.enqueue(jsChunk{eof: true})
		return nil
	})

	onError := js.FuncOf(func(_ js.Value, args []js.Value) any {
		msg := "connection error"
		if len(args) > 0 && args[0].Type() == js.TypeString {
			msg = args[0].String()
		}
		e := fmt.Errorf("%s", msg)
		// Hard failure: tear down at once instead of draining. Whatever is
		// still queued belongs to a stream that is no longer trustworthy.
		conn.shutdown(e)
		select {
		case done <- e:
		default:
		}
		return nil
	})

	js.Global().Call("TCPDial", host, port, onConnect, onData, onClose, onError)

	if err := <-done; err != nil {
		return nil, err
	}
	return conn, nil
}

// enqueue hands one inbound event to the writer goroutine without ever
// blocking the caller (a JS callback).
//
// INVARIANT: c.inbox is never closed, by anyone, ever. Shutdown is signalled
// solely by closing c.closed, which makes a send on a closed channel
// impossible by construction no matter how a JS callback interleaves with
// Close; the buffered queue is simply collected along with the connection.
func (c *jsConn) enqueue(chunk jsChunk) {
	select {
	case c.inbox <- chunk:
	case <-c.closed:
		// Already torn down: the writer is gone and the pipe is closed, so
		// there is nobody left to hand this to.
	default:
		// Queue full. Dropping the chunk would silently corrupt the stream,
		// so the connection is failed instead and the reader gets an explicit
		// error rather than a plausible-looking but wrong byte stream.
		c.shutdown(errInboxOverflow)
	}
}

// writeLoop is the ONLY writer to c.pw. Being the only one is what preserves
// byte order: chunks reach the pipe in exactly the order onData enqueued them.
// Per-chunk goroutines would not, because the scheduler's runnext slot tends to
// run the most recently spawned goroutine first and the pipe's mutex is not
// FIFO. It runs from dialTCP until the connection is torn down.
func (c *jsConn) writeLoop() {
	for {
		select {
		case chunk := <-c.inbox:
			if chunk.eof {
				// Everything enqueued before this marker has been written, so
				// reporting EOF now cannot truncate the stream.
				c.pw.Close()
				return
			}
			if _, err := c.pw.Write(chunk.data); err != nil {
				// The pipe is gone (reader closed it, or a teardown ran).
				// Stop writing and surface the reason instead of dropping it.
				c.shutdown(err)
				return
			}
		case <-c.closed:
			return
		}
	}
}

// shutdown tears the connection down at most once: it stops the writer
// goroutine and fails the pipe with err, so a blocked reader observes a clean,
// explicit failure instead of silently truncated or corrupted data.
func (c *jsConn) shutdown(err error) {
	c.downOnce.Do(func() {
		close(c.closed)
		c.pw.CloseWithError(err)
	})
}

func (c *jsConn) Read(b []byte) (int, error) { return c.pr.Read(b) }

func (c *jsConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeEnded {
		return 0, io.ErrClosedPipe
	}
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	jsData := js.Global().Get("Uint8Array").New(len(b))
	js.CopyBytesToJS(jsData, b)
	// The coordinated host must return the accepted byte count. An absent
	// count cannot truthfully be translated into successful delivery.
	result := c.socket.Call("write", jsData)
	if result.Type() != js.TypeNumber {
		return 0, errors.ErrUnsupported
	}
	n := result.Int()
	if n < 0 || n > len(b) {
		return 0, io.ErrShortWrite
	}
	if n < len(b) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func (c *jsConn) Close() error {
	// shutdown is idempotent, so this is a no-op if a callback already ran it;
	// it also unblocks the writer goroutine should it be parked on pw.Write.
	c.shutdown(net.ErrClosed)
	c.closeOnce.Do(func() {
		c.pr.CloseWithError(net.ErrClosed)
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		if !c.writeEnded {
			c.writeEnded = true
			c.socket.Call("end")
		}
	})
	return nil
}

func (c *jsConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (c *jsConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (c *jsConn) SetDeadline(t time.Time) error      { return errors.ErrUnsupported }
func (c *jsConn) SetReadDeadline(t time.Time) error  { return errors.ErrUnsupported }
func (c *jsConn) SetWriteDeadline(t time.Time) error { return errors.ErrUnsupported }

// CloseWrite preserves the read side while asking the JS host to send FIN.
// TCPDial's host must keep reads alive after socket.end(), and report peer
// FIN through onClose without disabling writes (half-open TCP semantics).
func (c *jsConn) CloseWrite() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.writeEnded {
		return nil
	}
	select {
	case <-c.closed:
		return net.ErrClosed
	default:
	}
	c.writeEnded = true
	c.socket.Call("end")
	return nil
}
