package protocol

import (
	"errors"
	"io"
	"net"
	"sync/atomic"
)

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
