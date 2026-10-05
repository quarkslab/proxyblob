package protocol

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

type writeRequest struct {
	data   []byte
	done   chan error
	id     uuid.UUID
	credit bool
}

func (h *BaseHandler) signalWriter() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *BaseHandler) enqueue(cmd byte, id uuid.UUID, data []byte, stop <-chan struct{}, confirmed bool) (chan error, error) {
	if len(data) > h.flow.DataFrame && cmd == CmdData || len(data) > 12 && cmd != CmdData {
		return nil, ErrMalformedPacket
	}
	h.queueMu.Lock()
	defer h.queueMu.Unlock()
	select {
	case <-h.draining:
		return nil, net.ErrClosed
	case <-h.Ctx.Done():
		return nil, h.Ctx.Err()
	case <-stop:
		return nil, net.ErrClosed
	default:
	}
	req := &writeRequest{data: NewPacket(cmd, id, data).Encode(), id: id}
	if confirmed {
		req.done = make(chan error, 1)
	}
	if cmd == CmdData {
		// sendMu permits only one pending DATA per stream. FIFO admission gives
		// ready streams a turn before a producer can submit its next frame.
		if len(h.data) >= h.flow.MaxStreams {
			return nil, ErrCapacity
		}
		h.data = append(h.data, req)
	} else {
		if len(h.controls) >= h.flow.ControlSlots {
			h.Cancel()
			return nil, ErrCapacity
		}
		h.controls = append(h.controls, req)
	}
	h.signalWriter()
	return req.done, nil
}

// Credit has one coalescing slot per stream, never an unbounded goroutine or
// a blocking send from the shared receiver/application reader.
func (h *BaseHandler) queueCredit(id uuid.UUID, consumed uint64) {
	h.queueMu.Lock()
	defer h.queueMu.Unlock()
	select {
	case <-h.draining:
		return
	case <-h.Ctx.Done():
		return
	default:
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, consumed)
	if req := h.credits[id]; req != nil {
		req.data = NewPacket(CmdCredit, id, b).Encode()
		return
	}
	if len(h.controls) >= h.flow.ControlSlots {
		h.Cancel()
		return
	}
	req := &writeRequest{data: NewPacket(CmdCredit, id, b).Encode(), id: id, credit: true}
	h.credits[id] = req
	h.controls = append(h.controls, req)
	h.signalWriter()
}

func (h *BaseHandler) sendPacket(cmd byte, id uuid.UUID, data []byte) byte {
	if _, err := h.enqueue(cmd, id, data, nil, false); err != nil {
		return ErrHandlerStopped
	}
	return ErrNone
}
func (h *BaseHandler) sendConfirmed(cmd byte, id uuid.UUID, data []byte, stop <-chan struct{}) error {
	done, err := h.enqueue(cmd, id, data, stop, true)
	if err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-h.Ctx.Done():
		return h.Ctx.Err()
	case <-stop:
		return net.ErrClosed
	}
}

func (h *BaseHandler) sendBytes(id uuid.UUID, data []byte, stop <-chan struct{}) (int, error) {
	v, ok := h.Connections.Load(id)
	if !ok {
		return 0, net.ErrClosed
	}
	c := v.(*Connection)
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.sendEnded {
		return 0, io.ErrClosedPipe
	}
	n := 0
	for len(data) > 0 {
		select {
		case <-c.Closed:
			return n, net.ErrClosed
		case <-h.Ctx.Done():
			return n, h.Ctx.Err()
		default:
		}
		size, err := c.acquireCredit(min(len(data), h.flow.DataFrame), stop)
		if err != nil {
			return n, err
		}
		if err = h.sendConfirmed(CmdData, id, data[:size], c.Closed); err != nil {
			return n, err
		}
		n += size
		data = data[size:]
	}
	return n, nil
}

// writeAll advances only by the transport's reported count. Never replay an
// error-bearing write: even a partially accepted frame makes the tunnel fatal.
func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n < 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (h *BaseHandler) writeLoop() {
	defer close(h.writerDone)
	controlRun := 0
	for {
		h.queueMu.Lock()
		batch := make([]*writeRequest, 0, 16)
		buf := make([]byte, 0, h.flow.BatchBytes)
		started := time.Now()
		for len(h.controls)+len(h.data) > 0 && time.Since(started) < time.Millisecond {
			control := len(h.controls) > 0 && (controlRun < 8 || len(h.data) == 0)
			queue := &h.data
			if control {
				queue = &h.controls
			}
			req := (*queue)[0]
			if len(buf)+len(req.data) > h.flow.BatchBytes {
				break
			}
			(*queue)[0] = nil
			*queue = (*queue)[1:]
			if req.credit {
				delete(h.credits, req.id)
			}
			if control {
				controlRun++
			} else {
				controlRun = 0
			}
			batch = append(batch, req)
			buf = append(buf, req.data...)
		}
		empty := len(h.controls)+len(h.data) == 0
		h.queueMu.Unlock()
		if len(batch) == 0 {
			if empty {
				select {
				case <-h.draining:
					return
				default:
				}
			}
			select {
			case <-h.Ctx.Done():
				return
			case <-h.wake:
				continue
			case <-h.draining:
				continue
			}
		}
		err := writeAll(h.conn, buf)
		if err != nil {
			h.writeErrMu.Lock()
			h.writeErr = err
			h.writeErrMu.Unlock()
		}
		for _, req := range batch {
			if req.done != nil {
				req.done <- err
			}
		}
		if err != nil {
			h.Cancel()
			return
		}
		select {
		case <-h.Ctx.Done():
			return
		default:
		}
	}
}

// Drain stops accepting writes and waits for every admitted record to reach
// the transport. A deadline forces cancellation and returns failure. Success
// means transport acceptance, never an acknowledgement by the remote service.
func (h *BaseHandler) Drain(ctx context.Context) error {
	h.drainOnce.Do(func() {
		close(h.draining)
		h.signalWriter()
	})
	select {
	case <-h.writerDone:
		h.writeErrMu.Lock()
		err := h.writeErr
		h.writeErrMu.Unlock()
		if err != nil {
			return err
		}
		return h.Ctx.Err()
	case <-ctx.Done():
		h.Abort()
		return ctx.Err()
	}
}

// Forward copies both directions, propagating each clean EOF with CloseWrite.
// A failure aborts both endpoints and wakes the other copy before returning.
func Forward(a, b net.Conn) error {
	results := make(chan error, 2)
	var abort sync.Once
	closeBoth := func() { abort.Do(func() { a.Close(); b.Close() }) }
	copyDirection := func(dst, src net.Conn) {
		_, err := io.Copy(dst, src)
		if err == nil {
			if cw, ok := dst.(interface{ CloseWrite() error }); ok {
				err = cw.CloseWrite()
			} else {
				err = errors.ErrUnsupported
			}
		}
		if err != nil {
			closeBoth()
		}
		results <- err
	}
	go copyDirection(a, b)
	go copyDirection(b, a)
	err1, err2 := <-results, <-results
	return errors.Join(err1, err2)
}
