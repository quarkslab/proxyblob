package protocol

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/google/uuid"
)

type writeRequest struct {
	data []byte
	done chan error
}

func (h *BaseHandler) enqueue(cmd byte, id uuid.UUID, data []byte, stop <-chan struct{}, confirmed bool) (chan error, error) {
	if len(data) > MaxPacketDataSize {
		return nil, ErrMalformedPacket
	}
	req := writeRequest{data: NewPacket(cmd, id, data).Encode()}
	if confirmed {
		req.done = make(chan error, 1)
	}
	h.admissionMu.RLock()
	defer h.admissionMu.RUnlock()
	select {
	case <-h.draining:
		return nil, net.ErrClosed
	case <-h.Ctx.Done():
		return nil, h.Ctx.Err()
	case <-stop:
		return nil, net.ErrClosed
	default:
	}
	select {
	case h.writeCh <- req:
		return req.done, nil
	case <-h.draining:
		return nil, net.ErrClosed
	case <-h.Ctx.Done():
		return nil, h.Ctx.Err()
	case <-stop:
		return nil, net.ErrClosed
	}
}

// Control records retain asynchronous admission so the shared receiver never
// waits on a peer that may itself be sending. Drain reports their final result.
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

func (h *BaseHandler) sendData(id uuid.UUID, data []byte, stop <-chan struct{}) error {
	if _, ok := h.Connections.Load(id); !ok {
		return net.ErrClosed
	}
	return h.sendConfirmed(CmdData, id, data, stop)
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
	for {
		var first writeRequest
		var ok bool
		select {
		case <-h.Ctx.Done():
			return
		case first, ok = <-h.writeCh:
			if !ok {
				return
			}
		}
		batch := []writeRequest{first}
		buf := append([]byte(nil), first.data...)
	collect:
		for {
			select {
			case more, open := <-h.writeCh:
				if !open {
					break collect
				}
				batch = append(batch, more)
				buf = append(buf, more.data...)
			default:
				break collect
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
	}
}

// Drain stops accepting writes and waits for every admitted record to reach
// the transport. A deadline forces cancellation and returns failure. Success
// means transport acceptance, never an acknowledgement by the remote service.
func (h *BaseHandler) Drain(ctx context.Context) error {
	h.drainOnce.Do(func() {
		close(h.draining)
		h.admissionMu.Lock()
		close(h.writeCh)
		h.admissionMu.Unlock()
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
