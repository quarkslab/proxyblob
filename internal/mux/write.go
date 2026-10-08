package mux

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"proxyblob/internal/diag"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

const maxPendingDataRecordsPerStream = 64

type dataReservation struct{ bytes, records int }

type writeRequest struct {
	isData   bool
	payload  int
	progress *atomic.Int64
	data     []byte
	done     chan error
	id       uuid.UUID
	credit   bool
}

func (h *BaseHandler) signalWriter() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *BaseHandler) enqueue(cmd byte, id uuid.UUID, data []byte, stop <-chan struct{}, confirmed bool, progress ...*atomic.Int64) (chan error, error) {
	limit := 16 // largest control payload: CREDIT
	switch cmd {
	case CmdData:
		limit = h.flow.DataFrame
	case CmdDatagram:
		limit = MaxDatagramSize
	}
	if len(data) > limit {
		return nil, diag.ErrMalformedPacket
	}

	h.queueMu.Lock()
	defer h.queueMu.Unlock()
	for cmd == CmdData || cmd == CmdDatagram {
		if cmd == CmdDatagram && len(data) > h.flow.StreamWindow {
			return nil, diag.ErrDatagramDropped
		}
		pending := h.pendingData[id]
		if len(data) > h.flow.StreamWindow {
			return nil, diag.ErrMalformedPacket
		}
		if pending.bytes+len(data) <= h.flow.StreamWindow &&
			h.dataBytes+len(data) <= h.flow.TunnelWindow &&
			pending.records < maxPendingDataRecordsPerStream && h.dataRecords < maxPendingDataRecordsPerStream*h.flow.MaxStreams {
			break
		}
		if cmd == CmdDatagram {
			return nil, diag.ErrDatagramDropped
		}
		space := h.dataSpace
		h.queueMu.Unlock()
		select {
		case <-space:
		case <-stop:
		case <-h.Ctx.Done():
		case <-h.draining:
		}
		h.queueMu.Lock()
		select {
		case <-stop:
			return nil, net.ErrClosed
		case <-h.Ctx.Done():
			return nil, h.Ctx.Err()
		case <-h.draining:
			return nil, net.ErrClosed
		default:
		}
	}
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
	if cmd == CmdData || cmd == CmdDatagram {
		// Charge queued AND in-flight records until the transport write ends.
		req.isData = true
		req.payload = len(data)
		if len(progress) > 0 {
			req.progress = progress[0]
		}
		pending := h.pendingData[id]
		pending.bytes += len(data)
		pending.records++
		h.pendingData[id] = pending
		h.dataBytes += len(data)
		h.dataRecords++
		if len(h.data[id]) == 0 {
			h.dataReady = append(h.dataReady, id)
		}
		h.data[id] = append(h.data[id], req)
	} else {
		if len(h.controls) >= h.flow.ControlSlots {
			h.Cancel()
			return nil, diag.ErrCapacity
		}
		h.controls = append(h.controls, req)
	}
	h.signalWriter()
	return req.done, nil
}

// discardData removes unsent records after stream cancellation. An in-flight
// batch remains charged until Write returns and necessarily precedes CLOSE.
func (h *BaseHandler) discardData(id uuid.UUID) {
	h.queueMu.Lock()
	defer h.queueMu.Unlock()
	queue := h.data[id]
	if len(queue) == 0 {
		return
	}
	pending := h.pendingData[id]
	for _, req := range queue {
		pending.bytes -= req.payload
		pending.records--
		h.dataBytes -= req.payload
		h.dataRecords--
		if req.done != nil {
			req.done <- net.ErrClosed
		}
	}
	delete(h.data, id)
	if pending.records == 0 {
		delete(h.pendingData, id)
	} else {
		h.pendingData[id] = pending
	}
	for i, ready := range h.dataReady {
		if ready == id {
			h.dataReady = append(h.dataReady[:i], h.dataReady[i+1:]...)
			break
		}
	}
	close(h.dataSpace)
	h.dataSpace = make(chan struct{})
}

// Credit has one coalescing slot per stream, never an unbounded goroutine or
// a blocking send from the shared receiver/application reader. It carries the
// cumulative bytes consumed and the receiver's current window.
func (h *BaseHandler) queueCredit(id uuid.UUID, consumed uint64, window int) {
	h.queueMu.Lock()
	defer h.queueMu.Unlock()
	select {
	case <-h.draining:
		return
	case <-h.Ctx.Done():
		return
	default:
	}
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, consumed)
	binary.BigEndian.PutUint64(b[8:], uint64(window))
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
		return diag.ErrHandlerStopped
	}
	return diag.ErrNone
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
		size, err := c.acquireCredit(min(len(data), h.flow.DataFrame, h.flow.StreamWindow), stop)
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
	defer func() {
		h.queueMu.Lock()
		defer h.queueMu.Unlock()
		h.data = nil
		h.dataReady = nil
		h.pendingData = nil
		h.controls = nil
		h.credits = nil
		h.dataBytes = 0
		h.dataRecords = 0
		close(h.dataSpace)
	}()
	controlRun := 0
	for {
		h.queueMu.Lock()
		req := h.nextRequest(&controlRun)
		empty := len(h.controls)+len(h.data) == 0
		h.queueMu.Unlock()
		if req == nil {
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
		// One record per write: the transport (aznet) coalesces queued writes
		// into storage requests itself, so batching here would only add a copy.
		err := writeAll(h.conn, req.data)
		if err != nil {
			h.writeErrMu.Lock()
			h.writeErr = err
			h.writeErrMu.Unlock()
		}
		h.queueMu.Lock()
		if req.isData {
			pending := h.pendingData[req.id]
			pending.bytes -= req.payload
			pending.records--
			if pending.records == 0 {
				delete(h.pendingData, req.id)
			} else {
				h.pendingData[req.id] = pending
			}
			h.dataBytes -= req.payload
			h.dataRecords--
			if err == nil && req.progress != nil {
				req.progress.Add(int64(req.payload))
			}
		}
		if req.done != nil {
			req.done <- err
		}
		close(h.dataSpace)
		h.dataSpace = make(chan struct{})
		h.queueMu.Unlock()
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

// nextRequest dequeues the next record under queueMu: controls first, but
// after eight consecutive controls a waiting data record goes next; data
// streams take turns. It returns nil when nothing is queued.
func (h *BaseHandler) nextRequest(controlRun *int) *writeRequest {
	if len(h.controls)+len(h.data) == 0 {
		return nil
	}
	control := len(h.controls) > 0 && (*controlRun < 8 || len(h.data) == 0)
	var req *writeRequest
	if control {
		req = h.controls[0]
		h.controls[0] = nil
		h.controls = h.controls[1:]
		*controlRun++
	} else {
		id := h.dataReady[0]
		queue := h.data[id]
		req = queue[0]
		queue[0] = nil
		queue = queue[1:]
		h.dataReady = h.dataReady[1:]
		if len(queue) == 0 {
			delete(h.data, id)
		} else {
			h.data[id] = queue
			h.dataReady = append(h.dataReady, id)
		}
		*controlRun = 0
	}
	if req.credit {
		delete(h.credits, req.id)
	}
	return req
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
	closeBoth := func() {
		abort.Do(func() {
			for _, conn := range []net.Conn{a, b} {
				if pc, ok := conn.(*ProtocolConn); ok {
					pc.Shutdown()
				} else {
					conn.Close()
				}
			}
		})
	}
	copyDirection := func(dst, src net.Conn) {
		var err error
		if pc, ok := dst.(*ProtocolConn); ok {
			_, err = pc.copyFrom(src)
		} else {
			_, err = io.Copy(dst, src)
		}
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
	err := errors.Join(err1, err2)
	if err != nil {
		// Both copies have finished: a shutdown error must not mask a concurrent
		// failure from the other direction in the peer's close diagnosis.
		for _, conn := range []net.Conn{a, b} {
			if pc, ok := conn.(*ProtocolConn); ok {
				pc.closeWithCode(diag.StreamErrorCode(err))
			}
		}
	}
	return err
}
