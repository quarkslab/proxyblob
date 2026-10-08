package mux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"proxyblob/internal/diag"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
)

type shortConn struct {
	net.Conn
	mu      sync.Mutex
	wire    bytes.Buffer
	limit   int
	failure error
}

func (c *shortConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := min(c.limit, len(p))
	c.wire.Write(p[:n])
	return n, c.failure
}
func (c *shortConn) SetDeadline(time.Time) error { return nil }

func TestConfirmedWritesAndShortCounts(t *testing.T) {
	for _, tc := range []struct {
		name      string
		limit     int
		err       error
		wantError bool
	}{
		{"short progress", 3, nil, false}, {"zero progress", 0, nil, true}, {"partial error", 3, io.ErrUnexpectedEOF, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := &shortConn{limit: tc.limit, failure: tc.err}
			h := NewBaseHandler(context.Background(), wire)
			defer h.Abort()
			id := uuid.New()
			c := NewConnection(id, h.Ctx.Done())
			h.Connections.Store(id, c)
			c.setPeerWindow(uint64(DefaultFlowConfig().StreamWindow))
			pc := NewProtocolConn(h.Ctx, id, h)
			c.SetProtocolConn(pc)
			n, err := pc.Write([]byte("request"))
			if tc.wantError {
				if err == nil || n != 0 {
					t.Fatalf("discarded bytes reported successful: %d %v", n, err)
				}
				return
			}
			if err != nil || n != 7 {
				t.Fatalf("Write: %d %v", n, err)
			}
			if err := pc.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if n, err := pc.Write([]byte("late")); n != 0 || err == nil {
				t.Fatalf("write after EOF: %d %v", n, err)
			}
			if err := h.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
			want := append(NewPacket(CmdData, id, []byte("request")).Encode(), NewPacket(CmdEOF, id, nil).Encode()...)
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if !bytes.Equal(wire.wire.Bytes(), want) {
				t.Fatal("short writes lost, duplicated, or reordered records")
			}
		})
	}
}

func TestBlockedWriteAbortAndBoundedDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, drain := range []bool{false, true} {
			local, peer := net.Pipe()
			h := NewBaseHandler(context.Background(), local)
			id := uuid.New()
			c := NewConnection(id, h.Ctx.Done())
			h.Connections.Store(id, c)
			c.setPeerWindow(uint64(DefaultFlowConfig().StreamWindow))
			pc := NewProtocolConn(h.Ctx, id, h)
			c.SetProtocolConn(pc)
			result := make(chan error, 1)
			go func() {
				n, err := pc.Write([]byte("pending"))
				if n != 0 || err == nil {
					result <- errors.New("reported pending bytes as delivered")
					return
				}
				result <- nil
			}()
			synctest.Wait()
			if drain {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				if err := h.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Drain: %v", err)
				}
				cancel()
			} else {
				h.Abort()
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			<-h.writerDone
			peer.Close()
			local.Close()
		}
	})
}

func TestConnectionDrainsReservedBufferBeforeEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := NewConnection(uuid.New(), ctx.Done())
		pc := NewProtocolConn(ctx, c.ID, nil)
		c.SetProtocolConn(pc)
		// Queue bytes before the worker exists, so EOF cannot overtake admission.
		for i := 0; i < 1024; i++ {
			if !c.Deliver([]byte{byte(i)}) {
				t.Fatal("rejected")
			}
		}
		c.FinishDelivery()
		c.StartDelivery()
		data, err := io.ReadAll(pc)
		if err != nil || len(data) != 1024 {
			t.Fatalf("drain: %d %v", len(data), err)
		}
		for i, b := range data {
			if b != byte(i) {
				t.Fatal("reordered data")
			}
		}
		if c.Deliver([]byte("late")) {
			t.Fatal("accepted after EOF")
		}
		c.Close()
	})
}

type closeCounter struct {
	net.Conn
	closes atomic.Int32
}

func (c *closeCounter) Close() error { c.closes.Add(1); return nil }

func TestAttachmentAndConcurrentClose(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := NewConnection(uuid.New(), nil)
		dst := &closeCounter{}
		pc := NewProtocolConn(context.Background(), c.ID, nil)
		var wg sync.WaitGroup
		wg.Add(10)
		go func() { defer wg.Done(); c.AttachDestination(dst) }()
		go func() { defer wg.Done(); c.SetProtocolConn(pc) }()
		for j := 0; j < 8; j++ {
			go func() { defer wg.Done(); c.Close() }()
		}
		wg.Wait()
		if dst.closes.Load() != 1 {
			t.Fatalf("destination disposed %d times", dst.closes.Load())
		}
		if _, err := pc.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("late protocol attachment survived: %v", err)
		}
	}
}

func TestProtocolDeadlinesAreHonest(t *testing.T) {
	pc := NewProtocolConn(context.Background(), uuid.New(), nil)
	for _, set := range []func(time.Time) error{pc.SetDeadline, pc.SetReadDeadline, pc.SetWriteDeadline} {
		for _, d := range []time.Time{{}, time.Now().Add(time.Second)} {
			if !errors.Is(set(d), errors.ErrUnsupported) {
				t.Fatal("unsupported deadline claimed success")
			}
		}
	}
}

func TestConcurrentDeliveryEOFAndAbort(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := NewConnection(uuid.New(), nil)
		pc := NewProtocolConn(context.Background(), c.ID, nil)
		c.SetProtocolConn(pc)
		c.StartDelivery()
		var wg sync.WaitGroup
		wg.Add(4)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Deliver([]byte("data"))
			}
		}()
		go func() { defer wg.Done(); c.FinishDelivery() }()
		go func() { defer wg.Done(); c.Close() }()
		go func() { defer wg.Done(); io.Copy(io.Discard, pc) }()
		wg.Wait()
	}
}

func TestAbortReleasesBlockedDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewConnection(uuid.New(), make(chan struct{}))
		pc := NewProtocolConn(context.Background(), c.ID, nil)
		c.SetProtocolConn(pc)
		c.StartDelivery()
		done := make(chan struct{})
		go func() {
			defer close(done)
			for c.Deliver([]byte("x")) {
			}
		}()
		synctest.Wait() // producer has filled the bounded receive buffer
		c.Close()
		<-done
	})
}

type streamHandler struct{ *BaseHandler }

func (h *streamHandler) Stop() { h.Abort() }
func (h *streamHandler) OnData(id uuid.UUID, data []byte) byte {
	value, ok := h.Connections.Load(id)
	if !ok {
		return diag.ErrConnectionNotFound
	}
	if !value.(*Connection).Deliver(data) {
		return diag.ErrConnectionClosed
	}
	return diag.ErrNone
}

type finalReadConn struct {
	shortConn
	data []byte
	err  error
}

func (c *finalReadConn) Read(p []byte) (int, error) {
	n := copy(p, c.data)
	c.data = c.data[n:]
	if len(c.data) == 0 {
		return n, c.err
	}
	return n, nil
}

func TestReceiveDrainsAcceptedBytesBeforeTerminalError(t *testing.T) {
	for _, terminal := range []error{io.EOF, io.ErrClosedPipe} {
		t.Run(terminal.Error(), func(t *testing.T) {
			id := uuid.New()
			want := bytes.Repeat([]byte("tail"), 15000)
			transport := &finalReadConn{shortConn: shortConn{limit: 1 << 20}, data: NewPacket(CmdData, id, want).Encode(), err: terminal}
			h := &streamHandler{NewBaseHandler(context.Background(), transport)}
			h.PacketHandler = h
			c := NewConnection(id, h.Ctx.Done())
			pc := NewProtocolConn(h.Ctx, id, h.BaseHandler)
			c.SetProtocolConn(pc)
			c.StartDelivery()
			h.Connections.Store(id, c)
			c.setPeerWindow(uint64(DefaultFlowConfig().StreamWindow))
			done := make(chan struct{})
			go func() { h.ReceiveLoop(); close(done) }()
			got, err := io.ReadAll(pc)
			if !bytes.Equal(got, want) {
				t.Fatalf("lost trailing bytes: %d", len(got))
			}
			if terminal == io.EOF {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, terminal) {
				t.Fatalf("lost terminal error: %v", err)
			}
			c.Close()
			<-done
		})
	}
}

func TestReceiveTruncatedRecordIsNotGracefulEOF(t *testing.T) {
	id := uuid.New()
	wire := append(NewPacket(CmdData, id, []byte("complete")).Encode(), byte(CmdData))
	transport := &finalReadConn{shortConn: shortConn{limit: 1 << 20}, data: wire, err: io.EOF}
	h := &streamHandler{NewBaseHandler(context.Background(), transport)}
	h.PacketHandler = h
	c := NewConnection(id, h.Ctx.Done())
	pc := NewProtocolConn(h.Ctx, id, h.BaseHandler)
	c.SetProtocolConn(pc)
	c.StartDelivery()
	h.Connections.Store(id, c)
	c.setPeerWindow(uint64(DefaultFlowConfig().StreamWindow))
	done := make(chan struct{})
	go func() { h.ReceiveLoop(); close(done) }()
	got, err := io.ReadAll(pc)
	if string(got) != "complete" || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated frame: %q %v", got, err)
	}
	c.Close()
	<-done
}

func TestStopUnblocksTransportRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, peer := net.Pipe()
		defer local.Close()
		defer peer.Close()
		h := &streamHandler{NewBaseHandler(context.Background(), local)}
		h.PacketHandler = h
		done := make(chan struct{})
		go func() { h.ReceiveLoop(); close(done) }()
		synctest.Wait()
		h.Stop()
		<-done
		<-h.writerDone
	})
}

func TestForwardFailureUnblocksOtherDirection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, peerA := net.Pipe()
		b, peerB := net.Pipe()
		defer peerA.Close()
		defer peerB.Close()
		done := make(chan error, 1)
		go func() { done <- Forward(a, b) }()
		synctest.Wait()
		peerA.Close()
		// net.Pipe has no CloseWrite. Honest failure must abort the other read.
		if err := <-done; !errors.Is(err, errors.ErrUnsupported) {
			t.Fatalf("Forward: %v", err)
		}
	})
}

func TestPeerCloseForcesUnreadDrainWithError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := NewBaseHandler(context.Background(), &shortConn{limit: 1 << 20})
		defer h.Abort()
		c := NewConnection(uuid.New(), h.Ctx.Done())
		pc := NewProtocolConn(h.Ctx, c.ID, h)
		c.SetProtocolConn(pc)
		c.StartDelivery()
		h.Connections.Store(c.ID, c)
		for i := 0; i < 1500; i++ {
			if !c.Deliver([]byte("unread")) {
				t.Fatal("delivery rejected")
			}
		}
		synctest.Wait()
		start := time.Now()
		if code := h.PeerClose(c.ID, diag.ErrNone); code != diag.ErrNone {
			t.Fatalf("PeerClose: %d", code)
		}
		<-c.Closed
		if elapsed := time.Since(start); elapsed != DrainTimeout {
			t.Fatalf("forced after %v, want %v", elapsed, DrainTimeout)
		}
		if n, err := pc.Read(make([]byte, 32)); n != 0 || !errors.Is(err, net.ErrClosed) {
			t.Fatalf("discarded bytes exposed as clean EOF: %d %v", n, err)
		}
		if _, ok := h.Connections.Load(c.ID); ok {
			t.Fatal("forced stream retained")
		}
	})
}
