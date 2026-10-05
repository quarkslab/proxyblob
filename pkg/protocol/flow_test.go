package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/google/uuid"
)

// flowHandler uses the same NEW/ACK and DATA/EOF dispatch as the production
// endpoints; applications interact solely through the returned net.Conn.
type flowHandler struct{ *BaseHandler }

func (h *flowHandler) Stop() { h.Abort() }
func (h *flowHandler) OnNew(id uuid.UUID, data []byte) byte {
	c, err := h.AcceptConnection(id, data)
	if err != nil {
		return ErrInvalidState
	}
	c.SetProtocolConn(NewProtocolConn(h.Ctx, id, h.BaseHandler))
	return h.SendConnAck(id)
}
func (h *flowHandler) OnAck(id uuid.UUID, _ []byte) byte {
	v, ok := h.Connections.Load(id)
	if !ok {
		return ErrConnectionNotFound
	}
	if !v.(*Connection).SetProtocolConn(NewProtocolConn(h.Ctx, id, h.BaseHandler)) {
		return ErrInvalidState
	}
	return ErrNone
}
func (h *flowHandler) OnData(id uuid.UUID, data []byte) byte {
	v, ok := h.Connections.Load(id)
	if !ok {
		return ErrConnectionNotFound
	}
	if !v.(*Connection).Deliver(data) {
		return ErrInvalidPacket
	}
	return ErrNone
}
func (h *flowHandler) OnClose(id uuid.UUID, code byte) byte { return h.PeerClose(id, code) }

func flowPair(t testing.TB, cfg FlowConfig) (*flowHandler, *flowHandler) {
	t.Helper()
	a, b := net.Pipe()
	return flowPairOn(t, cfg, a, b)
}

func flowPairOn(t testing.TB, cfg FlowConfig, a, b net.Conn) (*flowHandler, *flowHandler) {
	t.Helper()
	makeHandler := func(c net.Conn) *flowHandler {
		base, err := NewBaseHandlerWithConfig(context.Background(), c, cfg)
		if err != nil {
			t.Fatal(err)
		}
		h := &flowHandler{base}
		h.PacketHandler = h
		go h.ReceiveLoop()
		return h
	}
	x, y := makeHandler(a), makeHandler(b)
	t.Cleanup(func() {
		x.Abort()
		y.Abort()
		a.Close()
		b.Close()
		<-x.writerDone
		<-y.writerDone
	})
	return x, y
}
func openFlow(t testing.TB, a, b *flowHandler) (*ProtocolConn, *ProtocolConn) {
	t.Helper()
	c := NewConnection(uuid.New(), a.Ctx.Done())
	if err := a.RegisterConnection(c); err != nil {
		t.Fatal(err)
	}
	if code := a.SendNewConnection(c.ID); code != ErrNone {
		t.Fatal(code)
	}
	select {
	case <-c.Established():
	case <-c.Closed:
		t.Fatal("stream refused")
	case <-a.Ctx.Done():
		t.Fatal(a.Ctx.Err())
	}
	v, ok := b.Connections.Load(c.ID)
	if !ok {
		t.Fatal("missing peer")
	}
	return c.ProtocolConn(), v.(*Connection).ProtocolConn()
}
func tinyFlow() FlowConfig {
	cfg := DefaultFlowConfig()
	cfg.StreamWindow = 32
	cfg.TunnelWindow = 128
	cfg.MaxStreams = 4
	cfg.DataFrame = 8
	cfg.BatchBytes = 64
	cfg.ControlSlots = 16
	return cfg
}

func TestFlowStalledConsumerHealthyStreamAndResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := flowPair(t, tinyFlow())
		slow, reader := openFlow(t, a, b)
		payload := bytes.Repeat([]byte("0123456789"), 20)
		done := make(chan error, 1)
		go func() {
			_, err := slow.Write(payload)
			if err == nil {
				err = slow.CloseWrite()
			}
			done <- err
		}()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("zero credit did not pause: %v", err)
		default:
		}
		reader.owner.deliveryMu.Lock()
		used := reader.owner.used
		reader.owner.deliveryMu.Unlock()
		if used != 32 {
			t.Fatalf("reservation occupancy=%d", used)
		}
		// The shared dispatcher must still admit NEW/ACK, DATA, CREDIT and FIN.
		healthy, peer := openFlow(t, a, b)
		go func() { healthy.Write([]byte("healthy")); healthy.CloseWrite() }()
		got, err := io.ReadAll(peer)
		if err != nil || string(got) != "healthy" {
			t.Fatalf("healthy: %q %v", got, err)
		}
		select {
		case <-reader.closed:
			t.Fatal("slow stream reset")
		default:
		}
		got, err = io.ReadAll(reader)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("resume: %d %v", len(got), err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestFlowBidirectionalSaturationAndOrderedEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := flowPair(t, tinyFlow())
		x, y := openFlow(t, a, b)
		left := bytes.Repeat([]byte("left"), 100)
		right := bytes.Repeat([]byte("right"), 100)
		result := make(chan error, 2)
		for _, d := range []struct {
			c *ProtocolConn
			b []byte
		}{{x, left}, {y, right}} {
			go func() {
				_, err := d.c.Write(d.b)
				if err == nil {
					err = d.c.CloseWrite()
				}
				result <- err
			}()
		}
		synctest.Wait() // both directions have exhausted their independent windows
		var wg sync.WaitGroup
		for _, d := range []struct {
			c *ProtocolConn
			b []byte
		}{{y, left}, {x, right}} {
			wg.Go(func() {
				got, err := io.ReadAll(d.c)
				if err != nil || !bytes.Equal(got, d.b) {
					t.Errorf("bytes/EOF: %d %v", len(got), err)
				}
			})
		}
		wg.Wait()
		for i := 0; i < 2; i++ {
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestFlowAggregateReservationsAndCloseWhileWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := tinyFlow()
		cfg.TunnelWindow = 64
		a, b := flowPair(t, cfg)
		x, y := openFlow(t, a, b)
		openFlow(t, a, b)
		c := NewConnection(uuid.New(), a.Ctx.Done())
		if !errors.Is(a.RegisterConnection(c), ErrCapacity) {
			t.Fatal("aggregate budget exceeded")
		}
		a.flowMu.Lock()
		reserved := a.reserved
		a.flowMu.Unlock()
		if reserved != 64 {
			t.Fatalf("outstanding grants not reserved: %d", reserved)
		}
		done := make(chan error, 1)
		go func() { _, err := x.Write(make([]byte, 128)); done <- err }()
		synctest.Wait()
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Go(func() { x.Close() })
		}
		wg.Wait()
		if err := <-done; err == nil {
			t.Fatal("waiting write silently succeeded")
		}
		synctest.Wait()
		select {
		case <-y.closed:
		default:
			t.Fatal("CLOSE starved")
		}
		if err := a.RegisterConnection(c); err != nil {
			t.Fatalf("reservation not released: %v", err)
		}
		c.Close()
	})
}

func TestFlowCreditValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []uint64
		valid  bool
	}{
		{"duplicate delayed", []uint64{16, 16, 8, 32}, true},
		{"future", []uint64{33}, false},
		{"overflow", []uint64{math.MaxUint64}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a, b := flowPair(t, tinyFlow())
				x, _ := openFlow(t, a, b)
				if _, err := x.Write(make([]byte, 32)); err != nil {
					t.Fatal(err)
				}
				for _, v := range tc.values {
					data := make([]byte, 8)
					binary.BigEndian.PutUint64(data, v)
					b.sendPacket(CmdCredit, x.id, data)
				}
				synctest.Wait()
				select {
				case <-x.closed:
					if tc.valid {
						t.Fatal("valid cumulative update rejected")
					}
				default:
					if !tc.valid {
						t.Fatal("invalid credit accepted")
					}
				}
				if tc.valid {
					x.owner.creditMu.Lock()
					credit := x.owner.peerWindow - (x.owner.sent - x.owner.peerConsumed)
					x.owner.creditMu.Unlock()
					if credit != 32 {
						t.Fatalf("duplicate credit inflated allowance: %d", credit)
					}
				}
			})
		})
	}
	synctest.Test(t, func(t *testing.T) {
		a, b := flowPair(t, tinyFlow())
		x, _ := openFlow(t, a, b)
		b.sendPacket(CmdCredit, x.id, []byte{1})
		synctest.Wait()
		select {
		case <-x.closed:
		default:
			t.Fatal("malformed credit accepted")
		}
	})
}

func TestFlowUnsupportedVersions(t *testing.T) {
	for _, data := range [][]byte{nil, {0, 0, 0, 1}, {0, 0, 0, 3}} {
		synctest.Test(t, func(t *testing.T) {
			a, b := flowPair(t, tinyFlow())
			a.sendPacket(CmdNew, uuid.New(), data)
			synctest.Wait()
			if b.Ctx.Err() == nil {
				t.Fatal("unsupported version accepted")
			}
		})
	}
}

func TestFlowCancellationAtZeroCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := flowPair(t, tinyFlow())
		x, _ := openFlow(t, a, b)
		done := make(chan error, 1)
		go func() { _, err := x.Write(make([]byte, 128)); done <- err }()
		synctest.Wait()
		a.Abort()
		if err := <-done; err == nil {
			t.Fatal("cancellation lost")
		}
	})
}

type batchConn struct {
	shortConn
	gate  chan struct{}
	sizes []int
}

func (c *batchConn) Write(p []byte) (int, error) {
	<-c.gate
	c.mu.Lock()
	c.sizes = append(c.sizes, len(p))
	c.mu.Unlock()
	return c.shortConn.Write(p)
}
func TestFlowBoundedBatchesAndFairControl(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wire := &batchConn{shortConn: shortConn{limit: 1 << 20}, gate: make(chan struct{})}
		cfg := tinyFlow()
		h, err := NewBaseHandlerWithConfig(context.Background(), wire, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Abort()
		h.sendPacket(CmdEOF, uuid.New(), nil)
		synctest.Wait() // writer is held at the transport, queues remain accessible
		for i := 0; i < 12; i++ {
			if h.sendPacket(CmdAck, uuid.New(), h.handshake()) != ErrNone {
				t.Fatal("control rejected")
			}
		}
		for i := 0; i < 4; i++ {
			if _, err := h.enqueue(CmdData, uuid.New(), []byte("12345678"), nil, false); err != nil {
				t.Fatal(err)
			}
		}
		close(wire.gate)
		if err := h.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		wire.mu.Lock()
		defer wire.mu.Unlock()
		for _, n := range wire.sizes {
			if n > cfg.BatchBytes {
				t.Fatalf("unbounded batch %d", n)
			}
		}
		buf := wire.wire.Bytes()
		controls, data := 0, 0
		for len(buf) > 0 {
			p, n, err := ParseNext(buf)
			if err != nil {
				t.Fatal(err)
			}
			buf = buf[n:]
			if p.Command == CmdData {
				if data == 0 && controls > 8 {
					t.Fatal("control flood starved data")
				}
				data++
			} else if p.Command == CmdAck {
				controls++
			}
		}
		if controls != 12 || data != 4 {
			t.Fatalf("lost records: control=%d data=%d", controls, data)
		}
	})
}

func TestFlowCoalescesCreditAndBoundsControlCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wire := &batchConn{shortConn: shortConn{limit: 1 << 20}, gate: make(chan struct{})}
		h, err := NewBaseHandlerWithConfig(context.Background(), wire, tinyFlow())
		if err != nil {
			t.Fatal(err)
		}
		defer h.Abort()
		h.sendPacket(CmdEOF, uuid.New(), nil)
		synctest.Wait()
		id := uuid.New()
		for i := uint64(1); i <= 10000; i++ {
			h.queueCredit(id, i)
		}
		h.queueMu.Lock()
		count := len(h.controls)
		h.queueMu.Unlock()
		if count != 1 {
			t.Fatalf("credit allocated %d slots", count)
		}
		for i := 1; i < h.flow.ControlSlots; i++ {
			h.sendPacket(CmdEOF, uuid.New(), nil)
		}
		if h.sendPacket(CmdEOF, uuid.New(), nil) == ErrNone {
			t.Fatal("unbounded control admission")
		}
		if h.Ctx.Err() == nil {
			t.Fatal("control flood not terminated")
		}
		close(wire.gate)
	})
}

func TestFlowRejectsDataBeyondReservationAndAfterEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := flowPair(t, tinyFlow())
		x, y := openFlow(t, a, b)
		if _, err := x.Write(make([]byte, 32)); err != nil {
			t.Fatal(err)
		}
		// Bypass the sender to model a peer violating credit on the wire.
		a.sendConfirmed(CmdData, x.id, []byte{1}, nil)
		synctest.Wait()
		select {
		case <-y.closed:
		default:
			t.Fatal("unreserved data accepted")
		}
		healthy, reader := openFlow(t, a, b)
		if err := healthy.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		a.sendConfirmed(CmdData, healthy.id, []byte{1}, nil)
		synctest.Wait()
		select {
		case <-reader.closed:
		default:
			t.Fatal("data after EOF accepted")
		}
	})
}

func TestFlowConfiguration(t *testing.T) {
	for _, edit := range []func(*FlowConfig){
		func(c *FlowConfig) { c.StreamWindow = 0 }, func(c *FlowConfig) { c.TunnelWindow = 1 },
		func(c *FlowConfig) { c.MaxStreams = 0 }, func(c *FlowConfig) { c.DataFrame = 0 },
		func(c *FlowConfig) { c.BatchBytes = 1 }, func(c *FlowConfig) { c.ControlSlots = 1 }, func(c *FlowConfig) { c.DrainTimeout = 0 },
	} {
		cfg := DefaultFlowConfig()
		edit(&cfg)
		if cfg.validate() == nil {
			t.Fatal("invalid limit accepted")
		}
	}
	t.Setenv("PROXYBLOB_STREAM_WINDOW", "32768")
	t.Setenv("PROXYBLOB_DRAIN_TIMEOUT", "2s")
	cfg, err := FlowConfigFromEnv()
	if err != nil || cfg.StreamWindow != 32768 || cfg.DrainTimeout.String() != "2s" {
		t.Fatalf("configuration: %+v %v", cfg, err)
	}
	t.Setenv("PROXYBLOB_DATA_FRAME", "invalid")
	if _, err := FlowConfigFromEnv(); err == nil {
		t.Fatal("invalid environment ignored")
	}
}

func TestReceiveStorageBound(t *testing.T) {
	c := NewConnection(uuid.New(), nil)
	defer c.Close()
	payload := make([]byte, 16<<10)
	accepted := 0
	for i := 0; i < 256; i++ {
		if !c.Deliver(payload) {
			break
		}
		accepted += len(payload)
	}
	if accepted > 64<<10 {
		t.Fatalf("stalled stream retained %d bytes, limit 65536", accepted)
	}
}

type observedConn struct {
	net.Conn
	maximum atomic.Int64
}

func (c *observedConn) Write(p []byte) (int, error) {
	for old := c.maximum.Load(); int64(len(p)) > old; old = c.maximum.Load() {
		if c.maximum.CompareAndSwap(old, int64(len(p))) {
			break
		}
	}
	return c.Conn.Write(p)
}
func TestFlowSustainedProducersKeepHealthyControlMoving(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, right := net.Pipe()
		wire := &observedConn{Conn: left}
		cfg := tinyFlow()
		a, b := flowPairOn(t, cfg, wire, right)
		writers, readers := make([]*ProtocolConn, 3), make([]*ProtocolConn, 3)
		results := make(chan error, 6)
		payload := bytes.Repeat([]byte("sustained"), 1000)
		for i := range writers {
			writers[i], readers[i] = openFlow(t, a, b)
			go func() {
				_, err := writers[i].Write(payload)
				if err == nil {
					err = writers[i].CloseWrite()
				}
				results <- err
			}()
		}
		synctest.Wait() // All producers have filled their initial credit.
		for i := range readers {
			go func() {
				got, err := io.ReadAll(readers[i])
				if err == nil && !bytes.Equal(got, payload) {
					err = io.ErrUnexpectedEOF
				}
				results <- err
			}()
		}
		healthy, peer := openFlow(t, a, b) // ACK must progress while DATA continuously requeues.
		for i := 0; i < 20; i++ {
			if _, err := healthy.Write([]byte{byte(i)}); err != nil {
				t.Fatal(err)
			}
			var got [1]byte
			if _, err := io.ReadFull(peer, got[:]); err != nil || got[0] != byte(i) {
				t.Fatalf("healthy transfer %d: %v", i, err)
			}
		}
		if err := healthy.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		if _, err := peer.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("healthy FIN: %v", err)
		}
		for i := 0; i < 6; i++ {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if maximum := wire.maximum.Load(); maximum > int64(cfg.BatchBytes) {
			t.Fatalf("sustained batch exceeded limit: %d", maximum)
		}
	})
}

func TestFlowRegistrationRacesAbort(t *testing.T) {
	for i := 0; i < 100; i++ {
		h := NewBaseHandler(context.Background(), &shortConn{limit: 1 << 20})
		c := NewConnection(uuid.New(), h.Ctx.Done())
		var wg sync.WaitGroup
		wg.Go(func() { h.RegisterConnection(c) })
		wg.Go(func() { h.Abort() })
		wg.Wait()
		h.flowMu.Lock()
		reserved := h.reserved
		h.flowMu.Unlock()
		if reserved != 0 {
			t.Fatalf("reservation survived cancellation: %d", reserved)
		}
		h.Connections.Range(func(_, _ any) bool { t.Error("stream published after abort"); return true })
	}
}
