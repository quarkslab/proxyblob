package protocol

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/google/uuid"
)

func TestForwardPipelinesWhileTransportBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wire := &batchConn{shortConn: shortConn{limit: 1 << 20}, gate: make(chan struct{})}
		cfg := DefaultFlowConfig()
		cfg.StreamWindow = 256 << 10
		h, err := NewBaseHandlerWithConfig(context.Background(), wire, cfg)
		if err != nil {
			t.Fatal(err)
		}
		c := NewConnection(uuid.New(), h.Ctx.Done())
		if err := h.RegisterConnection(c); err != nil {
			t.Fatal(err)
		}
		c.setPeerWindow(uint64(cfg.StreamWindow))
		pc := NewProtocolConn(h.Ctx, c.ID, h)
		c.SetProtocolConn(pc)
		source, peer := net.Pipe()
		result := make(chan error, 1)
		go func() { result <- Forward(pc, source) }()
		written := make(chan struct{})
		go func() {
			defer close(written)
			for range 4 {
				if _, err := peer.Write(make([]byte, cfg.DataFrame)); err != nil {
					return
				}
			}
		}()
		synctest.Wait()
		select {
		case <-written:
		default:
			t.Error("source stalls after one frame while transport write is pending")
		}
		close(wire.gate)
		pc.Close()
		peer.Close()
		source.Close()
		h.Abort()
		<-result
	})
}

func TestPipelineAggregateIncludesInflightAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wire := &batchConn{shortConn: shortConn{limit: 1 << 20}, gate: make(chan struct{})}
		cfg := tinyFlow()
		h, err := NewBaseHandlerWithConfig(context.Background(), wire, cfg)
		if err != nil {
			t.Fatal(err)
		}
		ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
		for _, id := range ids {
			for range 4 {
				if _, err := h.enqueue(CmdData, id, make([]byte, 8), nil, false); err != nil {
					t.Fatal(err)
				}
			}
		}
		synctest.Wait()
		h.queueMu.Lock()
		if h.dataBytes != 128 || h.dataRecords != 16 {
			t.Errorf("in-flight storage not charged: %d bytes/%d records", h.dataBytes, h.dataRecords)
		}
		h.queueMu.Unlock()
		result := make(chan error, 1)
		stop := make(chan struct{})
		go func() { _, err := h.enqueue(CmdData, uuid.New(), []byte{1}, stop, false); result <- err }()
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("over-budget admission returned: %v", err)
		default:
		}
		close(stop)
		if err := <-result; err == nil {
			t.Fatal("cancelled admission succeeded")
		}
		if h.Ctx.Err() != nil {
			t.Fatal("capacity pressure aborted tunnel")
		}
		close(wire.gate)
		if err := h.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		h.queueMu.Lock()
		if h.dataBytes != 0 || h.dataRecords != 0 {
			t.Error("completed data reservation retained")
		}
		h.queueMu.Unlock()
		h.Abort()
	})
}

func TestPipelineUploadFailureInterruptsIdleSource(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wire := &shortConn{limit: 1, failure: io.ErrUnexpectedEOF}
		h := NewBaseHandler(context.Background(), wire)
		defer h.Abort()
		c := NewConnection(uuid.New(), h.Ctx.Done())
		h.RegisterConnection(c)
		c.setPeerWindow(1024)
		pc := NewProtocolConn(h.Ctx, c.ID, h)
		c.SetProtocolConn(pc)
		source, peer := net.Pipe()
		defer source.Close()
		defer peer.Close()
		result := make(chan error, 1)
		go func() { _, err := pc.copyFrom(source); result <- err }()
		peer.Write([]byte("pending"))
		if err := <-result; !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("lost upload failure: %v", err)
		}
	})
}

func TestPipelineRoundRobinPreservesStreamOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wire := &batchConn{shortConn: shortConn{limit: 1 << 20}, gate: make(chan struct{})}
		h, err := NewBaseHandlerWithConfig(context.Background(), wire, tinyFlow())
		if err != nil {
			t.Fatal(err)
		}
		defer h.Abort()
		h.sendPacket(CmdEOF, uuid.New(), nil)
		synctest.Wait()
		ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
		for _, id := range ids {
			for i := 0; i < 4; i++ {
				if _, err := h.enqueue(CmdData, id, []byte{byte(i)}, nil, false); err != nil {
					t.Fatal(err)
				}
			}
		}
		close(wire.gate)
		if err := h.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		wire.mu.Lock()
		defer wire.mu.Unlock()
		data := wire.wire.Bytes()
		index := 0
		for len(data) > 0 {
			p, n, err := ParseNext(data)
			if err != nil {
				t.Fatal(err)
			}
			data = data[n:]
			if p.Command != CmdData {
				continue
			}
			if p.ConnectionID != ids[index%3] || p.Data[0] != byte(index/3) {
				t.Fatalf("unfair or reordered record %d", index)
			}
			index++
		}
		if index != 12 {
			t.Fatalf("lost records: %d", index)
		}
	})
}

func TestPipelineBidirectionalIdentityAndOrderedEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := flowPair(t, tinyFlow())
		left, right := openFlow(t, a, b)
		payload := bytes.Repeat([]byte{0, 1, 2, 255}, 100)
		var wg sync.WaitGroup
		for _, pc := range []*ProtocolConn{left, right} {
			wg.Go(func() {
				n, err := pc.copyFrom(&workloadSource{Reader: bytes.NewReader(payload)})
				if err != nil || n != int64(len(payload)) {
					t.Errorf("pipeline: %d %v", n, err)
					return
				}
				if err := pc.CloseWrite(); err != nil {
					t.Error(err)
				}
			})
			wg.Go(func() {
				got, err := io.ReadAll(pc)
				if err != nil || !bytes.Equal(got, payload) {
					t.Errorf("identity/EOF: %d %v", len(got), err)
				}
			})
		}
		wg.Wait()
	})
}

func TestPipelineStalledConsumerLeavesHealthyStreamProgressing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := flowPair(t, tinyFlow())
		slow, _ := openFlow(t, a, b)
		stalled := make(chan error, 1)
		go func() {
			_, err := slow.copyFrom(&workloadSource{Reader: bytes.NewReader(make([]byte, 128))})
			stalled <- err
		}()
		synctest.Wait()
		select {
		case err := <-stalled:
			t.Fatalf("stalled pipeline returned: %v", err)
		default:
		}
		healthy, peer := openFlow(t, a, b)
		done := make(chan error, 1)
		go func() {
			_, err := healthy.copyFrom(&workloadSource{Reader: bytes.NewReader([]byte("healthy"))})
			if err == nil {
				err = healthy.CloseWrite()
			}
			done <- err
		}()
		got, err := io.ReadAll(peer)
		if err != nil || string(got) != "healthy" {
			t.Fatalf("healthy stream: %q %v", got, err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		slow.Close()
		if err := <-stalled; err == nil {
			t.Fatal("closed pipeline returned success")
		}
		if a.Ctx.Err() != nil || b.Ctx.Err() != nil {
			t.Fatal("slow consumer aborted tunnel")
		}
	})
}

func TestPipelineCloseDiscardsQueuedDataBeforeCloseRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wire := &batchConn{shortConn: shortConn{limit: 1 << 20}, gate: make(chan struct{})}
		h, err := NewBaseHandlerWithConfig(context.Background(), wire, tinyFlow())
		if err != nil {
			t.Fatal(err)
		}
		defer h.Abort()
		h.sendPacket(CmdEOF, uuid.New(), nil)
		synctest.Wait()
		c := NewConnection(uuid.New(), h.Ctx.Done())
		h.RegisterConnection(c)
		for range 4 {
			h.enqueue(CmdData, c.ID, make([]byte, 8), c.Closed, false)
		}
		h.SendClose(c.ID, ErrConnectionClosed)
		h.queueMu.Lock()
		if h.dataBytes != 0 || h.dataRecords != 0 {
			t.Error("cancelled queue retains capacity")
		}
		h.queueMu.Unlock()
		close(wire.gate)
		if err := h.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		wire.mu.Lock()
		defer wire.mu.Unlock()
		data := wire.wire.Bytes()
		for len(data) > 0 {
			p, n, err := ParseNext(data)
			if err != nil {
				t.Fatal(err)
			}
			data = data[n:]
			if p.Command == CmdData {
				t.Fatal("cancelled DATA sent after CLOSE")
			}
		}
	})
}
