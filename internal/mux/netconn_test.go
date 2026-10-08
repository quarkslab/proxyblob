package mux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"proxyblob/internal/diag"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
)

// Opt-in measurement, not a timing assertion. net.Pipe exercises the native
// multiplexing runtime without Azure credentials or storage latency claims.
func TestFlowWorkloads(t *testing.T) {
	if os.Getenv("PROXYBLOB_MEASURE") == "" {
		t.Skip("set PROXYBLOB_MEASURE=1 for workload measurements")
	}
	window := DefaultFlowConfig().StreamWindow
	if s := os.Getenv("PROXYBLOB_MEASURE_WINDOW"); s != "" {
		var err error
		window, err = strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, concurrency := range []int{1, 16, 64, 128} {
		for _, mode := range []string{"idle", "interactive", "bulk", "slow"} {
			t.Run(fmt.Sprintf("%s/%d", mode, concurrency), func(t *testing.T) {
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				var peak atomic.Uint64
				peak.Store(before.HeapAlloc)
				stop, stopped := make(chan struct{}), make(chan struct{})
				go func() {
					defer close(stopped)
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-stop:
							return
						case <-ticker.C:
							var m runtime.MemStats
							runtime.ReadMemStats(&m)
							for old := peak.Load(); m.HeapAlloc > old; old = peak.Load() {
								if peak.CompareAndSwap(old, m.HeapAlloc) {
									break
								}
							}
						}
					}
				}()
				cfg := DefaultFlowConfig()
				cfg.StreamWindow = window
				cfg.TunnelWindow = window * 128
				a, b := flowPair(t, cfg)
				writers, readers := make([]*ProtocolConn, concurrency), make([]*ProtocolConn, concurrency)
				for i := range writers {
					writers[i], readers[i] = openFlow(t, a, b)
				}
				var samples []time.Duration
				var mu sync.Mutex
				var wg sync.WaitGroup
				start := time.Now()
				var transferred atomic.Int64
				for i := range writers {
					if mode == "idle" {
						continue
					}
					if mode == "slow" && i < concurrency-1 {
						wg.Go(func() { writers[i].copyFrom(&workloadSource{Reader: bytes.NewReader(make([]byte, window*2))}) })
						continue
					}
					wg.Go(func() {
						payload := bytes.Repeat([]byte{byte(i)}, 256)
						rounds := 20
						if mode == "bulk" {
							payload = bytes.Repeat([]byte{byte(i)}, 256<<10)
							rounds = 4
						}
						done := make(chan error, 1)
						go func() {
							for r := 0; r < rounds; r++ {
								buf := make([]byte, len(payload))
								if _, err := io.ReadFull(readers[i], buf); err != nil {
									done <- err
									return
								}
								if !bytes.Equal(buf, payload) {
									done <- fmt.Errorf("byte mismatch")
									return
								}
								if _, err := readers[i].Write([]byte{1}); err != nil {
									done <- err
									return
								}
							}
							done <- nil
						}()
						for r := 0; r < rounds; r++ {
							begin := time.Now()
							if _, err := writers[i].copyFrom(&workloadSource{Reader: bytes.NewReader(payload)}); err != nil {
								t.Error(err)
								break
							}
							var ack [1]byte
							if _, err := io.ReadFull(writers[i], ack[:]); err != nil {
								t.Error(err)
								break
							}
							mu.Lock()
							samples = append(samples, time.Since(begin))
							mu.Unlock()
							transferred.Add(int64(len(payload)))
						}
						if err := <-done; err != nil {
							t.Error(err)
						}
					})
				}
				if mode == "slow" { // healthy exchange runs while every other consumer stalls
					// Wait for the healthy stream's fixed rounds, then cancel stalled writes.
					deadline := time.Now().Add(10 * time.Second)
					for transferred.Load() < 20*256 && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					if transferred.Load() != 20*256 {
						t.Error("healthy stream stalled")
					}
					for i := 0; i < concurrency-1; i++ {
						writers[i].Close()
					}
				}
				if mode == "idle" {
					time.Sleep(10 * time.Millisecond)
				}
				wg.Wait()
				elapsed := time.Since(start)
				var last runtime.MemStats
				runtime.ReadMemStats(&last)
				if last.HeapAlloc > peak.Load() {
					peak.Store(last.HeapAlloc)
				}
				close(stop)
				<-stopped
				slices.Sort(samples)
				var p50, p95 time.Duration
				if len(samples) > 0 {
					p50 = samples[len(samples)/2]
					p95 = samples[(len(samples)-1)*95/100]
				}
				t.Logf("window=%d concurrency=%d mode=%s reserved_per_endpoint=%d peak_heap_delta=%d alloc_bytes=%d allocations=%d p50=%s p95=%s bytes=%d elapsed=%s", window, concurrency, mode, window*concurrency, peak.Load()-before.HeapAlloc, last.TotalAlloc-before.TotalAlloc, last.Mallocs-before.Mallocs, p50, p95, transferred.Load(), elapsed)
			})
		}
	}
}

type delayedWriteConn struct {
	net.Conn
	delay  time.Duration
	writes atomic.Int64
}

func (c *delayedWriteConn) Write(p []byte) (int, error) {
	time.Sleep(c.delay)
	c.writes.Add(1)
	return c.Conn.Write(p)
}

// Compare batching under a fixed request cost, separately from local CPU/heap
// measurements. The count is transport writes, not billed Azure transactions.
func TestFlowStorageLatency(t *testing.T) {
	if os.Getenv("PROXYBLOB_MEASURE_LATENCY") == "" {
		t.Skip("opt-in transport request-cost comparison")
	}
	for _, batch := range []int{64 << 10, 128 << 10, 512 << 10} {
		for _, concurrency := range []int{1, 16} {
			t.Run(fmt.Sprintf("batch%d/streams%d", batch, concurrency), func(t *testing.T) {
				cfg := DefaultFlowConfig()
				cfg.BatchBytes = batch
				rawA, rawB := net.Pipe()
				aWire, bWire := &delayedWriteConn{Conn: rawA, delay: 10 * time.Millisecond}, &delayedWriteConn{Conn: rawB, delay: 10 * time.Millisecond}
				a, b := flowPairOn(t, cfg, aWire, bWire)
				writers, readers := make([]*ProtocolConn, concurrency), make([]*ProtocolConn, concurrency)
				for i := range writers {
					writers[i], readers[i] = openFlow(t, a, b)
				}
				before := aWire.writes.Load() + bWire.writes.Load()
				start := time.Now()
				results := make(chan error, concurrency*2)
				for i := range writers {
					payload := bytes.Repeat([]byte{byte(i)}, 256<<10)
					go func() {
						_, err := writers[i].copyFrom(&workloadSource{Reader: bytes.NewReader(payload)})
						if err == nil {
							err = writers[i].CloseWrite()
						}
						results <- err
					}()
					go func() {
						got, err := io.ReadAll(readers[i])
						if err == nil && !bytes.Equal(got, payload) {
							err = io.ErrUnexpectedEOF
						}
						results <- err
					}()
				}
				for i := 0; i < concurrency*2; i++ {
					if err := <-results; err != nil {
						t.Fatal(err)
					}
				}
				t.Logf("batch=%d streams=%d bytes=%d elapsed=%v transport_writes=%d", batch, concurrency, concurrency*(256<<10), time.Since(start), aWire.writes.Load()+bWire.writes.Load()-before)
			})
		}
	}
}

// A finite source exercises the same bounded pipeline used by TCP Forward.
type workloadSource struct {
	net.Conn
	Reader *bytes.Reader
}

func (s *workloadSource) Read(p []byte) (int, error) { return s.Reader.Read(p) }

func (s *workloadSource) Close() error { return nil }

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
		h.SendClose(c.ID, diag.ErrConnectionClosed)
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
