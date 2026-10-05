package protocol

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Opt-in measurement, not a timing assertion. net.Pipe exercises the native
// multiplexing runtime without Azure credentials or storage latency claims.
func TestFlowWorkloads(t *testing.T) {
	if os.Getenv("PROXYBLOB_MEASURE") == "" {
		t.Skip("set PROXYBLOB_MEASURE=1 for workload measurements")
	}
	window := 64 << 10
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
						wg.Go(func() { writers[i].Write(make([]byte, window*2)) })
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
							if _, err := writers[i].Write(payload); err != nil {
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
				t.Logf("window=%d concurrency=%d mode=%s reserved_per_endpoint=%d peak_heap_delta=%d p50=%s p95=%s bytes=%d elapsed=%s", window, concurrency, mode, window*concurrency, peak.Load()-before.HeapAlloc, p50, p95, transferred.Load(), elapsed)
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
	for _, batch := range []int{64 << 10, 128 << 10} {
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
						_, err := writers[i].Write(payload)
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
