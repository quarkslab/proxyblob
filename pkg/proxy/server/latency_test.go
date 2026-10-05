package proxy

import (
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"proxyblob/pkg/protocol"
)

// A fixed transport write cost models the serial request cost that net.Pipe
// alone hides. This is a local comparison tool, not an Azure benchmark.
type latencyConn struct {
	net.Conn
	delay  time.Duration
	writes atomic.Int64
}

func (c *latencyConn) Write(p []byte) (int, error) {
	time.Sleep(c.delay)
	c.writes.Add(1)
	return c.Conn.Write(p)
}

func TestStorageLatencyTransfer(t *testing.T) {
	value := os.Getenv("PROXYBLOB_LATENCY_MS")
	if value == "" {
		t.Skip("set PROXYBLOB_LATENCY_MS for opt-in transport latency measurement")
	}
	ms, err := strconv.Atoi(value)
	if err != nil || ms < 0 || ms > 250 {
		t.Fatal("latency must be 0..250 ms")
	}
	cfg, err := protocol.FlowConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	rawA, rawB := net.Pipe()
	a, b := &latencyConn{Conn: rawA, delay: time.Duration(ms) * time.Millisecond}, &latencyConn{Conn: rawB, delay: time.Duration(ms) * time.Millisecond}
	begin := time.Now()
	exerciseHalfCloseRequestResponse(t, a, b, cfg)
	t.Logf("delay_ms=%d elapsed=%v writes=%d payload_bytes=530000", ms, time.Since(begin), a.writes.Load()+b.writes.Load())
}
