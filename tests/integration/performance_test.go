package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	socks "proxyblob/internal/agent"
	"proxyblob/internal/mux"
	proxy "proxyblob/internal/proxy"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type fixedLatencyConn struct {
	net.Conn
	delay  time.Duration
	writes atomic.Int64
	bytes  atomic.Int64
	max    atomic.Int64
}

func (c *fixedLatencyConn) Write(p []byte) (int, error) {
	time.Sleep(c.delay)
	c.writes.Add(1)
	c.bytes.Add(int64(len(p)))
	for old := c.max.Load(); int64(len(p)) > old; old = c.max.Load() {
		if c.max.CompareAndSwap(old, int64(len(p))) {
			break
		}
	}
	return c.Conn.Write(p)
}

func TestStorageFixedTransfer(t *testing.T) {
	if os.Getenv("PROXYBLOB_LATENCY_MS") == "" {
		t.Skip("set PROXYBLOB_LATENCY_MS for storage latency measurement")
	}
	ms, err := strconv.Atoi(os.Getenv("PROXYBLOB_LATENCY_MS"))
	if err != nil {
		t.Fatal(err)
	}
	begin := time.Now()

	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	request := bytes.Repeat([]byte("q"), 1<<20)
	response := bytes.Repeat([]byte("r"), 1<<20)
	result := make(chan error, 1)
	go func() {
		c, err := target.Accept()
		if err != nil {
			result <- err
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(60 * time.Second))
		got := make([]byte, len(request))
		_, err = io.ReadFull(c, got)
		if err != nil {
			result <- err
			return
		}
		if !bytes.Equal(got, request) {
			result <- io.ErrUnexpectedEOF
			return
		}
		_, err = c.Write(response)
		if err == nil {
			var ack [1]byte
			_, err = io.ReadFull(c, ack[:])
		}
		result <- err
	}()
	rawA, rawB := net.Pipe()
	a, b := &fixedLatencyConn{Conn: rawA, delay: time.Duration(ms) * time.Millisecond}, &fixedLatencyConn{Conn: rawB, delay: time.Duration(ms) * time.Millisecond}
	defer func() {
		t.Logf("delay_ms=%d elapsed=%v writes=%d bytes=%d max_batch=%d", ms, time.Since(begin), a.writes.Load()+b.writes.Load(), a.bytes.Load()+b.bytes.Load(), max(a.max.Load(), b.max.Load()))
	}()

	defer a.Close()
	defer b.Close()
	cfg, err := mux.FlowConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	s, err := proxy.NewProxyServerWithConfig(context.Background(), a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	h, err := socks.NewSocksHandlerWithConfig(context.Background(), b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Stop()
	h.Start("")
	s.Start("127.0.0.1:0")
	client, err := net.Dial("tcp", s.ListenerAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(60 * time.Second))
	if _, err := client.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	auth := make([]byte, 2)
	if _, err := io.ReadFull(client, auth); err != nil || !bytes.Equal(auth, []byte{5, 0}) {
		t.Fatalf("auth: %x %v", auth, err)
	}
	address := target.Addr().(*net.TCPAddr)
	connect := []byte{5, 1, 0, 1, 127, 0, 0, 1, 0, 0}
	binary.BigEndian.PutUint16(connect[8:], uint16(address.Port))
	if _, err := client.Write(connect); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(client, reply); err != nil || reply[1] != 0 {
		t.Fatalf("connect: %x %v", reply, err)
	}
	if _, err := client.Write(request); err != nil {
		t.Fatal(err)
	}

	got := make([]byte, len(response))
	_, err = io.ReadFull(client, got)
	if err != nil || !bytes.Equal(got, response) {
		t.Fatalf("response: got %d want %d, error %v", len(got), len(response), err)
	}
	if _, err := client.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

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
	cfg, err := mux.FlowConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	rawA, rawB := net.Pipe()
	a, b := &latencyConn{Conn: rawA, delay: time.Duration(ms) * time.Millisecond}, &latencyConn{Conn: rawB, delay: time.Duration(ms) * time.Millisecond}
	begin := time.Now()
	exerciseHalfCloseRequestResponse(t, a, b, cfg)
	t.Logf("delay_ms=%d elapsed=%v writes=%d payload_bytes=530000", ms, time.Since(begin), a.writes.Load()+b.writes.Load())
}
