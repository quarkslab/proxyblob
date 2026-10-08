//go:build js && wasm

package netenv

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"proxyblob/internal/agent/netenv/netenvtest"
	"proxyblob/internal/mux"
	"strconv"
	"syscall/js"
	"testing"
	"time"
)

func TestJSCallbacksReleasedAfterHostDetachment(t *testing.T) {
	for _, udp := range []bool{false, true} {
		t.Run(strconv.FormatBool(udp), func(t *testing.T) {
			state := netenvtest.Host(t, "connect")
			count := 5
			if udp {
				c, err := ListenUDP()
				if err != nil {
					t.Fatal(err)
				}
				c.Close()
				c.Close()
				count = 3
			} else {
				c, err := DialTCP("127.0.0.1:80")
				if err != nil {
					t.Fatal(err)
				}
				c.Close()
				c.Close()
			}
			netenvtest.AssertDisposed(t, state)
			if got := state.Call("released").Int(); got != count {
				t.Fatalf("released %d callbacks, want %d", got, count)
			}
		})
	}
}

func TestJSCloseBeforeConnectReturns(t *testing.T) {
	state := netenvtest.Host(t, "close")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := DialTCPContext(ctx, "127.0.0.1:80")
	if c != nil || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close before connect: %v %v", c, err)
	}
	netenvtest.AssertDisposed(t, state)
	state.Call("emit", 0, state.Get("socket"))
	state.Call("emit", 3, "late error")
	netenvtest.Flush(state)
	netenvtest.AssertDisposed(t, state)
}
func TestJSSetupErrorAndCancellation(t *testing.T) {
	for _, udp := range []bool{false, true} {
		for _, mode := range []string{"error", "pending"} {
			t.Run(strconv.FormatBool(udp)+mode, func(t *testing.T) {
				state := netenvtest.Host(t, mode)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				var err error
				if udp {
					_, err = ListenUDPContext(ctx)
				} else {
					_, err = DialTCPContext(ctx, "127.0.0.1:80")
				}
				if err == nil {
					t.Fatal("setup succeeded")
				}
				if mode == "pending" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
				netenvtest.AssertDisposed(t, state)
				state.Call("emit", 0, state.Get("socket"), 12345)
				netenvtest.Flush(state)
				netenvtest.AssertDisposed(t, state)
			})
		}
	}
}
func TestJSPeerEOFLeavesWritesOpen(t *testing.T) {
	state := netenvtest.Host(t, "connect")
	state.Set("payload", "trailing")
	state.Set("writeCount", 8)
	conn, err := DialTCP("127.0.0.1:80")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	got, err := io.ReadAll(conn)
	if string(got) != "trailing" || err != nil {
		t.Fatalf("read: %q %v", got, err)
	}
	if n, err := conn.Write([]byte("response")); n != 8 || err != nil {
		t.Fatalf("write after EOF: %d %v", n, err)
	}
	c := conn.(*jsConn)
	c.CloseWrite()
	c.CloseWrite()
	if _, err := c.Write([]byte("late")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	if state.Call("counts").Get("ends").Int() != 1 {
		t.Fatal("FIN not exactly once")
	}
	c.Close()
	c.Close()
	netenvtest.AssertDisposed(t, state)
}
func TestJSLocalEOFLeavesReadsOpen(t *testing.T) {
	state := netenvtest.Host(t, "connect")
	state.Set("payload", "response")
	conn, err := DialTCP("127.0.0.1:80")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.(*jsConn).CloseWrite()
	got, err := io.ReadAll(conn)
	if string(got) != "response" || err != nil {
		t.Fatalf("read after local EOF: %q %v", got, err)
	}
}
func TestJSShortWritesAndUnsupportedDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count any
		n     int
		err   error
	}{{"short", 2, 2, io.ErrShortWrite}, {"zero", 0, 0, io.ErrShortWrite}, {"unknown", nil, 0, errors.ErrUnsupported}} {
		t.Run(tc.name, func(t *testing.T) {
			state := netenvtest.Host(t, "connect")
			state.Set("writeCount", tc.count)
			c, err := DialTCP("127.0.0.1:80")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			n, err := c.Write([]byte("data"))
			if n != tc.n || !errors.Is(err, tc.err) {
				t.Fatalf("write: %d %v", n, err)
			}
			for _, set := range []func(time.Time) error{c.SetDeadline, c.SetReadDeadline, c.SetWriteDeadline} {
				if !errors.Is(set(time.Now()), errors.ErrUnsupported) {
					t.Fatal("deadline reported success")
				}
			}
		})
	}
}
func TestJSErrorAfterConnectDisposesAndUnblocksRead(t *testing.T) {
	state := netenvtest.Host(t, "connect")
	c, err := DialTCP("127.0.0.1:80")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result := make(chan error, 1)
	go func() { _, e := c.Read(make([]byte, 1)); result <- e }()
	state.Call("emit", 3, "read failure")
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("read blocked")
	}
	c.Close()
	netenvtest.AssertDisposed(t, state)
}
func TestJSTCPRejectsUncreditedOrOversizedData(t *testing.T) {
	for _, size := range []int{1, jsTCPChunkBytes + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			state := netenvtest.Host(t, "connect")
			c, err := DialTCP("127.0.0.1:80")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			state.Call("emit", 1, state.Get("socket"), js.Global().Get("Uint8Array").New(size))
			netenvtest.Flush(state)
			_, err = c.Read(make([]byte, 1))
			if !errors.Is(err, errJSHostProtocol) {
				t.Fatal(err)
			}
			netenvtest.AssertDisposed(t, state)
		})
	}
}
func TestJSUDPBoundsDeadlineAndError(t *testing.T) {
	state := netenvtest.Host(t, "connect")
	relay, err := ListenUDP()
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	c := relay.(*jsUDPConn)
	// Ten maximum datagrams exceed the byte bound; only four can be retained.
	for i := 0; i < 10; i++ {
		state.Call("emit", 1, state.Get("socket"), js.Global().Get("Uint8Array").New(65507), 9, "127.0.0.1")
	}
	netenvtest.Flush(state)
	for i := 0; i < 4; i++ {
		if n, _, err := c.ReadFrom(make([]byte, 65507)); n != 65507 || err != nil {
			t.Fatalf("datagram: %d %v", n, err)
		}
	}
	result := make(chan error, 1)
	go func() { _, _, e := c.ReadFrom(make([]byte, 1)); result <- e }()
	c.SetReadDeadline(time.Now())
	select {
	case err := <-result:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("deadline did not interrupt read")
	}
	c.SetReadDeadline(time.Time{})
	go func() { _, _, e := c.ReadFrom(make([]byte, 1)); result <- e }()
	state.Call("emit", 2, "UDP failure")
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("error did not interrupt read")
	}
	c.Close()
	netenvtest.AssertDisposed(t, state)
}
func TestJSLegacyHostRejectedBeforeSetup(t *testing.T) {
	state := netenvtest.Host(t, "connect")
	js.Global().Set("ProxyBlobSocketHostVersion", 1)
	if _, err := DialTCP("127.0.0.1:80"); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := ListenUDP(); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal(err)
	}
	if state.Call("counts").Get("callbacks").Int() != 0 {
		t.Fatal("legacy host invoked")
	}
}

func realTCP(t *testing.T, name string) net.Conn {
	t.Helper()
	port := netenvtest.RealPeer(t, name)
	c, err := DialTCP(net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func TestJSLoopbackHalfClose(t *testing.T) {
	c := realTCP(t, "response")
	if _, err := c.Write([]byte("request after half close")); err != nil {
		t.Fatal(err)
	}
	if err := c.(*jsConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if string(got) != "request after half close" || err != nil {
		t.Fatalf("response: %q %v", got, err)
	}
	peer := realTCP(t, "peerFIN")
	got, err = io.ReadAll(peer)
	if string(got) != "peer-fin" || err != nil {
		t.Fatalf("peer EOF: %q %v", got, err)
	}
	if n, err := peer.Write([]byte("after FIN")); n != 9 || err != nil {
		t.Fatalf("write after FIN: %d %v", n, err)
	}
}
func TestJSLoopbackSlowReaderByteIdentity(t *testing.T) {
	c := realTCP(t, "bulk")
	before := js.Global().Call("ProxyBlobHostStats").Get("tcpDelivered").Int()
	time.Sleep(25 * time.Millisecond)
	if got := js.Global().Call("ProxyBlobHostStats").Get("tcpDelivered").Int(); got != before {
		t.Fatal("host delivered without receive credit")
	}
	// A healthy peer progresses while bulk receives no credits.
	healthy := realTCP(t, "response")
	healthy.Write([]byte("healthy"))
	healthy.(*jsConn).CloseWrite()
	got, err := io.ReadAll(healthy)
	if string(got) != "healthy" || err != nil {
		t.Fatalf("healthy stream: %q %v", got, err)
	}
	total := 0
	b := make([]byte, 8191)
	for {
		n, err := c.Read(b)
		for i, v := range b[:n] {
			if v != byte((total+i)%251) {
				t.Fatalf("corrupt byte %d", total+i)
			}
		}
		total += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if total != 4*1024*1024 {
		t.Fatalf("truncated: %d", total)
	}
	stats := js.Global().Call("ProxyBlobHostStats")
	if stats.Get("maxTCPChunk").Int() > jsTCPChunkBytes {
		t.Fatal("unbounded host callback")
	}
	if stats.Get("maxTCPReadable").Int() > 9*jsTCPChunkBytes {
		t.Fatalf("host readable bound exceeded: %d", stats.Get("maxTCPReadable").Int())
	}
}
func TestJSLoopbackUDP(t *testing.T) {
	port := netenvtest.RealPeer(t, "udp")
	c, err := ListenUDP()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.LocalPort() == 0 {
		t.Fatal("missing bound port")
	}
	c.SetReadDeadline(time.Now().Add(time.Second))
	if err := c.WriteTo([]byte("datagram"), &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: port}); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 100)
	n, addr, err := c.ReadFrom(b)
	if string(b[:n]) != "datagram" || err != nil || addr.Port != port {
		t.Fatalf("UDP echo: %q %v %v", b[:n], addr, err)
	}
}

func TestJSWriteBackpressureAndCancellation(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(strconv.FormatBool(abort), func(t *testing.T) {
			state := netenvtest.Host(t, "connect")
			state.Set("writeCount", "would-block")
			c, err := DialTCP("127.0.0.1:80")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			result := make(chan error, 1)
			go func() {
				n, e := c.Write([]byte("data"))
				if e == nil && n != 4 {
					e = io.ErrShortWrite
				}
				result <- e
			}()
			// Barrier: on a stalled write no completion is allowed before readiness.
			netenvtest.Flush(state)
			select {
			case err := <-result:
				t.Fatalf("write completed before readiness: %v", err)
			default:
			}
			if abort {
				c.Close()
			} else {
				state.Set("writeCount", 4)
				state.Call("emit", 4)
			}
			select {
			case err := <-result:
				if abort && !errors.Is(err, net.ErrClosed) || !abort && err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("write did not wake")
			}
			c.Close()
			netenvtest.AssertDisposed(t, state)
		})
	}
}

func TestJSLoopbackSlowWriterByteIdentity(t *testing.T) {
	c := realTCP(t, "slowSink")
	before := js.Global().Call("ProxyBlobHostStats").Get("writeStalls").Int()
	payload := make([]byte, 4*1024*1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	n, err := c.Write(payload)
	if n != len(payload) || err != nil {
		t.Fatalf("slow write: %d %v", n, err)
	}
	if err = c.(*jsConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("slow write echo length %d: %v", len(got), err)
	}
	stats := js.Global().Call("ProxyBlobHostStats")
	if stats.Get("writeStalls").Int() <= before {
		t.Fatal("test did not exercise host write backpressure")
	}
	if stats.Get("maxTCPWritable").Int() > 256*1024 {
		t.Fatal("host write queue exceeded byte bound")
	}
}

func TestJSUDPReportsTruncationAndWriteRefusal(t *testing.T) {
	state := netenvtest.Host(t, "connect")
	socket, err := ListenUDP()
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	state.Call("emit", 1, state.Get("socket"), js.Global().Get("Uint8Array").New(16), 9, "127.0.0.1")
	netenvtest.Flush(state)
	n, _, err := socket.ReadFrom(make([]byte, 1))
	if n != 1 || !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("silent truncation: %d %v", n, err)
	}
	refuse := js.Global().Get("Function").New("return false")
	state.Get("socket").Set("send", refuse)
	if err = socket.WriteTo([]byte("drop"), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("write refusal lost: %v", err)
	}
}

func TestJSSetupFailureDoesNotCarryHostMessage(t *testing.T) {
	netenvtest.Host(t, "error") // host reports the text "setup failure"
	conn, err := DialTCPContext(context.Background(), "127.0.0.1:9")
	if conn != nil || !errors.Is(err, mux.Error(mux.ErrTransportError)) || err.Error() != "22" {
		t.Fatalf("unsanitized host failure: %v", err)
	}
}
