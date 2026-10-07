//go:build js && wasm

package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"proxyblob/pkg/protocol"
	"strconv"
	"syscall/js"
	"testing"
	"time"
)

// This deterministic host drives the real setup boundary in Bun and Node.
// All dispatch consults one detachable callback table, including queued events.
func testJSHost(t *testing.T, mode string) js.Value {
	t.Helper()
	f := js.Global().Get("Function").New(`return (() => {
 let callbacks, probes=[], disposed=0, reads=0, ends=0;
 const state={mode:"connect",payload:"",writeCount:4};
 const socket={write:b=>state.writeCount,end:()=>ends++,send:()=>true};
 const handle={dispose(){if(callbacks){callbacks=undefined;disposed++}},
 read(n){reads++;if(state.payload){queueMicrotask(()=>{if(callbacks){const b=new TextEncoder().encode(state.payload);callbacks[1](socket,b);callbacks[2]()}})}}};
 function emit(i,...args){queueMicrotask(()=>callbacks?.[i](...args))}
 function start(c){callbacks=c;probes=c.slice();queueMicrotask(()=>{if(!callbacks)return;
 if(state.mode==="connect")callbacks[0](socket,12345);
 if(state.mode==="close")callbacks[2]();
 if(state.mode==="error")callbacks[c.length===3?2:3]("setup failure");
 });return handle}
 state.tcp=(host,port,...c)=>start(c);state.udp=(...c)=>start(c);
 // Diagnostic references are never dispatched as socket events. Probing them
 // after detachment checks the real Go callback registry has released each id.
 state.released=()=>{let count=0;const old=console.error;console.error=(message)=>{if(message==="call to released function")count++};
 try{for(const f of probes)f(socket,new Uint8Array(1),12345,"127.0.0.1")}finally{console.error=old}return count};
 state.flush=done=>queueMicrotask(done);state.emit=emit;state.socket=socket;state.counts=()=>({disposed,reads,ends,callbacks:callbacks?.length??0,expected:probes.length});
 return state;
 })()`)
	state := f.Invoke()
	state.Set("mode", mode)
	tcp, udp, version := js.Global().Get("TCPDial"), js.Global().Get("UDPListen"), js.Global().Get("ProxyBlobSocketHostVersion")
	js.Global().Set("TCPDial", state.Get("tcp"))
	js.Global().Set("UDPListen", state.Get("udp"))
	js.Global().Set("ProxyBlobSocketHostVersion", 2)
	t.Cleanup(func() {
		js.Global().Set("TCPDial", tcp)
		js.Global().Set("UDPListen", udp)
		js.Global().Set("ProxyBlobSocketHostVersion", version)
	})
	return state
}
func flushJSHost(state js.Value) {
	done := make(chan struct{})
	callback := js.FuncOf(func(js.Value, []js.Value) any { close(done); return nil })
	state.Call("flush", callback)
	<-done
	callback.Release()
}

func assertDisposed(t *testing.T, state js.Value) {
	t.Helper()
	counts := state.Call("counts")
	if counts.Get("disposed").Int() != 1 || counts.Get("callbacks").Int() != 0 {
		t.Fatalf("cleanup: %s", js.Global().Get("JSON").Call("stringify", counts))
	}
	if got := state.Call("released").Int(); got != counts.Get("expected").Int() {
		t.Fatalf("callback release count: %d", got)
	}
}

func TestJSCallbacksReleasedAfterHostDetachment(t *testing.T) {
	for _, udp := range []bool{false, true} {
		t.Run(strconv.FormatBool(udp), func(t *testing.T) {
			state := testJSHost(t, "connect")
			count := 5
			if udp {
				c, err := listenUDP()
				if err != nil {
					t.Fatal(err)
				}
				c.Close()
				c.Close()
				count = 3
			} else {
				c, err := dialTCP("127.0.0.1:80")
				if err != nil {
					t.Fatal(err)
				}
				c.Close()
				c.Close()
			}
			assertDisposed(t, state)
			if got := state.Call("released").Int(); got != count {
				t.Fatalf("released %d callbacks, want %d", got, count)
			}
		})
	}
}

func TestJSCloseBeforeConnectReturns(t *testing.T) {
	state := testJSHost(t, "close")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := dialTCPContext(ctx, "127.0.0.1:80")
	if c != nil || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close before connect: %v %v", c, err)
	}
	assertDisposed(t, state)
	state.Call("emit", 0, state.Get("socket"))
	state.Call("emit", 3, "late error")
	flushJSHost(state)
	assertDisposed(t, state)
}
func TestJSSetupErrorAndCancellation(t *testing.T) {
	for _, udp := range []bool{false, true} {
		for _, mode := range []string{"error", "pending"} {
			t.Run(strconv.FormatBool(udp)+mode, func(t *testing.T) {
				state := testJSHost(t, mode)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				var err error
				if udp {
					_, err = listenUDPContext(ctx)
				} else {
					_, err = dialTCPContext(ctx, "127.0.0.1:80")
				}
				if err == nil {
					t.Fatal("setup succeeded")
				}
				if mode == "pending" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
				assertDisposed(t, state)
				state.Call("emit", 0, state.Get("socket"), 12345)
				flushJSHost(state)
				assertDisposed(t, state)
			})
		}
	}
}
func TestJSPeerEOFLeavesWritesOpen(t *testing.T) {
	state := testJSHost(t, "connect")
	state.Set("payload", "trailing")
	state.Set("writeCount", 8)
	conn, err := dialTCP("127.0.0.1:80")
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
	assertDisposed(t, state)
}
func TestJSLocalEOFLeavesReadsOpen(t *testing.T) {
	state := testJSHost(t, "connect")
	state.Set("payload", "response")
	conn, err := dialTCP("127.0.0.1:80")
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
			state := testJSHost(t, "connect")
			state.Set("writeCount", tc.count)
			c, err := dialTCP("127.0.0.1:80")
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
	state := testJSHost(t, "connect")
	c, err := dialTCP("127.0.0.1:80")
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
	assertDisposed(t, state)
}
func TestJSTCPRejectsUncreditedOrOversizedData(t *testing.T) {
	for _, size := range []int{1, jsTCPChunkBytes + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			state := testJSHost(t, "connect")
			c, err := dialTCP("127.0.0.1:80")
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			state.Call("emit", 1, state.Get("socket"), js.Global().Get("Uint8Array").New(size))
			flushJSHost(state)
			_, err = c.Read(make([]byte, 1))
			if !errors.Is(err, errJSHostProtocol) {
				t.Fatal(err)
			}
			assertDisposed(t, state)
		})
	}
}
func TestJSUDPBoundsDeadlineAndError(t *testing.T) {
	state := testJSHost(t, "connect")
	relay, err := listenUDP()
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	c := relay.(*jsUDPConn)
	// Ten maximum datagrams exceed the byte bound; only four can be retained.
	for i := 0; i < 10; i++ {
		state.Call("emit", 1, state.Get("socket"), js.Global().Get("Uint8Array").New(65507), 9, "127.0.0.1")
	}
	flushJSHost(state)
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
	assertDisposed(t, state)
}
func TestJSLegacyHostRejectedBeforeSetup(t *testing.T) {
	state := testJSHost(t, "connect")
	js.Global().Set("ProxyBlobSocketHostVersion", 1)
	if _, err := dialTCP("127.0.0.1:80"); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err := listenUDP(); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal(err)
	}
	if state.Call("counts").Get("callbacks").Int() != 0 {
		t.Fatal("legacy host invoked")
	}
}

func realPeer(t *testing.T, name string) int {
	t.Helper()
	v := js.Global().Get("ProxyBlobTestPeers")
	if v.IsUndefined() {
		t.Skip("requires Bun loopback harness")
	}
	return v.Get(name).Int()
}
func realTCP(t *testing.T, name string) net.Conn {
	t.Helper()
	port := realPeer(t, name)
	c, err := dialTCP(net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
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
	port := realPeer(t, "udp")
	c, err := listenUDP()
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
			state := testJSHost(t, "connect")
			state.Set("writeCount", "would-block")
			c, err := dialTCP("127.0.0.1:80")
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
			flushJSHost(state)
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
			assertDisposed(t, state)
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
	state := testJSHost(t, "connect")
	socket, err := listenUDP()
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	state.Call("emit", 1, state.Get("socket"), js.Global().Get("Uint8Array").New(16), 9, "127.0.0.1")
	flushJSHost(state)
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
	testJSHost(t, "error") // host reports the text "setup failure"
	conn, err := dialTCPContext(context.Background(), "127.0.0.1:9")
	if conn != nil || !errors.Is(err, protocol.Error(protocol.ErrTransportError)) || err.Error() != "22" {
		t.Fatalf("unsanitized host failure: %v", err)
	}
}
