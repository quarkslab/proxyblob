//go:build js

package agent

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"proxyblob/internal/agent/netenv/netenvtest"
	"proxyblob/internal/diag"
	"proxyblob/internal/mux"
	"proxyblob/internal/relay"
	"proxyblob/internal/socks5"
)

// agentStream opens one stream to a WASM agent over the real multiplexed
// protocol, as the proxy would. Target sockets and DNS are real Bun host
// operations; the native client-facing proxy is covered by tests/integration.
func agentStream(t *testing.T) *mux.ProtocolConn {
	t.Helper()
	a, b := net.Pipe()
	agent := New(context.Background(), b)
	agent.Start()
	peer, err := mux.NewSession(context.Background(), a, mux.DefaultFlowConfig())
	if err != nil {
		t.Fatal(err)
	}
	peer.StartReceiving()
	t.Cleanup(func() { peer.Stop(); agent.Stop(); a.Close(); b.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := peer.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func TestJSUDPTunnelIPv4IPv6DomainAndControlClose(t *testing.T) {
	port := netenvtest.RealPeer(t, "udp")
	stream := agentStream(t)
	d := stream.EnableDatagrams()
	if err := relay.WriteRequest(stream, relay.UDP, socks5.UDPAddress(&net.UDPAddr{IP: net.IPv4zero})); err != nil {
		t.Fatal(err)
	}
	if code, _, err := relay.ReadReply(stream); err != nil || code != diag.ErrNone {
		t.Fatalf("associate %d %v", code, err)
	}
	for _, host := range []string{"127.0.0.1", "::1", "localhost"} {
		address := socks5.UDPAddress(&net.UDPAddr{IP: net.ParseIP(host), Port: port})
		if host == "localhost" {
			address = append([]byte{socks5.Domain, 9}, []byte(host)...)
			address = append(address, byte(port>>8), byte(port))
		}
		for _, size := range []int{0, 6000} {
			payload := bytes.Repeat([]byte{71}, size)
			packet := append([]byte{0, 0, 0}, address...)
			packet = append(packet, payload...)
			if err := d.Send(packet); err != nil {
				t.Fatal(err)
			}
			got, err := d.Receive()
			if err != nil {
				t.Fatal(err)
			}
			source, header, code := relay.ParseDatagram(got)
			addr, e := net.ResolveUDPAddr("udp", source)
			if code != 0 || e != nil || !addr.IP.IsLoopback() || addr.Port != port || !bytes.Equal(got[header:], payload) {
				t.Fatalf("%s tunnel reply: %s %v", host, source, e)
			}
		}
	}
	// The proxy's EOF ends the association; the agent then closes the stream.
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	var end [1]byte
	if _, err := stream.Read(end[:]); err != io.EOF {
		t.Fatalf("association EOF: %v", err)
	}
}

func TestJSUDPControlCloseCancelsPendingBind(t *testing.T) {
	state := netenvtest.Host(t, "pending")
	stream := agentStream(t)
	stream.EnableDatagrams()
	if err := relay.WriteRequest(stream, relay.UDP, socks5.UDPAddress(&net.UDPAddr{IP: net.IPv4zero})); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for state.Call("counts").Get("callbacks").Int() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("UDP bind did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	for state.Call("counts").Get("disposed").Int() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("proxy EOF did not cancel pending bind")
		}
		time.Sleep(time.Millisecond)
	}
	netenvtest.AssertDisposed(t, state)
}
