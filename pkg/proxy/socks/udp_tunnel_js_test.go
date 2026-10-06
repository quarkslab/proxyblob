//go:build js

package proxy

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"proxyblob/pkg/protocol"
)

// This peer runs the real multiplexed protocol against the WASM agent handler.
// The separate Linux namespace harness tests the native client-facing proxy.
// Here the target UDP socket and DNS are real Bun host operations.
type udpTestPeer struct {
	*protocol.BaseHandler
	ready chan *protocol.Datagrams
}

func (h *udpTestPeer) Stop()                        { h.Abort() }
func (h *udpTestPeer) OnNew(uuid.UUID, []byte) byte { return protocol.ErrUnexpectedPacket }
func (h *udpTestPeer) OnAck(id uuid.UUID, _ []byte) byte {
	v, ok := h.Connections.Load(id)
	if !ok {
		return protocol.ErrConnectionNotFound
	}
	if !v.(*protocol.Connection).SetProtocolConn(protocol.NewProtocolConn(h.Ctx, id, h.BaseHandler)) {
		return protocol.ErrConnectionClosed
	}
	return protocol.ErrNone
}
func (h *udpTestPeer) OnData(id uuid.UUID, b []byte) byte {
	v, ok := h.Connections.Load(id)
	if !ok || !v.(*protocol.Connection).Deliver(b) {
		return protocol.ErrConnectionClosed
	}
	return protocol.ErrNone
}
func (h *udpTestPeer) OnClose(id uuid.UUID, code byte) byte { return h.PeerClose(id, code) }
func (h *udpTestPeer) OnUDPAssociate(c *protocol.Connection, _ []byte) byte {
	d := c.EnableDatagrams()
	if d == nil {
		return protocol.ErrInvalidState
	}
	h.ready <- d
	go d.Ready([]byte{1, 127, 0, 0, 1, 4, 56})
	return protocol.ErrNone
}
func TestJSUDPTunnelIPv4IPv6DomainAndControlClose(t *testing.T) {
	port := realPeer(t, "udp")
	peer, c := udpTestControl(t)
	control := c.ProtocolConn()
	control.Write([]byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0})
	var reply [10]byte
	if _, err := io.ReadFull(control, reply[:]); err != nil || reply[1] != 0 {
		t.Fatalf("associate %v %v", reply, err)
	}
	d := <-peer.ready
	for _, host := range []string{"127.0.0.1", "::1", "localhost"} {
		address := UDPAddress(&net.UDPAddr{IP: net.ParseIP(host), Port: port})
		if host == "localhost" {
			address = append([]byte{Domain, 9}, []byte(host)...)
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
			source, header, code := ExtractUDPHeader(got)
			addr, e := net.ResolveUDPAddr("udp", source)
			if code != 0 || e != nil || !addr.IP.IsLoopback() || addr.Port != port || !bytes.Equal(got[header:], payload) {
				t.Fatalf("%s tunnel reply: %s %v", host, source, e)
			}
		}
	}
	if err := control.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	var end [1]byte
	if _, err := control.Read(end[:]); err != io.EOF {
		t.Fatalf("control EOF: %v", err)
	}
	c.Close()
}

func udpTestControl(t *testing.T) (*udpTestPeer, *protocol.Connection) {
	t.Helper()
	a, b := net.Pipe()
	agent := NewSocksHandler(context.Background(), b)
	base := protocol.NewBaseHandler(context.Background(), a)
	peer := &udpTestPeer{base, make(chan *protocol.Datagrams, 1)}
	base.PacketHandler = peer
	agent.Start("")
	go peer.ReceiveLoop()
	t.Cleanup(func() { peer.Stop(); agent.Stop(); a.Close(); b.Close() })
	c := protocol.NewConnection(uuid.New(), peer.Ctx.Done())
	if err := peer.RegisterConnection(c); err != nil {
		t.Fatal(err)
	}
	if peer.SendNewConnection(c.ID) != 0 {
		t.Fatal("new connection")
	}
	select {
	case <-c.Established():
	case <-time.After(time.Second):
		t.Fatal("ACK")
	}
	control := c.ProtocolConn()
	control.Write([]byte{5, 1, 0})
	var auth [2]byte
	if _, err := io.ReadFull(control, auth[:]); err != nil || auth != [2]byte{5, 0} {
		t.Fatalf("auth %v %v", auth, err)
	}
	return peer, c
}

func TestJSUDPControlCloseCancelsPendingBind(t *testing.T) {
	state := testJSHost(t, "pending")
	_, c := udpTestControl(t)
	control := c.ProtocolConn()
	if _, err := control.Write([]byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for state.Call("counts").Get("callbacks").Int() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("UDP bind did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if err := control.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	for state.Call("counts").Get("disposed").Int() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("control EOF did not cancel pending bind")
		}
		time.Sleep(time.Millisecond)
	}
	assertDisposed(t, state)
}
