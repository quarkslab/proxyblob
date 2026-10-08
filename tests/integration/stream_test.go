package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	socks "proxyblob/internal/agent"
	"proxyblob/internal/mux"
	proxy "proxyblob/internal/proxy"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestGracefulClosePreservesAcceptedDelivery(t *testing.T) {
	for _, agent := range []bool{false, true} {
		name := "proxy"
		if agent {
			name = "agent"
		}
		t.Run(name, func(t *testing.T) {
			local, peer := net.Pipe()
			defer local.Close()
			defer peer.Close()
			var base *mux.BaseHandler
			var handler mux.PacketHandler
			if agent {
				h := socks.NewSocksHandler(context.Background(), local)
				base = h.BaseHandler
				handler = h
			} else {
				s := proxy.NewProxyServer(context.Background(), local)
				base = s.BaseHandler
				handler = s
			}
			defer handler.Stop()
			id := uuid.New()
			c := mux.NewConnection(id, base.Ctx.Done())
			base.Connections.Store(id, c)
			pc := mux.NewProtocolConn(base.Ctx, id, base)
			c.SetProtocolConn(pc)
			c.StartDelivery()
			// Many small records must drain in order from the reserved buffer.
			var want []byte
			for i := 0; i < 1500; i++ {
				b := []byte{byte(i)}
				want = append(want, b...)
				if handler.OnData(id, b) != mux.ErrNone {
					t.Fatal("data rejected")
				}
			}
			if handler.OnClose(id, mux.ErrNone) != mux.ErrNone {
				t.Fatal("close rejected")
			}
			got, err := io.ReadAll(pc)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("trailing bytes: got %d want %d, error %v", len(got), len(want), err)
			}
		})
	}
}

// Real TCP endpoints exercise both production forwarding paths through a
// multiplexed tunnel, including SOCKS negotiation and request/response FIN.
func TestHalfCloseRequestResponse(t *testing.T) {
	a, b := net.Pipe()
	exerciseHalfCloseRequestResponse(t, a, b, mux.DefaultFlowConfig())
}

func exerciseHalfCloseRequestResponse(t *testing.T, a, b net.Conn, cfg mux.FlowConfig) {
	t.Helper()

	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	request := bytes.Repeat([]byte("request"), 30000)
	response := bytes.Repeat([]byte("response"), 40000)
	result := make(chan error, 1)
	go func() {
		c, err := target.Accept()
		if err != nil {
			result <- err
			return
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(30 * time.Second))
		got, err := io.ReadAll(c)
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
			err = c.(*net.TCPConn).CloseWrite()
		}
		result <- err
	}()
	defer a.Close()
	defer b.Close()
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
	client.SetDeadline(time.Now().Add(30 * time.Second))
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
	if err := client.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(client)
	if err != nil || !bytes.Equal(got, response) {
		t.Fatalf("response: got %d want %d, error %v", len(got), len(response), err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
