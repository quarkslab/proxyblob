package proxy

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"proxyblob/internal/mux"
	"sync"
	"testing"
	"time"
)

func TestConcurrentStartStop(t *testing.T) {
	for i := 0; i < 20; i++ {
		conn, peer := net.Pipe()
		s := NewProxyServer(context.Background(), conn)
		s.StartReceiving()
		var wg sync.WaitGroup
		for j := 0; j < 8; j++ {
			wg.Add(1)
			go func() { defer wg.Done(); s.Start("127.0.0.1:0"); s.Stop() }()
		}
		wg.Wait()
		if s.Ctx.Err() == nil {
			t.Fatal("server not stopped")
		}
		if s.ListenerAddr() != nil {
			t.Fatal("listener survived stop")
		}

		conn.Close()
		peer.Close()
	}
}

func TestLocalStopAndBindFailurePreserveSession(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	s := NewProxyServer(context.Background(), conn)
	defer s.Stop()
	s.StartReceiving()
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	s.Start(occupied.Addr().String())
	if s.Ctx.Err() != nil || s.ListenerAddr() != nil {
		t.Fatal("bind failure damaged session")
	}
	for i := 0; i < 2; i++ {
		s.Start("127.0.0.1:0")
		if s.ListenerAddr() == nil {
			t.Fatal("could not start SOCKS")
		}
		s.StopListening()
		if s.Ctx.Err() != nil {
			t.Fatal("local stop canceled session")
		}
	}
}

func TestStopListeningClosesPendingACK(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	s := NewProxyServer(context.Background(), conn)
	defer s.Stop()
	s.Start("127.0.0.1:0")
	client, err := net.Dial("tcp", s.ListenerAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	deadline := time.Now().Add(time.Second)
	for {
		found := false
		s.Connections.Range(func(_, _ any) bool { found = true; return false })
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("client not registered")
		}
		time.Sleep(time.Millisecond)
	}
	s.StopListening()
	client.SetReadDeadline(time.Now().Add(time.Second))
	var buf [1]byte
	if _, err := client.Read(buf[:]); err != io.EOF {
		t.Fatalf("pending client survived local stop: %v", err)
	}
	if s.Ctx.Err() != nil {
		t.Fatal("session canceled")
	}
}
func TestLateLocalAcceptance(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	s := NewProxyServer(context.Background(), conn)
	defer s.Stop()
	s.Start("127.0.0.1:0")
	old := s.listener
	s.StopListening()
	s.Start("127.0.0.1:0")
	client, remote := net.Pipe()
	defer remote.Close()
	done := make(chan struct{})
	go func() { s.handleConnection(old, client); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("old handler survived restart")
	}
	s.Connections.Range(func(_, _ any) bool { t.Error("stale connection published"); return false })
	if s.ListenerAddr() == nil || s.Ctx.Err() != nil {
		t.Fatal("replacement affected")
	}
}

func TestLocalStopNotifiesPeerAfterNew(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	s := NewProxyServer(context.Background(), conn)
	defer s.Stop()
	s.Start("127.0.0.1:0")
	client, err := net.Dial("tcp", s.ListenerAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// NEW is sent only once SOCKS negotiation produced a request.
	client.SetDeadline(time.Now().Add(time.Second))
	if _, err := client.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	var auth [2]byte
	if _, err := io.ReadFull(client, auth[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte{5, 1, 0, 1, 127, 0, 0, 1, 0, 80}); err != nil {
		t.Fatal(err)
	}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	readRecord := func(want byte) {
		t.Helper()
		header := make([]byte, mux.HeaderSize)
		if _, err := io.ReadFull(peer, header); err != nil {
			t.Fatalf("missing peer record %d: %v", want, err)
		}
		if header[0] != want {
			t.Fatalf("record %d, want %d", header[0], want)
		}
		payload := make([]byte, binary.BigEndian.Uint32(header[17:]))
		if _, err := io.ReadFull(peer, payload); err != nil {
			t.Fatal(err)
		}
	}
	// The stream is announced and awaiting its ACK when the local listener stops.
	readRecord(mux.CmdNew)
	s.StopListening()
	readRecord(mux.CmdClose)
}
