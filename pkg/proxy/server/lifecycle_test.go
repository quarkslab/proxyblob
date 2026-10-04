package proxy

import (
	"context"
	"net"
	"sync"
	"testing"
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
