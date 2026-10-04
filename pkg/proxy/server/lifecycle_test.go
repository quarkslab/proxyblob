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
		if s.Listener != nil {
			if _, err := s.Listener.Accept(); err == nil {
				t.Fatal("listener survived stop")
			}
		}
		conn.Close()
		peer.Close()
	}
}
