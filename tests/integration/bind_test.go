package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	tunnel "proxyblob/internal/agent"
	proxy "proxyblob/internal/proxy"
	"proxyblob/internal/socks5"
	"testing"
	"time"
)

func socksClient(t *testing.T, s *proxy.ProxyServer, auth []byte) *net.TCPConn {
	t.Helper()
	c, err := net.Dial("tcp", s.ListenerAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(5 * time.Second))
	// Deliberately fragment negotiation and requests at the public interface.
	for _, b := range auth {
		if _, err = c.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	return c.(*net.TCPConn)
}

func socksReply(t *testing.T, c net.Conn, want byte) *net.TCPAddr {
	t.Helper()
	var h [4]byte
	if _, err := io.ReadFull(c, h[:]); err != nil {
		t.Fatal(err)
	}
	if h[0] != 5 || h[1] != want || h[2] != 0 {
		t.Fatalf("reply %v", h)
	}
	size := 4
	if h[3] == 4 {
		size = 16
	} else if h[3] != 1 {
		t.Fatalf("address type %d", h[3])
	}
	b := make([]byte, size+2)
	if _, err := io.ReadFull(c, b); err != nil {
		t.Fatal(err)
	}
	return &net.TCPAddr{IP: net.IP(b[:size]), Port: int(binary.BigEndian.Uint16(b[size:]))}
}

func socksCommand(t *testing.T, c net.Conn, cmd byte, addr []byte) {
	t.Helper()
	var auth [2]byte
	if _, err := io.ReadFull(c, auth[:]); err != nil || auth != [2]byte{5, 0} {
		t.Fatalf("auth %v %v", auth, err)
	}
	for _, b := range append([]byte{5, cmd, 0}, addr...) {
		if _, err := c.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRejectedAuthIsTwoBytesThenEOF(t *testing.T) {
	s, _ := udpTunnel(t, "127.0.0.1:0")
	c := socksClient(t, s, []byte{5, 1, 2})
	b, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(b, []byte{5, 255}) {
		t.Fatalf("reply %x %v", b, err)
	}
}

func TestBindRepliesAndHalfClose(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1", "localhost"} {
		t.Run(host, func(t *testing.T) {
			s, _ := udpTunnel(t, "127.0.0.1:0")
			c := socksClient(t, s, []byte{5, 1, 0})
			addr := socks5.UDPAddress(&net.UDPAddr{IP: net.ParseIP(host)})
			if host == "localhost" {
				addr = append([]byte{3, 9}, []byte(host)...)
				addr = append(addr, 0, 0)
			}
			socksCommand(t, c, 2, addr)
			bound := socksReply(t, c, 0)
			if bound.IP.IsUnspecified() || bound.Port == 0 {
				t.Fatalf("unusable bind %v", bound)
			}
			peer, err := net.DialTCP("tcp", nil, bound)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(5 * time.Second))
			accepted := socksReply(t, c, 0)
			if accepted.String() != peer.LocalAddr().String() {
				t.Fatalf("peer %v want %v", accepted, peer.LocalAddr())
			}
			payload := bytes.Repeat([]byte("bind-request"), 10000)
			done := make(chan error, 1)
			go func() {
				_, err := c.Write(payload)
				if err == nil {
					err = c.CloseWrite()
				}
				done <- err
			}()
			got, err := io.ReadAll(peer)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("request %d %v", len(got), err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if _, err = peer.Write([]byte("response after EOF")); err != nil {
				t.Fatal(err)
			}
			peer.CloseWrite()
			got, err = io.ReadAll(c)
			if err != nil || string(got) != "response after EOF" {
				t.Fatalf("response %q %v", got, err)
			}
		})
	}
}

func TestConnectReplyUsesActualAddressFamily(t *testing.T) {
	for _, network := range []string{"tcp4", "tcp6"} {
		t.Run(network, func(t *testing.T) {
			host := "127.0.0.1"
			if network == "tcp6" {
				host = "::1"
			}
			dst, err := net.Listen(network, net.JoinHostPort(host, "0"))
			if err != nil {
				t.Fatal(err)
			}
			defer dst.Close()
			s, _ := udpTunnel(t, "127.0.0.1:0")
			c := socksClient(t, s, []byte{5, 1, 0})
			address := dst.Addr().(*net.TCPAddr)
			socksCommand(t, c, 1, socks5.UDPAddress(&net.UDPAddr{IP: address.IP, Port: address.Port}))
			bound := socksReply(t, c, 0)
			peer, err := dst.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			if bound.String() != peer.RemoteAddr().String() {
				t.Fatalf("CONNECT bound %v want %v", bound, peer.RemoteAddr())
			}
		})
	}
}

func TestBindListenerClosesWhileWaiting(t *testing.T) {
	for _, mode := range []string{"client", "tunnel"} {
		t.Run(mode, func(t *testing.T) {
			for i := 0; i < 5; i++ {
				s, agent := udpTunnel(t, "127.0.0.1:0")
				c := socksClient(t, s, []byte{5, 1, 0})
				socksCommand(t, c, 2, []byte{1, 127, 0, 0, 1, 0, 0})
				bound := socksReply(t, c, 0)
				if mode == "client" {
					c.Close()
				} else {
					agent.Stop()
				}
				until := time.Now().Add(time.Second)
				for {
					listener, err := net.Listen("tcp", bound.String())
					if err == nil {
						listener.Close()
						break
					}
					if time.Now().After(until) {
						t.Fatalf("BIND listener survived %s close: %v", mode, err)
					}
					time.Sleep(time.Millisecond)
				}
			}
		})
	}
}

func TestBindRejectsUnexpectedPeerPort(t *testing.T) {
	s, _ := udpTunnel(t, "127.0.0.1:0")
	c := socksClient(t, s, []byte{5, 1, 0})
	reserve, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	expected := reserve.Addr().(*net.TCPAddr)
	defer reserve.Close()
	socksCommand(t, c, 2, socks5.UDPAddress(&net.UDPAddr{IP: expected.IP, Port: expected.Port}))
	bound := socksReply(t, c, 0)
	bad, err := net.DialTCP("tcp4", nil, bound)
	if err != nil {
		t.Fatal(err)
	}
	bad.SetDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if n, err := bad.Read(b[:]); n != 0 || err != io.EOF {
		t.Fatalf("unexpected peer not rejected: %d %v", n, err)
	}
	bad.Close()
	reserve.Close()
	good, err := net.DialTCP("tcp4", expected, bound)
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()
	if got := socksReply(t, c, 0); got.String() != expected.String() {
		t.Fatalf("accepted %v", got)
	}
}

func TestMalformedSOCKSRequestsReturnFailureAndEOF(t *testing.T) {
	cases := [][]byte{
		{5, 1, 1, 1, 127, 0, 0, 1, 0, 80}, // reserved byte
		{5, 2, 0, 3, 0, 0, 0},             // empty domain
		{5, 9, 0, 1, 127, 0, 0, 1, 0, 80}, // unsupported command
		{5, 1, 0, 9},                      // unsupported address
	}
	for _, request := range cases {
		t.Run(string([]byte{request[1], request[3]}), func(t *testing.T) {
			s, _ := udpTunnel(t, "127.0.0.1:0")
			c := socksClient(t, s, []byte{5, 1, 0})
			var auth [2]byte
			if _, err := io.ReadFull(c, auth[:]); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Write(request); err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(c)
			if err != nil || len(got) != 10 || got[0] != 5 || got[1] == 0 || got[1] == 255 {
				t.Fatalf("failure %x %v", got, err)
			}
		})
	}
}

func TestBindTimeoutSendsSecondFailureAndReleasesListener(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	agent := tunnel.New(context.Background(), b, tunnel.WithBindTimeout(100*time.Millisecond))
	defer agent.Stop()
	agent.Start()
	s := proxy.NewProxyServer(context.Background(), a)
	defer s.Stop()
	s.Start("127.0.0.1:0")
	c := socksClient(t, s, []byte{5, 1, 0})
	socksCommand(t, c, 2, []byte{1, 127, 0, 0, 1, 0, 0})
	bound := socksReply(t, c, 0)
	socksReply(t, c, socks5.TTLExpired)
	var buf [1]byte
	if _, err := c.Read(buf[:]); err != io.EOF {
		t.Fatalf("timeout EOF %v", err)
	}
	listener, err := net.Listen("tcp", bound.String())
	if err != nil {
		t.Fatalf("timeout leaked listener: %v", err)
	}
	listener.Close()
}

func TestPartialSOCKSRequestClosesWithoutListener(t *testing.T) {
	s, _ := udpTunnel(t, "127.0.0.1:0")
	c := socksClient(t, s, []byte{5, 1, 0})
	var auth [2]byte
	if _, err := io.ReadFull(c, auth[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte{5, 2, 0, 4, 0, 0}); err != nil {
		t.Fatal(err)
	}
	c.CloseWrite()
	got, err := io.ReadAll(c)
	if err != nil || len(got) != 0 {
		t.Fatalf("partial request %x %v", got, err)
	}
}

func TestBindAcceptedSocketClosesOnConcurrentTunnelAbort(t *testing.T) {
	for i := 0; i < 10; i++ {
		s, agent := udpTunnel(t, "127.0.0.1:0")
		c := socksClient(t, s, []byte{5, 1, 0})
		socksCommand(t, c, 2, []byte{1, 127, 0, 0, 1, 0, 0})
		bound := socksReply(t, c, 0)
		peer, err := net.DialTCP("tcp4", nil, bound)
		if err != nil {
			t.Fatal(err)
		}
		socksReply(t, c, 0)
		// Both copy directions are waiting; concurrent abort must release both sockets.
		done := make(chan struct{}, 2)
		for j := 0; j < 2; j++ {
			go func() { agent.Stop(); done <- struct{}{} }()
		}
		<-done
		<-done
		peer.SetDeadline(time.Now().Add(time.Second))
		var buf [1]byte
		if _, err := peer.Read(buf[:]); err == nil {
			t.Fatal("peer remained open")
		} else if e, ok := err.(net.Error); ok && e.Timeout() {
			t.Fatal("accepted socket leaked")
		}
		peer.Close()
		c.Close()
		remaining := 0
		agent.Connections.Range(func(_, _ any) bool { remaining++; return true })
		if remaining != 0 {
			t.Fatalf("remaining agent streams %d", remaining)
		}
		listener, err := net.Listen("tcp", bound.String())
		if err != nil {
			t.Fatal(err)
		}
		listener.Close()
	}
}
