//go:build linux

package proxy

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestBindRejectsUnexpectedPeerIP(t *testing.T) {
	s, _ := udpTunnel(t, "127.0.0.1:0")
	c := socksClient(t, s, []byte{5, 1, 0})
	socksCommand(t, c, 2, []byte{1, 127, 0, 0, 2, 0, 0})
	bound := socksReply(t, c, 0)
	bad, err := net.DialTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}, bound)
	if err != nil {
		t.Fatal(err)
	}
	bad.SetDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if n, err := bad.Read(b[:]); n != 0 || err != io.EOF {
		t.Fatalf("unexpected IP not rejected: %d %v", n, err)
	}
	bad.Close()
	good, err := net.DialTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 2)}, bound)
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()
	if got := socksReply(t, c, 0); !got.IP.Equal(net.IPv4(127, 0, 0, 2)) {
		t.Fatalf("accepted wrong peer %v", got)
	}
}
