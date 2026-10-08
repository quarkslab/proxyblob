//go:build !js

package agent

import (
	"errors"
	"io"
	"net"
	"proxyblob/internal/agent/netenv"
	"testing"
	"time"
)

func TestNativeUDPReportsTruncation(t *testing.T) {
	relay, err := netenv.ListenUDP()
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	sender, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: relay.LocalPort()})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err = sender.Write([]byte("complete datagram")); err != nil {
		t.Fatal(err)
	}
	relay.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := relay.ReadFrom(make([]byte, 1))
	if n != 1 || !errors.Is(err, io.ErrShortBuffer) {
		t.Fatalf("silent truncation: %d %v", n, err)
	}
}
