//go:build !js

package proxy

import (
	"context"
	"net"
	"testing"
)

// Exercise actual route selection and listener creation with resolved candidates.
func TestBindTriesUsableCandidateAfterUnusableAddress(t *testing.T) {
	listener, err := listenBind(context.Background(), []net.IPAddr{{IP: net.IP{1}}, {IP: net.IPv4(127, 0, 0, 1)}})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if !listener.Addr().(*net.TCPAddr).IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("listener %v", listener.Addr())
	}
	peer, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	accepted.Close()
}
