package relay

import (
	"bytes"
	"errors"
	"net"
	"testing"

	"proxyblob/internal/diag"
)

func TestRequestRoundTripAllAddressTypes(t *testing.T) {
	for _, addr := range [][]byte{
		{1, 127, 0, 0, 1, 0, 80},
		append(append([]byte{4}, net.IPv6loopback...), 1, 187),
		append(append([]byte{3, 11}, "example.com"...), 0, 53),
	} {
		var b bytes.Buffer
		if err := WriteRequest(&b, Bind, addr); err != nil {
			t.Fatal(err)
		}
		cmd, got, err := ReadRequest(&b)
		if err != nil || cmd != Bind || !bytes.Equal(got, addr) || b.Len() != 0 {
			t.Fatalf("request %v: %d %v %v, %d left", addr, cmd, got, err, b.Len())
		}
	}
}

func TestReadRequestRejectsUnknownAddressType(t *testing.T) {
	_, _, err := ReadRequest(bytes.NewReader([]byte{Connect, 9}))
	if !errors.Is(err, diag.Error(diag.ErrAddressNotSupported)) {
		t.Fatalf("unknown address type: %v", err)
	}
}

func TestReplyEncodesBoundAddressFamily(t *testing.T) {
	for _, tc := range []struct {
		bound net.Addr
		want  string
	}{
		{&net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 4000}, "10.0.0.1:4000"},
		{&net.UDPAddr{IP: net.IPv6loopback, Port: 53}, "[::1]:53"},
		{nil, "0.0.0.0:0"},
	} {
		var b bytes.Buffer
		if err := WriteReply(&b, diag.ErrHostUnreachable, tc.bound); err != nil {
			t.Fatal(err)
		}
		code, addr, err := ReadReply(&b)
		host, _ := ParseAddress(addr)
		if err != nil || code != diag.ErrHostUnreachable || host != tc.want {
			t.Fatalf("reply %v: %d %q %v", tc.bound, code, host, err)
		}
	}
}

func TestDatagramRoundTrip(t *testing.T) {
	source := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 7), Port: 9000}
	packet := Datagram(source, []byte("payload"))
	target, offset, code := ParseDatagram(packet)
	if code != diag.ErrNone || target != "192.0.2.7:9000" || string(packet[offset:]) != "payload" {
		t.Fatalf("datagram: %q %d %d", target, offset, code)
	}
	if _, _, code := ParseDatagram([]byte{0, 0, 1, 1}); code == diag.ErrNone {
		t.Fatal("fragmented datagram accepted")
	}
}
