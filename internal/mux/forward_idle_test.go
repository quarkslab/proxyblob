package mux

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"proxyblob/internal/diag"
)

// tcpPair returns both ends of a loopback TCP connection.
func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	dialed, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dialed.Close(); accepted.Close() })
	return dialed.(*net.TCPConn), accepted.(*net.TCPConn)
}

func TestForwardClosesAnIdleHalfClosedStream(t *testing.T) {
	client, a := tcpPair(t)
	b, target := tcpPair(t)
	const idle = 200 * time.Millisecond
	result := make(chan error, 1)
	go func() { result <- forward(a, b, idle) }()

	client.CloseWrite()
	if _, err := io.ReadAll(target); err != nil { // the client's EOF reaches the target
		t.Fatal(err)
	}
	// Traffic the other way keeps the stream open past the idle limit.
	start := time.Now()
	for range 4 {
		time.Sleep(idle / 2)
		if _, err := target.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-result:
		t.Fatalf("closed after %v while traffic flowed: %v", time.Since(start), err)
	default:
	}
	// The target then goes quiet without closing.
	select {
	case err := <-result:
		if !errors.Is(err, diag.ErrHalfClosedIdle) {
			t.Fatalf("got %v, want the idle diagnostic", err)
		}
	case <-time.After(5 * idle):
		t.Fatal("an idle half-closed stream stayed open")
	}
}

func TestForwardWithoutHalfCloseNeverIdlesOut(t *testing.T) {
	client, a := tcpPair(t)
	b, target := tcpPair(t)
	result := make(chan error, 1)
	go func() { result <- forward(a, b, 50*time.Millisecond) }()
	select {
	case err := <-result:
		t.Fatalf("an open, quiet stream was closed: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	client.CloseWrite()
	target.CloseWrite()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
