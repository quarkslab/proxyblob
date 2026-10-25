package mux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"proxyblob/internal/diag"

	"github.com/google/uuid"
)

func sessionPair(t *testing.T, cfg FlowConfig, opts ...SessionOption) (*Session, *Session) {
	t.Helper()
	a, b := net.Pipe()
	opener, err := NewSession(context.Background(), a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	acceptor, err := NewSession(context.Background(), b, cfg, opts...)
	if err != nil {
		t.Fatal(err)
	}
	opener.StartReceiving()
	acceptor.StartReceiving()
	t.Cleanup(func() { opener.Stop(); acceptor.Stop(); a.Close(); b.Close() })
	return opener, acceptor
}

func openAccepted(t *testing.T, opener, acceptor *Session) (*ProtocolConn, *ProtocolConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	local, err := opener.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	remote, err := acceptor.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return local, remote
}

func TestSessionOpenAcceptHalfClose(t *testing.T) {
	opener, acceptor := sessionPair(t, DefaultFlowConfig(), WithAccept())
	local, remote := openAccepted(t, opener, acceptor)
	request := bytes.Repeat([]byte("request"), 100000)
	go func() {
		local.Write(request)
		local.CloseWrite()
	}()
	got, err := io.ReadAll(remote)
	if err != nil || !bytes.Equal(got, request) {
		t.Fatalf("request: %d bytes, %v", len(got), err)
	}
	// The other direction stays writable after the peer's EOF.
	if _, err := remote.Write([]byte("response")); err != nil {
		t.Fatal(err)
	}
	remote.CloseWrite()
	if got, err := io.ReadAll(local); err != nil || string(got) != "response" {
		t.Fatalf("response: %q %v", got, err)
	}
}

func TestSessionRejectsStreamsWithoutAccept(t *testing.T) {
	opener, _ := sessionPair(t, DefaultFlowConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := opener.Open(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unaccepted stream: %v", err)
	}
}

func TestSessionReserveRefusesLocallyAndReleases(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxStreams = 1
	cfg.ControlSlots = 4
	opener, acceptor := sessionPair(t, cfg, WithAccept())
	r, err := opener.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opener.Reserve(); !errors.Is(err, diag.ErrCapacity) {
		t.Fatalf("second reservation: %v", err)
	}
	r.Release()
	local, _ := openAccepted(t, opener, acceptor)
	local.Close()
}

func TestSessionGracefulCloseKeepsSentBytes(t *testing.T) {
	opener, acceptor := sessionPair(t, DefaultFlowConfig(), WithAccept())
	local, remote := openAccepted(t, opener, acceptor)
	if _, err := remote.Write([]byte("reply")); err != nil {
		t.Fatal(err)
	}
	remote.CloseWithCode(diag.ErrNone)
	if got, err := io.ReadAll(local); err != nil || string(got) != "reply" {
		t.Fatalf("graceful close lost bytes: %q %v", got, err)
	}
}

func TestSessionStreamDatagrams(t *testing.T) {
	opener, acceptor := sessionPair(t, DefaultFlowConfig(), WithAccept())
	local, remote := openAccepted(t, opener, acceptor)
	send, receive := local.EnableDatagrams(), remote.EnableDatagrams()
	if send == nil || receive == nil || local.EnableDatagrams() != nil {
		t.Fatal("datagram enablement")
	}
	for _, size := range []int{0, 1, MaxDatagramSize} {
		message := bytes.Repeat([]byte{byte(size)}, size)
		if err := send.Send(message); err != nil {
			t.Fatal(err)
		}
		got, err := receive.Receive()
		if err != nil || !bytes.Equal(got, message) {
			t.Fatalf("datagram of %d bytes: got %d, %v", size, len(got), err)
		}
	}
}

func TestSessionReserveWaitQueuesUntilSlotFrees(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxStreams = 1
	cfg.ControlSlots = 4
	opener, _ := sessionPair(t, cfg, WithAccept())
	first, err := opener.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	// A bounded wait on a full session ends with the capacity error.
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := opener.ReserveWait(short); !errors.Is(err, diag.ErrCapacity) {
		t.Fatalf("full session: %v", err)
	}
	// A waiter is admitted as soon as the slot is released.
	got := make(chan error, 1)
	go func() {
		r, err := opener.ReserveWait(context.Background())
		if err == nil {
			r.Release()
		}
		got <- err
	}()
	time.Sleep(20 * time.Millisecond)
	first.Release()
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("waiter: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter was not admitted after release")
	}
}

func TestLatePacketForClosedStreamGetsNoReply(t *testing.T) {
	opener, acceptor := sessionPair(t, DefaultFlowConfig(), WithAccept())
	var mu sync.Mutex
	var codes []byte
	acceptor.OnError = func(_ uuid.UUID, code byte) { mu.Lock(); codes = append(codes, code); mu.Unlock() }
	local, remote := openAccepted(t, opener, acceptor)
	id := local.ID()
	local.Close()
	remote.Close()
	// The acceptor sends DATA and EOF for a stream the opener has already closed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := opener.Connections.Load(id); !ok {
			break
		}
		time.Sleep(time.Millisecond)
	}
	acceptor.sendPacket(CmdData, id, []byte("late"))
	acceptor.sendPacket(CmdEOF, id, nil)
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for _, c := range codes {
		if c == diag.ErrConnectionNotFound {
			t.Fatalf("peer was told %d for a late packet", c)
		}
	}
}
