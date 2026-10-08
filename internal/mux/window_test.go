package mux

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func receiveWindow(c *ProtocolConn) (window, ring int) {
	c.owner.deliveryMu.Lock()
	defer c.owner.deliveryMu.Unlock()
	return c.owner.window, len(c.owner.buffer)
}

func sendWindow(c *ProtocolConn) uint64 {
	c.owner.creditMu.Lock()
	defer c.owner.creditMu.Unlock()
	return c.owner.peerWindow
}

// transfer sends size bytes from local to remote; the remote reads with read.
func transfer(t *testing.T, local, remote *ProtocolConn, size int, read func(io.Reader) ([]byte, error)) {
	t.Helper()
	payload := bytes.Repeat([]byte{7}, size)
	errs := make(chan error, 1)
	go func() {
		_, err := local.Write(payload)
		if err == nil {
			err = local.CloseWrite()
		}
		errs <- err
	}()
	got, err := read(remote)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("transfer: %d of %d bytes, %v", len(got), size, err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

func TestWindowGrowsForFastReader(t *testing.T) {
	cfg := DefaultFlowConfig()
	opener, acceptor := sessionPair(t, cfg, WithAccept())
	local, remote := openAccepted(t, opener, acceptor)
	transfer(t, local, remote, 16<<20, io.ReadAll)
	if window, _ := receiveWindow(remote); window != cfg.MaxStreamWindow {
		t.Fatalf("fast reader window %d, want %d", window, cfg.MaxStreamWindow)
	}
	// The sender learns each grown window from the receiver's credit.
	deadline := time.Now().Add(time.Second)
	for sendWindow(local) != uint64(cfg.MaxStreamWindow) {
		if time.Now().After(deadline) {
			t.Fatalf("sender window %d", sendWindow(local))
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWindowStaysForSmallExchanges(t *testing.T) {
	cfg := DefaultFlowConfig()
	opener, acceptor := sessionPair(t, cfg, WithAccept())
	local, remote := openAccepted(t, opener, acceptor)
	transfer(t, local, remote, cfg.StreamWindow-1, io.ReadAll)
	if window, _ := receiveWindow(remote); window != cfg.StreamWindow {
		t.Fatalf("exchange below one window grew it to %d", window)
	}
}

func TestWindowGrowthKeepsAdmission(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxStreams = 4
	cfg.ControlSlots = 16
	cfg.TunnelWindow = cfg.MaxStreams*cfg.StreamWindow + cfg.StreamWindow
	opener, acceptor := sessionPair(t, cfg, WithAccept())
	local, remote := openAccepted(t, opener, acceptor)
	transfer(t, local, remote, 8<<20, io.ReadAll)
	if window, _ := receiveWindow(remote); window != 2*cfg.StreamWindow {
		t.Fatalf("growth took %d, beyond the spare budget", window)
	}
	// Every remaining stream slot is still admitted at the initial window.
	for i := 1; i < cfg.MaxStreams; i++ {
		if _, err := acceptor.Reserve(); err != nil {
			t.Fatalf("stream %d refused after growth: %v", i+1, err)
		}
	}
}

func TestWindowGrowthRespectsTunnelBudget(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.MaxStreams = 1
	cfg.ControlSlots = 4
	cfg.TunnelWindow = 2 * cfg.StreamWindow
	opener, acceptor := sessionPair(t, cfg, WithAccept())
	local, remote := openAccepted(t, opener, acceptor)
	transfer(t, local, remote, 8<<20, io.ReadAll)
	if window, _ := receiveWindow(remote); window != cfg.TunnelWindow {
		t.Fatalf("window %d beyond or below the %d budget", window, cfg.TunnelWindow)
	}
	// Closing the stream returns its grown window to the budget.
	remote.Close()
	local.Close()
	deadline := time.Now().Add(time.Second)
	for {
		r, err := acceptor.Reserve()
		if err == nil {
			r.Release()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("closing did not release the grown window: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	acceptor.flowMu.Lock()
	reserved := acceptor.reserved
	acceptor.flowMu.Unlock()
	if reserved != 0 {
		t.Fatalf("%d bytes still reserved", reserved)
	}
}

func TestReceiveMemoryAllocatedOnDemand(t *testing.T) {
	opener, acceptor := sessionPair(t, DefaultFlowConfig(), WithAccept())
	local, remote := openAccepted(t, opener, acceptor)
	if _, ring := receiveWindow(remote); ring != 0 {
		t.Fatalf("idle stream holds %d bytes", ring)
	}
	if _, err := local.Write([]byte("small")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(remote, make([]byte, 5)); err != nil {
		t.Fatal(err)
	}
	if _, ring := receiveWindow(remote); ring != minRing {
		t.Fatalf("small exchange allocated %d bytes", ring)
	}
}
