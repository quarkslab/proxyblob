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

func TestWindowStaysForSlowReader(t *testing.T) {
	cfg := DefaultFlowConfig()
	opener, acceptor := sessionPair(t, cfg, WithAccept())
	local, remote := openAccepted(t, opener, acceptor)
	// The reader lets the full window pile up before each read: the backlog,
	// not the window, limits this transfer, so more memory would not help.
	transfer(t, local, remote, 4*cfg.StreamWindow, func(r io.Reader) ([]byte, error) {
		var got []byte
		buf := make([]byte, cfg.StreamWindow)
		for {
			deadline := time.Now().Add(time.Second)
			for {
				remote.owner.deliveryMu.Lock()
				used, ended := remote.owner.used, remote.owner.deliveryEnded
				remote.owner.deliveryMu.Unlock()
				if used == cfg.StreamWindow || ended || time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			n, err := io.ReadFull(r, buf)
			got = append(got, buf[:n]...)
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return got, nil
			}
			if err != nil {
				return got, err
			}
		}
	})
	if window, _ := receiveWindow(remote); window != cfg.StreamWindow {
		t.Fatalf("slow reader window grew to %d", window)
	}
}

func TestWindowGrowthRespectsTunnelBudget(t *testing.T) {
	cfg := DefaultFlowConfig()
	cfg.TunnelWindow = 2 * cfg.StreamWindow
	opener, acceptor := sessionPair(t, cfg, WithAccept())
	local, remote := openAccepted(t, opener, acceptor)
	transfer(t, local, remote, 8<<20, io.ReadAll)
	if window, _ := receiveWindow(remote); window != cfg.TunnelWindow {
		t.Fatalf("window %d beyond or below the %d budget", window, cfg.TunnelWindow)
	}
	// The grown window is charged to the tunnel until the stream closes.
	if _, err := acceptor.Reserve(); err == nil {
		t.Fatal("budget not charged for the grown window")
	}
	remote.Close()
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
