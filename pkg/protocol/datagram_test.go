package protocol

import (
	"bytes"
	"context"
	"errors"
	"github.com/google/uuid"
	"io"
	"testing"
	"testing/synctest"
)

func TestDatagramSlowConsumerDropsWholePacketsWithoutBlockingTCP(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := flowPair(t, DefaultFlowConfig())
		x, y := openFlow(t, a, b)
		send, receive := x.owner.EnableDatagrams(), y.owner.EnableDatagrams()
		packet := bytes.Repeat([]byte{42}, 65507)
		// Drain each transport write, while deliberately never consuming UDP.
		for i := 0; i < 100; i++ {
			if err := send.Send(packet); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
		}
		healthy, peer := openFlow(t, a, b)
		done := make(chan error, 1)
		go func() { _, err := healthy.Write([]byte("healthy")); done <- err }()
		got := make([]byte, 7)
		if _, err := io.ReadFull(peer, got); err != nil || string(got) != "healthy" {
			t.Fatalf("healthy TCP: %q %v", got, err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		// 256KiB admits four complete packets, never part of a fifth.
		for i := 0; i < 4; i++ {
			got, err := receive.Receive()
			if err != nil || !bytes.Equal(got, packet) {
				t.Fatalf("packet %d: %d %v", i, len(got), err)
			}
		}
		waiting := make(chan error, 1)
		go func() { _, err := receive.Receive(); waiting <- err }()
		synctest.Wait()
		select {
		case err := <-waiting:
			t.Fatalf("excess packet retained: %v", err)
		default:
		}
		y.Close()
		if err := <-waiting; err == nil {
			t.Fatal("close did not interrupt receive")
		}
	})
}

func TestDatagramCapacityDoesNotUseTCPCredit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := tinyFlow()
		a, b := flowPair(t, cfg)
		x, y := openFlow(t, a, b)
		send, receive := x.owner.EnableDatagrams(), y.owner.EnableDatagrams()
		// An oversized datagram cannot wait for a capacity that can never exist.
		if err := send.Send(make([]byte, cfg.StreamWindow+1)); !errors.Is(err, ErrDatagramDropped) {
			t.Fatalf("oversize: %v", err)
		}
		packet := []byte{0, 0, 0, 1, 127, 0, 0, 1, 0, 9, 1}
		for i := 0; i < 100; i++ {
			if err := send.Send(packet); err != nil {
				t.Fatal(err)
			}
			got, err := receive.Receive()
			if err != nil || !bytes.Equal(got, packet) {
				t.Fatalf("datagram %d: %v", i, err)
			}
		}
	})
}

func TestDatagramOutboundLimitIncludesInflightAndRejectsWholePackets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		wire := &batchConn{shortConn: shortConn{limit: 1 << 20}, gate: make(chan struct{})}
		cfg := tinyFlow()
		h, err := NewBaseHandlerWithConfig(context.Background(), wire, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Abort()
		defer close(wire.gate)
		for stream := 0; stream < 4; stream++ {
			c := NewConnection(uuid.New(), h.Ctx.Done())
			if err = h.RegisterConnection(c); err != nil {
				t.Fatal(err)
			}
			d := c.EnableDatagrams()
			for i := 0; i < 2; i++ {
				if err = d.Send(make([]byte, 16)); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
			}
			if err = d.Send(make([]byte, 16)); !errors.Is(err, ErrDatagramDropped) {
				t.Fatalf("accepted beyond outbound reservation: %v", err)
			}
		}
		if err = h.RegisterConnection(NewConnection(uuid.New(), h.Ctx.Done())); !errors.Is(err, ErrCapacity) {
			t.Fatalf("accepted fifth stream: %v", err)
		}
	})
}

func TestDatagramShortDomainWithEmptyPayload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := flowPair(t, DefaultFlowConfig())
		x, y := openFlow(t, a, b)
		send, receive := x.owner.EnableDatagrams(), y.owner.EnableDatagrams()
		for _, domain := range []string{"a", "ab"} {
			packet := append([]byte{0, 0, 0, 3, byte(len(domain))}, []byte(domain)...)
			packet = append(packet, 0, 53)
			if err := send.Send(packet); err != nil {
				t.Fatal(err)
			}
			got, err := receive.Receive()
			if err != nil || !bytes.Equal(packet, got) {
				t.Fatalf("short domain: %v %v", got, err)
			}
		}
	})
}
