package mux

import (
	"context"
	"errors"
	"net"
	"os"
	"proxyblob/internal/diag"
	"syscall"
	"testing"

	"github.com/google/uuid"
)

func TestInvalidFlowEnvironmentDoesNotExposeInput(t *testing.T) {
	for _, name := range []string{"PROXYBLOB_STREAM_WINDOW", "PROXYBLOB_DRAIN_TIMEOUT"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "private-query?sig=secret")
			_, err := FlowConfigFromEnv()
			if !errors.Is(err, diag.ErrInvalidFlowConfig) || err.Error() != "47" {
				t.Fatalf("unsanitized config failure: %v", err)
			}
		})
	}
}

type failedReadConn struct {
	net.Conn
	failure error
}

func (c failedReadConn) Read([]byte) (int, error) { return 0, c.failure }

func TestForwardPublishesOriginalFailureBeforeClosing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
		want    byte
	}{
		{"reset", syscall.ECONNRESET, diag.ErrStreamReset},
		{"disconnected socket", syscall.ENOTCONN, diag.ErrStreamNotConnected},
		{"timeout", os.ErrDeadlineExceeded, diag.ErrTransportTimeout},
		{"broken pipe", syscall.EPIPE, diag.ErrStreamBrokenPipe},
		{"local close", net.ErrClosed, diag.ErrStreamCanceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := &shortConn{limit: 1 << 20}
			h := NewBaseHandler(context.Background(), wire)
			defer h.Abort()
			c := NewConnection(uuid.New(), h.Ctx.Done())
			if err := h.RegisterConnection(c); err != nil {
				t.Fatal(err)
			}
			pc := NewProtocolConn(h.Ctx, c.ID, h)
			c.SetProtocolConn(pc)
			a, b := net.Pipe()
			defer b.Close()
			err := Forward(pc, failedReadConn{a, tc.failure})
			if !errors.Is(err, tc.failure) {
				t.Fatalf("lost original error: %v", err)
			}
			if err := h.Drain(context.Background()); err != nil {
				t.Fatal(err)
			}
			wire.mu.Lock()
			defer wire.mu.Unlock()
			packet, _, err := ParseNext(wire.wire.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if packet.Command != CmdClose || len(packet.Data) != 1 || packet.Data[0] != tc.want {
				t.Fatalf("wrong close: %+v", packet)
			}
		})
	}
}
