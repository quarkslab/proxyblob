package protocol

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
)

func TestStreamErrorCodePreservesFailuresAlongsideShutdown(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want byte
	}{
		{"success", nil, ErrNone},
		{"explicit close", net.ErrClosed, ErrStreamCanceled},
		{"canceled", context.Canceled, ErrStreamCanceled},
		{"wrapped closed pipe", fmt.Errorf("copy: %w", io.ErrClosedPipe), ErrStreamCanceled},
		{"reset and cleanup", errors.Join(syscall.ECONNRESET, net.ErrClosed), ErrStreamReset},
		{"cleanup before reset", errors.Join(net.ErrClosed, syscall.ECONNRESET), ErrStreamReset},
		{"timeout and cleanup", errors.Join(os.ErrDeadlineExceeded, net.ErrClosed), ErrTransportTimeout},
		{"numeric failure", errors.Join(net.ErrClosed, ErrFlowControl), byte(ErrFlowControl)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := StreamErrorCode(tc.err); got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
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
		{"reset", syscall.ECONNRESET, ErrStreamReset},
		{"timeout", os.ErrDeadlineExceeded, ErrTransportTimeout},
		{"broken pipe", syscall.EPIPE, ErrStreamBrokenPipe},
		{"local close", net.ErrClosed, ErrStreamCanceled},
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
