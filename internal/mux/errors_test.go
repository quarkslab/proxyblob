package mux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/google/uuid"
)

type privateError struct{}

func (privateError) Error() string { panic("must not format raw SDK errors") }

func TestDiagnosticCodesDoNotFormatPrivateCauses(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want byte
	}{
		{nil, ErrNone}, {privateError{}, ErrTransportError},
		{fmt.Errorf("wrapper: %w", ErrFlowControl), byte(ErrFlowControl)},
		{context.Canceled, ErrContextCanceled}, {context.DeadlineExceeded, ErrTransportTimeout},
	} {
		if got := ErrorCode(tc.err); got != tc.want {
			t.Fatalf("code %d, want %d", got, tc.want)
		}
	}
	if !errors.Is(fmt.Errorf("wrapped: %w", ErrFlowControl), ErrFlowControl) {
		t.Fatal("numeric sentinel lost identity")
	}
	if ErrFlowControl.Error() != "44" {
		t.Fatal("agent diagnostic is not numeric")
	}
}

func TestInvalidFlowEnvironmentDoesNotExposeInput(t *testing.T) {
	for _, name := range []string{"PROXYBLOB_STREAM_WINDOW", "PROXYBLOB_DRAIN_TIMEOUT"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "private-query?sig=secret")
			_, err := FlowConfigFromEnv()
			if !errors.Is(err, ErrInvalidFlowConfig) || err.Error() != "47" {
				t.Fatalf("unsanitized config failure: %v", err)
			}
		})
	}
}

func TestNumericBindFailuresPreserveSOCKSMapping(t *testing.T) {
	for _, tc := range []struct{ before, after error }{
		{&net.AddrError{Err: "no BIND peer addresses"}, ErrNoBindPeers},
		{&net.DNSError{Err: "no addresses"}, ErrNoDNSAddresses},
		{&net.AddrError{Err: "no usable interface for wildcard BIND"}, ErrNoBindInterface},
	} {
		if MapNetError(tc.before) != MapNetError(tc.after) {
			t.Fatalf("SOCKS mapping changed for %v", tc.after)
		}
	}
}

func TestStreamErrorCodePreservesFailuresAlongsideShutdown(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want byte
	}{
		{"success", nil, ErrNone},
		{"shutdown after peer reset", &net.OpError{Op: "close", Net: "tcp", Err: &os.SyscallError{Syscall: "shutdown", Err: syscall.ENOTCONN}}, ErrStreamNotConnected},
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
		{"disconnected socket", syscall.ENOTCONN, ErrStreamNotConnected},
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

func TestJoinedClosuresCannotHideWarnings(t *testing.T) {
	for _, closure := range []error{net.ErrClosed, syscall.EPIPE, syscall.ECONNRESET, syscall.ENOTCONN} {
		for _, failure := range []error{os.ErrDeadlineExceeded, ErrFlowControl, errors.New("unknown failure")} {
			want := StreamErrorCode(failure)
			for _, joined := range []error{
				errors.Join(closure, failure), errors.Join(failure, closure),
				errors.Join(closure, fmt.Errorf("wrapped: %w", errors.Join(net.ErrClosed, failure))),
			} {
				if got := StreamErrorCode(joined); got != want {
					t.Fatalf("closure %v masked %v: got %d want %d", closure, failure, got, want)
				}
			}
		}
	}
}
