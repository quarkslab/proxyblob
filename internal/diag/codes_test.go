package diag

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
)

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

type privateError struct{}

func (privateError) Error() string { panic("must not format raw SDK errors") }

func TestProxyDescriptionsCoverLocalAndWireCodes(t *testing.T) {
	for _, code := range []byte{ErrBufferFull, byte(ErrUnsupportedVersion), byte(ErrJSHostProtocol), byte(ErrInvalidFlowConfig)} {
		if Description(code) == "unknown protocol error" {
			t.Fatalf("missing description for %d", code)
		}
	}
}
