package protocol

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
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
