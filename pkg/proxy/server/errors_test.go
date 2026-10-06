package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"proxyblob/pkg/protocol"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

func TestRemoteCloseDecodesNumericErrorWithoutChangingCleanup(t *testing.T) {
	// The real server boundary receives a numeric code; descriptions stay here.
	for _, code := range []byte{protocol.ErrConnectionRefused, 255, protocol.ErrNone} {
		a, b := net.Pipe()
		s := NewProxyServer(context.Background(), a)
		c := protocol.NewConnection(uuid.New(), s.Ctx.Done())
		if err := s.RegisterConnection(c); err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		s.OnError = protocolErrorReporter(zerolog.New(&output))
		result := s.OnClose(c.ID, code)
		if result != protocol.ErrNone {
			t.Fatalf("close result %d", result)
		}
		if code == protocol.ErrNone {
			if output.Len() != 0 {
				t.Fatalf("successful close logged an error: %s", &output)
			}
		} else {
			select {
			case <-c.Closed:
			default:
				t.Fatal("failed peer close did not abort connection")
			}
			var record struct {
				Code    byte   `json:"code"`
				ID      string `json:"conn_id"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record.Code != code || record.ID != c.ID.String() || record.Message != ErrorDescription(code) {
				t.Fatalf("missing proxy diagnosis: %+v", record)
			}
			if code == 255 && record.Message != "unknown protocol error" {
				t.Fatal("unknown code has no fallback")
			}
		}
		s.Stop()
		a.Close()
		b.Close()
	}
}

func TestProxyDescriptionsCoverLocalAndWireCodes(t *testing.T) {
	for _, code := range []byte{protocol.ErrBufferFull, byte(protocol.ErrUnsupportedVersion), byte(protocol.ErrJSHostProtocol), byte(protocol.ErrInvalidFlowConfig)} {
		if ErrorDescription(code) == "unknown protocol error" {
			t.Fatalf("missing description for %d", code)
		}
	}
}

func TestRejectedVersionIsDecodedOnlyAtProxy(t *testing.T) {
	a, b := net.Pipe()
	s := NewProxyServer(context.Background(), a)
	defer s.Stop()
	defer a.Close()
	defer b.Close()
	var output bytes.Buffer
	s.OnError = protocolErrorReporter(zerolog.New(&output))
	done := make(chan struct{})
	go func() { s.ReceiveLoop(); close(done) }()
	if _, err := b.Write(protocol.NewPacket(protocol.CmdNew, uuid.New(), nil).Encode()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("unsupported version did not terminate receive loop")
	}
	if !bytes.Contains(output.Bytes(), []byte("unsupported tunnel protocol version")) {
		t.Fatalf("missing proxy description: %s", &output)
	}
}

func TestStreamDiagnosticSeverity(t *testing.T) {
	for _, tc := range []struct {
		code  byte
		level string
	}{
		{protocol.ErrStreamCanceled, "debug"},
		{protocol.ErrConnectionClosed, "warn"},
		{protocol.ErrTransportError, "warn"},
		{protocol.ErrTransportTimeout, "warn"},
		{protocol.ErrStreamReset, "warn"},
		{protocol.ErrStreamBrokenPipe, "warn"},
	} {
		var output bytes.Buffer
		protocolErrorReporter(zerolog.New(&output))(uuid.New(), tc.code)
		var record struct {
			Level string `json:"level"`
			Code  byte   `json:"code"`
		}
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record.Level != tc.level || record.Code != tc.code {
			t.Fatalf("unexpected diagnostic: %+v", record)
		}
	}
}
