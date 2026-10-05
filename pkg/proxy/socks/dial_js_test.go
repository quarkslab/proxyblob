//go:build js && wasm

package proxy

import (
	"errors"
	"io"
	"syscall/js"
	"testing"
	"time"
)

// This fixture validates the Go adapter's contract, not a production TCPDial
// host. The real host must implement half-open sockets and numeric write counts.
func mockJSConn(t *testing.T, writeResult any) (*jsConn, *int) {
	t.Helper()
	pr, pw := io.Pipe()
	c := &jsConn{pr: pr, pw: pw, inbox: make(chan jsChunk, inboxCapacity), closed: make(chan struct{})}
	ends := 0
	write := js.FuncOf(func(_ js.Value, args []js.Value) any { return writeResult })
	end := js.FuncOf(func(_ js.Value, args []js.Value) any { ends++; return nil })
	socket := js.Global().Get("Object").New()
	socket.Set("write", write)
	socket.Set("end", end)
	c.socket = socket
	go c.writeLoop()
	t.Cleanup(func() { c.Close(); write.Release(); end.Release() })
	return c, &ends
}

func TestJSPeerEOFLeavesWritesOpen(t *testing.T) {
	c, ends := mockJSConn(t, 8)
	c.enqueue(jsChunk{data: []byte("trailing")})
	c.enqueue(jsChunk{eof: true})
	got, err := io.ReadAll(c)
	if string(got) != "trailing" || err != nil {
		t.Fatalf("Read: %q %v", got, err)
	}
	if n, err := c.Write([]byte("response")); n != 8 || err != nil {
		t.Fatalf("write after peer EOF: %d %v", n, err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Write([]byte("late")); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write after local EOF: %d %v", n, err)
	}
	c.Close()
	if *ends != 1 {
		t.Fatalf("end calls: %d", *ends)
	}
}

func TestJSLocalEOFLeavesReadsOpen(t *testing.T) {
	c, _ := mockJSConn(t, 0)
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	c.enqueue(jsChunk{data: []byte("response")})
	c.enqueue(jsChunk{eof: true})
	got, err := io.ReadAll(c)
	if string(got) != "response" || err != nil {
		t.Fatalf("read after local EOF: %q %v", got, err)
	}
}

func TestJSShortWritesAndUnsupportedDeadlines(t *testing.T) {
	for _, tc := range []struct {
		name    string
		count   any
		wantN   int
		wantErr error
	}{
		{"short", 2, 2, io.ErrShortWrite}, {"zero", 0, 0, io.ErrShortWrite}, {"unknown", nil, 0, errors.ErrUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := mockJSConn(t, tc.count)
			n, err := c.Write([]byte("data"))
			if n != tc.wantN || !errors.Is(err, tc.wantErr) {
				t.Fatalf("Write: %d %v", n, err)
			}
			for _, set := range []func(time.Time) error{c.SetDeadline, c.SetReadDeadline, c.SetWriteDeadline} {
				if !errors.Is(set(time.Now()), errors.ErrUnsupported) {
					t.Fatal("unsupported deadline reported success")
				}
			}
		})
	}
}
