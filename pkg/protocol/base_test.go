package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
)

type receiveRead struct {
	data []byte
	err  error
}

// Embedding net.Conn makes unexpected transport operations fail the test.
// A scripted result larger than p is drained across reads before its error.
type receiveConn struct {
	net.Conn
	t     *testing.T
	reads []receiveRead
	calls int
}

func (c *receiveConn) Read(p []byte) (int, error) {
	c.calls++
	if len(p) != 64*1024 {
		c.t.Fatalf("receive scratch size = %d, want 65536", len(p))
	}
	if len(c.reads) == 0 {
		c.t.Fatal("unexpected read after script exhausted")
	}
	r := &c.reads[0]
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) > 0 {
		return n, nil
	}
	err := r.err
	c.reads = c.reads[1:]
	return n, err
}

type receiveHandler struct {
	PacketHandler
	cancel  context.CancelFunc
	packets []*Packet
	stops   int
}

func (h *receiveHandler) OnData(id uuid.UUID, data []byte) byte {
	h.packets = append(h.packets, NewPacket(CmdData, id, append([]byte(nil), data...)))
	return ErrNone
}

func (h *receiveHandler) Stop() { h.stops++; h.cancel() }

func runReceive(t *testing.T, reads []receiveRead, want []*Packet, calls int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &receiveConn{t: t, reads: reads}
	handler := &receiveHandler{cancel: cancel}
	// Construct just the receiver: these cases must not start a writer goroutine.
	h := &BaseHandler{flow: DefaultFlowConfig(), conn: conn, Ctx: ctx, Cancel: cancel, PacketHandler: handler}
	h.ReceiveLoop()
	if handler.stops != 1 {
		t.Errorf("Stop calls = %d, want 1", handler.stops)
	}
	if conn.calls != calls {
		t.Errorf("Read calls = %d, want %d", conn.calls, calls)
	}
	if len(handler.packets) != len(want) {
		t.Fatalf("delivered %d records, want %d", len(handler.packets), len(want))
	}
	for i := range want {
		assertPacketEqual(t, handler.packets[i], want[i], i)
	}
}

func TestReceiveLoopReassembly(t *testing.T) {
	var want []*Packet
	var wire []byte
	for i, size := range []int{1, 50, 2*64*1024 + 123, 17} {
		p := NewPacket(CmdData, uuid.New(), makePayload(size, byte(i)))
		want = append(want, p)
		wire = append(wire, p.Encode()...)
	}
	for _, chunk := range []int{1, 7, HeaderSize, 8192, 64 * 1024} {
		for _, eofWithData := range []bool{false, true} {
			t.Run(fmt.Sprintf("chunk_%d/eof_with_data_%t", chunk, eofWithData), func(t *testing.T) {
				var reads []receiveRead
				for start := 0; start < len(wire); start += chunk {
					reads = append(reads, receiveRead{data: wire[start:min(start+chunk, len(wire))]})
				}
				if eofWithData {
					reads[len(reads)-1].err = io.EOF
				} else {
					reads = append(reads, receiveRead{err: io.EOF})
				}
				runReceive(t, reads, want, len(reads))
			})
		}
	}
}

func TestReceiveLoopBytesWithTerminalError(t *testing.T) {
	for _, err := range []error{io.EOF, net.ErrClosed, io.ErrClosedPipe, os.ErrDeadlineExceeded} {
		t.Run(err.Error(), func(t *testing.T) {
			first := NewPacket(CmdData, uuid.New(), []byte("first"))
			last := NewPacket(CmdData, uuid.New(), []byte("final payload"))
			wire := append(first.Encode(), last.Encode()...)
			// Complete one fragmented record and drain a coalesced record before Stop.
			runReceive(t, []receiveRead{{data: wire[:3]}, {data: wire[3:], err: fmt.Errorf("transport: %w", err)}}, []*Packet{first, last}, 2)
		})
	}
}

func TestReceiveLoopBytesWithTransientError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := NewPacket(CmdData, uuid.New(), []byte("payload split across an error"))
		wire := p.Encode()
		runReceive(t, []receiveRead{{data: wire[:HeaderSize+2], err: errors.New("temporary")}, {data: wire[HeaderSize+2:], err: io.EOF}}, []*Packet{p}, 2)
	})
}

func TestReceiveLoopBoundedRetries(t *testing.T) {
	for _, withData := range []bool{false, true} {
		t.Run(fmt.Sprintf("with_data_%t", withData), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var reads []receiveRead
				var want []*Packet
				for i := 0; i < 20; i++ {
					r := receiveRead{err: errors.New("persistent")}
					if withData {
						p := NewPacket(CmdData, uuid.New(), []byte{byte(i)})
						r.data = p.Encode()
						want = append(want, p)
					}
					reads = append(reads, r)
				}
				start := time.Now()
				runReceive(t, reads, want, 20)
				// 100ms through 3.2s, then thirteen capped 5s waits; no wait after error 20.
				if elapsed := time.Since(start); elapsed != 71300*time.Millisecond {
					t.Errorf("retry elapsed = %v, want 71.3s", elapsed)
				}
			})
		})
	}
}
