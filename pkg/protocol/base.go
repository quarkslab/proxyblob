package protocol

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// PacketHandler processes protocol packets and manages connection lifecycle.
// Implementations must be safe for concurrent use by multiple goroutines.
type PacketHandler interface {
	// Start begins packet processing and listens on the specified address (listen only on proxy side)
	Start(string)

	// Stop gracefully terminates all connections and processing
	Stop()

	// ReceiveLoop processes incoming packets until stopped
	ReceiveLoop()

	// OnNew handles connection establishment request
	OnNew(uuid.UUID, []byte) byte

	// OnAck handles connection establishment acknowledgment
	OnAck(uuid.UUID, []byte) byte

	// OnData handles payload transfer for established connection
	OnData(uuid.UUID, []byte) byte

	// OnClose handles connection termination request
	OnClose(uuid.UUID, byte) byte
}

// BaseHandler implements common protocol functionality for proxy and agent.
// It provides connection management, packet routing, and error handling.
type BaseHandler struct {
	// conn handles underlying packet transmission (direct net.Conn)
	conn net.Conn

	// writeCh buffers encoded packets for the writeLoop goroutine
	writeCh chan []byte

	// Connections maps UUIDs to active Connection objects
	Connections sync.Map

	// Ctx controls handler lifecycle
	Ctx context.Context

	// Cancel terminates handler context
	Cancel context.CancelFunc

	// OnReceive is called on every successful packet read (optional)
	OnReceive func()

	// PacketHandler routes packets to specific handlers
	PacketHandler
}

// NewBaseHandler creates a handler with specified context and connection.
// Uses background context if parent context is nil.
func NewBaseHandler(parentCtx context.Context, conn net.Conn) *BaseHandler {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(parentCtx)
	h := &BaseHandler{
		conn:    conn,
		writeCh: make(chan []byte, 1024),
		Ctx:     ctx,
		Cancel:  cancel,
	}
	go h.writeLoop()
	return h
}

// ReceiveLoop processes incoming packets until the transport dies or the
// context is cancelled. Uses exponential backoff on transient errors but never
// exits silently.
func (h *BaseHandler) ReceiveLoop() {
	// A panic here would otherwise unwind to the top of this goroutine and kill
	// the process, taking down every listener and every connected agent at once.
	// Contain it and tear down only this handler.
	defer func() {
		if r := recover(); r != nil {
			log.Error().
				Interface("panic", r).
				Str("stack", string(debug.Stack())).
				Msg("Recovered panic in protocol receive loop")
			h.Stop()
		}
	}()

	consecutiveErrors := 0
	const maxBackoff = 5 * time.Second
	// Highest shift applied to the 100ms base delay. 100ms<<6 = 6.4s already
	// exceeds maxBackoff, and clamping keeps the shift far away from the point
	// (58) where the int64 shift wraps negative, which would defeat the cap and
	// make time.After fire immediately in a 100% CPU hot loop.
	const maxBackoffShift = 6

	// A transport can fail permanently with an error outside the terminal set
	// above. Retrying such an error never succeeds, and without a bound the loop
	// would spin indefinitely while no payload moves and no connection is reset,
	// which is externally indistinguishable from an idle tunnel. Since the
	// backoff saturates at maxBackoff, this bound is reached only after a long
	// run of uninterrupted failures, well beyond any transient outage.
	const maxConsecutiveErrors = 20

	// The transport is a plain byte stream: it does not preserve message
	// boundaries. A single Read may return a fragment of a record, several
	// records back to back, or a whole record plus the head of the next one.
	// The accumulator below therefore lives OUTSIDE the read loop so that a
	// record straddling two Read calls is reassembled rather than discarded.
	// The read size only bounds how many iterations a large payload takes, never
	// how it is framed, so it costs nothing to keep it small.
	buffer := make([]byte, 64*1024)
	acc := make([]byte, 0, HeaderSize+MaxPacketDataSize)

	for {
		select {
		case <-h.Ctx.Done():
			return
		default:
		}

		n, err := h.conn.Read(buffer)
		if err != nil {
			// Terminal transport errors: the connection will never yield bytes
			// again, so backing off would spin forever while h.Stop() never runs
			// and every logical connection hangs instead of being reset.
			if errors.Is(err, io.EOF) ||
				errors.Is(err, net.ErrClosed) ||
				errors.Is(err, io.ErrClosedPipe) ||
				errors.Is(err, os.ErrDeadlineExceeded) {
				h.Stop()
				return
			}

			if h.Ctx.Err() != nil {
				return
			}

			// Transient error: exponential backoff (100ms, 200ms, 400ms, ... capped at 5s)
			consecutiveErrors++
			if consecutiveErrors >= maxConsecutiveErrors {
				log.Error().
					Err(err).
					Int("consecutive", consecutiveErrors).
					Msg("Transport failing persistently, tearing down handler")
				h.Stop()
				return
			}
			shift := consecutiveErrors - 1
			if shift > maxBackoffShift {
				shift = maxBackoffShift
			}
			backoff := time.Duration(100<<uint(shift)) * time.Millisecond
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			// Interruptible sleep
			select {
			case <-h.Ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}

		consecutiveErrors = 0
		if h.OnReceive != nil {
			h.OnReceive()
		}

		if n == 0 {
			continue
		}

		acc = append(acc, buffer[:n]...)

		// Drain every complete record currently buffered. offset tracks how much
		// of acc has been consumed so the remainder can be kept for the next Read.
		offset := 0
		for {
			packet, consumed, perr := ParseNext(acc[offset:])
			if perr != nil {
				if errors.Is(perr, ErrShortPacket) {
					// Nothing was consumed: the trailing bytes are the head of a
					// record whose tail has not arrived yet. Keep them.
					break
				}

				// Malformed framing is unrecoverable. A length-prefixed stream has
				// no resync point, and the uuid in a bogus header is garbage, so
				// closing "just that connection" would target a random one while
				// the stream stayed misaligned. Tear the handler down.
				log.Error().
					Err(perr).
					Int("buffered", len(acc)-offset).
					Msg("Malformed protocol framing, tearing down handler")
				h.Stop()
				return
			}
			offset += consumed

			errCode := h.handlePacket(packet)
			if errCode != ErrNone {
				if h.Ctx.Err() != nil {
					break
				}
				// Async close dispatch to avoid blocking ReceiveLoop on writes
				go h.SendClose(packet.ConnectionID, errCode)
			}
		}

		// Compact only when something was consumed. copy has memmove semantics
		// and the destination index is <= the source index, so the overlapping
		// slide is safe.
		if offset > 0 {
			if offset == len(acc) {
				acc = acc[:0]
			} else {
				acc = acc[:copy(acc, acc[offset:])]
			}
		}
	}
}

// handlePacket routes packet to appropriate handler based on command.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) handlePacket(packet *Packet) byte {
	switch packet.Command {
	case CmdNew:
		return h.PacketHandler.OnNew(packet.ConnectionID, packet.Data)
	case CmdAck:
		return h.PacketHandler.OnAck(packet.ConnectionID, packet.Data)
	case CmdData:
		return h.PacketHandler.OnData(packet.ConnectionID, packet.Data)
	case CmdClose:
		// A close carries a one byte error code; indexing an empty payload would
		// panic. This is a per-connection error and deliberately NOT malformed
		// framing: the length prefix was intact and the byte stream is still in
		// sync, so it must not tear down the whole handler.
		if len(packet.Data) == 0 {
			return ErrInvalidPacket
		}
		return h.PacketHandler.OnClose(packet.ConnectionID, packet.Data[0])
	default:
		return ErrInvalidCommand
	}
}

// SendNewConnection initiates a new connection.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) SendNewConnection(connectionID uuid.UUID) byte {
	return h.sendPacket(CmdNew, connectionID, nil)
}

// SendConnAck acknowledges connection.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) SendConnAck(connectionID uuid.UUID) byte {
	return h.sendPacket(CmdAck, connectionID, nil)
}

// SendData sends data.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) SendData(connectionID uuid.UUID, data []byte) byte {
	// Verify connection exists
	if _, exists := h.Connections.Load(connectionID); !exists {
		return ErrConnectionNotFound
	}

	return h.sendPacket(CmdData, connectionID, data)
}

// SendClose sends a connection termination packet with an error code.
func (h *BaseHandler) SendClose(connectionID uuid.UUID, errCode byte) byte {
	connObj, exists := h.Connections.Load(connectionID)
	if !exists {
		return ErrConnectionNotFound
	}
	conn := connObj.(*Connection)

	conn.Close()
	h.Connections.Delete(connectionID)
	return h.sendPacket(CmdClose, connectionID, []byte{errCode})
}

// sendPacket encodes a packet and submits it to the write coalescing channel.
// The actual aznet write happens asynchronously in writeLoop.
func (h *BaseHandler) sendPacket(cmd byte, connectionID uuid.UUID, data []byte) byte {
	if h.Ctx.Err() != nil {
		return ErrHandlerStopped
	}

	packet := NewPacket(cmd, connectionID, data)
	if packet == nil {
		return ErrInvalidPacket
	}

	encoded := packet.Encode()
	if encoded == nil {
		return ErrInvalidPacket
	}

	select {
	case h.writeCh <- encoded:
		return ErrNone
	case <-h.Ctx.Done():
		return ErrHandlerStopped
	}
}

// writeLoop coalesces queued packets and writes them in batches to aznet.
// Only this goroutine calls conn.Write, eliminating fmu contention.
func (h *BaseHandler) writeLoop() {
	for {
		var buf []byte

		// Wait for the first packet
		select {
		case first := <-h.writeCh:
			buf = append(buf, first...)
		case <-h.Ctx.Done():
			// sendPacket already reported success to its callers (ultimately to
			// io.Copy) for everything still queued, so returning here without
			// draining would silently discard records the peer was told to
			// expect. Flush what is left, best effort, then exit.
			h.drainWriteCh()
			return
		}

		// Drain all queued packets (non-blocking)
		for {
			select {
			case more := <-h.writeCh:
				buf = append(buf, more...)
			default:
				goto flush
			}
		}

	flush:
		if _, err := h.conn.Write(buf); err != nil {
			// The underlying Write can return (0, err) even when part of the batch
			// already reached the peer, so there is no safe retry: resending the
			// batch would duplicate bytes on the wire and desynchronize the
			// length-prefixed stream. Cancelling without retry stays correct.
			h.Cancel()
			return
		}
	}
}

// drainWriteCh empties writeCh without blocking and makes a single final write
// attempt with whatever was still queued. Used on shutdown so that records
// already acknowledged by sendPacket get one chance to reach the peer.
func (h *BaseHandler) drainWriteCh() {
	var buf []byte
	for {
		select {
		case more := <-h.writeCh:
			buf = append(buf, more...)
		default:
			if len(buf) > 0 {
				// Best effort: the transport may already be gone. The same
				// no-retry rule as in writeLoop applies to any error here.
				_, _ = h.conn.Write(buf)
			}
			return
		}
	}
}

func (h *BaseHandler) CloseAllConnections() {
	h.Connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		conn.Close()
		h.Connections.Delete(key)
		return true
	})
}
