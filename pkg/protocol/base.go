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

	// Stop aborts all connections and processing
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

	// The writer owns bounded DATA and control queues.
	queueMu                sync.Mutex
	controls               []*writeRequest
	data                   map[uuid.UUID][]*writeRequest
	dataReady              []uuid.UUID
	pendingData            map[uuid.UUID]dataReservation
	dataBytes, dataRecords int
	dataSpace              chan struct{}
	credits                map[uuid.UUID]*writeRequest
	wake                   chan struct{}
	flow                   FlowConfig
	flowMu                 sync.Mutex
	reserved, streams      int
	drainOnce              sync.Once
	draining               chan struct{}
	writerDone             chan struct{}
	abortDone              chan struct{}
	writeErrMu             sync.Mutex
	writeErr               error

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
	h, err := NewBaseHandlerWithConfig(parentCtx, conn, DefaultFlowConfig())
	if err != nil {
		panic(err)
	}
	return h
}

func NewBaseHandlerWithConfig(parentCtx context.Context, conn net.Conn, cfg FlowConfig) (*BaseHandler, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(parentCtx)
	h := &BaseHandler{
		conn:        conn,
		flow:        cfg,
		wake:        make(chan struct{}, 1),
		credits:     make(map[uuid.UUID]*writeRequest),
		pendingData: make(map[uuid.UUID]dataReservation),
		data:        make(map[uuid.UUID][]*writeRequest),
		dataSpace:   make(chan struct{}),
		draining:    make(chan struct{}),
		writerDone:  make(chan struct{}),
		abortDone:   make(chan struct{}),
		Ctx:         ctx,
		Cancel:      cancel,
	}
	go h.writeLoop()
	go func() {
		<-ctx.Done()
		// Interrupt I/O without taking session resource ownership from the caller.
		if err := conn.SetDeadline(time.Now()); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
			// A transport without deadlines must implement net.Conn.Close cancellation.
			conn.Close()
		}
		h.CloseAllConnections()
		close(h.abortDone)
	}()
	return h, nil
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
		// Read may return both bytes and an error. Dispatch every complete
		// record before applying the transport error policy below.
		if err == nil {
			consecutiveErrors = 0
		}
		if h.OnReceive != nil && (n > 0 || err == nil) {
			h.OnReceive()
		}
		if n > 0 {
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
					// Control admission is bounded and nonblocking.
					h.rejectStream(packet.ConnectionID, errCode)
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

		if err != nil {
			// Terminal transport errors: the connection will never yield bytes
			// again, so backing off would spin forever while h.Stop() never runs
			// and every logical connection hangs instead of being reset.
			if errors.Is(err, io.EOF) ||
				errors.Is(err, net.ErrClosed) ||
				errors.Is(err, io.ErrClosedPipe) ||
				errors.Is(err, os.ErrDeadlineExceeded) {
				if len(acc) > 0 && errors.Is(err, io.EOF) {
					err = io.ErrUnexpectedEOF
				}
				h.drainReceived(err)
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
		}
	}
}

// handlePacket routes packet to appropriate handler based on command.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) handlePacket(packet *Packet) byte {
	switch packet.Command {
	case CmdNew:
		if _, err := peerWindow(packet.Data); err != nil {
			return h.badHandshake(err)
		}
		return h.PacketHandler.OnNew(packet.ConnectionID, packet.Data)
	case CmdAck:
		window, err := peerWindow(packet.Data)
		if err != nil {
			return h.badHandshake(err)
		}
		if v, ok := h.Connections.Load(packet.ConnectionID); ok {
			if err := v.(*Connection).setPeerWindow(window); err != nil {
				return ErrInvalidState
			}
		}
		return h.PacketHandler.OnAck(packet.ConnectionID, packet.Data)
	case CmdCredit:
		return h.receiveCredit(packet.ConnectionID, packet.Data)
	case CmdData:
		return h.PacketHandler.OnData(packet.ConnectionID, packet.Data)
	case CmdEOF:
		if len(packet.Data) != 0 {
			return ErrInvalidPacket
		}
		return h.FinishConnection(packet.ConnectionID)
	case CmdClose:
		// A close carries a one byte error code; indexing an empty payload would
		// panic. This is a per-connection error and deliberately NOT malformed
		// framing: the length prefix was intact and the byte stream is still in
		// sync, so it must not tear down the whole handler.
		if len(packet.Data) != 1 {
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
	return h.sendPacket(CmdNew, connectionID, h.handshake())
}

// SendConnAck acknowledges connection.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) SendConnAck(connectionID uuid.UUID) byte {
	return h.sendPacket(CmdAck, connectionID, h.handshake())
}

// SendData sends data.
// Returns error code indicating success or specific failure.
func (h *BaseHandler) SendData(connectionID uuid.UUID, data []byte) byte {
	// Verify connection exists
	if _, exists := h.Connections.Load(connectionID); !exists {
		return ErrConnectionNotFound
	}

	if _, err := h.sendBytes(connectionID, data, nil); err != nil {
		return ErrPacketSendFailed
	}
	return ErrNone
}

// SendClose sends a connection termination packet with an error code.
func (h *BaseHandler) SendClose(connectionID uuid.UUID, errCode byte) byte {
	connObj, exists := h.Connections.LoadAndDelete(connectionID)
	if !exists {
		return ErrConnectionNotFound
	}
	conn := connObj.(*Connection)

	conn.Close()
	return h.sendPacket(CmdClose, connectionID, []byte{errCode})
}

func (h *BaseHandler) CloseAllConnections() {
	// Snapshot under the admission lock. Once cancellation is visible, a new
	// registration either belongs to this snapshot or is rejected; it cannot
	// publish after the cancellation worker has finished cleanup.
	h.flowMu.Lock()
	var connections []*Connection
	h.Connections.Range(func(_, value any) bool {
		connections = append(connections, value.(*Connection))
		return true
	})
	h.flowMu.Unlock()
	for _, c := range connections {
		c.Close()
		h.Connections.CompareAndDelete(c.ID, c)
	}
}

// FinishConnection preserves all accepted incoming bytes before exposing EOF.
func (h *BaseHandler) FinishConnection(id uuid.UUID) byte {
	value, ok := h.Connections.Load(id)
	if !ok {
		return ErrConnectionNotFound
	}
	value.(*Connection).FinishDelivery()
	return ErrNone
}

// DrainTimeout bounds cleanup after tunnel EOF. It is not a flow-control or
// transfer timeout: normal directional EOF can wait for a response indefinitely.
const DrainTimeout = 5 * time.Second

func (h *BaseHandler) drainReceived(readErr error) {
	ctx, cancel := context.WithTimeout(h.Ctx, h.flow.DrainTimeout)
	defer cancel()
	// Bound the application drain; ordinary stream EOF has no transfer timeout.
	done := make(chan struct{})
	go func() {
		h.Connections.Range(func(_, value any) bool { value.(*Connection).finishDelivery(readErr); return true })
		h.Connections.Range(func(_, value any) bool {
			select {
			case <-value.(*Connection).Closed:
				return true
			case <-ctx.Done():
				return false
			}
		})
		close(done)
	}()
	select {
	case <-done:
		if h.writerDone != nil {
			if err := h.Drain(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Warn().Err(err).Msg("Tunnel write drain failed")
			}
		}
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			log.Warn().Err(ctx.Err()).Msg("Tunnel delivery drain forced to abort")
		}
	}
}

// Abort stops admission and interrupts pending I/O. Session Close remains with
// the listener/agent owner; the transport deadline releases its reader/writer.
func (h *BaseHandler) Abort() { h.Cancel(); <-h.abortDone }

// WaitWriter waits for the sole transport writer to exit. After Abort and a
// successful wait, the session owner may replace the abort write deadline for
// transport-level shutdown without reviving an interrupted protocol write.
func (h *BaseHandler) WaitWriter(ctx context.Context) error {
	select {
	case <-h.writerDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PeerClose is a full-stream graceful close or an explicit failure. Unlike
// directional EOF, full close bounds the time allowed for application draining.
func (h *BaseHandler) PeerClose(id uuid.UUID, code byte) byte {
	value, ok := h.Connections.Load(id)
	if !ok {
		return ErrNone
	}
	c := value.(*Connection)
	if code != ErrNone {
		c.Close()
		h.Connections.Delete(id)
		return ErrNone
	}
	c.peerCloseOnce.Do(func() {
		go func() {
			timer := time.NewTimer(h.flow.DrainTimeout)
			defer timer.Stop()
			select {
			case <-c.Closed:
			case <-timer.C:
				log.Warn().Str("conn_id", id.String()).Msg("Peer close drain forced to abort")
				c.Close()
				h.Connections.CompareAndDelete(id, c)
			}
		}()
	})
	c.FinishDelivery()
	return ErrNone
}

func (h *BaseHandler) badHandshake(err error) byte {
	log.Error().Err(err).Uint32("supported_version", ProtocolVersion).Msg("Tunnel protocol negotiation rejected")
	h.Cancel()
	return ErrInvalidPacket
}

func (h *BaseHandler) rejectStream(id uuid.UUID, code byte) {
	if v, ok := h.Connections.Load(id); ok {
		v.(*Connection).Close()
		h.Connections.CompareAndDelete(id, v)
	}
	h.sendPacket(CmdClose, id, []byte{code})
}

// AcceptConnection negotiates the sender window after reserving our receive
// memory. Used by the agent before acknowledging a NEW.
func (h *BaseHandler) AcceptConnection(id uuid.UUID, data []byte) (*Connection, error) {
	window, err := peerWindow(data)
	if err != nil {
		return nil, err
	}
	c := NewConnection(id, h.Ctx.Done())
	if err := h.RegisterConnection(c); err != nil {
		return nil, err
	}
	if err := c.setPeerWindow(window); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
