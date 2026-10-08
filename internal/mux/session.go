package mux

import (
	"context"
	"net"
	"os"
	"sync"
	"time"

	"proxyblob/internal/diag"

	"github.com/google/uuid"
)

// AckTimeout bounds how long Open waits for the peer's acknowledgment.
//
// It covers stream setup only: no payload flows until the acknowledgment
// arrives, and once the stream is established no further timeout applies. All
// streams share one transport, so acknowledgments serialize and the last stream
// opened waits behind every other one; the bound must cover that queueing on a
// high-latency driver, not just one round trip.
const AckTimeout = 120 * time.Second

// Session is one end of a multiplexed tunnel. Either end can Open streams;
// only a session created WithAccept receives the peer's streams through Accept.
// Each stream is a *ProtocolConn: a net.Conn with half-close, close codes and
// optional datagrams.
type Session struct {
	*BaseHandler
	accept      bool
	accepted    chan *ProtocolConn
	receiveOnce sync.Once
	stopOnce    sync.Once
}

// SessionOption configures a Session.
type SessionOption func(*Session)

// WithAccept lets the peer open streams, delivered through Accept. Without it,
// incoming streams are rejected.
func WithAccept() SessionOption { return func(s *Session) { s.accept = true } }

// NewSession starts the session's writer. Call StartReceiving to begin reading.
func NewSession(ctx context.Context, conn net.Conn, cfg FlowConfig, opts ...SessionOption) (*Session, error) {
	base, err := NewBaseHandlerWithConfig(ctx, conn, cfg)
	if err != nil {
		return nil, err
	}
	s := &Session{BaseHandler: base, accepted: make(chan *ProtocolConn, cfg.MaxStreams)}
	for _, opt := range opts {
		opt(s)
	}
	base.PacketHandler = s
	return s, nil
}

// Start implements PacketHandler; the address is unused.
func (s *Session) Start(string) { s.StartReceiving() }

// StartReceiving starts the single tunnel reader. Safe to call repeatedly.
func (s *Session) StartReceiving() { s.receiveOnce.Do(func() { go s.ReceiveLoop() }) }

// Stop aborts every stream and pending I/O. Closing the transport remains with
// the session owner.
func (s *Session) Stop() {
	s.stopOnce.Do(func() {
		s.CloseAllConnections()
		s.Abort()
	})
}

// Reservation holds one stream's receive memory and stream slot before
// anything is sent, so admission can be refused without tunnel traffic.
type Reservation struct {
	s *Session
	c *Connection
}

// Reserve claims a stream slot locally. It fails with diag.ErrCapacity when the
// session's stream or memory limits are reached.
func (s *Session) Reserve() (*Reservation, error) {
	c := NewConnection(uuid.New(), s.Ctx.Done())
	if err := s.RegisterConnection(c); err != nil {
		c.Close()
		return nil, err
	}
	return &Reservation{s: s, c: c}, nil
}

// AttachDestination ties dst to the reservation: closing the reservation or
// its stream closes dst. It reports false, having closed dst, if the
// reservation is already closed.
func (r *Reservation) AttachDestination(dst net.Conn) bool { return r.c.AttachDestination(dst) }

// Done is closed when the reservation or its stream is closed.
func (r *Reservation) Done() <-chan struct{} { return r.c.Closed }

// Release abandons a reservation that was never opened.
func (r *Reservation) Release() {
	r.c.Close()
	r.s.Connections.CompareAndDelete(r.c.ID, r.c)
}

// Open announces the reserved stream and waits for the peer's acknowledgment.
// On failure the peer is told to close the stream and the reservation is gone.
func (r *Reservation) Open(ctx context.Context) (*ProtocolConn, error) {
	s, c := r.s, r.c
	if code := s.SendNewConnection(c.ID); code != diag.ErrNone {
		r.Release()
		return nil, diag.Error(code)
	}
	timer := time.NewTimer(AckTimeout)
	defer timer.Stop()
	var err error
	code := diag.ErrHandlerStopped
	select {
	case <-c.Established():
		return c.ProtocolConn(), nil
	case <-ctx.Done():
		err = ctx.Err()
	case <-s.Ctx.Done():
		err = net.ErrClosed
	case <-c.Closed:
		err = net.ErrClosed
	case <-timer.C:
		code, err = diag.ErrTransportTimeout, os.ErrDeadlineExceeded
	}
	s.SendClose(c.ID, code)
	s.Connections.Delete(c.ID)
	return nil, err
}

// Open reserves and opens a stream.
func (s *Session) Open(ctx context.Context) (*ProtocolConn, error) {
	r, err := s.Reserve()
	if err != nil {
		return nil, err
	}
	return r.Open(ctx)
}

// Accept returns the next stream opened by the peer. It requires WithAccept.
func (s *Session) Accept(ctx context.Context) (*ProtocolConn, error) {
	select {
	case pc := <-s.accepted:
		return pc, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.Ctx.Done():
		return nil, net.ErrClosed
	}
}

// OnNew admits a peer stream: it reserves receive memory, acknowledges, and
// hands the stream to Accept. Acknowledgment is written off the receive loop.
func (s *Session) OnNew(id uuid.UUID, data []byte) byte {
	if !s.accept {
		return diag.ErrUnexpectedPacket
	}
	if _, ok := s.Connections.Load(id); ok {
		return diag.ErrConnectionExists
	}
	c, err := s.AcceptConnection(id, data)
	if err != nil {
		return diag.ErrInvalidState
	}
	if s.Ctx.Err() != nil {
		c.Close()
		s.Connections.Delete(c.ID)
		return diag.ErrHandlerStopped
	}
	pc := NewProtocolConn(s.Ctx, id, s.BaseHandler)
	if !c.SetProtocolConn(pc) {
		return diag.ErrConnectionClosed
	}
	go func() {
		if code := s.SendConnAck(id); code != diag.ErrNone {
			s.SendClose(id, diag.ErrConnectionClosed)
			return
		}
		// Registered streams never exceed MaxStreams, the channel's capacity.
		select {
		case s.accepted <- pc:
		case <-s.Ctx.Done():
		}
	}()
	return diag.ErrNone
}

// OnAck establishes a stream this side opened.
func (s *Session) OnAck(id uuid.UUID, _ []byte) byte {
	value, ok := s.Connections.Load(id)
	if !ok {
		return diag.ErrConnectionNotFound
	}
	c := value.(*Connection)
	if c.ProtocolConn() != nil {
		return diag.ErrInvalidState
	}
	if !c.SetProtocolConn(NewProtocolConn(s.Ctx, id, s.BaseHandler)) {
		return diag.ErrConnectionClosed
	}
	return diag.ErrNone
}

// OnData admits bytes into the stream's reservation without blocking dispatch.
// The sender must pause before exhausting its negotiated receive credit.
func (s *Session) OnData(id uuid.UUID, data []byte) byte {
	value, ok := s.Connections.Load(id)
	if !ok {
		return diag.ErrConnectionNotFound
	}
	c := value.(*Connection)
	// Without an established stream there is no reader; dropping the payload
	// would be silent data loss, so report the unexpected state.
	if c.ProtocolConn() == nil {
		return diag.ErrInvalidState
	}
	if !c.Deliver(data) {
		return diag.ErrConnectionClosed
	}
	return diag.ErrNone
}

// OnClose applies the peer's close. Its code is reported only when an OnError
// handler is installed, so an agent never prints peer diagnostics.
func (s *Session) OnClose(id uuid.UUID, code byte) byte {
	if s.OnError != nil {
		s.ReportError(id, code)
	}
	return s.PeerClose(id, code)
}
