package mux

import (
	"context"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Connection owns a logical stream and its attached destination. Close aborts;
// FinishDelivery seals incoming data and lets the reader drain to EOF.
type Connection struct {
	ID                             uuid.UUID
	protoConn                      atomic.Pointer[ProtocolConn]
	established                    chan struct{}
	mu                             sync.Mutex
	destination                    net.Conn
	datagrams                      *Datagrams
	disposed                       bool
	Closed                         chan struct{}
	closeOnce                      sync.Once
	peerCloseOnce                  sync.Once
	handler                        *BaseHandler
	buffer                         []byte
	head, used                     int
	consumed, credited             uint64
	changed                        chan struct{}
	sendEnded                      bool
	sendMu                         sync.Mutex
	creditMu                       sync.Mutex
	creditWake                     chan struct{}
	peerWindow, sent, peerConsumed uint64
	deliveryMu                     sync.Mutex
	deliveryEnded                  bool
	receiveDone                    chan struct{}
	deliveryErr                    error
	stop                           <-chan struct{}
	CreatedAt                      time.Time
}

var neverStop = make(chan struct{})

func NewConnection(id uuid.UUID, stop <-chan struct{}) *Connection {
	if stop == nil {
		stop = neverStop
	}
	return &Connection{ID: id, Closed: make(chan struct{}), receiveDone: make(chan struct{}), established: make(chan struct{}), changed: make(chan struct{}), creditWake: make(chan struct{}), stop: stop, CreatedAt: time.Now()}
}

func (c *Connection) ProtocolConn() *ProtocolConn { return c.protoConn.Load() }

// SetProtocolConn rejects late or duplicate attachment and disposes the loser.
func (c *Connection) SetProtocolConn(pc *ProtocolConn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disposed || c.protoConn.Load() != nil {
		pc.Shutdown()
		return false
	}
	pc.owner = c
	c.protoConn.Store(pc)
	close(c.established)
	return true
}

// AttachDestination transfers ownership even on failure: a late destination is
// closed here, so a dial racing teardown cannot leak a socket.
func (c *Connection) AttachDestination(dst net.Conn) bool {
	c.mu.Lock()
	if c.disposed || c.destination != nil {
		c.mu.Unlock()
		dst.Close()
		return false
	}
	c.destination = dst
	c.mu.Unlock()
	return true
}

func (c *Connection) Established() <-chan struct{} { return c.established }

func (c *Connection) Close() byte {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.disposed = true
		dst := c.destination
		dgrams := c.datagrams
		pc := c.protoConn.Load()
		close(c.Closed)
		c.mu.Unlock()
		if dgrams != nil {
			dgrams.clear()
		}
		if pc != nil {
			pc.Shutdown()
		}
		if dst != nil {
			dst.Close()
		}
		c.deliveryMu.Lock()
		c.buffer = nil
		c.used = 0
		h := c.handler
		c.deliveryMu.Unlock()
		if h != nil {
			h.discardData(c.ID)
			h.releaseReservation(c)
		}
	})
	return ErrNone
}

// Deliver copies into the one reserved receive ring. It never waits for the
// application: data beyond the advertised window is a protocol violation.
func (c *Connection) Deliver(data []byte) bool {
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	if c.deliveryEnded {
		return false
	}
	select {
	case <-c.Closed:
		return false
	case <-c.stop:
		return false
	default:
	}
	if c.buffer == nil {
		c.buffer = make([]byte, DefaultFlowConfig().StreamWindow)
	}
	available := len(c.buffer) - c.used
	if c.handler != nil {
		available -= int(c.consumed - c.credited)
	}
	if len(data) > available {
		return false
	}
	tail := (c.head + c.used) % len(c.buffer)
	n := copy(c.buffer[tail:], data)
	copy(c.buffer, data[n:])
	c.used += len(data)
	c.returnCredit()
	c.notifyReader()
	return true
}

func (c *Connection) notifyReader() { close(c.changed); c.changed = make(chan struct{}) }

// ReceiveDone signals peer EOF without consuming buffered bytes. Setup operations
// can stop waiting for a peer when their client has gone away.
func (c *Connection) ReceiveDone() <-chan struct{} { return c.receiveDone }

func (c *Connection) FinishDelivery() { c.finishDelivery(io.EOF) }
func (c *Connection) finishDelivery(err error) {
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	if !c.deliveryEnded {
		c.deliveryEnded = true
		close(c.receiveDone)
		c.deliveryErr = err
		c.notifyReader()
	}
}

// StartDelivery is retained for callers; delivery now goes straight into the
// reserved ring, with no worker or second queue.
func (c *Connection) StartDelivery() {}

func (c *Connection) read(ctx context.Context, closed <-chan struct{}, b []byte) (int, error) {
	for {
		c.deliveryMu.Lock()
		select {
		case <-c.Closed:
			c.deliveryMu.Unlock()
			return 0, net.ErrClosed
		case <-closed:
			c.deliveryMu.Unlock()
			return 0, net.ErrClosed
		case <-ctx.Done():
			c.deliveryMu.Unlock()
			return 0, ctx.Err()
		default:
		}
		if c.used > 0 {
			n := min(len(b), c.used)
			first := copy(b[:n], c.buffer[c.head:min(c.head+n, len(c.buffer))])
			copy(b[first:n], c.buffer[:n-first])
			c.head = (c.head + n) % len(c.buffer)
			c.used -= n
			if c.consumed > math.MaxUint64-uint64(n) {
				c.deliveryMu.Unlock()
				return 0, ErrFlowControl
			}
			c.consumed += uint64(n)
			// The bytes have left the ring. Read's caller now owns its own bounded
			// buffer; no protocol queue retains the delivered allocation.
			c.returnCredit()
			c.deliveryMu.Unlock()
			return n, nil
		}
		if c.deliveryEnded {
			err := c.deliveryErr
			c.deliveryMu.Unlock()
			return 0, err
		}
		wake := c.changed
		c.deliveryMu.Unlock()
		select {
		case <-wake:
		case <-c.Closed:
			return 0, net.ErrClosed
		case <-closed:
			return 0, net.ErrClosed
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// returnCredit runs under deliveryMu. Batch small reads while the peer still
// has usable credit, but flush even one released byte once its grant has been
// received in full. Deliver must also call this: the final in-flight DATA can
// exhaust the grant after the application has stopped reading.
func (c *Connection) returnCredit() {
	if c.handler == nil {
		return
	}
	pending := c.consumed - c.credited
	if pending == 0 {
		return
	}
	grantExhausted := uint64(len(c.buffer)-c.used) == pending
	if pending >= uint64(max(len(c.buffer)/2, 1)) || grantExhausted {
		c.credited = c.consumed
		c.handler.queueCredit(c.ID, c.consumed)
	}
}
