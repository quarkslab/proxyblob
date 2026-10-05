package protocol

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Connection owns a logical stream and its attached destination. Close aborts;
// FinishDelivery seals incoming data and lets the reader drain to EOF.
type Connection struct {
	ID            uuid.UUID
	protoConn     atomic.Pointer[ProtocolConn]
	established   chan struct{}
	mu            sync.Mutex
	destination   net.Conn
	disposed      bool
	Closed        chan struct{}
	closeOnce     sync.Once
	deliveryOnce  sync.Once
	deliveryMu    sync.Mutex
	deliveryEnded bool
	deliveryErr   error
	deliverCh     chan []byte
	stop          <-chan struct{}
	CreatedAt     time.Time
}

var neverStop = make(chan struct{})

func NewConnection(id uuid.UUID, stop <-chan struct{}) *Connection {
	if stop == nil {
		stop = neverStop
	}
	return &Connection{ID: id, Closed: make(chan struct{}), established: make(chan struct{}), deliverCh: make(chan []byte, 1024), stop: stop, CreatedAt: time.Now()}
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
		pc := c.protoConn.Load()
		close(c.Closed)
		c.mu.Unlock()
		if pc != nil {
			pc.Shutdown()
		}
		if dst != nil {
			dst.Close()
		}
	})
	return ErrNone
}

// Deliver serializes admission with the EOF marker. Acceptance is queue
// ownership, not proof of delivery; abort is always reported as an error.
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
	select {
	case c.deliverCh <- append([]byte(nil), data...):
		return true
	case <-c.Closed:
		return false
	case <-c.stop:
		return false
	}
}

func (c *Connection) FinishDelivery() { c.finishDelivery(io.EOF) }

func (c *Connection) finishDelivery(err error) {
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	if !c.deliveryEnded {
		c.deliveryEnded = true
		c.deliveryErr = err
		close(c.deliverCh)
	}
}

func (c *Connection) StartDelivery() {
	c.deliveryOnce.Do(func() {
		pc := c.ProtocolConn()
		go func() {
			for {
				select {
				case data, ok := <-c.deliverCh:
					if !ok {
						pc.finishRead(c.deliveryErr)
						return
					}
					if !pc.DeliverData(data) {
						return
					}
				case <-c.Closed:
					return
				case <-c.stop:
					return
				}
			}
		}()
	})
}
