package mux

import (
	"net"
	"proxyblob/internal/diag"
	"sync"

	"github.com/google/uuid"
)

// Datagrams are whole messages on a stream, delivered outside TCP receive
// credit with drop admission: each stream retains at most UDPQueuePackets and
// UDPQueueBytes incoming; outgoing datagrams share the fair bounded DATA queue.
const MaxDatagramSize = 65507
const DatagramQueueBytes = 256 << 10
const DatagramQueuePackets = 64

// ErrDatagramDropped is a numeric sentinel declared in errors.go.

type Datagrams struct {
	conn    *Connection
	mu      sync.Mutex
	queue   [][]byte
	bytes   int
	changed chan struct{}
}

func (c *Connection) EnableDatagrams() *Datagrams {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disposed || c.datagrams != nil {
		return nil
	}
	d := &Datagrams{conn: c, changed: make(chan struct{})}
	c.datagrams = d
	return d
}
func (c *Connection) Datagrams() *Datagrams { c.mu.Lock(); defer c.mu.Unlock(); return c.datagrams }
func (d *Datagrams) clear()                 { d.mu.Lock(); defer d.mu.Unlock(); d.queue = nil; d.bytes = 0 }
func (d *Datagrams) deliver(b []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	select {
	case <-d.conn.Closed:
		return
	default:
	}
	if len(d.queue) >= d.conn.handler.flow.UDPQueuePackets || len(b) > d.conn.handler.flow.UDPQueueBytes-d.bytes {
		return
	}
	d.queue = append(d.queue, append([]byte(nil), b...))
	d.bytes += len(b)
	close(d.changed)
	d.changed = make(chan struct{})
}
func (d *Datagrams) Receive() ([]byte, error) {
	for {
		d.mu.Lock()
		select {
		case <-d.conn.Closed:
			d.mu.Unlock()
			return nil, net.ErrClosed
		default:
		}
		if len(d.queue) > 0 {
			b := d.queue[0]
			d.queue[0] = nil
			d.queue = d.queue[1:]
			d.bytes -= len(b)
			d.mu.Unlock()
			return b, nil
		}
		wake := d.changed
		d.mu.Unlock()
		select {
		case <-wake:
		case <-d.conn.Closed:
			return nil, net.ErrClosed
		case <-d.conn.stop:
			return nil, net.ErrClosed
		}
	}
}
func (d *Datagrams) Send(b []byte) error {
	_, err := d.conn.handler.enqueue(CmdDatagram, d.conn.ID, b, d.conn.Closed, false)
	return err
}
func (h *BaseHandler) receiveDatagram(id uuid.UUID, b []byte) byte {
	v, ok := h.Connections.Load(id)
	if !ok {
		return diag.ErrNone
	}
	d := v.(*Connection).Datagrams()
	if d == nil {
		return diag.ErrInvalidState
	}
	// Content is the application's: mux only bounds the size.
	if len(b) > MaxDatagramSize {
		return diag.ErrInvalidPacket
	}
	d.deliver(b)
	return diag.ErrNone
}
