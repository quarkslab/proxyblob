package protocol

import (
	"errors"
	"net"
	"sync"

	"github.com/google/uuid"
)

// UDP uses whole-datagram drop admission, independently of TCP receive credit.
// Association counts share MaxStreams. Each association retains at most 256 KiB
// and 64 incoming records; outgoing records share the fair bounded DATA queue.
const MaxDatagramSize = 65507
const DatagramQueueBytes = 256 << 10
const DatagramQueuePackets = 64

var ErrDatagramDropped = errors.New("protocol: datagram dropped at finite queue limit")

type Datagrams struct {
	conn    *Connection
	mu      sync.Mutex
	queue   [][]byte
	bytes   int
	changed chan struct{}
	ready   chan []byte
}

func (c *Connection) EnableDatagrams() *Datagrams {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.disposed || c.datagrams != nil {
		return nil
	}
	d := &Datagrams{conn: c, changed: make(chan struct{}), ready: make(chan []byte, 1)}
	c.datagrams = d
	return d
}
func (c *Connection) Datagrams() *Datagrams { c.mu.Lock(); defer c.mu.Unlock(); return c.datagrams }
func (c *Connection) DestinationAddresses() (net.Addr, net.Addr) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.destination == nil {
		return nil, nil
	}
	return c.destination.LocalAddr(), c.destination.RemoteAddr()
}
func (d *Datagrams) clear() { d.mu.Lock(); defer d.mu.Unlock(); d.queue = nil; d.bytes = 0 }
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
func (d *Datagrams) Request(address []byte) ([]byte, error) {
	if err := d.conn.handler.sendConfirmed(CmdUDPAssociate, d.conn.ID, address, d.conn.Closed); err != nil {
		return nil, err
	}
	select {
	case b := <-d.ready:
		return b, nil
	case <-d.conn.Closed:
		return nil, net.ErrClosed
	case <-d.conn.stop:
		return nil, net.ErrClosed
	}
}
func (d *Datagrams) Ready(address []byte) error {
	return d.conn.handler.sendConfirmed(CmdUDPReady, d.conn.ID, append([]byte{0}, address...), d.conn.Closed)
}

// Reject reports a SOCKS setup failure to the negotiating agent.
func (d *Datagrams) Reject(code byte) error {
	return d.conn.handler.sendConfirmed(CmdUDPReady, d.conn.ID, []byte{code, 1, 0, 0, 0, 0, 0, 0}, d.conn.Closed)
}

func (h *BaseHandler) receiveDatagram(cmd byte, id uuid.UUID, b []byte) byte {
	v, ok := h.Connections.Load(id)
	if !ok {
		return ErrNone
	}
	c := v.(*Connection)
	if cmd == CmdUDPAssociate {
		if len(b) > 259 || len(b) < 7 {
			return ErrInvalidPacket
		}
		receiver, ok := h.PacketHandler.(interface {
			OnUDPAssociate(*Connection, []byte) byte
		})
		if !ok {
			return ErrUnexpectedPacket
		}
		return receiver.OnUDPAssociate(c, b)
	}
	d := c.Datagrams()
	if d == nil {
		return ErrInvalidState
	}
	if cmd == CmdUDPReady {
		if len(b) < 8 || len(b) > 20 {
			return ErrInvalidPacket
		}
		select {
		case d.ready <- append([]byte(nil), b...):
			return ErrNone
		default:
			return ErrInvalidState
		}
	}
	if len(b) > MaxDatagramSize || len(b) < 10 {
		return ErrInvalidPacket
	}
	d.deliver(b)
	return ErrNone
}
