package mux

import (
	"encoding/binary"
	"math"
	"net"
	"os"
	"proxyblob/internal/diag"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// FlowConfig bounds memory owned by multiplexing, independently of the aznet
// transport and application/socket buffers. Each admitted stream reserves its
// current receive window against TunnelWindow, including grants that have not
// arrived yet; receive memory itself is allocated only as data arrives.
type FlowConfig struct {
	StreamWindow    int // initial receive window of every stream
	MaxStreamWindow int // limit for a window grown under sustained transfer
	TunnelWindow    int
	MaxStreams      int
	DataFrame       int
	ControlSlots    int
	DrainTimeout    time.Duration
	UDPQueueBytes   int
	UDPQueuePackets int
	UDPDestinations int
}

func DefaultFlowConfig() FlowConfig {
	return FlowConfig{
		StreamWindow: 512 << 10, MaxStreamWindow: 4 << 20, TunnelWindow: 64 << 20, MaxStreams: 128,
		UDPQueueBytes: DatagramQueueBytes, UDPQueuePackets: DatagramQueuePackets, UDPDestinations: 64,
		DataFrame: 32 << 10, ControlSlots: 512, DrainTimeout: DrainTimeout,
	}
}

func (c FlowConfig) validate() error {
	if c.StreamWindow < 1 || c.MaxStreamWindow < c.StreamWindow || c.MaxStreamWindow > MaxWindow ||
		c.TunnelWindow < c.StreamWindow || // growth beyond the budget is refused at run time
		c.MaxStreams < 1 || c.MaxStreams > 65536 ||
		c.DataFrame < 1 || c.DataFrame > MaxPacketDataSize ||
		c.ControlSlots < 4*c.MaxStreams || c.ControlSlots > 1<<20 ||
		c.DrainTimeout <= 0 || c.UDPQueueBytes < 1 || c.UDPQueueBytes > 16<<20 || c.UDPQueuePackets < 1 || c.UDPQueuePackets > 4096 || c.UDPDestinations < 1 || c.UDPDestinations > 4096 {
		return diag.ErrInvalidFlowConfig
	}
	return nil
}

// MaxWindow bounds any stream window a peer may announce or grow to.
const MaxWindow = 64 << 20

// Version 3 adds proxy-owned UDP associations and datagram records.
// Empty (legacy) NEW/ACK payloads are explicitly unsupported, never sniffed.
const ProtocolVersion uint32 = 3

func (h *BaseHandler) handshake() []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint32(b, ProtocolVersion)
	binary.BigEndian.PutUint64(b[4:], uint64(h.flow.StreamWindow))
	return b
}

func peerWindow(b []byte) (uint64, error) {
	if len(b) < 4 || binary.BigEndian.Uint32(b) != ProtocolVersion {
		return 0, diag.ErrUnsupportedVersion
	}
	if len(b) != 12 {
		return 0, diag.ErrFlowControl
	}
	n := binary.BigEndian.Uint64(b[4:])
	if n == 0 || n > MaxWindow {
		return 0, diag.ErrFlowControl
	}
	return n, nil
}

// RegisterConnection reserves actual storage before advertising any credit.
// Stream IDs must be fresh for the tunnel lifetime (the proxy uses random UUIDs).
func (h *BaseHandler) RegisterConnection(c *Connection) error {
	h.flowMu.Lock()
	defer h.flowMu.Unlock()
	if h.Ctx.Err() != nil {
		return h.Ctx.Err()
	}
	if h.streams >= h.flow.MaxStreams || h.reserved > h.flow.TunnelWindow-h.flow.StreamWindow {
		return diag.ErrCapacity
	}
	if _, exists := h.Connections.Load(c.ID); exists {
		return diag.Error(diag.ErrConnectionExists)
	}
	c.deliveryMu.Lock()
	select {
	case <-c.Closed:
		c.deliveryMu.Unlock()
		return net.ErrClosed
	default:
	}
	c.window = h.flow.StreamWindow
	c.handler = h
	c.deliveryMu.Unlock()
	h.streams++
	h.reserved += h.flow.StreamWindow
	h.Connections.Store(c.ID, c)
	return nil
}

func (h *BaseHandler) releaseReservation(window int) {
	h.flowMu.Lock()
	h.streams--
	h.reserved -= window
	h.flowMu.Unlock()
}

// growReservation claims delta more of the tunnel budget for a growing window.
func (h *BaseHandler) growReservation(delta int) bool {
	h.flowMu.Lock()
	defer h.flowMu.Unlock()
	if h.reserved > h.flow.TunnelWindow-delta {
		return false
	}
	h.reserved += delta
	return true
}

func (c *Connection) setPeerWindow(window uint64) error {
	c.creditMu.Lock()
	defer c.creditMu.Unlock()
	if c.peerWindow != 0 {
		return diag.ErrFlowControl
	}
	c.peerWindow = window
	c.creditChanged()
	return nil
}

func (c *Connection) creditChanged() { close(c.creditWake); c.creditWake = make(chan struct{}) }

// Duplicate or delayed cumulative updates are harmless. Future consumption,
// counter wrap and a shrinking or oversized window are violations.
func (c *Connection) updateCredit(consumed, window uint64) error {
	c.creditMu.Lock()
	defer c.creditMu.Unlock()
	if c.peerWindow == 0 || consumed > c.sent || window < c.peerWindow || window > MaxWindow {
		return diag.ErrFlowControl
	}
	if consumed <= c.peerConsumed && window == c.peerWindow {
		return nil
	}
	c.peerConsumed = max(c.peerConsumed, consumed)
	c.peerWindow = window
	c.creditChanged()
	return nil
}

func (c *Connection) acquireCredit(want int, stop <-chan struct{}) (int, error) {
	for {
		c.creditMu.Lock()
		available := c.peerWindow - (c.sent - c.peerConsumed)
		if available > 0 {
			n := min(want, int(available))
			if c.sent > math.MaxUint64-uint64(n) {
				c.creditMu.Unlock()
				return 0, diag.ErrFlowControl
			}
			c.sent += uint64(n)
			c.creditMu.Unlock()
			return n, nil
		}
		wake := c.creditWake
		c.creditMu.Unlock()
		select {
		case <-wake:
		case <-c.Closed:
			return 0, net.ErrClosed
		case <-c.stop:
			return 0, net.ErrClosed
		case <-stop:
			return 0, net.ErrClosed
		case <-c.handler.draining:
			return 0, net.ErrClosed
		}
	}
}

func (h *BaseHandler) receiveCredit(id uuid.UUID, data []byte) byte {
	if len(data) != 16 {
		return diag.ErrInvalidPacket
	}
	v, ok := h.Connections.Load(id)
	if !ok {
		return diag.ErrNone
	} // delayed control for a disposed stream
	if err := v.(*Connection).updateCredit(binary.BigEndian.Uint64(data), binary.BigEndian.Uint64(data[8:])); err != nil {
		return diag.ErrInvalidPacket
	}
	return diag.ErrNone
}

// FlowConfigFromEnv is shared by native and WASM commands. Byte/count values
// are decimal integers; the drain duration uses Go duration syntax.
func FlowConfigFromEnv() (FlowConfig, error) {
	c := DefaultFlowConfig()
	for name, target := range map[string]*int{
		"PROXYBLOB_STREAM_WINDOW":     &c.StreamWindow,
		"PROXYBLOB_MAX_STREAM_WINDOW": &c.MaxStreamWindow,
		"PROXYBLOB_TUNNEL_WINDOW":     &c.TunnelWindow,
		"PROXYBLOB_MAX_STREAMS":       &c.MaxStreams,
		"PROXYBLOB_DATA_FRAME":        &c.DataFrame,
		"PROXYBLOB_CONTROL_SLOTS":     &c.ControlSlots,
		"PROXYBLOB_UDP_QUEUE_BYTES":   &c.UDPQueueBytes,
		"PROXYBLOB_UDP_QUEUE_PACKETS": &c.UDPQueuePackets,
		"PROXYBLOB_UDP_DESTINATIONS":  &c.UDPDestinations,
	} {
		if value, ok := os.LookupEnv(name); ok {
			n, err := strconv.Atoi(value)
			if err != nil {
				return c, diag.ErrInvalidFlowConfig
			}
			*target = n
		}
	}
	if value, ok := os.LookupEnv("PROXYBLOB_DRAIN_TIMEOUT"); ok {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return c, diag.ErrInvalidFlowConfig
		}
		c.DrainTimeout = duration
	}
	return c, c.validate()
}
