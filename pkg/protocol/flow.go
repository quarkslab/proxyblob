package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// FlowConfig bounds memory owned by multiplexing, independently of the aznet
// transport and application/socket buffers. Each admitted stream reserves its
// whole receive window, including grants that have not arrived yet.
type FlowConfig struct {
	StreamWindow int
	TunnelWindow int
	MaxStreams   int
	DataFrame    int
	BatchBytes   int
	ControlSlots int
	DrainTimeout time.Duration
}

func DefaultFlowConfig() FlowConfig {
	return FlowConfig{
		StreamWindow: 64 << 10, TunnelWindow: 8 << 20, MaxStreams: 128,
		DataFrame: 16 << 10, BatchBytes: 64 << 10, ControlSlots: 512, DrainTimeout: DrainTimeout,
	}
}

func (c FlowConfig) validate() error {
	if c.StreamWindow < 1 || c.StreamWindow > MaxPacketDataSize ||
		c.TunnelWindow < c.StreamWindow ||
		c.MaxStreams < 1 || c.MaxStreams > 65536 ||
		c.DataFrame < 1 || c.DataFrame > MaxPacketDataSize ||
		c.BatchBytes < max(c.DataFrame, 12)+HeaderSize || c.BatchBytes > 16<<20 ||
		c.ControlSlots < 4*c.MaxStreams || c.ControlSlots > 1<<20 ||
		c.DrainTimeout <= 0 {
		return errors.New("protocol: invalid finite flow limits")
	}
	return nil
}

// Version 2 requires receive-window negotiation and cumulative consumption.
// Empty (legacy) NEW/ACK payloads are explicitly unsupported, never sniffed.
const ProtocolVersion uint32 = 2

var ErrUnsupportedVersion = errors.New("protocol: unsupported version")
var ErrFlowControl = errors.New("protocol: invalid receive credit")
var ErrCapacity = errors.New("protocol: tunnel stream reservation exhausted")

func (h *BaseHandler) handshake() []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint32(b, ProtocolVersion)
	binary.BigEndian.PutUint64(b[4:], uint64(h.flow.StreamWindow))
	return b
}

func peerWindow(b []byte) (uint64, error) {
	if len(b) < 4 || binary.BigEndian.Uint32(b) != ProtocolVersion {
		return 0, ErrUnsupportedVersion
	}
	if len(b) != 12 {
		return 0, ErrFlowControl
	}
	n := binary.BigEndian.Uint64(b[4:])
	if n == 0 || n > MaxPacketDataSize {
		return 0, ErrFlowControl
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
		return ErrCapacity
	}
	if _, exists := h.Connections.Load(c.ID); exists {
		return fmt.Errorf("protocol: duplicate stream")
	}
	c.deliveryMu.Lock()
	select {
	case <-c.Closed:
		c.deliveryMu.Unlock()
		return net.ErrClosed
	default:
	}
	c.buffer = make([]byte, h.flow.StreamWindow)
	c.handler = h
	c.deliveryMu.Unlock()
	h.streams++
	h.reserved += h.flow.StreamWindow
	h.Connections.Store(c.ID, c)
	return nil
}

func (h *BaseHandler) releaseReservation(c *Connection) {
	h.flowMu.Lock()
	h.streams--
	h.reserved -= h.flow.StreamWindow
	h.flowMu.Unlock()
}

func (c *Connection) setPeerWindow(window uint64) error {
	c.creditMu.Lock()
	defer c.creditMu.Unlock()
	if c.peerWindow != 0 {
		return ErrFlowControl
	}
	c.peerWindow = window
	c.creditChanged()
	return nil
}

func (c *Connection) creditChanged() { close(c.creditWake); c.creditWake = make(chan struct{}) }

// Duplicate or delayed cumulative updates are harmless. Future consumption
// and counter wrap are violations; a grant can never exceed the peer's window.
func (c *Connection) updateCredit(consumed uint64) error {
	c.creditMu.Lock()
	defer c.creditMu.Unlock()
	if c.peerWindow == 0 || consumed > c.sent {
		return ErrFlowControl
	}
	if consumed <= c.peerConsumed {
		return nil
	}
	c.peerConsumed = consumed
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
				return 0, ErrFlowControl
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
	if len(data) != 8 {
		return ErrInvalidPacket
	}
	v, ok := h.Connections.Load(id)
	if !ok {
		return ErrNone
	} // delayed control for a disposed stream
	if err := v.(*Connection).updateCredit(binary.BigEndian.Uint64(data)); err != nil {
		return ErrInvalidPacket
	}
	return ErrNone
}

// FlowConfigFromEnv is shared by native and WASM commands. Byte/count values
// are decimal integers; the drain duration uses Go duration syntax.
func FlowConfigFromEnv() (FlowConfig, error) {
	c := DefaultFlowConfig()
	for name, target := range map[string]*int{
		"PROXYBLOB_STREAM_WINDOW": &c.StreamWindow,
		"PROXYBLOB_TUNNEL_WINDOW": &c.TunnelWindow,
		"PROXYBLOB_MAX_STREAMS":   &c.MaxStreams,
		"PROXYBLOB_DATA_FRAME":    &c.DataFrame,
		"PROXYBLOB_BATCH_BYTES":   &c.BatchBytes,
		"PROXYBLOB_CONTROL_SLOTS": &c.ControlSlots,
	} {
		if value, ok := os.LookupEnv(name); ok {
			n, err := strconv.Atoi(value)
			if err != nil {
				return c, fmt.Errorf("%s: %w", name, err)
			}
			*target = n
		}
	}
	if value, ok := os.LookupEnv("PROXYBLOB_DRAIN_TIMEOUT"); ok {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return c, err
		}
		c.DrainTimeout = duration
	}
	return c, c.validate()
}
