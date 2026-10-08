// Package mux multiplexes logical streams over one tunnel connection: packet
// framing, stream setup, credit-based flow control and half-close.
//
// The protocol uses a binary packet format with fixed-size headers and variable-length
// payloads. Each packet contains a command type, connection ID, and optional data.
package mux

import (
	"bytes"
	"encoding/binary"

	"github.com/google/uuid"
)

// MaxPacketDataSize is the largest payload a single packet may declare.
// It bounds the size of a reassembly accumulator: a caller buffering a packet
// never needs to hold more than HeaderSize + MaxPacketDataSize bytes.
const MaxPacketDataSize = 1 << 20

// Command types for protocol operations.
const (
	CmdNew          byte = iota + 1 // Request new connection
	CmdAck                          // Acknowledge connection
	CmdData                         // Transfer data
	CmdClose                        // Terminate connection
	CmdEOF                          // End only the sender's data direction (coordinated rollout)
	CmdCredit                       // Cumulative bytes consumed from the receive reservation
	CmdUDPAssociate                 // Agent requests a proxy-local UDP relay
	CmdUDPReady                     // Proxy returns SOCKS address
	CmdDatagram                     // Complete SOCKS UDP datagram, separate from TCP credit
)

// Protocol packet field sizes in bytes.
const (
	CommandSize    = 1  // Command field
	UUIDSize       = 16 // Connection ID field
	DataLengthSize = 4  // Payload length field
	HeaderSize     = CommandSize + UUIDSize + DataLengthSize
)

// Packet represents a protocol message with the following binary format:
//
//	+---------+----------------+--------------+---------+
//	| Command | Connection ID  | Data Length  | Payload |
//	+---------+----------------+--------------+---------+
//	|    1B   |      16B       |     4B       |   var   |
type Packet struct {
	Command      byte      // Operation type (CmdNew, CmdAck, etc.)
	ConnectionID uuid.UUID // Unique connection identifier
	Data         []byte    // Optional payload data
}

// NewPacket creates a protocol packet with the given parameters.
// The data parameter is optional and may be nil.
func NewPacket(command byte, connectionID uuid.UUID, data []byte) *Packet {
	return &Packet{
		Command:      command,
		ConnectionID: connectionID,
		Data:         data,
	}
}

// EncodedSize returns the total size of the encoded packet in bytes.
func (p *Packet) EncodedSize() int {
	return HeaderSize + len(p.Data)
}

// Encode serializes the packet into a byte slice following the protocol format.
// Returns nil if any encoding operation fails.
func (p *Packet) Encode() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, HeaderSize+len(p.Data)))

	if err := buf.WriteByte(p.Command); err != nil {
		return nil
	}

	if _, err := buf.Write(p.ConnectionID[:]); err != nil {
		return nil
	}

	if err := binary.Write(buf, binary.BigEndian, uint32(len(p.Data))); err != nil {
		return nil
	}

	if len(p.Data) > 0 {
		if _, err := buf.Write(p.Data); err != nil {
			return nil
		}
	}

	return buf.Bytes()
}

// ParseNext parses one packet from the front of buf, which may hold a partial
// packet, exactly one packet, or several concatenated packets. The underlying
// transport is a byte stream and does not preserve message boundaries, so a
// caller must accumulate bytes and call ParseNext repeatedly, consuming the
// reported number of bytes after each success.
//
// It returns:
//
//	(packet, consumed, nil)       success; consumed bytes may be dropped from buf
//	(nil, 0, ErrShortPacket)      partial packet; caller should read more and retry
//	(nil, 0, ErrMalformedPacket)  invalid header; the stream is desynchronized
//
// A zero-length payload is valid for every command, including CmdClose.
// Rejecting it here would leave the byte stream out of sync, so any semantic
// check on an empty payload belongs to the caller.
func ParseNext(buf []byte) (*Packet, int, error) {
	if len(buf) < HeaderSize {
		return nil, 0, ErrShortPacket
	}

	command := buf[0]
	if command < CmdNew || command > CmdDatagram {
		return nil, 0, ErrMalformedPacket
	}

	dataLength := binary.BigEndian.Uint32(buf[CommandSize+UUIDSize : HeaderSize])

	// The length cap MUST be checked before the "do we have the whole body yet"
	// check below. A corrupted length field would otherwise be reported as
	// ErrShortPacket, making the caller buffer up to 4 GiB waiting for a body
	// that never arrives. Rejecting oversized lengths first bounds the caller's
	// accumulator at HeaderSize + MaxPacketDataSize.
	if dataLength > MaxPacketDataSize {
		return nil, 0, ErrMalformedPacket
	}

	totalSize := HeaderSize + int(dataLength)
	if len(buf) < totalSize {
		return nil, 0, ErrShortPacket
	}

	var id uuid.UUID
	copy(id[:], buf[CommandSize:CommandSize+UUIDSize])

	// The payload MUST be copied into a fresh allocation and must never alias
	// buf. The returned Packet.Data is handed to another goroutine and outlives
	// this call, while the caller's accumulator is compacted and re-appended in
	// place; a sub-slice of buf would be read after it has been overwritten.
	// Do not "optimize" this copy away.
	var packetData []byte
	if dataLength > 0 {
		packetData = make([]byte, dataLength)
		copy(packetData, buf[HeaderSize:totalSize])
	}

	return NewPacket(command, id, packetData), totalSize, nil
}
