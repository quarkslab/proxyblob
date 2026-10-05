package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/google/uuid"
)

// makePayload builds a deterministic payload of n bytes whose content depends on
// seed, so that a packet swapped with another is detected by a byte comparison.
func makePayload(n int, seed byte) []byte {
	if n == 0 {
		return nil
	}
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i) ^ seed
	}
	return data
}

// encodeHeader builds a raw 21-byte header with an arbitrary declared length,
// bypassing Encode so that malformed framing can be constructed.
func encodeHeader(command byte, id uuid.UUID, dataLength uint32) []byte {
	header := make([]byte, HeaderSize)
	header[0] = command
	copy(header[CommandSize:CommandSize+UUIDSize], id[:])
	binary.BigEndian.PutUint32(header[CommandSize+UUIDSize:HeaderSize], dataLength)
	return header
}

func TestParseNext(t *testing.T) {
	id := uuid.New()

	onePacket := NewPacket(CmdData, id, makePayload(32, 0xA5)).Encode()
	emptyPacket := NewPacket(CmdClose, id, nil).Encode()

	multi := NewPacket(CmdNew, id, makePayload(4, 0x11)).Encode()
	multi = append(multi, NewPacket(CmdAck, id, nil).Encode()...)
	multi = append(multi, NewPacket(CmdData, id, makePayload(64, 0x22)).Encode()...)

	tests := []struct {
		name string
		buf  []byte
		// want describes the packets expected from parsing buf back to back.
		want []*Packet
		// wantErr is the error expected once want has been drained.
		wantErr error
		// wantTrailing is the number of unconsumed bytes left after parsing.
		wantTrailing int
	}{
		{
			name:         "buffer shorter than header",
			buf:          onePacket[:HeaderSize-1],
			wantErr:      ErrShortPacket,
			wantTrailing: HeaderSize - 1,
		},
		{
			name:         "empty buffer",
			buf:          nil,
			wantErr:      ErrShortPacket,
			wantTrailing: 0,
		},
		{
			name:         "exactly one complete packet",
			buf:          onePacket,
			want:         []*Packet{NewPacket(CmdData, id, makePayload(32, 0xA5))},
			wantErr:      ErrShortPacket,
			wantTrailing: 0,
		},
		{
			name: "several concatenated packets",
			buf:  multi,
			want: []*Packet{
				NewPacket(CmdNew, id, makePayload(4, 0x11)),
				NewPacket(CmdAck, id, nil),
				NewPacket(CmdData, id, makePayload(64, 0x22)),
			},
			wantErr:      ErrShortPacket,
			wantTrailing: 0,
		},
		{
			name:         "zero length payload on close",
			buf:          emptyPacket,
			want:         []*Packet{NewPacket(CmdClose, id, nil)},
			wantErr:      ErrShortPacket,
			wantTrailing: 0,
		},
		{
			name:         "invalid command zero",
			buf:          encodeHeader(0, id, 0),
			wantErr:      ErrMalformedPacket,
			wantTrailing: HeaderSize,
		},
		{
			name:         "invalid command six",
			buf:          encodeHeader(CmdEOF+1, id, 0),
			wantErr:      ErrMalformedPacket,
			wantTrailing: HeaderSize,
		},
		{
			name:         "declared length above maximum",
			buf:          encodeHeader(CmdData, id, MaxPacketDataSize+1),
			wantErr:      ErrMalformedPacket,
			wantTrailing: HeaderSize,
		},
		{
			name:         "header present but body truncated",
			buf:          onePacket[:HeaderSize+10],
			wantErr:      ErrShortPacket,
			wantTrailing: HeaderSize + 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rest := tt.buf

			for i, want := range tt.want {
				got, consumed, err := ParseNext(rest)
				if err != nil {
					t.Fatalf("packet %d: unexpected error: %v", i, err)
				}
				if consumed != want.EncodedSize() {
					t.Fatalf("packet %d: consumed = %d, want %d", i, consumed, want.EncodedSize())
				}
				assertPacketEqual(t, got, want, i)
				rest = rest[consumed:]
			}

			got, consumed, err := ParseNext(rest)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if got != nil {
				t.Errorf("packet = %+v, want nil on error", got)
			}
			if consumed != 0 {
				t.Errorf("consumed = %d, want 0 on error", consumed)
			}
			if len(rest) != tt.wantTrailing {
				t.Errorf("trailing bytes = %d, want %d", len(rest), tt.wantTrailing)
			}
		})
	}
}

// TestParseNextRejectsOversizedLengthImmediately guards the ordering of the
// length cap against the completeness check. If the cap were applied after the
// "do we have the whole body" test, a corrupted length field would be reported
// as ErrShortPacket and the caller would grow its accumulator without bound.
func TestParseNextRejectsOversizedLengthImmediately(t *testing.T) {
	id := uuid.New()

	lengths := []uint32{
		MaxPacketDataSize + 1,
		MaxPacketDataSize * 4,
		1 << 31,
		^uint32(0),
	}

	for _, dataLength := range lengths {
		// Only the header is available: the body for such a length would never
		// arrive, so the answer must be malformed rather than short.
		buf := encodeHeader(CmdData, id, dataLength)

		packet, consumed, err := ParseNext(buf)
		if errors.Is(err, ErrShortPacket) {
			t.Fatalf("dataLength %d: got ErrShortPacket, caller would buffer unboundedly", dataLength)
		}
		if !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("dataLength %d: error = %v, want ErrMalformedPacket", dataLength, err)
		}
		if packet != nil || consumed != 0 {
			t.Fatalf("dataLength %d: got (%+v, %d), want (nil, 0)", dataLength, packet, consumed)
		}
	}

	// A length exactly at the cap is legal framing, so it must be reported as
	// short (waiting for the body) and not rejected.
	buf := encodeHeader(CmdData, id, MaxPacketDataSize)
	if _, _, err := ParseNext(buf); !errors.Is(err, ErrShortPacket) {
		t.Fatalf("dataLength at cap: error = %v, want ErrShortPacket", err)
	}
}

// chunkedReader returns at most k bytes per Read call, simulating a byte-stream
// transport that splits and merges writes arbitrarily.
type chunkedReader struct {
	data []byte
	k    int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := min(min(r.k, len(p)), len(r.data))
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

// TestParseNextReassemblyAcrossReadBoundaries is the property the original code
// violated: packets must be recovered intact no matter where the transport
// happens to split the byte stream.
func TestParseNextReassemblyAcrossReadBoundaries(t *testing.T) {
	sizes := []int{0, 1, 20, 21, 100, 5000, 40000}
	commands := []byte{CmdNew, CmdAck, CmdData, CmdClose}

	want := make([]*Packet, 0, len(sizes))
	var stream []byte
	for i, size := range sizes {
		p := NewPacket(commands[i%len(commands)], uuid.New(), makePayload(size, byte(i+1)))
		want = append(want, p)
		stream = append(stream, p.Encode()...)
	}

	for _, k := range []int{1, 7, 21, 8192} {
		t.Run("chunk_"+strconv.Itoa(k), func(t *testing.T) {
			reader := &chunkedReader{data: append([]byte(nil), stream...), k: k}

			var (
				acc  []byte
				got  []*Packet
				temp = make([]byte, 4096)
			)

			// Mirror the real caller: append what was read, drain every complete
			// packet, stop on ErrShortPacket and keep the remainder for later.
			for {
				n, err := reader.Read(temp)
				if n > 0 {
					acc = append(acc, temp[:n]...)

					offset := 0
					for {
						packet, consumed, perr := ParseNext(acc[offset:])
						if errors.Is(perr, ErrShortPacket) {
							break
						}
						if perr != nil {
							t.Fatalf("unexpected parse error: %v", perr)
						}
						got = append(got, packet)
						offset += consumed
					}

					// Compact the remainder down to the front of the same backing
					// array, exactly as the real accumulator does, then scribble
					// over the freed region. Any payload returned as a sub-slice
					// of acc rather than a copy is corrupted by this.
					remaining := copy(acc, acc[offset:])
					freed := acc[remaining:cap(acc)]
					for i := range freed {
						freed[i] = 0xFF
					}
					acc = acc[:remaining]
				}
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					t.Fatalf("read error: %v", err)
				}
			}

			if len(acc) != 0 {
				t.Errorf("leftover bytes in accumulator = %d, want 0", len(acc))
			}
			if len(got) != len(want) {
				t.Fatalf("recovered %d packets, want %d", len(got), len(want))
			}
			for i := range want {
				assertPacketEqual(t, got[i], want[i], i)
			}
		})
	}
}

func assertPacketEqual(t *testing.T, got, want *Packet, index int) {
	t.Helper()

	if got == nil {
		t.Fatalf("packet %d: got nil packet", index)
	}
	if got.Command != want.Command {
		t.Errorf("packet %d: Command = %d, want %d", index, got.Command, want.Command)
	}
	if got.ConnectionID != want.ConnectionID {
		t.Errorf("packet %d: ConnectionID = %s, want %s", index, got.ConnectionID, want.ConnectionID)
	}
	if len(got.Data) != len(want.Data) {
		t.Fatalf("packet %d: len(Data) = %d, want %d", index, len(got.Data), len(want.Data))
	}
	if !bytes.Equal(got.Data, want.Data) {
		t.Errorf("packet %d: Data payload differs from the encoded input", index)
	}
}
