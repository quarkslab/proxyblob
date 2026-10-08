package bootstrap

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/user"
)

// MaxIdentityLen bounds the identity payload so its 2-byte length prefix can
// never disagree with the bytes that follow.
const MaxIdentityLen = 512

// LocalIdentity returns "user@hostname", with "unknown" for either part that
// cannot be determined.
func LocalIdentity() string {
	username := "unknown"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s@%s", username, hostname)
}

// WriteIdentity sends the identity frame, the first bytes on a new session: a
// 2-byte big-endian length N (1 <= N <= MaxIdentityLen) followed by exactly N
// bytes, in a single Write so no other writer can interleave. Longer
// identities are truncated. A failed write leaves the peer's stream
// unframed, so the caller must close the connection.
func WriteIdentity(w io.Writer, identity string) error {
	if len(identity) > MaxIdentityLen {
		identity = identity[:MaxIdentityLen]
	}
	frame := make([]byte, 2+len(identity))
	binary.BigEndian.PutUint16(frame[:2], uint16(len(identity)))
	copy(frame[2:], identity)
	_, err := w.Write(frame)
	return err
}

// ReadIdentity reads one identity frame. Short reads are errors, so identity
// bytes are never left at the head of the stream.
func ReadIdentity(r io.Reader) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", fmt.Errorf("read identity length: %w", err)
	}
	length := binary.BigEndian.Uint16(lenBuf[:])
	if length == 0 || int(length) > MaxIdentityLen {
		return "", fmt.Errorf("identity length %d out of range (1-%d)", length, MaxIdentityLen)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("read identity payload (%d bytes): %w", length, err)
	}
	return string(buf), nil
}
