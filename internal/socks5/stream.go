package socks5

import (
	"io"

	"proxyblob/internal/diag"
)

// ReadAddress reads one ATYP|ADDR|PORT address from r and returns it in wire
// encoding. An unknown address type is diag.ErrAddressNotSupported; a short
// read returns the reader's error.
func ReadAddress(r io.Reader) ([]byte, error) {
	var atyp [1]byte
	if _, err := io.ReadFull(r, atyp[:]); err != nil {
		return nil, err
	}
	var size int
	switch atyp[0] {
	case IPv4:
		size = 4
	case IPv6:
		size = 16
	case Domain:
		var length [1]byte
		if _, err := io.ReadFull(r, length[:]); err != nil {
			return nil, err
		}
		addr := make([]byte, 2+int(length[0])+2)
		addr[0], addr[1] = Domain, length[0]
		if _, err := io.ReadFull(r, addr[2:]); err != nil {
			return nil, err
		}
		return addr, nil
	default:
		return nil, diag.Error(diag.ErrAddressNotSupported)
	}
	addr := make([]byte, 1+size+2)
	addr[0] = atyp[0]
	if _, err := io.ReadFull(r, addr[1:]); err != nil {
		return nil, err
	}
	return addr, nil
}

// Reply builds a server reply, VER REP RSV followed by addr; a nil addr is the
// unspecified IPv4 address 0.0.0.0:0.
func Reply(rep byte, addr []byte) []byte {
	if addr == nil {
		addr = []byte{IPv4, 0, 0, 0, 0, 0, 0}
	}
	return append([]byte{Version5, rep, 0}, addr...)
}
