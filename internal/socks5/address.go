package socks5

import (
	"encoding/binary"
	"fmt"
	"net"

	"proxyblob/internal/mux"
)

// ParseAddress extracts a target address from SOCKS5 address data.
// It returns the address in host:port format and any error encountered.
// The address format follows RFC 1928 Section 4.
func ParseAddress(data []byte) (string, byte) {
	if len(data) == 0 {
		return "", mux.ErrAddressNotSupported
	}
	addr, _, err := ParseNetworkAddress(data[0], data[1:])
	if err != mux.ErrNone {
		return "", err
	}
	return addr, mux.ErrNone
}

// ParseNetworkAddress parses a network address from SOCKS5 formatted data.
// The format is:
//
//	+------+----------+----------+
//	| ATYP | DST.ADDR | DST.PORT |
//	+------+----------+----------+
//	|  1   | Variable |    2     |
//
// Returns the address string in host:port format, bytes consumed, and any error.
func ParseNetworkAddress(addrType byte, data []byte) (string, int, byte) {
	cursor := 0
	var addr string

	switch addrType {
	case IPv4:
		if len(data) < cursor+4+2 { // 4 bytes IPv4 + 2 bytes port
			return "", 0, mux.ErrAddressNotSupported
		}
		ip := net.IPv4(data[cursor], data[cursor+1], data[cursor+2], data[cursor+3])
		addr = ip.String()
		cursor += 4

	case IPv6:
		if len(data) < cursor+16+2 { // 16 bytes IPv6 + 2 bytes port
			return "", 0, mux.ErrAddressNotSupported
		}
		ip := net.IP(data[cursor : cursor+16])
		addr = fmt.Sprintf("[%s]", ip.String())
		cursor += 16

	case Domain:
		if len(data) < cursor+1 { // Need length byte
			return "", 0, mux.ErrAddressNotSupported
		}
		domainLen := int(data[cursor])
		cursor++
		if domainLen == 0 || len(data) < cursor+domainLen+2 { // +2 for port
			return "", 0, mux.ErrAddressNotSupported
		}
		addr = string(data[cursor : cursor+domainLen])
		cursor += domainLen

	default:
		return "", 0, mux.ErrAddressNotSupported
	}

	if len(data) < cursor+2 {
		return "", 0, mux.ErrAddressNotSupported
	}

	port := binary.BigEndian.Uint16(data[cursor : cursor+2])
	cursor += 2

	return fmt.Sprintf("%s:%d", addr, port), cursor, mux.ErrNone
}

// ExtractUDPHeader parses a SOCKS5 UDP datagram header and returns the target address.
// The format is:
//
//	+-----+------+------+----------+----------+----------+
//	| RSV | FRAG | ATYP | DST.ADDR | DST.PORT |   DATA   |
//	+-----+------+------+----------+----------+----------+
//	|  2  |  1   |  1   | Variable |    2     | Variable |
//
// Returns the target address, header length, and any error encountered.
func ExtractUDPHeader(data []byte) (string, int, byte) {
	if len(data) < 4 || data[0] != 0 || data[1] != 0 || data[2] != 0 {
		return "", 0, mux.ErrInvalidPacket
	}
	headerLen := 4 // RSV(2) + FRAG(1) + ATYP(1)

	// Parse the address part of the header
	addr, addrLen, err := ParseNetworkAddress(data[3], data[4:]) // Use ATYP and pass remaining data
	if err != mux.ErrNone {
		return "", 0, err
	}
	return addr, headerLen + addrLen, mux.ErrNone
}

// UDPAddress encodes an actual socket address without losing its address family.
func UDPAddress(addr *net.UDPAddr) []byte {
	var b []byte
	if ip := addr.IP.To4(); ip != nil {
		b = append([]byte{IPv4}, ip...)
	} else {
		b = append([]byte{IPv6}, addr.IP.To16()...)
	}
	return binary.BigEndian.AppendUint16(b, uint16(addr.Port))
}
