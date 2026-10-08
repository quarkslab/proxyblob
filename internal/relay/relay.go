// Package relay is ProxyBlob's stream protocol between the proxy and an agent.
//
// The proxy negotiates SOCKS5 with its client locally, then opens one mux
// stream per request and writes a Request. The agent carries it out and writes
// a Reply; BIND writes two (the listening address, then the accepted peer).
// After a successful reply the stream carries the TCP payload in both
// directions, or for UDP, datagrams in the SOCKS5 UDP request format.
//
// Addresses use the SOCKS5 encoding (ATYP|ADDR|PORT); reply codes are diag
// codes, diag.ErrNone on success.
package relay

import (
	"io"
	"net"

	"proxyblob/internal/socks5"
)

// Request commands, numbered like their SOCKS5 counterparts.
const (
	Connect byte = 1 // dial a TCP target
	Bind    byte = 2 // listen for one inbound TCP peer
	UDP     byte = 3 // relay UDP datagrams to arbitrary targets
)

// WriteRequest sends a request in one write: [cmd][address].
func WriteRequest(w io.Writer, cmd byte, addr []byte) error {
	_, err := w.Write(append([]byte{cmd}, addr...))
	return err
}

// ReadRequest reads a request. An unknown address type is
// diag.ErrAddressNotSupported; the command is validated by the caller.
func ReadRequest(r io.Reader) (cmd byte, addr []byte, err error) {
	var c [1]byte
	if _, err = io.ReadFull(r, c[:]); err != nil {
		return 0, nil, err
	}
	addr, err = socks5.ReadAddress(r)
	return c[0], addr, err
}

// WriteReply sends a reply in one write: [code][bound address]. A nil bound
// address is encoded as 0.0.0.0:0.
func WriteReply(w io.Writer, code byte, bound net.Addr) error {
	var addr []byte
	switch a := bound.(type) {
	case *net.TCPAddr:
		addr = socks5.UDPAddress(&net.UDPAddr{IP: a.IP, Port: a.Port})
	case *net.UDPAddr:
		addr = socks5.UDPAddress(a)
	default:
		addr = []byte{socks5.IPv4, 0, 0, 0, 0, 0, 0}
	}
	_, err := w.Write(append([]byte{code}, addr...))
	return err
}

// ReadReply reads a reply: a diag code and the bound address in wire encoding.
func ReadReply(r io.Reader) (code byte, addr []byte, err error) {
	var c [1]byte
	if _, err = io.ReadFull(r, c[:]); err != nil {
		return 0, nil, err
	}
	addr, err = socks5.ReadAddress(r)
	return c[0], addr, err
}

// ParseAddress decodes a wire address to host:port.
func ParseAddress(addr []byte) (string, byte) { return socks5.ParseAddress(addr) }

// ParseDatagram returns a datagram's target address and the payload offset.
func ParseDatagram(b []byte) (target string, payload int, code byte) {
	return socks5.ExtractUDPHeader(b)
}

// Datagram builds a datagram carrying payload from source.
func Datagram(source *net.UDPAddr, payload []byte) []byte {
	b := append([]byte{0, 0, 0}, socks5.UDPAddress(source)...)
	return append(b, payload...)
}
