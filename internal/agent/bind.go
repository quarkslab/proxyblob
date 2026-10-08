package agent

import (
	"net"
	"proxyblob/internal/socks5"
	"proxyblob/pkg/protocol"
)

// Both CONNECT and BIND replies describe the actual socket, not the request's
// address family (a domain can resolve to either IPv4 or IPv6).
func (h *SocksHandler) sendTCPReply(c *protocol.Connection, code byte, addr *net.TCPAddr) byte {
	address := socks5.UDPAddress(&net.UDPAddr{IP: addr.IP, Port: addr.Port})
	return h.SendData(c.ID, append([]byte{socks5.Version5, code, 0}, address...))
}
