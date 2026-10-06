package proxy

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"proxyblob/pkg/protocol"
	socks "proxyblob/pkg/proxy/socks"
)

// OnUDPAssociate only admits once. Socket setup and I/O never block dispatch.
func (s *ProxyServer) OnUDPAssociate(c *protocol.Connection, address []byte) byte {
	d := c.EnableDatagrams()
	if d == nil {
		return protocol.ErrInvalidState
	}
	go s.serveUDP(c, d, append([]byte(nil), address...))
	return protocol.ErrNone
}
func (s *ProxyServer) serveUDP(c *protocol.Connection, d *protocol.Datagrams, request []byte) {
	defer s.SendClose(c.ID, protocol.ErrNone)
	local, remote := c.DestinationAddresses()
	l, ok := local.(*net.TCPAddr)
	if !ok {
		return
	}
	r, ok := remote.(*net.TCPAddr)
	if !ok {
		return
	}
	requested, code := socks.ParseAddress(request)
	if code != protocol.ErrNone {
		return
	}
	host, _, err := net.SplitHostPort(requested)
	if err != nil {
		return
	}
	// The associated source is always the TCP peer. A domain source hint
	// does not override it and does not require DNS at the proxy.
	ip := net.ParseIP(host)
	if ip != nil && !ip.IsUnspecified() && !ip.Equal(r.IP) {
		s.rejectUDPSetup(c, d, 2)
		return
	}
	sourcePort := int(binary.BigEndian.Uint16(request[len(request)-2:]))
	socket, err := net.ListenUDP("udp", &net.UDPAddr{IP: l.IP, Zone: l.Zone})
	if err != nil {
		s.rejectUDPSetup(c, d, 1)
		return
	}
	defer socket.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-c.Closed:
		case <-s.Ctx.Done():
		case <-done:
		}
		socket.Close()
	}()
	addr := socket.LocalAddr().(*net.UDPAddr)
	if err = d.Ready(socks.UDPAddress(addr)); err != nil {
		return
	}
	var mu sync.Mutex
	var client *net.UDPAddr
	finished := make(chan error, 1)
	go func() {
		buf := make([]byte, 65535)
		for {
			n, from, e := socket.ReadFromUDP(buf)
			if e != nil {
				finished <- e
				c.Close()
				return
			}
			if n > protocol.MaxDatagramSize {
				continue
			}
			if !from.IP.Equal(r.IP) || sourcePort != 0 && from.Port != sourcePort {
				continue
			}
			if _, _, code := socks.ExtractUDPHeader(buf[:n]); code != protocol.ErrNone {
				continue
			}
			mu.Lock()
			if client == nil {
				client = from
			}
			allowed := client.Port == from.Port && client.IP.Equal(from.IP)
			mu.Unlock()
			if !allowed {
				continue
			}
			if e = d.Send(buf[:n]); e != nil && !errors.Is(e, protocol.ErrDatagramDropped) {
				finished <- e
				c.Close()
				socket.Close()
				return
			}
		}
	}()
	// Join the read worker on every exit, including a reply write failure.
	defer func() {
		socket.Close()
		if err := <-finished; err != nil && !errors.Is(err, net.ErrClosed) {
			log.Warn().Err(err).Msg("Proxy UDP relay stopped")
		}
	}()

	for {
		packet, e := d.Receive()
		if e != nil {
			return
		}
		if _, _, code := socks.ExtractUDPHeader(packet); code != protocol.ErrNone {
			continue
		}
		mu.Lock()
		peer := client
		mu.Unlock()
		if peer == nil {
			continue
		}
		n, e := socket.WriteToUDP(packet, peer)
		if e != nil || n != len(packet) {
			log.Warn().Err(e).Msg("Proxy UDP reply write failed")
			return
		}
	}
}

func (s *ProxyServer) rejectUDPSetup(c *protocol.Connection, d *protocol.Datagrams, code byte) {
	if d.Reject(code) != nil {
		return
	}
	// Let the agent flush the SOCKS error reply before closing the control stream.
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-c.Closed:
	case <-s.Ctx.Done():
	case <-timer.C:
	}
}
