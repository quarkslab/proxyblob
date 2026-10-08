package proxy

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"proxyblob/internal/diag"
	"proxyblob/internal/mux"
	"proxyblob/internal/relay"
	"proxyblob/internal/socks5"

	"github.com/rs/zerolog/log"
)

// serveUDP handles UDP ASSOCIATE. The proxy owns the client-facing UDP socket
// and checks the association's source locally; the agent owns the target-facing
// socket. Datagrams cross the tunnel on one stream, as SOCKS5 UDP requests.
// The association lasts as long as both the control connection and the stream.
func (s *ProxyServer) serveUDP(client net.Conn, r *mux.Reservation, request []byte) {
	local, ok := client.LocalAddr().(*net.TCPAddr)
	remote, ok2 := client.RemoteAddr().(*net.TCPAddr)
	if !ok || !ok2 {
		r.Release()
		return
	}
	requested, code := socks5.ParseAddress(request)
	if code != diag.ErrNone {
		reject(client, replyCode(code))
		r.Release()
		return
	}
	host, _, err := net.SplitHostPort(requested)
	if err != nil {
		reject(client, socks5.AddressTypeNotSupported)
		r.Release()
		return
	}
	// The associated source is always the TCP peer. A domain source hint
	// does not override it and does not require DNS at the proxy.
	if ip := net.ParseIP(host); ip != nil && !ip.IsUnspecified() && !ip.Equal(remote.IP) {
		reject(client, socks5.ConnectionNotAllowed)
		r.Release()
		return
	}
	sourcePort := int(binary.BigEndian.Uint16(request[len(request)-2:]))
	socket, err := net.ListenUDP("udp", &net.UDPAddr{IP: local.IP, Zone: local.Zone})
	if err != nil {
		reject(client, socks5.GeneralFailure)
		r.Release()
		return
	}
	defer socket.Close()

	stream, err := r.Open(s.Ctx)
	if err != nil {
		return
	}
	defer stream.CloseWithCode(diag.ErrNone)
	d := stream.EnableDatagrams()
	if d == nil || relay.WriteRequest(stream, relay.UDP, socks5.UDPAddress(&net.UDPAddr{IP: net.IPv4zero})) != nil {
		return
	}
	code, _, err = relay.ReadReply(stream)
	if err != nil {
		return
	}
	if code != diag.ErrNone {
		reject(client, replyCode(code))
		return
	}
	if _, err := client.Write(socks5.Reply(socks5.Succeeded, socks5.UDPAddress(socket.LocalAddr().(*net.UDPAddr)))); err != nil {
		return
	}
	// Closing the control connection ends the association; the stream's close
	// (the agent's, or the session's) closes the client and the socket.
	go func() { io.Copy(io.Discard, client); stream.CloseWithCode(diag.ErrNone) }()
	go func() { <-stream.Done(); socket.Close() }()
	s.relayClientUDP(stream, d, socket, remote, sourcePort)
}

// relayClientUDP accepts datagrams only from the control connection's IP and,
// if the request named one, port; the first such sender becomes the client.
func (s *ProxyServer) relayClientUDP(stream *mux.ProtocolConn, d *mux.Datagrams, socket *net.UDPConn, remote *net.TCPAddr, sourcePort int) {
	var mu sync.Mutex
	var client *net.UDPAddr
	finished := make(chan error, 1)
	go func() {
		buf := make([]byte, 65535)
		for {
			n, from, e := socket.ReadFromUDP(buf)
			if e != nil {
				finished <- e
				stream.CloseWithCode(diag.ErrNone)
				return
			}
			if n > mux.MaxDatagramSize {
				continue
			}
			if !from.IP.Equal(remote.IP) || sourcePort != 0 && from.Port != sourcePort {
				continue
			}
			if _, _, code := relay.ParseDatagram(buf[:n]); code != diag.ErrNone {
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
			if e = d.Send(buf[:n]); e != nil && !errors.Is(e, diag.ErrDatagramDropped) {
				finished <- e
				stream.CloseWithCode(diag.ErrNone)
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
		if _, _, code := relay.ParseDatagram(packet); code != diag.ErrNone {
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
