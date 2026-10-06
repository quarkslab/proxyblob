//go:build !js

package proxy

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"

	"proxyblob/pkg/protocol"
)

// BIND accepts only the requested peer IP(s) and port; unspecified IP and port
// zero are explicit wildcards. DNS is resolved once so the policy cannot change
// between replies. The setup timeout is configurable through WithBindTimeout.
func (h *SocksHandler) handleBind(c *protocol.Connection, data []byte) byte {
	target, code := ParseAddress(data[3:])
	if code != protocol.ErrNone {
		h.SendError(c, code)
		return code
	}
	setup, cancel := socketSetupContext(h.Ctx, c.Closed)
	defer cancel()
	ctx, deadline := context.WithTimeout(setup, h.bindTimeout)
	defer deadline()
	go func() {
		select {
		case <-c.ReceiveDone():
			cancel()
		case <-ctx.Done():
		}
	}()
	ips, port, err := bindPeer(ctx, target)
	if err != nil {
		code = protocol.MapNetError(err)
		h.SendError(c, code)
		return code
	}
	listener, err := listenBind(ctx, ips)
	if err != nil {
		code = protocol.MapNetError(err)
		h.SendError(c, code)
		return code
	}
	var once sync.Once
	closeListener := func() { once.Do(func() { listener.Close() }) }
	defer closeListener()
	stop := context.AfterFunc(ctx, closeListener)
	defer stop()
	if code = h.sendTCPReply(c, Succeeded, listener.Addr().(*net.TCPAddr)); code != protocol.ErrNone {
		return code
	}
	for {
		peer, err := listener.Accept()
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				code = protocol.ErrTTLExpired
			} else {
				code = protocol.MapNetError(err)
			}
			h.SendError(c, code)
			return code
		}
		if ctx.Err() != nil {
			peer.Close()
			h.SendError(c, protocol.ErrTTLExpired)
			return protocol.ErrTTLExpired
		}
		remote := peer.RemoteAddr().(*net.TCPAddr)
		allowed := port == 0 || remote.Port == port
		match := false
		for _, ip := range ips {
			if (ip.IP.IsUnspecified() && (ip.IP.To4() != nil) == (remote.IP.To4() != nil)) || ip.IP.Equal(remote.IP) {
				match = true
				break
			}
		}
		if !allowed || !match {
			peer.Close()
			continue
		}
		closeListener()
		// Cancel only setup watchers; the accepted socket belongs to Connection.
		deadline()
		cancel()
		owned := &bindConn{TCPConn: peer.(*net.TCPConn)}
		if !c.AttachDestination(owned) {
			return protocol.ErrConnectionClosed
		}
		if code = h.sendTCPReply(c, Succeeded, remote); code != protocol.ErrNone {
			return code
		}
		return h.handleTCPDataTransfer(c, owned)
	}
}

// Try all DNS candidates; an unavailable family must not hide a usable route.
func listenBind(ctx context.Context, ips []net.IPAddr) (net.Listener, error) {
	var last error
	for _, ip := range ips {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		local, err := bindLocal(ctx, ip)
		if err != nil {
			last = err
			continue
		}
		network := "tcp6"
		if local.IP.To4() != nil {
			network = "tcp4"
		}
		lc := net.ListenConfig{}
		listener, err := lc.Listen(ctx, network, net.JoinHostPort(local.IP.String(), "0"))
		if err == nil {
			return listener, nil
		}
		last = err
	}
	if last == nil {
		last = &net.AddrError{Err: "no BIND peer addresses"}
	}
	return nil, last
}

func bindPeer(ctx context.Context, target string) ([]net.IPAddr, int, error) {
	host, p, err := net.SplitHostPort(target)
	if err != nil {
		return nil, 0, err
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		return nil, 0, err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, 0, err
	}
	if len(ips) == 0 {
		return nil, 0, &net.DNSError{Err: "no addresses", Name: host}
	}
	return ips, port, nil
}

// A UDP connect selects the route's local interface without transmitting data.
// For a wildcard peer, advertise an up non-loopback interface of that family;
// NAT/public address discovery is outside SOCKS and must be arranged by deployment.
func bindLocal(ctx context.Context, peer net.IPAddr) (*net.UDPAddr, error) {
	if peer.IP.IsUnspecified() {
		interfaces, err := net.Interfaces()
		if err != nil {
			return nil, err
		}
		for _, iface := range interfaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				ip, _, err := net.ParseCIDR(addr.String())
				if err == nil && !ip.IsLinkLocalUnicast() && (ip.To4() != nil) == (peer.IP.To4() != nil) {
					return &net.UDPAddr{IP: ip}, nil
				}
			}
		}
		return nil, &net.AddrError{Err: "no usable interface for wildcard BIND", Addr: peer.String()}
	}
	network := "udp6"
	if peer.IP.To4() != nil {
		network = "udp4"
	}
	route, err := (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(peer.String(), "9"))
	if err != nil {
		return nil, err
	}
	defer route.Close()
	return route.LocalAddr().(*net.UDPAddr), nil
}

// Forwarding failure, stream disposal and cancellation can converge on Close.
// Keep the accepted socket's release exactly once while retaining TCP half-close.
type bindConn struct {
	*net.TCPConn
	once     sync.Once
	closeErr error
}

func (c *bindConn) Close() error {
	c.once.Do(func() { c.closeErr = c.TCPConn.Close() })
	return c.closeErr
}
