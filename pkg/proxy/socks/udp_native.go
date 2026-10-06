//go:build !js

package proxy

import (
	"context"
	"io"
	"net"
	"time"
)

type nativeUDPConn struct {
	conn *net.UDPConn
}

func listenUDP() (UDPRelayConn, error) {
	c, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return nil, err
	}
	return &nativeUDPConn{c}, nil
}

func (c *nativeUDPConn) LocalPort() int {
	return c.conn.LocalAddr().(*net.UDPAddr).Port
}

func (c *nativeUDPConn) ReadFrom(b []byte) (int, *net.UDPAddr, error) {
	if len(b) >= 65535 {
		return c.conn.ReadFromUDP(b)
	}
	packet := make([]byte, 65535)
	n, addr, err := c.conn.ReadFromUDP(packet)
	copied := copy(b, packet[:n])
	if err == nil && copied < n {
		err = io.ErrShortBuffer
	}
	return copied, addr, err
}

func (c *nativeUDPConn) WriteTo(b []byte, addr *net.UDPAddr) error {
	n, err := c.conn.WriteToUDP(b, addr)
	if err == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return err
}

func (c *nativeUDPConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *nativeUDPConn) Close() error {
	return c.conn.Close()
}

func listenUDPContext(ctx context.Context) (UDPRelayConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return listenUDP()
}

func resolveUDPContext(ctx context.Context, address string) (*net.UDPAddr, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	p, err := net.LookupPort("udp", port)
	if err != nil {
		return nil, err
	}
	return &net.UDPAddr{IP: ips[0].IP, Zone: ips[0].Zone, Port: p}, nil
}
