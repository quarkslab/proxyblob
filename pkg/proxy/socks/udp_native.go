//go:build !js

package proxy

import (
	"context"
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
	return c.conn.ReadFromUDP(b)
}

func (c *nativeUDPConn) WriteTo(b []byte, addr *net.UDPAddr) error {
	_, err := c.conn.WriteToUDP(b, addr)
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
