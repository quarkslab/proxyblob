//go:build !js

package netenv

import (
	"context"
	"net"
	"time"
)

func DialTCP(target string) (net.Conn, error) {
	return DialTCPContext(context.Background(), target)
}

func DialTCPContext(ctx context.Context, target string) (net.Conn, error) {
	return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "tcp", target)
}
