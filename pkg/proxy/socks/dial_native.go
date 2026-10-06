//go:build !js

package proxy

import (
	"context"
	"net"
	"time"
)

func dialTCP(target string) (net.Conn, error) {
	return dialTCPContext(context.Background(), target)
}

func dialTCPContext(ctx context.Context, target string) (net.Conn, error) {
	return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "tcp", target)
}
