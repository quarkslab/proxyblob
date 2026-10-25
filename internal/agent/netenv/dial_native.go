//go:build !js

package netenv

import (
	"context"
	"net"

	"proxyblob/internal/bootstrap"
)

func DialTCP(target string) (net.Conn, error) {
	return DialTCPContext(context.Background(), target)
}

// DialTCPContext reaches target through the machine's egress proxy when its
// settings name one, and directly otherwise (see bootstrap.DialTarget).
func DialTCPContext(ctx context.Context, target string) (net.Conn, error) {
	return bootstrap.DialTarget(ctx, target)
}
