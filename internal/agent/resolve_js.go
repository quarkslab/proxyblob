//go:build js

package agent

import (
	"context"
	"net"
	"proxyblob/pkg/protocol"
	"strconv"
	"syscall/js"
	"time"
)

// Literal destinations use the existing v2 host. Domain destinations require
// its UDPResolve extension, with the same immediate disposal barrier.
func resolveUDPContext(parent context.Context, address string) (*net.UDPAddr, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); ip != nil {
		return &net.UDPAddr{IP: ip, Port: p}, nil
	}
	if err = requireJSHost("UDPResolve"); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	type result struct {
		ip  net.IP
		err error
	}
	done := make(chan result, 1)
	finish := func(r result) {
		select {
		case done <- r:
		default:
		}
	}
	success := js.FuncOf(func(_ js.Value, args []js.Value) any {
		ip := net.ParseIP(args[0].String())
		if ip == nil {
			finish(result{err: protocol.ErrJSHostProtocol})
		} else {
			finish(result{ip: ip})
		}
		return nil
	})
	failure := js.FuncOf(func(_ js.Value, args []js.Value) any {
		finish(result{err: jsSocketError()})
		return nil
	})
	life := jsLifetime{callbacks: []js.Func{success, failure}}
	life.handle = js.Global().Call("UDPResolve", host, success, failure)
	defer life.dispose()
	select {
	case r := <-done:
		return &net.UDPAddr{IP: r.ip, Port: p}, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
