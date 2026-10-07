//go:build js

package proxy

import (
	"errors"
	"proxyblob/pkg/protocol"
	"sync"
	"syscall/js"
)

// Version 2 requires immediate handles, synchronous callback detachment on
// dispose, and pull-based TCP receive. See the reference host in examples/bun/host.ts.
func requireJSHost(name string) error {
	v := js.Global().Get("ProxyBlobSocketHostVersion")
	if v.Type() != js.TypeNumber || v.Float() != 2 || js.Global().Get(name).Type() != js.TypeFunction {
		return errors.Join(protocol.ErrJSHostUnsupported, errors.ErrUnsupported)
	}
	return nil
}

type jsLifetime struct {
	handle    js.Value
	callbacks []js.Func
	once      sync.Once
}

func (l *jsLifetime) dispose() {
	l.once.Do(func() {
		// A conforming host must not throw and must detach synchronously, including
		// callbacks retained by pending operations, before returning from dispose.
		l.handle.Call("dispose")
		for _, f := range l.callbacks {
			f.Release()
		}
		l.callbacks = nil
	})
}

func jsSocketError() error {
	return protocol.Error(protocol.ErrTransportError)
}
