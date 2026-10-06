//go:build js

package proxy

import (
	"errors"
	"fmt"
	"sync"
	"syscall/js"
)

// Version 2 requires immediate handles, synchronous callback detachment on
// dispose, and pull-based TCP receive. See docs/js-socket-host.md.
func requireJSHost(name string) error {
	v := js.Global().Get("ProxyBlobSocketHostVersion")
	if v.Type() != js.TypeNumber || v.Float() != 2 || js.Global().Get(name).Type() != js.TypeFunction {
		return fmt.Errorf("%s requires ProxyBlob socket host version 2: %w", name, errors.ErrUnsupported)
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

func jsSocketError(args []js.Value, fallback string) error {
	if len(args) > 0 && args[0].Type() == js.TypeString {
		return errors.New(args[0].String())
	}
	return errors.New(fallback)
}
