//go:build js

// Package netenvtest drives a deterministic in-process JS socket host and the
// Bun loopback peers, for wasm tests of netenv and of the agent above it.
package netenvtest

import (
	"syscall/js"
	"testing"
)

// This deterministic host drives the real setup boundary in Bun and Node.
// All dispatch consults one detachable callback table, including queued events.
func Host(t *testing.T, mode string) js.Value {
	t.Helper()
	f := js.Global().Get("Function").New(`return (() => {
 let callbacks, probes=[], disposed=0, reads=0, ends=0;
 const state={mode:"connect",payload:"",writeCount:4};
 const socket={write:b=>state.writeCount,end:()=>ends++,send:()=>true};
 const handle={dispose(){if(callbacks){callbacks=undefined;disposed++}},
 read(n){reads++;if(state.payload){queueMicrotask(()=>{if(callbacks){const b=new TextEncoder().encode(state.payload);callbacks[1](socket,b);callbacks[2]()}})}}};
 function emit(i,...args){queueMicrotask(()=>callbacks?.[i](...args))}
 function start(c){callbacks=c;probes=c.slice();queueMicrotask(()=>{if(!callbacks)return;
 if(state.mode==="connect")callbacks[0](socket,12345);
 if(state.mode==="close")callbacks[2]();
 if(state.mode==="error")callbacks[c.length===3?2:3]("setup failure");
 });return handle}
 state.tcp=(host,port,...c)=>start(c);state.udp=(...c)=>start(c);
 // Diagnostic references are never dispatched as socket events. Probing them
 // after detachment checks the real Go callback registry has released each id.
 state.released=()=>{let count=0;const old=console.error;console.error=(message)=>{if(message==="call to released function")count++};
 try{for(const f of probes)f(socket,new Uint8Array(1),12345,"127.0.0.1")}finally{console.error=old}return count};
 state.flush=done=>queueMicrotask(done);state.emit=emit;state.socket=socket;state.counts=()=>({disposed,reads,ends,callbacks:callbacks?.length??0,expected:probes.length});
 return state;
 })()`)
	state := f.Invoke()
	state.Set("mode", mode)
	tcp, udp, version := js.Global().Get("TCPDial"), js.Global().Get("UDPListen"), js.Global().Get("ProxyBlobSocketHostVersion")
	js.Global().Set("TCPDial", state.Get("tcp"))
	js.Global().Set("UDPListen", state.Get("udp"))
	js.Global().Set("ProxyBlobSocketHostVersion", 2)
	t.Cleanup(func() {
		js.Global().Set("TCPDial", tcp)
		js.Global().Set("UDPListen", udp)
		js.Global().Set("ProxyBlobSocketHostVersion", version)
	})
	return state
}

func Flush(state js.Value) {
	done := make(chan struct{})
	callback := js.FuncOf(func(js.Value, []js.Value) any { close(done); return nil })
	state.Call("flush", callback)
	<-done
	callback.Release()
}

func AssertDisposed(t *testing.T, state js.Value) {
	t.Helper()
	counts := state.Call("counts")
	if counts.Get("disposed").Int() != 1 || counts.Get("callbacks").Int() != 0 {
		t.Fatalf("cleanup: %s", js.Global().Get("JSON").Call("stringify", counts))
	}
	if got := state.Call("released").Int(); got != counts.Get("expected").Int() {
		t.Fatalf("callback release count: %d", got)
	}
}

func RealPeer(t *testing.T, name string) int {
	t.Helper()
	v := js.Global().Get("ProxyBlobTestPeers")
	if v.IsUndefined() {
		t.Skip("requires Bun loopback harness")
	}
	return v.Get(name).Int()
}
