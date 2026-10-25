package agent

import (
	"net"

	"proxyblob/internal/agent/netenv"
	"proxyblob/internal/diag"
	"proxyblob/internal/mux"
	"proxyblob/internal/relay"
)

// connect dials the requested TCP target, replies with the local address the
// target sees, and forwards the stream to it.
func (a *Agent) connect(stream *mux.ProtocolConn, addr []byte) {
	target, code := relay.ParseAddress(addr)
	if code != diag.ErrNone {
		a.fail(stream, code)
		return
	}
	setupCtx, cancelSetup := socketSetupContext(a.Ctx, stream.Done())
	// A client that has gone away (EOF before the reply) must not keep the dial,
	// and its stream slot, alive until the dial times out.
	go func() {
		select {
		case <-stream.ReceiveDone():
			cancelSetup()
		case <-setupCtx.Done():
		}
	}()
	// Through the machine's egress proxy when it has one; the JS host on WASM.
	targetConn, err := netenv.DialTCPContext(setupCtx, target)
	cancelSetup()
	if err != nil {
		a.fail(stream, diag.MapNetError(err))
		return
	}
	// Disable Nagle's algorithm for better TLS performance.
	if tcpConn, ok := targetConn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}
	if !stream.AttachDestination(targetConn) {
		return
	}
	if relay.WriteReply(stream, diag.ErrNone, targetConn.LocalAddr()) != nil {
		return
	}
	a.forward(stream, targetConn)
}
