// Package agent runs on the remote side of the tunnel. It accepts the proxy's
// streams and carries out each relay request: CONNECT dials a TCP target,
// BIND (native only) waits for one inbound peer, and UDP relays datagrams.
// SOCKS itself is negotiated by the proxy; the agent never parses it.
package agent

import (
	"context"
	"net"

	"proxyblob/internal/diag"
	"proxyblob/internal/mux"
	"proxyblob/internal/relay"
)

// Agent serves relay requests on one tunnel session. It is safe for
// concurrent use.
type Agent struct {
	*mux.Session
	udpDestinations int
}

// New creates an agent with the default flow configuration.
func New(ctx context.Context, conn net.Conn) *Agent {
	a, err := NewWithConfig(ctx, conn, mux.DefaultFlowConfig())
	if err != nil {
		panic(err)
	}
	return a
}

// NewWithConfig creates an agent over conn.
func NewWithConfig(ctx context.Context, conn net.Conn, cfg mux.FlowConfig) (*Agent, error) {
	session, err := mux.NewSession(ctx, conn, cfg, mux.WithAccept())
	if err != nil {
		return nil, err
	}
	return &Agent{Session: session, udpDestinations: cfg.UDPDestinations}, nil
}

// Start begins serving the proxy's streams.
func (a *Agent) Start() {
	a.StartReceiving()
	go func() {
		for {
			stream, err := a.Accept(a.Ctx)
			if err != nil {
				return
			}
			go a.serve(stream)
		}
	}()
}

// serve carries out one relay request.
func (a *Agent) serve(stream *mux.ProtocolConn) {
	cmd, addr, err := relay.ReadRequest(stream)
	if err != nil {
		if code := diag.ErrorCode(err); code == diag.ErrAddressNotSupported {
			a.fail(stream, code)
			return
		}
		stream.CloseWithCode(diag.ErrNone)
		return
	}
	switch cmd {
	case relay.Connect:
		a.connect(stream, addr)
	case relay.Bind:
		a.bind(stream, addr)
	case relay.UDP:
		a.udp(stream)
	default:
		a.fail(stream, diag.ErrUnsupportedCommand)
	}
}

// fail replies with code, then closes gracefully so the proxy reads the reply
// before EOF; a nonzero tunnel close would discard it.
func (a *Agent) fail(stream *mux.ProtocolConn, code byte) {
	relay.WriteReply(stream, code, nil)
	stream.CloseWithCode(diag.ErrNone)
}

// forward pipes stream and target until both directions end, then closes the
// stream with the forwarding outcome.
func (a *Agent) forward(stream *mux.ProtocolConn, target net.Conn) {
	if err := mux.Forward(target, stream); err != nil {
		stream.CloseWithCode(diag.StreamErrorCode(err))
		return
	}
	stream.CloseWithCode(diag.ErrNone)
}

// A setup operation belongs to both its agent and its stream. Stop its watcher
// when setup returns, even if the stream remains open afterwards.
func socketSetupContext(parent context.Context, closed <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	select {
	case <-closed:
		cancel()
	default:
	}
	go func() {
		select {
		case <-closed:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
