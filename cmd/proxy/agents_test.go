package main

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"proxyblob/internal/operator"

	"github.com/atsika/aznet"
	"github.com/desertbit/grumble"
)

// oneConnListener hands out a single connection, then blocks until closed.
type oneConnListener struct {
	conn   chan net.Conn
	closed chan struct{}
}

func (l *oneConnListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conn:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *oneConnListener) Close() error                    { close(l.closed); return nil }
func (*oneConnListener) Addr() net.Addr                    { return nil }
func (*oneConnListener) ConnectionString() (string, error) { return "test", nil }

type failedCloseConn struct{ net.Conn }

func (c failedCloseConn) Close() error { c.Conn.Close(); return os.ErrDeadlineExceeded }

func TestRemoveAgentClearsSelectionOnCleanupFailure(t *testing.T) {
	c, peer := net.Pipe()
	defer peer.Close()
	l := &oneConnListener{conn: make(chan net.Conn, 1), closed: make(chan struct{})}
	l.conn <- failedCloseConn{c}
	oldOp := op
	op = operator.New([]operator.ListenerConfig{{Name: "removal", Driver: "fake", Address: "https://example.invalid", StorageAccountName: "account", StorageAccountKey: "key"}},
		operator.WithListen(func(string, string, ...aznet.Option) (net.Listener, error) { return l, nil }))
	defer func() { op = oldOp }()
	if err := op.StartListener("removal"); err != nil {
		t.Fatal(err)
	}
	defer op.StopListener("removal")
	if _, err := peer.Write([]byte{0, 1, 'x'}); err != nil {
		t.Fatal(err)
	}
	var id string
	for deadline := time.Now().Add(3 * time.Second); id == "" && time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if ids := op.AgentIDs(); len(ids) == 1 {
			id = ids[0]
		}
	}
	if id == "" {
		t.Fatal("agent did not connect")
	}
	old := selectedAgent
	selectedAgent = id
	defer func() { selectedAgent = old }()
	app := grumble.New(&grumble.Config{Name: "removal-test"})
	AddCommands(app)
	if err := app.RunCommand([]string{"agent", "rm", id}); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("lost cleanup failure: %v", err)
	}
	if op.HasAgent(id) {
		t.Fatal("removed agent still listed")
	}
	if selectedAgent != "" {
		t.Fatalf("removed agent remains selected: %s", selectedAgent)
	}
}
