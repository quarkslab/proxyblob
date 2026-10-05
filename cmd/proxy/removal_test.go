package main

import (
	"errors"
	"net"
	"os"
	"testing"

	"github.com/desertbit/grumble"
)

type failedCloseConn struct{ net.Conn }

func (c failedCloseConn) Close() error { c.Conn.Close(); return os.ErrDeadlineExceeded }

func TestRemoveAgentClearsSelectionOnCleanupFailure(t *testing.T) {
	c, peer := net.Pipe()
	defer peer.Close()
	const id = "removal-cleanup-failure"
	agent := &AgentConnection{ID: id, Conn: failedCloseConn{c}}
	connectedAgents.Store(id, agent)
	defer connectedAgents.Delete(id)
	old := selectedAgent
	selectedAgent = id
	defer func() { selectedAgent = old }()
	app := grumble.New(&grumble.Config{Name: "removal-test"})
	AddCommands(app)
	if err := app.RunCommand([]string{"agent", "rm", id}); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("lost cleanup failure: %v", err)
	}
	if _, ok := connectedAgents.Load(id); ok {
		t.Fatal("removed agent still listed")
	}
	if selectedAgent != "" {
		t.Fatalf("removed agent remains selected: %s", selectedAgent)
	}
}
