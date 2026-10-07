package main

import (
	"net"
	"strings"
	"testing"
	"time"
)

type expiryConn struct {
	net.Conn
	expiry time.Time
	known  bool
}

func (c expiryConn) SessionExpiry() (time.Time, bool) { return c.expiry, c.known }

func TestAgentAuthorizationDisplay(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	l := newFakeListener()
	testGeneration(t, "expiry-display", l)
	for _, tc := range []struct {
		name   string
		expiry time.Time
		known  bool
	}{
		{"first", now.Add(time.Hour), true},
		{"later", now.Add(2 * time.Hour), true},
		{"expired", now.Add(-time.Second), true},
		{"unknown", now.Add(24 * time.Hour), false},
	} {
		c, peer := net.Pipe()
		t.Cleanup(func() { peer.Close() })
		l.results <- acceptResult{conn: expiryConn{c, tc.expiry, tc.known}}
		identity := append([]byte{0, byte(len(tc.name))}, []byte(tc.name)...)
		if _, err := peer.Write(identity); err != nil {
			t.Fatal(err)
		}
	}
	connectAgent(t, l) // a connection without the optional expiry capability
	eventually(t, func() bool { return len(ListAgents()) == 5 })
	agents := ListAgents()
	for _, a := range agents {
		rendered := RenderAgentTable([]AgentInfo{a}, now)
		want := map[string]string{"first": "1h0m0s", "later": "2h0m0s", "expired": "expired", "unknown": "unknown", "x": "unknown"}[a.Info]
		if !strings.Contains(rendered, want) {
			t.Fatalf("%s missing %s: %s", a.Info, want, rendered)
		}
		if a.Info == "first" {
			if !strings.Contains(rendered, "2026-10-05T13:00:00Z") {
				t.Fatal("missing exact expiry")
			}
			updated := RenderAgentTable([]AgentInfo{a}, now.Add(30*time.Minute))
			if !strings.Contains(updated, "30m0s") {
				t.Fatal("remaining time did not update")
			}
			if !strings.Contains(RenderAgentTable([]AgentInfo{a}, now.Add(time.Hour-time.Millisecond)), "<1s") {
				t.Fatal("positive lifetime rounded to expired")
			}
			if !strings.Contains(RenderAgentTable([]AgentInfo{a}, now.Add(time.Hour)), "expired") {
				t.Fatal("exact expiry boundary")
			}
		}
		if a.Info == "unknown" && strings.Contains(rendered, "2026-10-06T12:00:00Z") {
			t.Fatal("unknown expiry inferred")
		}
	}
	if len(ListAgents()) != 5 {
		t.Fatal("display removed agents")
	}
}
