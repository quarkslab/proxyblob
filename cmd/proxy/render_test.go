package main

import (
	"proxyblob/internal/operator"
	"strings"
	"testing"
	"time"
)

func TestAgentAuthorizationDisplay(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	// The operator reports the zero time when aznet does not know the expiry.
	var agents []operator.AgentInfo
	for name, expiry := range map[string]time.Time{
		"first": now.Add(time.Hour), "later": now.Add(2 * time.Hour),
		"expired": now.Add(-time.Second), "unknown": {},
	} {
		agents = append(agents, operator.AgentInfo{AgentConnection: &operator.AgentConnection{Info: name}, SessionExpiry: expiry})
	}
	for _, a := range agents {
		rendered := RenderAgentTable([]operator.AgentInfo{a}, now)
		want := map[string]string{"first": "1h0m0s", "later": "2h0m0s", "expired": "expired", "unknown": "unknown"}[a.Info]
		if !strings.Contains(rendered, want) {
			t.Fatalf("%s missing %s: %s", a.Info, want, rendered)
		}
		if a.Info == "first" {
			if !strings.Contains(rendered, "2026-10-05T13:00:00Z") {
				t.Fatal("missing exact expiry")
			}
			updated := RenderAgentTable([]operator.AgentInfo{a}, now.Add(30*time.Minute))
			if !strings.Contains(updated, "30m0s") {
				t.Fatal("remaining time did not update")
			}
			if !strings.Contains(RenderAgentTable([]operator.AgentInfo{a}, now.Add(time.Hour-time.Millisecond)), "<1s") {
				t.Fatal("positive lifetime rounded to expired")
			}
			if !strings.Contains(RenderAgentTable([]operator.AgentInfo{a}, now.Add(time.Hour)), "expired") {
				t.Fatal("exact expiry boundary")
			}
		}
		if a.Info == "unknown" && strings.Contains(rendered, "2026-10-06T12:00:00Z") {
			t.Fatal("unknown expiry inferred")
		}
	}
}
