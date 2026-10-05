package main

import (
	"github.com/atsika/aznet"
	"github.com/desertbit/grumble"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net"
	"os"
	"path/filepath"
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

func TestBootstrapDurationReachesIssuer(t *testing.T) {
	driver := &durationDriver{}
	aznet.RegisterFactory("duration", durationFactory{driver})
	defer aznet.UnregisterFactory("duration")
	old := config
	defer func() { config = old }()
	config = &Config{Listeners: []ListenerConfig{{Name: "duration", Driver: "duration", Address: "https://account.invalid", StorageAccountName: "account", StorageAccountKey: "key"}}}
	if err := StartListener("duration"); err != nil {
		t.Fatal(err)
	}
	defer func() { StopListener("duration"); listeners.Delete("duration") }()
	for _, duration := range []time.Duration{time.Hour, 7 * 24 * time.Hour} {
		if _, err := GenerateConnectionString("duration", duration); err != nil {
			t.Fatal(err)
		}
		if driver.duration != duration {
			t.Fatalf("bootstrap duration got %v want %v", driver.duration, duration)
		}
	}
	for _, duration := range []time.Duration{0, -time.Hour, time.Millisecond} {
		if _, err := GenerateConnectionString("duration", duration); err == nil {
			t.Fatal("invalid duration accepted")
		}
	}
	cli := grumble.New(&grumble.Config{Name: "duration-test"})
	AddCommands(cli)
	// Suppress generated bootstrap credentials in test output.
	previousLog := log.Logger
	log.Logger = zerolog.Nop()
	defer func() { log.Logger = previousLog }()
	for _, tc := range []struct {
		args []string
		want time.Duration
	}{
		{[]string{"new", "--listener", "duration"}, 7 * 24 * time.Hour},
		{[]string{"new", "--listener", "duration", "--duration", "3h"}, 3 * time.Hour},
	} {
		if err := cli.RunCommand(tc.args); err != nil {
			t.Fatal(err)
		}
		if driver.duration != tc.want {
			t.Fatalf("CLI bootstrap duration %v want %v", driver.duration, tc.want)
		}
	}
}

type durationDriver struct {
	contractDriver
	duration time.Duration
}

func (d *durationDriver) CreateBootstrapTokensFor(duration time.Duration) (string, string, error) {
	d.duration = duration
	return "handshake", "token", nil
}

type durationFactory struct{ driver *durationDriver }

func (f durationFactory) NewDriver(*aznet.Endpoint, *aznet.Config) (aznet.Driver, error) {
	return f.driver, nil
}

func TestListenerSessionDurationConfig(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{``, true}, {`,"session_duration":"2h"`, true},
		{`,"session_duration":"0s"`, false}, {`,"session_duration":"-1h"`, false},
		{`,"session_duration":"1ms"`, false}, {`,"session_duration":"nonsense"`, false},
		{`,"session_duration":7200`, false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			data := `{"listeners":[{"name":"duration","driver":"azblob","address":"https://account.invalid","storage_account":"account","storage_account_key":"key"` + tc.value + `}]}`
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfig(path)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
}
