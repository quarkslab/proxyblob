package main

import (
	"context"
	"testing"
	"time"

	"proxyblob/internal/operator"

	"github.com/atsika/aznet"
	"github.com/desertbit/grumble"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// durationDriver records the bootstrap validity requested through the CLI.
type durationDriver struct {
	aznet.Driver
	duration time.Duration
}

func (d *durationDriver) GetHandshakes(ctx context.Context) ([]aznet.Handshake, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (d *durationDriver) CleanupBootstrap(context.Context) error { return nil }
func (d *durationDriver) CreateBootstrapTokens() (string, string, error) {
	return "handshake", "token", nil
}
func (d *durationDriver) CreateBootstrapTokensFor(duration time.Duration) (string, string, error) {
	d.duration = duration
	return "handshake", "token", nil
}

type durationFactory struct{ driver *durationDriver }

func (f durationFactory) NewDriver(*aznet.Endpoint, *aznet.Config) (aznet.Driver, error) {
	return f.driver, nil
}

func TestNewCommandBootstrapDuration(t *testing.T) {
	driver := &durationDriver{}
	aznet.RegisterFactory("cliduration", durationFactory{driver})
	defer aznet.UnregisterFactory("cliduration")
	oldOp := op
	op = operator.New([]operator.ListenerConfig{{Name: "duration", Driver: "cliduration", Address: "https://account.invalid", StorageAccountName: "account", StorageAccountKey: "key"}})
	defer func() { op = oldOp }()
	if err := op.StartListener("duration"); err != nil {
		t.Fatal(err)
	}
	defer op.StopListener("duration")
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
