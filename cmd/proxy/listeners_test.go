package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"proxyblob/internal/bootstrap"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/atsika/aznet"
	"github.com/desertbit/grumble"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Exercise the selected aznet implementation, not a substitute AcceptError.
// These contract tests intentionally expose an obsolete published dependency.
type contractDriver struct {
	aznet.Driver
	cleanup atomic.Int32
	failure error
}

func (d *contractDriver) GetHandshakes(ctx context.Context) ([]aznet.Handshake, error) {
	if d.failure != nil {
		return nil, d.failure
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (d *contractDriver) CleanupBootstrap(context.Context) error { d.cleanup.Add(1); return nil }

func (d *contractDriver) CreateBootstrapTokensFor(time.Duration) (string, string, error) {
	return d.CreateBootstrapTokens()
}

func (d *contractDriver) CreateBootstrapTokens() (string, string, error) {
	return "handshake-sas", "token-sas", nil
}

type contractFactory struct{ driver *contractDriver }

func (f contractFactory) NewDriver(*aznet.Endpoint, *aznet.Config) (aznet.Driver, error) {
	return f.driver, nil
}

func TestAznetAcceptClassification(t *testing.T) {
	for _, status := range []int{403, 429, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			driver := &contractDriver{failure: &azcore.ResponseError{StatusCode: status}}
			aznet.RegisterFactory("contract", contractFactory{driver})
			defer aznet.UnregisterFactory("contract")
			l, err := aznet.Listen("contract", "https://account.invalid", aznet.WithAcceptPoll(time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			result := make(chan error, 1)
			go func() { _, err := l.Accept(); result <- err }()
			select {
			case err := <-result:
				var response *azcore.ResponseError
				if !errors.As(err, &response) || retryAccept(err) != (status != 403) {
					t.Fatalf("lost Accept classification: %v", err)
				}
			case <-time.After(300 * time.Millisecond):
				t.Fatal("aznet hides Accept failures; dependency update required")
			}
		})
	}
}

func TestAznetBootstrapOwnershipAndNamespaces(t *testing.T) {
	driver := &contractDriver{}
	aznet.RegisterFactory("contract", contractFactory{driver})
	defer aznet.UnregisterFactory("contract")
	oldConfig := config
	defer func() { config = oldConfig }()
	config = &Config{Listeners: []ListenerConfig{
		{Name: "account-one", Driver: "contract", Address: "https://account.invalid", StorageAccountName: "account", StorageAccountKey: "key"},
		{Name: "account-two", Driver: "contract", Address: "https://account.invalid", StorageAccountName: "account", StorageAccountKey: "key"},
	}}
	namespaces := map[string]bool{}
	for _, lc := range config.Listeners {
		if err := StartListener(lc.Name); err != nil {
			t.Fatal(err)
		}
		state := mustState(t, lc.Name)
		defer state.stop()
		defer listeners.Delete(lc.Name)
		credential, err := GenerateConnectionString(lc.Name, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := base64.RawStdEncoding.DecodeString(credential)
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(strings.SplitN(string(decoded), "|", 2)[1])
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		h, token := bootstrap.Endpoints(lc.Name)
		if q.Get(h) == "" || q.Get(token) == "" || q.Get("proxyblob-handshake") != h || q.Get("proxyblob-token") != token {
			t.Fatal("credential lost namespace")
		}
		if namespaces[h] || namespaces[token] {
			t.Fatal("shared namespace")
		}
		namespaces[h] = true
		namespaces[token] = true
	}
	StopListener("account-one")
	if !mustState(t, "account-two").IsRunning() {
		t.Fatal("second listener stopped")
	}
	StopListener("account-two")
	if driver.cleanup.Load() != 0 {
		t.Fatal("aznet Close deleted administrator-owned bootstrap; dependency update required")
	}
}

// Run against Azurite only; exercise actual SDK token issuance and the encrypted
// handshake through ProxyBlob's configuration, acceptance and display boundaries.
func TestAzuriteListenerAuthorizationLifetime(t *testing.T) {
	if os.Getenv("AZNET_AZURITE") != "1" {
		t.Skip("set AZNET_AZURITE=1 with Azurite on localhost:10000-10002")
	}
	const key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	previous := config
	defer func() { config = previous }()
	for i, driver := range []string{"azblob", "azqueue", "aztable"} {
		for _, policy := range []struct {
			setting  string
			duration time.Duration
		}{{"", 24 * time.Hour}, {"2h", 2 * time.Hour}} {
			t.Run(driver+"/"+policy.setting, func(t *testing.T) {
				id := uuid.NewString()
				config = &Config{Listeners: []ListenerConfig{{Name: id, Driver: driver, Address: fmt.Sprintf("http://127.0.0.1:%d/devstoreaccount1", 10000+i), StorageAccountName: "devstoreaccount1", StorageAccountKey: key, SessionDuration: policy.setting}}}
				if err := StartListener(id); err != nil {
					t.Fatal(err)
				}
				state := mustState(t, id)
				defer func() {
					if err := StopListener(id); err != nil {
						t.Error(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := state.listener.(*aznet.Listener).CleanupBootstrap(ctx); err != nil {
						t.Error(err)
					}
					listeners.Delete(id)
				}()
				var firstExpiry time.Time
				for _, bootstrapExpiry := range []time.Duration{time.Hour, 7 * 24 * time.Hour} {
					before := time.Now().Truncate(time.Second)
					credential, err := GenerateConnectionString(id, bootstrapExpiry)
					if err != nil {
						t.Fatal(err)
					}
					decoded, err := base64.RawStdEncoding.DecodeString(credential)
					if err != nil {
						t.Fatal("decode bootstrap")
					}
					address := strings.SplitN(string(decoded), "|", 2)[1]
					u, err := url.Parse(address)
					if err != nil {
						t.Fatal("parse bootstrap")
					}
					handshake, token := bootstrap.Endpoints(id)
					// Inspect only signed expiry; never print credentials or signatures.
					for _, name := range []string{handshake, token} {
						raw, err := base64.URLEncoding.DecodeString(u.Query().Get(name))
						if err != nil {
							t.Fatal("decode token")
						}
						values, err := url.ParseQuery(string(raw))
						if err != nil {
							t.Fatal("parse token")
						}
						expires, err := time.Parse(time.RFC3339, values.Get("se"))
						if err != nil {
							t.Fatal("parse expiry")
						}
						if expires.Before(before.Add(bootstrapExpiry)) || expires.After(time.Now().Add(bootstrapExpiry)) {
							t.Fatal("bootstrap duration not honored")
						}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					conn, err := aznet.Dial(driver, address, aznet.WithContext(ctx), aznet.WithEndpoints(handshake, token), aznet.WithPing(0))
					if err != nil {
						t.Fatal(err)
					}
					defer conn.Close()
					if _, err := conn.Write([]byte{0, 1, 'x'}); err != nil {
						t.Fatal(err)
					}
					expiry, known := aznet.GetSessionExpiry(conn)
					if !known || expiry.Before(before.Add(policy.duration)) || expiry.After(time.Now().Add(policy.duration)) {
						t.Fatal("session duration not honored")
					}
					eventually(t, func() bool {
						for _, a := range ListAgents() {
							if a.ListenerID == id && a.SessionExpiry.Equal(expiry) {
								return true
							}
						}
						return false
					})
					if !strings.Contains(RenderAgentTable(ListAgents(), expiry.Add(-30*time.Minute)), "30m0s") {
						t.Fatal("session metadata missing from display")
					}
					if firstExpiry.IsZero() {
						firstExpiry = expiry
					} else {
						foundFirst := false
						for _, a := range ListAgents() {
							if a.ListenerID == id && a.SessionExpiry.Equal(firstExpiry) {
								foundFirst = true
							}
						}
						if !foundFirst {
							t.Fatal("bootstrap issuance changed or removed the existing session")
						}
					}
				}
				// Stop the server while peers remain connected; deferred client closes follow.
				if err := StopListener(id); err != nil {
					t.Fatal(err)
				}
			})
		}
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
