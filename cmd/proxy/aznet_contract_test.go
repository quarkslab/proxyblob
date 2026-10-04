package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/atsika/aznet"
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
		h, token := bootstrapEndpoints(lc.Name)
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
