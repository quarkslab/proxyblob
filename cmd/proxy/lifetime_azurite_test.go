package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/atsika/aznet"
	"github.com/google/uuid"
)

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
				for _, bootstrap := range []time.Duration{time.Hour, 7 * 24 * time.Hour} {
					before := time.Now().Truncate(time.Second)
					credential, err := GenerateConnectionString(id, bootstrap)
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
					handshake, token := bootstrapEndpoints(id)
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
						if expires.Before(before.Add(bootstrap)) || expires.After(time.Now().Add(bootstrap)) {
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
