package operator

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"proxyblob/internal/proxy"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atsika/aznet"
)

// Keep the selected aznet's real handshake, framing, deadlines and Close path;
// replace only the storage driver so no Azure credentials are needed.
type removalDriver struct {
	aznet.Driver
	noise *aznet.Noise
	token []byte
	fin   atomic.Bool
}

type removalFactory struct{ driver *removalDriver }

func (f removalFactory) NewDriver(*aznet.Endpoint, *aznet.Config) (aznet.Driver, error) {
	return f.driver, nil
}

func (d *removalDriver) PostHandshake(_ context.Context, _ string, msg []byte) error {
	var err error
	d.noise, err = aznet.NewNoiseServer()
	if err != nil {
		return err
	}
	if _, err = d.noise.ReadMessage(msg); err != nil {
		return err
	}
	d.token, err = d.noise.WriteMessage([]byte(`{"req":"r","res":"s"}`))
	return err
}

func (d *removalDriver) GetToken(context.Context, string) ([]byte, error) { return d.token, nil }

func (d *removalDriver) NewTransport(context.Context, string, aznet.SessionTokens, bool) (aznet.Transport, error) {
	return &removalTransport{driver: d}, nil
}

type removalTransport struct{ driver *removalDriver }

func (tr *removalTransport) WriteRaw(ctx context.Context, _ uint64, r io.ReadSeeker) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	plain, _, err := tr.driver.noise.UnsealData(nil, raw, 1024*1024)
	if err != nil {
		return err
	}
	for len(plain) >= 5 {
		if plain[4] == aznet.MsgTypeFin {
			tr.driver.fin.Store(true)
		}
		plain = plain[5+int(binary.BigEndian.Uint32(plain)):]
	}
	return nil
}

func (*removalTransport) ReadRaw(ctx context.Context) (io.ReadCloser, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*removalTransport) Close() error { return nil }

func (*removalTransport) LocalAddr() net.Addr { return nil }

func (*removalTransport) RemoteAddr() net.Addr { return nil }

func (*removalTransport) MaxRawSize() int { return 1024 * 1024 }

func TestRemoveAgentAznetSendsFin(t *testing.T) {
	d := &removalDriver{}
	aznet.RegisterFactory("removaltest", removalFactory{d})
	defer aznet.UnregisterFactory("removaltest")
	conn, err := aznet.Dial("removaltest", "http://example.invalid", aznet.WithPing(0))
	if err != nil {
		t.Fatal(err)
	}
	agent := &AgentConnection{ID: "removal-fin", Conn: conn, op: New(nil), server: proxy.NewProxyServer(context.Background(), conn)}
	agent.server.StartReceiving()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := agent.close(); err != nil {
				t.Errorf("agent removal: %v", err)
			}
		})
	}
	wg.Wait()
	if !d.fin.Load() {
		t.Error("remote transport received no FIN")
	}
}

type expiryConn struct {
	net.Conn
	expiry time.Time
	known  bool
}

func (c expiryConn) SessionExpiry() (time.Time, bool) { return c.expiry, c.known }

// Agents reports aznet's session expiry when the connection knows it, and the
// zero time otherwise; it never estimates one.
func TestAgentsReportSessionExpiry(t *testing.T) {
	o := New(nil)
	l := newFakeListener()
	testGeneration(t, o, "expiry", l)
	known := time.Date(2026, 10, 5, 13, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		known bool
	}{{"known", true}, {"unknown", false}} {
		c, peer := net.Pipe()
		t.Cleanup(func() { peer.Close() })
		l.results <- acceptResult{conn: expiryConn{c, known, tc.known}}
		if _, err := peer.Write(append([]byte{0, byte(len(tc.name))}, tc.name...)); err != nil {
			t.Fatal(err)
		}
	}
	connectAgent(t, o, l) // a connection without the optional expiry capability
	eventually(t, func() bool { return len(o.Agents()) == 3 })
	for _, a := range o.Agents() {
		if want := map[string]time.Time{"known": known}[a.Info]; !a.SessionExpiry.Equal(want) {
			t.Fatalf("%s: expiry %v want %v", a.Info, a.SessionExpiry, want)
		}
	}
}
