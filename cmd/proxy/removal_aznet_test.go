package main

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/atsika/aznet"
	proxy "proxyblob/pkg/proxy/server"
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
func (*removalTransport) Close() error         { return nil }
func (*removalTransport) LocalAddr() net.Addr  { return nil }
func (*removalTransport) RemoteAddr() net.Addr { return nil }
func (*removalTransport) MaxRawSize() int      { return 1024 * 1024 }
func TestRemoveAgentAznetSendsFin(t *testing.T) {
	d := &removalDriver{}
	aznet.RegisterFactory("removaltest", removalFactory{d})
	defer aznet.UnregisterFactory("removaltest")
	conn, err := aznet.Dial("removaltest", "http://example.invalid", aznet.WithPing(0))
	if err != nil {
		t.Fatal(err)
	}
	agent := &AgentConnection{ID: "removal-fin", Conn: conn, server: proxy.NewProxyServer(context.Background(), conn)}
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
