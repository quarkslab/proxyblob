package main

import (
	"errors"
	"io"
	"net/url"
	"os"
	"proxyblob/pkg/protocol"
	"testing"

	"github.com/atsika/aznet"
)

func TestAgentDiagnosticOutputIsNumeric(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stderr
	os.Stderr = w
	reportError(protocol.ErrorCode(errors.New("sdk URL?sig=secret")))
	os.Stderr = saved
	w.Close()
	output, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(output) != "22\n" {
		t.Fatalf("unexpected agent diagnostic %q %v", output, err)
	}
}

func TestIncompleteNamespaceHasNumericError(t *testing.T) {
	_, err := connectionOptions("https://example.invalid/?proxyblob-handshake=h&h=private")
	if !errors.Is(err, protocol.ErrBootstrapNamespace) || err.Error() != "55" {
		t.Fatalf("unexpected namespace error %v", err)
	}
}

func TestConnectionNamespace(t *testing.T) {
	opts, err := connectionOptions("https://account.invalid/?proxyblob-handshake=pbh123&proxyblob-token=pbt123&pbh123=a&pbt123=b")
	if err != nil || len(opts) != 1 {
		t.Fatalf("namespace options: %v", err)
	}
	cfg := new(aznet.Config)
	for _, opt := range opts {
		opt(cfg)
	}
	u, _ := url.Parse("https://account.invalid")
	encoded := aznet.NewEndpoint(u).BuildConnURL(cfg, "a", "b")
	parsed, _ := url.Parse(encoded)
	if parsed.Query().Get("pbh123") == "" || parsed.Query().Get("pbt123") == "" {
		t.Fatal("Dial namespace not restored")
	}
	if _, err := connectionOptions("https://account.invalid/?proxyblob-handshake=only"); err == nil {
		t.Fatal("partial namespace accepted")
	}
	if opts, err := connectionOptions("https://account.invalid/?handshake=a&token=b"); err != nil || len(opts) != 0 {
		t.Fatal("legacy defaults lost")
	}
}
