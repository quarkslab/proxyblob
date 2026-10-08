package bootstrap

import (
	"errors"
	"net/url"
	"proxyblob/pkg/protocol"
	"testing"

	"github.com/atsika/aznet"
)

func TestIncompleteNamespaceHasNumericError(t *testing.T) {
	_, err := DialOptions("https://example.invalid/?proxyblob-handshake=h&h=private")
	if !errors.Is(err, protocol.ErrBootstrapNamespace) || err.Error() != "55" {
		t.Fatalf("unexpected namespace error %v", err)
	}
}

func TestConnectionNamespace(t *testing.T) {
	opts, err := DialOptions("https://account.invalid/?proxyblob-handshake=pbh123&proxyblob-token=pbt123&pbh123=a&pbt123=b")
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
	if _, err := DialOptions("https://account.invalid/?proxyblob-handshake=only"); err == nil {
		t.Fatal("partial namespace accepted")
	}
	if opts, err := DialOptions("https://account.invalid/?handshake=a&token=b"); err != nil || len(opts) != 0 {
		t.Fatal("legacy defaults lost")
	}
}
