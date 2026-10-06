package main

import (
	"errors"
	"io"
	"os"
	"proxyblob/pkg/protocol"
	"testing"
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
