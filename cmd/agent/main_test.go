package main

import (
	"errors"
	"io"
	"os"
	"proxyblob/internal/diag"
	"testing"
)

func TestAgentDiagnosticOutputIsNumeric(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stderr
	os.Stderr = w
	reportError(diag.ErrorCode(errors.New("sdk URL?sig=secret")))
	os.Stderr = saved
	w.Close()
	output, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(output) != "22\n" {
		t.Fatalf("unexpected agent diagnostic %q %v", output, err)
	}
}
