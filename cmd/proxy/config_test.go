package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestListenerSessionDurationConfig(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{``, true}, {`,"session_duration":"2h"`, true},
		{`,"session_duration":"0s"`, false}, {`,"session_duration":"-1h"`, false},
		{`,"session_duration":"1ms"`, false}, {`,"session_duration":"nonsense"`, false},
		{`,"session_duration":7200`, false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			data := `{"listeners":[{"name":"duration","driver":"azblob","address":"https://account.invalid","storage_account":"account","storage_account_key":"key"` + tc.value + `}]}`
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfig(path)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
}
