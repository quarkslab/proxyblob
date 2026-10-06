package main

import (
	"github.com/rs/zerolog"
	"testing"
)

func TestResolveLogLevel(t *testing.T) {
	for _, tc := range []struct {
		config, flag string
		want         zerolog.Level
		bad          bool
	}{
		{"", "", zerolog.InfoLevel, false},
		{"debug", "", zerolog.DebugLevel, false},
		{"warn", "debug", zerolog.DebugLevel, false},
		{"trace", "error", zerolog.ErrorLevel, false},
		{"WARN", "", zerolog.WarnLevel, false},
		{"quiet", "", zerolog.NoLevel, true},
		{"info", "verbose", zerolog.NoLevel, true},
	} {
		got, err := resolveLogLevel(tc.config, tc.flag)
		if (err != nil) != tc.bad || got != tc.want {
			t.Fatalf("%q/%q: %v %v", tc.config, tc.flag, got, err)
		}
	}
}
