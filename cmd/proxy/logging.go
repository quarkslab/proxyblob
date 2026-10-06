package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"strings"
)

// resolveLogLevel applies startup override > config > info.
func resolveLogLevel(configured, override string) (zerolog.Level, error) {
	value := configured
	if override != "" {
		value = override
	}
	if value == "" {
		value = "info"
	}
	switch strings.ToLower(value) {
	case "trace":
		return zerolog.TraceLevel, nil
	case "debug":
		return zerolog.DebugLevel, nil
	case "info":
		return zerolog.InfoLevel, nil
	case "warn":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	default:
		return zerolog.NoLevel, fmt.Errorf("log_level must be trace, debug, info, warn, or error")
	}
}
