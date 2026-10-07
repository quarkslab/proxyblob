package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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

// configureLogging sets up zerolog with appropriate formatting and level.
func configureLogging() {
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "15:04:05",
	})

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}
