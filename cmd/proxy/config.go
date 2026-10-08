package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"proxyblob/internal/operator"
)

var config *Config // app config

// Config holds multiple listener configurations.
type Config struct {
	LogLevel  string                    `json:"log_level,omitempty"`
	Listeners []operator.ListenerConfig `json:"listeners"` // array of listener configurations
}

// LoadConfig reads and parses config file.
func LoadConfig(configPath string) (*Config, error) {
	// Use default config path (./config.json) if none provided
	if configPath == "" {
		configPath = "./config.json"
	}

	// Get absolute path for clearer error messages
	absPath, err := filepath.Abs(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve config path: %v", err)
	}

	// Check if config file exists
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("configuration file not found at %s", absPath)
	}

	// Read and parse the configuration file
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %v", absPath, err)
	}

	cfg := new(Config)
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %v", absPath, err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks required config fields.
func (c *Config) Validate() error {
	if _, err := resolveLogLevel(c.LogLevel, ""); err != nil {
		return err
	}
	if len(c.Listeners) == 0 {
		return fmt.Errorf("at least one listener configuration is required")
	}

	// Track listener names to ensure uniqueness
	listenerNames := make(map[string]bool)

	for i, listener := range c.Listeners {
		if err := listener.Validate(); err != nil {
			return fmt.Errorf("listener[%d]: %v", i, err)
		}

		// Check for duplicate names
		if listenerNames[listener.Name] {
			return fmt.Errorf("listener[%d]: duplicate listener name '%s'", i, listener.Name)
		}
		listenerNames[listener.Name] = true
	}

	return nil
}
