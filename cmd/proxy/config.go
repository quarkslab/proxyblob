package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/atsika/aznet"
)

var config *Config // app config

// ListenerConfig holds Azure Storage credentials for a single listener.
type ListenerConfig struct {
	SessionDuration    string `json:"session_duration,omitempty"` // Go duration for newly issued sessions; default 24h
	Name               string `json:"name"`                       // listener name/ID
	Driver             string `json:"driver"`                     // azblob, aztable, or azqueue
	Address            string `json:"address"`                    // endpoint URL (e.g. http://127.0.0.1:10001)
	StorageAccountName string `json:"storage_account"`            // account ID
	StorageAccountKey  string `json:"storage_account_key"`        // access key
}

// Config holds multiple listener configurations.
type Config struct {
	LogLevel  string           `json:"log_level,omitempty"`
	Listeners []ListenerConfig `json:"listeners"` // array of listener configurations
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

// Validate checks required listener config fields.
func (lc *ListenerConfig) Validate() error {
	if lc.Name == "" {
		return fmt.Errorf("name is required")
	}
	if lc.Driver == "" {
		return fmt.Errorf("driver is required (azblob, aztable, or azqueue)")
	}
	if lc.Address == "" {
		return fmt.Errorf("address is required")
	}
	if lc.StorageAccountName == "" {
		return fmt.Errorf("storage_account is required")
	}
	if lc.StorageAccountKey == "" {
		return fmt.Errorf("storage_account_key is required")
	}
	_, err := lc.sessionDuration()
	return err
}

// sessionDuration keeps bootstrap validity independent from session policy.
func (lc *ListenerConfig) sessionDuration() (time.Duration, error) {
	if lc.SessionDuration == "" {
		return aznet.DefaultSASExpiry, nil
	}
	d, err := time.ParseDuration(lc.SessionDuration)
	if err != nil || d < time.Second {
		return 0, fmt.Errorf("session_duration must be a duration of at least 1s (for example 24h)")
	}
	return d, nil
}
