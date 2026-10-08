package operator

import (
	"fmt"
	"time"

	"github.com/atsika/aznet"
)

// ListenerConfig holds Azure Storage credentials for a single listener.
type ListenerConfig struct {
	SessionDuration    string `json:"session_duration,omitempty"` // Go duration for newly issued sessions; default 24h
	Name               string `json:"name"`                       // listener name/ID
	Driver             string `json:"driver"`                     // azblob, aztable, or azqueue
	Address            string `json:"address"`                    // endpoint URL (e.g. http://127.0.0.1:10001)
	StorageAccountName string `json:"storage_account"`            // account ID
	StorageAccountKey  string `json:"storage_account_key"`        // access key
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
