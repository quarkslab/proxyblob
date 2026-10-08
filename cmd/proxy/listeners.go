package main

import (
	"context"
	"fmt"
	"net/url"
	"proxyblob/internal/bootstrap"
	"sync"
	"time"

	"github.com/atsika/aznet"
	"github.com/rs/zerolog/log"
)

var listeners sync.Map // active listeners: map[listenerID]*ListenerState

// StartListener creates and starts an aznet listener for the given listener config.
func StartListener(listenerID string) error {
	listenerStartMu.Lock()
	defer listenerStartMu.Unlock()
	// Check if listener already exists
	if val, ok := listeners.Load(listenerID); ok {
		state := val.(*ListenerState)
		if state.status() != stopped {
			return fmt.Errorf("listener '%s' is already running", listenerID)
		}
	}

	// Find the listener config
	var listenerConfig *ListenerConfig
	for i := range config.Listeners {
		if config.Listeners[i].Name == listenerID {
			listenerConfig = &config.Listeners[i]
			break
		}
	}

	if listenerConfig == nil {
		return fmt.Errorf("listener '%s' not found in configuration", listenerID)
	}

	// Validate the listener config
	if err := listenerConfig.Validate(); err != nil {
		return fmt.Errorf("invalid listener config: %v", err)
	}

	// Build the listen address using url.URL for proper escaping
	u, err := url.Parse(listenerConfig.Address)
	if err != nil {
		return fmt.Errorf("invalid address format: %v", err)
	}

	listenURL := &url.URL{
		Scheme: u.Scheme,
		User:   url.UserPassword(listenerConfig.StorageAccountName, listenerConfig.StorageAccountKey),
		Host:   u.Host,
		Path:   u.Path,
	}
	listenAddr := listenURL.String()

	log.Debug().Str("listener_id", listenerID).Msg("Starting aznet listener")

	// Keep aznet polling defaults; retry policy here applies only to failures.
	ctx, cancel := context.WithCancel(context.Background())
	handshake, token := bootstrap.Endpoints(listenerID)
	sessionDuration, _ := listenerConfig.sessionDuration() // validated above
	l, err := listenAzure(listenerConfig.Driver, listenAddr, aznet.WithContext(ctx), aznet.WithEndpoints(handshake, token), aznet.WithSessionDuration(sessionDuration))
	if err != nil {
		cancel()
		return fmt.Errorf("failed to start aznet listener: %v", err)
	}

	listener, ok := l.(bootstrapListener)
	if !ok {
		cancel()
		l.Close()
		return fmt.Errorf("failed to cast to aznet.Listener")
	}

	// Store listener state
	state := &ListenerState{
		Config:     listenerConfig,
		ListenAddr: listenAddr,
		StartedAt:  time.Now(),
		listener:   listener,
		cancel:     cancel,
		done:       make(chan struct{}),
		acceptDone: make(chan struct{}),
	}
	state.phase = running
	listeners.Store(listenerID, state)

	log.Info().Str("listener_id", listenerID).Str("driver", listenerConfig.Driver).Str("addr", listenerConfig.Address).Msg("Aznet listener started")

	// Start accepting agent connections for this listener
	go AcceptAgentLoopForListener(ctx, listenerID, state)

	return nil
}

// StopListener waits for the captured generation's teardown, including acceptance.
func StopListener(listenerID string) error {
	val, ok := listeners.Load(listenerID)
	if !ok {
		return fmt.Errorf("listener '%s' not found", listenerID)
	}
	return val.(*ListenerState).stop()
}

// GenerateConnectionString creates a connection string for agents using the specified listener.
func GenerateConnectionString(listenerID string, expiry time.Duration) (string, error) {
	val, ok := listeners.Load(listenerID)
	if !ok {
		return "", fmt.Errorf("listener '%s' not found", listenerID)
	}

	state := val.(*ListenerState)
	listener := state.transport()
	if !state.IsRunning() || listener == nil {
		return "", fmt.Errorf("listener '%s' is not running", listenerID)
	}

	// Bootstrap issuance does not change the listener's session policy.
	issuer, ok := listener.(interface {
		ConnectionStringFor(time.Duration) (string, error)
	})
	if !ok {
		return "", aznet.ErrBootstrapDurationUnsupported
	}
	connStr, err := issuer.ConnectionStringFor(expiry)
	if err != nil {
		return "", err
	}

	handshake, token := bootstrap.Endpoints(listenerID)
	return bootstrap.Encode(state.Config.Driver, connStr, handshake, token)
}

// ListenerInfo tracks listener metadata for display.
type ListenerInfo struct {
	ID        string    // listener ID/name
	Status    string    // "running" or "stopped"
	Protocol  string    // protocol scheme
	Address   string    // listen address
	StartedAt time.Time // when started (if running)
}

// ListListeners returns information about all configured listeners.
func ListListeners() []ListenerInfo {
	var listenerInfos []ListenerInfo

	// Get all configured listeners
	for _, listenerConfig := range config.Listeners {
		listenerID := listenerConfig.Name
		info := ListenerInfo{
			ID:       listenerID,
			Status:   "stopped",
			Protocol: "",
			Address:  "",
		}

		// Check if listener is running
		if val, ok := listeners.Load(listenerID); ok {
			state := val.(*ListenerState)
			info.Status = state.status().String()
			if state.IsRunning() {
				info.Status = "running"
				info.StartedAt = state.StartedAt
				info.Protocol = state.Config.Driver
				info.Address = state.ListenAddr
			}
		}

		// If not running, still show protocol from config
		if info.Protocol == "" {
			info.Protocol = listenerConfig.Driver
		}

		listenerInfos = append(listenerInfos, info)
	}

	return listenerInfos
}
