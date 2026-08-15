// Package main implements the SOCKS proxy server using aznet.
package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/desertbit/grumble"
	"github.com/google/uuid"
	"github.com/jedib0t/go-pretty/table"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	proxy "proxyblob/pkg/proxy/server"

	"github.com/atsika/aznet"
)

// CLI banner with version.
const banner = `
     ____                      ____  _       _     
    |  _ \ _ __ _____  ___   _| __ )| | ___ | |__  
    | |_) | '__/ _ \ \/ / | | |  _ \| |/ _ \| '_ \ 
    |  __/| | | (_) >  <| |_| | |_) | | (_) | |_) |
    |_|   |_|  \___/_/\_\\__, |____/|_|\___/|_.__/ 
                         |___/                     

   SOCKS Proxy over More Azure Storage (v2.2 - wasm)
   -------------------------------------------------

`

// ListenerConfig holds Azure Storage credentials for a single listener.
type ListenerConfig struct {
	Name               string `json:"name"`                // listener name/ID
	Driver             string `json:"driver"`              // azblob, aztable, or azqueue
	Address            string `json:"address"`             // endpoint URL (e.g. http://127.0.0.1:10001)
	StorageAccountName string `json:"storage_account"`     // account ID
	StorageAccountKey  string `json:"storage_account_key"` // access key
}

// Config holds multiple listener configurations.
type Config struct {
	Listeners []ListenerConfig `json:"listeners"` // array of listener configurations
}

// ListenerState tracks the state of a listener.
//
// Two goroutines can decide to shut a listener down: the CLI, via StopListener,
// and the accept loop, when Accept has failed too many times in a row. running
// is the gate that makes exactly one of them the owner of the teardown.
type ListenerState struct {
	Config     *ListenerConfig // listener configuration
	ListenAddr string          // address where listener is bound
	StartedAt  time.Time       // when listener was started

	running atomic.Bool // whether listener is running

	mu       sync.Mutex         // guards listener and cancel
	listener *aznet.Listener    // aznet listener, nil once torn down
	cancel   context.CancelFunc // cancels the accept loop context, nil once torn down
}

// IsRunning reports whether the listener is accepting connections.
func (s *ListenerState) IsRunning() bool {
	return s.running.Load()
}

// claimStop returns true for the single caller that wins the right to tear this
// listener down; every later caller gets false and must not touch it.
func (s *ListenerState) claimStop() bool {
	return s.running.CompareAndSwap(true, false)
}

// transport returns the live listener, or nil once it has been torn down.
func (s *ListenerState) transport() *aznet.Listener {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener
}

// closeTransport cancels the accept context and closes the listener. Idempotent.
func (s *ListenerState) closeTransport() {
	s.mu.Lock()
	cancel, listener := s.cancel, s.listener
	s.cancel, s.listener = nil, nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if listener != nil {
		listener.Close()
	}
}

// AgentConnection tracks a connected agent.
type AgentConnection struct {
	ID         string   // connection ID
	Conn       net.Conn // aznet connection
	ListenerID string   // ID of the listener this agent connected to
	Info       string   // agent identity (user@host)

	// LastSeen is the Unix-nanos timestamp of the last message received from the
	// agent. It is written from the receive goroutine (via the OnReceive
	// callback) and read from the CLI goroutine, so it is accessed atomically to
	// avoid a torn read of a multi-word time.Time. Use lastSeen()/setLastSeen.
	LastSeen atomic.Int64

	CreatedAt time.Time // connection time
}

// lastSeen returns the last-seen time, or the zero time if never set.
func (a *AgentConnection) lastSeen() time.Time {
	ns := a.LastSeen.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// setLastSeen records t as the last-seen time.
func (a *AgentConnection) setLastSeen(t time.Time) {
	a.LastSeen.Store(t.UnixNano())
}

// AgentInfo tracks connected agent metadata for display.
type AgentInfo struct {
	*AgentConnection
	ProxyPort string // SOCKS port (if proxy is running)
}

// Global state.
var (
	config           *Config      // app config
	app              *grumble.App // grumble app instance for prompt updates
	listeners        sync.Map     // active listeners: map[listenerID]*ListenerState
	connectedAgents  sync.Map     // connected agents: map[connID]*AgentConnection
	runningProxies   sync.Map     // active proxies: map[connID]*proxy.ProxyServer
	selectedAgent    string       // currently selected agent ID
	selectedListener string       // currently selected/default listener ID
)

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

	return nil
}

// StartListener creates and starts an aznet listener for the given listener config.
func StartListener(listenerID string) error {
	// Check if listener already exists
	if val, ok := listeners.Load(listenerID); ok {
		state := val.(*ListenerState)
		if state.IsRunning() {
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

	log.Debug().Str("listener_id", listenerID).Str("listen_addr", listenAddr).Msg("Starting aznet listener")

	// Use ultra-aggressive polling (1ms) - this will increase Azure API costs!
	ctx, cancel := context.WithCancel(context.Background())
	l, err := aznet.Listen(listenerConfig.Driver, listenAddr, aznet.WithContext(ctx))
	if err != nil {
		cancel()
		return fmt.Errorf("failed to start aznet listener: %v", err)
	}

	listener, ok := l.(*aznet.Listener)
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
	}
	state.running.Store(true)
	listeners.Store(listenerID, state)

	// Set this listener as the default when started
	selectedListener = listenerID

	log.Info().Str("listener_id", listenerID).Str("driver", listenerConfig.Driver).Str("addr", listenerConfig.Address).Msg("Aznet listener started and set as default")

	// Start accepting agent connections for this listener
	go AcceptAgentLoopForListener(ctx, listenerID, listener)

	return nil
}

// StopListener stops an aznet listener.
func StopListener(listenerID string) error {
	val, ok := listeners.Load(listenerID)
	if !ok {
		return fmt.Errorf("listener '%s' not found", listenerID)
	}

	state := val.(*ListenerState)
	if !state.IsRunning() {
		return fmt.Errorf("listener '%s' is not running", listenerID)
	}

	// First, send FIN messages to all agents while listener is still open
	// This allows agents to gracefully receive the shutdown notification
	connectedAgents.Range(func(key, value interface{}) bool {
		agent := value.(*AgentConnection)
		if agent.ListenerID == listenerID {
			// Try to send FIN message using CloseWrite if supported
			// This allows agents to gracefully handle the shutdown
			if closeWriter, ok := agent.Conn.(interface {
				CloseWrite() error
			}); ok {
				_ = closeWriter.CloseWrite()
			}
		}
		return true
	})

	// Wait for FIN messages to be sent and propagate through Azure storage
	// This gives agents time to receive the shutdown notification
	time.Sleep(200 * time.Millisecond)

	// Claim the teardown only now. Claiming before the FIN window would report the
	// listener as stopped while it is still up, letting a restart bind a new
	// listener under this ID whose agents this teardown would then disconnect.
	if !state.claimStop() {
		return fmt.Errorf("listener '%s' is not running", listenerID)
	}

	// Close the transport after FIN messages have been sent, then drop the agents
	// that came in through it.
	dropped := teardownListener(listenerID, state, "listener stopped")

	// Clear default selection if this was the selected listener
	if selectedListener == listenerID {
		selectedListener = ""
		log.Info().Msg("Default listener cleared (listener was stopped)")
	}

	// Selection is CLI-owned state, so it is cleared here rather than in
	// teardownListener, which also runs on the accept goroutine.
	for _, agentID := range dropped {
		if selectedAgent == agentID {
			selectedAgent = ""
			// Update prompt to default if app is available
			if app != nil {
				app.SetPrompt("proxyblob » ")
			}
		}
	}

	log.Info().Str("listener_id", listenerID).Msg("Listener stopped")
	return nil
}

// teardownListener closes the listener's transport and disconnects every agent
// that arrived through it, returning their IDs. The caller must have won
// claimStop first, which is what keeps this to one execution per listener.
func teardownListener(listenerID string, state *ListenerState, reason string) []string {
	state.closeTransport()

	var dropped []string
	connectedAgents.Range(func(key, value interface{}) bool {
		agent := value.(*AgentConnection)
		if agent.ListenerID != listenerID {
			return true
		}
		agentID := key.(string)

		// Stop proxy if running
		if val, ok := runningProxies.LoadAndDelete(agentID); ok {
			if server, ok := val.(*proxy.ProxyServer); ok {
				server.Stop()
			}
		}

		// Close connection (this will also send FIN if not already sent)
		agent.Conn.Close()
		connectedAgents.Delete(key)
		dropped = append(dropped, agentID)

		log.Info().Str("agent_id", agentID).Str("listener_id", listenerID).Str("reason", reason).Msg("Agent disconnected")
		return true
	})

	return dropped
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

	// The expiry parameter is currently ignored by the library's ConnectionString method.
	// It uses the DefaultSASExpiry (24h) or the one set via WithSASExpiry during Listen.
	connStr, err := listener.ConnectionString()
	if err != nil {
		return "", err
	}

	// Prepend the driver name so the agent knows which network to use for aznet.Dial
	// Format: driver|connection_string
	fullConnStr := state.Config.Driver + "|" + connStr

	// Base64 encode the connection string to match expected format
	return base64.RawStdEncoding.EncodeToString([]byte(fullConnStr)), nil
}

// AcceptAgentLoopForListener accepts incoming agent connections for a specific listener and stores them.
func AcceptAgentLoopForListener(ctx context.Context, listenerID string, listener net.Listener) {
	// An Accept failure is usually transient -- a throttled poll, a dropped
	// request -- so back off and retry. But retrying forever turns a listener
	// whose transport is gone into a zombie: it logs on every iteration while
	// `listener list` still reports it as running and no agent can ever arrive.
	// Give up after a bounded run of failures and tear it down instead.
	const maxAcceptFailures = 20
	const maxAcceptBackoff = 5 * time.Second
	const maxAcceptBackoffShift = 6

	failures := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Check if listener was stopped
			val, ok := listeners.Load(listenerID)
			if !ok {
				return
			}
			state := val.(*ListenerState)
			if !state.IsRunning() {
				return
			}

			failures++
			if failures >= maxAcceptFailures {
				log.Error().
					Err(err).
					Int("consecutive", failures).
					Str("listener_id", listenerID).
					Msg("Accept failing persistently, stopping listener")
				// Lost the race against a concurrent CLI stop: that caller owns
				// the teardown, so just exit.
				if state.claimStop() {
					teardownListener(listenerID, state, "accept loop gave up")
				}
				return
			}

			log.Error().Err(err).Str("listener_id", listenerID).Msg("Failed to accept connection")

			shift := failures - 1
			if shift > maxAcceptBackoffShift {
				shift = maxAcceptBackoffShift
			}
			backoff := time.Duration(100<<uint(shift)) * time.Millisecond
			if backoff > maxAcceptBackoff {
				backoff = maxAcceptBackoff
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}

		failures = 0

		// Generate a unique ID for this agent connection
		agentID := uuid.New().String()

		// Read the agent identity (user@host) with a 5-second timeout.
		//
		// Wire format: 2-byte big-endian length N (1 <= N <= maxIdentityLen), followed by
		// exactly N bytes of identity payload. Both reads use io.ReadFull: a bare Read on a
		// byte stream may return fewer bytes than requested, and any leftover identity bytes
		// would then be parsed as a record header and desynchronize the stream permanently.
		//
		// FLAG DAY: this framing must match the writer in cmd/agent/main.go (NewAgent).
		// There is no backward compatibility with the old unframed exchange -- agent and
		// proxy must be rebuilt and deployed together.
		//
		// Any failure here is fatal for this connection: we close it and move on rather
		// than proceeding with an "unknown" identity over a possibly poisoned stream.
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		info, err := readAgentIdentity(conn)
		if err != nil {
			log.Error().Err(err).Str("listener_id", listenerID).Msg("Failed to read agent identity, dropping connection")
			conn.Close()
			continue
		}
		conn.SetReadDeadline(time.Time{})

		now := time.Now()

		// Store the connection
		agent := &AgentConnection{
			ID:         agentID,
			Conn:       conn,
			ListenerID: listenerID,
			Info:       info,
			CreatedAt:  now,
		}
		agent.setLastSeen(now)
		connectedAgents.Store(agentID, agent)

		log.Info().Str("agent_id", agentID).Str("info", info).Str("listener_id", listenerID).Msg("Agent connected")

		// Monitor connection in background
		go monitorAgent(ctx, agentID, conn)
	}
}

// maxIdentityLen bounds the identity payload the proxy is willing to accept.
// Must match MaxIdentityLen in cmd/agent/main.go.
const maxIdentityLen = 512

// readAgentIdentity reads one length-prefixed identity frame from conn:
// a 2-byte big-endian length followed by exactly that many bytes.
// It uses io.ReadFull for both reads so a short read is reported as an error
// instead of silently leaving identity bytes in the stream.
func readAgentIdentity(conn net.Conn) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return "", fmt.Errorf("read identity length: %w", err)
	}

	length := binary.BigEndian.Uint16(lenBuf[:])
	if length == 0 || int(length) > maxIdentityLen {
		return "", fmt.Errorf("identity length %d out of range (1-%d)", length, maxIdentityLen)
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return "", fmt.Errorf("read identity payload (%d bytes): %w", length, err)
	}

	return string(buf), nil
}

// monitorAgent watches for agent disconnection via proxy server context.
// Note: We don't read from the connection directly as that interferes with the transport layer.
func monitorAgent(ctx context.Context, agentID string, conn net.Conn) {
	// Wait for context cancellation - the proxy server will detect connection issues
	<-ctx.Done()

	// Clean up on context cancellation
	connectedAgents.Delete(agentID)

	// Stop proxy if running
	if val, ok := runningProxies.LoadAndDelete(agentID); ok {
		if server, ok := val.(*proxy.ProxyServer); ok {
			server.Stop()
		}
	}

	conn.Close()
	log.Info().Str("agent_id", agentID).Msg("Agent disconnected (context done)")
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

// ListAgents returns information about all connected agents.
func ListAgents() []AgentInfo {
	var agents []AgentInfo

	connectedAgents.Range(func(key, value interface{}) bool {
		agentID := key.(string)
		agent := value.(*AgentConnection)

		var proxyPort string
		if val, ok := runningProxies.Load(agentID); ok {
			if server, ok := val.(*proxy.ProxyServer); ok && server.Listener != nil {
				_, portStr, _ := net.SplitHostPort(server.Listener.Addr().String())
				proxyPort = portStr
			}
		}

		agents = append(agents, AgentInfo{
			AgentConnection: agent,
			ProxyPort:       proxyPort,
		})
		return true
	})

	return agents
}

// RenderListenerTable formats listener information into a human-readable table.
func RenderListenerTable(listeners []ListenerInfo, defaultID string) string {
	t := table.NewWriter()
	t.SetStyle(table.StyleRounded)

	t.AppendHeader(table.Row{
		"Name",
		"Status",
		"Protocol",
		"Started At",
	})

	// ANSI color codes
	const (
		colorReset = "\033[0m"
		colorRed   = "\033[31m"
		colorGreen = "\033[32m"
	)

	for _, l := range listeners {
		marker := ""
		if l.ID == defaultID {
			marker = " (default)"
		}

		startedAt := ""
		if l.Status == "running" && !l.StartedAt.IsZero() {
			startedAt = l.StartedAt.Format("2006-01-02 15:04:05")
		}

		// Color the status: green for running, red for stopped
		statusColor := colorReset
		switch l.Status {
		case "running":
			statusColor = colorGreen
		case "stopped":
			statusColor = colorRed
		}
		statusDisplay := statusColor + l.Status + colorReset

		t.AppendRow(table.Row{
			l.ID + marker,
			statusDisplay,
			l.Protocol,
			startedAt,
		})
	}

	return t.Render()
}

// RenderAgentTable formats agent information into a human-readable table.
func RenderAgentTable(agents []AgentInfo) string {
	t := table.NewWriter()
	t.SetStyle(table.StyleRounded)

	t.AppendHeader(table.Row{
		"Agent ID",
		"Info",
		"Listener",
		"Proxy Port",
		"Connected At",
		"Last Seen",
	})

	for _, a := range agents {
		t.AppendRow(table.Row{
			a.ID,
			a.Info,
			a.ListenerID,
			a.ProxyPort,
			a.CreatedAt.Format("2006-01-02 15:04:05"),
			formatRelativeTime(a.lastSeen()),
		})
	}

	return t.Render()
}

// formatRelativeTime returns a human-readable relative time string.
func formatRelativeTime(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// AddCommands registers all CLI commands with the application.
func AddCommands(app *grumble.App) {
	// Parent command for listener management
	listenerCmd := &grumble.Command{
		Name: "listener",
		Help: "manage listeners",
	}

	// Subcommand: listener list
	listenerCmd.AddCommand(&grumble.Command{
		Name:    "list",
		Aliases: []string{"ls"},
		Help:    "list all listeners",
		Run: func(c *grumble.Context) error {
			listenerInfos := ListListeners()
			if len(listenerInfos) == 0 {
				log.Info().Msg("No listeners configured")
				return nil
			}

			c.App.Println(RenderListenerTable(listenerInfos, selectedListener))
			return nil
		},
	})

	// Subcommand: listener start
	listenerCmd.AddCommand(&grumble.Command{
		Name: "start",
		Help: "start a listener",
		Args: func(a *grumble.Args) {
			a.StringList("listener-id", "ID of the listener to start")
		},
		Completer: CompleteListeners,
		Run: func(c *grumble.Context) error {
			listenerIDs := c.Args.StringList("listener-id")
			listenerID := ""
			if len(listenerIDs) != 0 {
				listenerID = listenerIDs[0]
			} else {
				listenerID = selectedListener
			}
			if listenerID == "" {
				log.Error().Msg("No listener specified and no default listener selected. Use 'listener select <id>' first or specify a listener ID")
				return nil
			}
			if err := StartListener(listenerID); err != nil {
				log.Error().Err(err).Str("listener_id", listenerID).Msg("Failed to start listener")
				return nil
			}
			return nil
		},
	})

	// Subcommand: listener stop
	listenerCmd.AddCommand(&grumble.Command{
		Name: "stop",
		Help: "stop a listener",
		Args: func(a *grumble.Args) {
			a.StringList("listener-id", "ID of the listener to stop")
		},
		Completer: CompleteListeners,
		Run: func(c *grumble.Context) error {
			listenerIDs := c.Args.StringList("listener-id")
			listenerID := ""
			if len(listenerIDs) != 0 {
				listenerID = listenerIDs[0]
			} else {
				listenerID = selectedListener
			}
			if listenerID == "" {
				log.Error().Msg("No listener specified and no default listener selected. Use 'listener select <id>' first or specify a listener ID")
				return nil
			}
			if err := StopListener(listenerID); err != nil {
				log.Error().Err(err).Str("listener_id", listenerID).Msg("Failed to stop listener")
				return nil
			}
			return nil
		},
	})

	// Subcommand: listener select
	listenerCmd.AddCommand(&grumble.Command{
		Name:    "select",
		Aliases: []string{"use"},
		Help:    "select a default listener",
		Args: func(a *grumble.Args) {
			a.String("listener-id", "ID of the listener to select as default")
		},
		Completer: CompleteListeners,
		Run: func(c *grumble.Context) error {
			listenerID := c.Args.String("listener-id")

			// Verify listener exists in config
			found := false
			for _, lc := range config.Listeners {
				if lc.Name == listenerID {
					found = true
					break
				}
			}

			if !found {
				log.Error().Str("listener_id", listenerID).Msg("Listener not found")
				return nil
			}

			selectedListener = listenerID
			log.Info().Str("listener_id", listenerID).Msg("Listener selected as default")
			return nil
		},
	})

	// Default action for listener command (list)
	listenerCmd.Run = func(c *grumble.Context) error {
		listenerInfos := ListListeners()
		if len(listenerInfos) == 0 {
			log.Info().Msg("No listeners configured")
			return nil
		}

		c.App.Println("Listeners:")
		c.App.Println(RenderListenerTable(listenerInfos, selectedListener))
		return nil
	}

	app.AddCommand(listenerCmd)

	// Command to create a new agent connection string
	app.AddCommand(&grumble.Command{
		Name: "new",
		Help: "generate a new connection string for an agent",
		Flags: func(f *grumble.Flags) {
			f.Duration("d", "duration", 7*24*time.Hour, "duration for the SAS token (default 7 days)")
			f.String("l", "listener", "", "listener ID to use (defaults to selected listener)")
		},
		Run: func(c *grumble.Context) error {
			listenerID := c.Flags.String("listener")
			if listenerID == "" {
				listenerID = selectedListener
			}

			if listenerID == "" {
				log.Error().Msg("No listener specified and no default listener selected. Use 'listener start <id>' to start a listener (it becomes the default), or use 'listener select <id>' to select a default, or use --listener flag to specify a listener")
				return nil
			}

			// Check if listener is running
			val, ok := listeners.Load(listenerID)
			if !ok {
				log.Error().Str("listener_id", listenerID).Msg("Listener not found or not started")
				return nil
			}

			state := val.(*ListenerState)
			if !state.IsRunning() {
				log.Error().Str("listener_id", listenerID).Msg("Listener is not running")
				return nil
			}

			expiry := c.Flags.Duration("duration")
			connString, err := GenerateConnectionString(listenerID, expiry)
			if err != nil {
				log.Error().Err(err).Str("listener_id", listenerID).Msg("Failed to generate connection string")
				return nil
			}

			log.Info().Str("listener_id", listenerID).Str("connection_string", connString).Msg("Connection string generated")
			return nil
		},
	})

	// Parent command for agent management
	agentCmd := &grumble.Command{
		Name: "agent",
		Help: "manage agents",
	}

	// Subcommand: agent list
	agentCmd.AddCommand(&grumble.Command{
		Name:    "list",
		Aliases: []string{"ls"},
		Help:    "list all connected agents",
		Run: func(c *grumble.Context) error {
			agents := ListAgents()
			if len(agents) == 0 {
				log.Info().Msg("No agents connected")
				return nil
			}

			c.App.Println(RenderAgentTable(agents))
			return nil
		},
	})

	// Subcommand: agent select
	agentCmd.AddCommand(&grumble.Command{
		Name:    "select",
		Aliases: []string{"use"},
		Help:    "select an agent for subsequent commands",
		Args: func(a *grumble.Args) {
			a.String("agent-id", "ID of the agent to select")
		},
		Completer: CompleteAgents,
		Run: func(c *grumble.Context) error {
			agentID := c.Args.String("agent-id")

			// Verify agent exists
			if _, ok := connectedAgents.Load(agentID); !ok {
				log.Error().Str("agent_id", agentID).Msg("Agent not found")
				return nil
			}

			selectedAgent = agentID
			log.Info().Str("agent_id", agentID).Msg("Agent selected")
			c.App.SetPrompt(agentID[:8] + " » ")

			return nil
		},
	})

	// Subcommand: agent start
	agentCmd.AddCommand(&grumble.Command{
		Name: "start",
		Help: "start SOCKS proxy for the selected agent",
		Flags: func(f *grumble.Flags) {
			f.String("l", "listen", "127.0.0.1:1080", "listen address for SOCKS server")
		},
		Run: func(c *grumble.Context) error {
			if selectedAgent == "" {
				log.Warn().Msg("No agent selected. Use 'agent select <agent-id>' first")
				return nil
			}

			// Check if proxy already running
			if _, exists := runningProxies.Load(selectedAgent); exists {
				log.Warn().Msg("Proxy already running for this agent")
				return nil
			}

			// Get the agent connection
			val, ok := connectedAgents.Load(selectedAgent)
			if !ok {
				log.Error().Msg("Selected agent no longer connected")
				selectedAgent = ""
				c.App.SetPrompt("proxyblob » ")
				return nil
			}

			agent := val.(*AgentConnection)
			ctx := context.Background()

			// Create a proxy server for this agent (direct connection, no transport wrapper)
			proxyServer := proxy.NewProxyServer(ctx, agent.Conn)

			// Set callback to update LastSeen on every received packet
			proxyServer.OnReceive = func() {
				agent.setLastSeen(time.Now())
			}

			listenAddr := c.Flags.String("listen")
			host, port, err := net.SplitHostPort(listenAddr)
			if err != nil {
				log.Error().Err(err).Str("listen", listenAddr).Msg("Failed to parse listen address")
				return nil
			}
			oldPort := port

			portInt, _ := strconv.Atoi(port)
			for {
				portAvailable := true
				runningProxies.Range(func(key, value interface{}) bool {
					server := value.(*proxy.ProxyServer)
					if server.Listener != nil {
						_, serverPort, _ := net.SplitHostPort(server.Listener.Addr().String())
						if serverPort == port {
							portAvailable = false
							return false
						}
					}
					return true
				})
				if portAvailable {
					break
				}
				portInt++
				port = strconv.Itoa(portInt)
			}

			if oldPort != port {
				log.Warn().Str("used_port", oldPort).Str("selected_port", port).Msg("Proxy already running on this port")
			}

			listenAddr = fmt.Sprintf("%s:%s", host, port)
			proxyServer.Start(listenAddr)

			if proxyServer.Listener == nil {
				log.Error().Str("addr", listenAddr).Msg("Failed to start proxy")
				return nil
			}

			runningProxies.Store(selectedAgent, proxyServer)

			_, portStr, _ := net.SplitHostPort(proxyServer.Listener.Addr().String())
			log.Info().Str("agent_id", selectedAgent).Str("port", portStr).Msg("Proxy started")

			return nil
		},
	})

	// Subcommand: agent stop
	agentCmd.AddCommand(&grumble.Command{
		Name: "stop",
		Help: "stop the proxy for the selected agent",
		Run: func(c *grumble.Context) error {
			if selectedAgent == "" {
				log.Warn().Msg("No agent selected. Use 'agent select <agent-id>' first")
				return nil
			}

			// Get and remove the proxy
			val, exists := runningProxies.LoadAndDelete(selectedAgent)
			if !exists {
				log.Warn().Msg("No proxy running for this agent")
				return nil
			}

			// Stop the proxy
			server := val.(*proxy.ProxyServer)
			server.Stop()

			log.Info().Str("agent_id", selectedAgent).Msg("Proxy stopped")

			return nil
		},
	})

	// Subcommand: agent remove
	agentCmd.AddCommand(&grumble.Command{
		Name:    "remove",
		Aliases: []string{"rm"},
		Help:    "remove an agent (disconnect and stop proxy)",
		Args: func(a *grumble.Args) {
			a.String("agent-id", "ID of the agent to disconnect", grumble.Default(selectedAgent))
		},
		Completer: CompleteAgents,
		Run: func(c *grumble.Context) error {
			agentID := c.Args.String("agent-id")
			if agentID == "" {
				agentID = selectedAgent
			}

			// Get the agent
			val, ok := connectedAgents.LoadAndDelete(agentID)
			if !ok {
				log.Error().Str("agent_id", agentID).Msg("Agent not found")
				return nil
			}

			agent := val.(*AgentConnection)

			// Stop proxy if running
			if val, ok := runningProxies.LoadAndDelete(agentID); ok {
				if server, ok := val.(*proxy.ProxyServer); ok {
					server.Stop()
				}
			}

			// Close the connection
			agent.Conn.Close()

			if selectedAgent == agentID {
				selectedAgent = ""
				c.App.SetPrompt("proxyblob » ")
			}

			log.Info().Str("agent_id", agentID).Msg("Agent disconnected")

			return nil
		},
	})

	// Default action for agent command (list)
	agentCmd.Run = func(c *grumble.Context) error {
		agents := ListAgents()
		if len(agents) == 0 {
			log.Info().Msg("No agents connected")
			return nil
		}

		c.App.Println(RenderAgentTable(agents))
		return nil
	}

	app.AddCommand(agentCmd)
}

// CompleteAgents provides tab completion for agent IDs.
func CompleteAgents(_ string, _ []string) []string {
	var completions []string
	connectedAgents.Range(func(key, _ interface{}) bool {
		completions = append(completions, key.(string))
		return true
	})
	return completions
}

// CompleteListeners provides tab completion for listener IDs.
func CompleteListeners(_ string, _ []string) []string {
	var completions []string
	for _, listenerConfig := range config.Listeners {
		completions = append(completions, listenerConfig.Name)
	}
	return completions
}

// main is the entry point for the application.
func main() {
	// Set up logging
	configureLogging()

	// Configure and create the CLI app
	app = setupCLI()

	// Add all command handlers
	AddCommands(app)

	// Run the application and handle any errors
	if err := app.Run(); err != nil {
		log.Fatal().Msg(err.Error())
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

// setupCLI initializes the command-line interface with basic configuration.
func setupCLI() *grumble.App {
	// Determine history file location
	var histFile string
	home, err := os.UserHomeDir()
	if err != nil {
		histFile = ".proxyblob"
	} else {
		histFile = filepath.Join(home, ".proxyblob")
	}

	// Create and configure the CLI app
	app := grumble.New(&grumble.Config{
		Name:        "proxyblob",
		HistoryFile: histFile,
		Flags: func(f *grumble.Flags) {
			f.String("c", "config", "config.json", "path to configuration file")
		},
	})

	// Set up banner
	app.SetPrintASCIILogo(func(a *grumble.App) {
		fmt.Print(banner)
	})

	// Initialize configuration when the app starts
	app.OnInit(func(a *grumble.App, flags grumble.FlagMap) error {
		// Load configuration
		var err error
		config, err = LoadConfig(flags.String("config"))
		if err != nil {
			return fmt.Errorf("failed to load configuration: %v", err)
		}

		// Note: Listeners are not auto-started. User must start them explicitly.
		// When a listener is started, it automatically becomes the default.
		log.Info().Int("listener_count", len(config.Listeners)).Msg("Configuration loaded. Use 'listener start <id>' to start a listener (it will become the default).")

		return nil
	})

	return app
}
