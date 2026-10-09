package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"proxyblob/internal/operator"
	"time"

	"github.com/desertbit/grumble"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	app              *grumble.App       // grumble app instance for prompt updates
	op               *operator.Operator // proxy control plane, created once the config loads
	selectedAgent    string             // currently selected agent ID
	selectedListener string             // currently selected/default listener ID
)

// CLI banner with version.
const banner = `
     ____                      ____  _       _     
    |  _ \ _ __ _____  ___   _| __ )| | ___ | |__  
    | |_) | '__/ _ \ \/ / | | |  _ \| |/ _ \| '_ \ 
    |  __/| | | (_) >  <| |_| | |_) | | (_) | |_) |
    |_|   |_|  \___/_/\_\\__, |____/|_|\___/|_.__/ 
                         |___/                     

    SOCKS Proxy over Azure Storage services (v2.4)
   -------------------------------------------------

`

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
			listenerInfos := op.Listeners()
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
			if err := op.StartListener(listenerID); err != nil {
				log.Error().Err(err).Str("listener_id", listenerID).Msg("Failed to start listener")
				return nil
			}
			selectedListener = listenerID
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
			if err := op.StopListener(listenerID); err != nil {
				log.Error().Err(err).Str("listener_id", listenerID).Msg("Failed to stop listener")
				return nil
			}
			log.Info().Str("listener_id", listenerID).Msg("Listener stopped")
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
		listenerInfos := op.Listeners()
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
			f.Duration("d", "duration", 7*24*time.Hour, "bootstrap connection-string validity (default 7 days; independent of session duration)")
			f.String("l", "listener", "", "listener ID to use (defaults to selected listener)")
		},
		Run: func(c *grumble.Context) error {
			listenerID := c.Flags.String("listener")
			if listenerID == "" {
				listenerID = selectedListener
			}

			if listenerID == "" {
				log.Error().Msg("No listener selected. Run 'listener start <id>' first.")
				return nil
			}

			expiry := c.Flags.Duration("duration")
			connString, err := op.ConnectionString(listenerID, expiry)
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
			agents := op.Agents()
			if len(agents) == 0 {
				log.Info().Msg("No agents connected")
				return nil
			}

			c.App.Println(RenderAgentTable(agents, time.Now()))
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
			if !op.HasAgent(agentID) {
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

			listenAddr := c.Flags.String("listen")
			addr, err := op.StartProxy(selectedAgent, listenAddr)
			switch {
			case errors.Is(err, operator.ErrProxyRunning):
				log.Warn().Msg("Proxy already running for this agent")
				return nil
			case errors.Is(err, operator.ErrAgentNotFound):
				log.Error().Msg("Selected agent no longer connected")
				selectedAgent = ""
				c.App.SetPrompt("proxyblob » ")
				return nil
			case errors.Is(err, operator.ErrAgentClosed):
				return err
			case err != nil:
				log.Error().Err(err).Str("listen", listenAddr).Msg("Failed to start proxy")
				return nil
			}
			_, portStr, _ := net.SplitHostPort(addr.String())
			if _, wanted, _ := net.SplitHostPort(listenAddr); wanted != portStr {
				log.Warn().Str("used_port", wanted).Str("selected_port", portStr).Msg("Proxy already running on this port")
			}
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

			if err := op.StopProxy(selectedAgent); err != nil {
				log.Warn().Msg("No proxy running for this agent")
				return nil
			}

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

			err := op.RemoveAgent(agentID)
			if errors.Is(err, operator.ErrAgentNotFound) {
				log.Error().Str("agent_id", agentID).Msg("Agent not found")
				return nil
			}

			// Selection follows local removal even when remote cleanup fails.
			if selectedAgent == agentID {
				selectedAgent = ""
				c.App.SetPrompt("proxyblob » ")
			}

			if err != nil {
				return err
			}

			log.Info().Str("agent_id", agentID).Msg("Agent disconnected")

			return nil
		},
	})

	// Default action for agent command (list)
	agentCmd.Run = func(c *grumble.Context) error {
		agents := op.Agents()
		if len(agents) == 0 {
			log.Info().Msg("No agents connected")
			return nil
		}

		c.App.Println(RenderAgentTable(agents, time.Now()))
		return nil
	}

	app.AddCommand(agentCmd)
}

// CompleteAgents provides tab completion for agent IDs.
func CompleteAgents(_ string, _ []string) []string {
	return op.AgentIDs()
}

// CompleteListeners provides tab completion for listener IDs.
func CompleteListeners(_ string, _ []string) []string {
	var completions []string
	for _, listenerConfig := range config.Listeners {
		completions = append(completions, listenerConfig.Name)
	}
	return completions
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
			f.String("", "log-level", "", "minimum log level: trace, debug, info, warn, error (overrides config)")
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
		op = operator.New(config.Listeners)

		// Note: Listeners are not auto-started. User must start them explicitly.
		// When a listener is started, it automatically becomes the default.
		level, err := resolveLogLevel(config.LogLevel, flags.String("log-level"))
		if err != nil {
			return err
		}
		zerolog.SetGlobalLevel(level)

		log.Info().Int("listener_count", len(config.Listeners)).Msg("Configuration loaded. Use 'listener start <id>' to start a listener (it will become the default).")

		return nil
	})

	return app
}
