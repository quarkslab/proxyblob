package main

import (
	"github.com/desertbit/grumble"
	"sync"
)

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
