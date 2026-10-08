// Package operator is the proxy side's control plane. It runs one aznet
// listener per configured storage account, accepts agents on them, and starts
// or stops each agent's local SOCKS port. A user interface only calls it and
// renders the results.
package operator

import (
	"errors"
	"net"
	"sync"

	"github.com/atsika/aznet"
)

var (
	ErrAgentNotFound   = errors.New("agent not found")
	ErrAgentClosed     = errors.New("agent disconnected")
	ErrProxyRunning    = errors.New("proxy already running for this agent")
	ErrProxyNotRunning = errors.New("no proxy running for this agent")
)

// ListenFunc opens a transport listener; aznet.Listen by default.
type ListenFunc func(network, address string, opts ...aznet.Option) (net.Listener, error)

// Option configures an Operator.
type Option func(*Operator)

// WithListen replaces aznet.Listen, for tests and alternative transports.
func WithListen(listen ListenFunc) Option {
	return func(o *Operator) { o.listen = listen }
}

// Operator owns every listener generation and connected agent of one proxy.
type Operator struct {
	listen    ListenFunc
	configs   []ListenerConfig
	startMu   sync.Mutex // serializes listener starts
	listeners sync.Map   // listener name → *ListenerState
	agents    sync.Map   // agent ID → *AgentConnection
	proxies   sync.Map   // agent ID → *proxy.ProxyServer with a running SOCKS port
}

// New returns an Operator for the given listener configurations. No listener
// starts until StartListener is called.
func New(configs []ListenerConfig, opts ...Option) *Operator {
	o := &Operator{listen: aznet.Listen, configs: configs}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// ListenerConfigs returns the configured listeners, started or not.
func (o *Operator) ListenerConfigs() []ListenerConfig { return o.configs }
