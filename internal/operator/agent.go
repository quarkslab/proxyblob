package operator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"proxyblob/internal/diag"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"proxyblob/internal/bootstrap"
	"proxyblob/internal/mux"
	"proxyblob/internal/proxy"

	"github.com/atsika/aznet"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// AgentConnection tracks a connected agent.
type AgentConnection struct {
	ID         string   // connection ID
	Conn       net.Conn // aznet connection
	ListenerID string   // ID of the listener this agent connected to
	Info       string   // agent identity (user@host)

	// LastSeen is the Unix-nanos timestamp of the last message received from the
	// agent. It is written from the receive goroutine (via the OnReceive
	// callback) and read from the CLI goroutine, so it is accessed atomically to
	// avoid a torn read of a multi-word time.Time. Use LastSeenTime/setLastSeen.
	LastSeen atomic.Int64

	CreatedAt  time.Time // connection time
	op         *Operator
	generation *ListenerState
	mu         sync.Mutex
	closed     bool
	closeOnce  sync.Once
	closeErr   error
	server     *proxy.ProxyServer
}

// LastSeenTime returns the last-seen time, or the zero time if never set.
func (a *AgentConnection) LastSeenTime() time.Time {
	ns := a.LastSeen.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (a *AgentConnection) setLastSeen(t time.Time) {
	a.LastSeen.Store(t.UnixNano())
}

// AgentInfo tracks connected agent metadata for display.
type AgentInfo struct {
	*AgentConnection
	ProxyPort     string    // SOCKS port (if proxy is running)
	SessionExpiry time.Time // zero means unknown; supplied by aznet, never estimated
}

// acceptAgents owns acceptance for exactly one generation.
func (o *Operator) acceptAgents(ctx context.Context, listenerID string, state *ListenerState) {
	o.acceptAgentLoop(ctx, listenerID, state, waitAcceptRetry)
}

func (o *Operator) acceptAgentLoop(ctx context.Context, listenerID string, state *ListenerState, wait func(context.Context, time.Duration) bool) {
	defer close(state.acceptDone)
	failures := 0
	for ctx.Err() == nil && state.IsRunning() {
		conn, err := state.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || !state.IsRunning() {
				return
			}
			failures++
			if !retryAccept(err) || failures >= 20 {
				log.Error().Err(err).Str("listener_id", listenerID).Msg("Accept failed, stopping listener")
				state.beginStop() // teardown waits for this loop; never wait here
				return
			}
			if !wait(ctx, acceptBackoff(failures)) {
				return
			}
			continue
		}
		failures = 0
		if !state.IsRunning() || ctx.Err() != nil {
			conn.Close()
			return
		}
		// Cancellation closes even a connection still reading its identity.
		cancelRead := context.AfterFunc(ctx, func() { conn.Close() })
		conn.SetReadDeadline(time.Now().Add(identityTimeout))
		info, err := bootstrap.ReadIdentity(conn)
		if !cancelRead() || err != nil {
			conn.Close()
			continue
		}
		conn.SetReadDeadline(time.Time{})
		agent := &AgentConnection{ID: uuid.NewString(), Conn: conn, ListenerID: listenerID,
			Info: info, CreatedAt: time.Now(), op: o, generation: state}
		agent.setLastSeen(agent.CreatedAt)
		cfg, err := mux.FlowConfigFromEnv()
		if err != nil {
			log.Error().Uint8("code", diag.ErrorCode(err)).Msg(diag.Description(diag.ErrorCode(err)))
			conn.Close()
			continue
		}
		agent.server, err = proxy.NewProxyServerWithConfig(ctx, conn, cfg)
		if err != nil {
			conn.Close()
			continue
		}
		agent.server.OnReceive = func() { agent.setLastSeen(time.Now()) }
		// Publication and the stopping transition share the generation lock.
		state.mu.Lock()
		if state.phase != running {
			state.mu.Unlock()
			agent.close()
			return
		}
		o.agents.Store(agent.ID, agent)
		state.mu.Unlock()
		log.Info().Str("agent_id", agent.ID).Str("info", info).Str("listener_id", listenerID).Msg("Agent connected")
		// Read the session even before SOCKS starts: this consumes heartbeats and
		// detects disconnect without a competing reader or an idle-agent timer.
		agent.server.StartReceiving()
		go monitorAgent(agent)
	}
}

// monitorAgent follows the actual session receiver, not listener or CLI activity.
func monitorAgent(agent *AgentConnection) {
	<-agent.server.Ctx.Done()
	if err := agent.close(); err != nil {
		log.Error().Err(err).Str("agent_id", agent.ID).Msg("Session cleanup failed")
	}
}

func (a *AgentConnection) close() error {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		if a.server != nil {
			a.server.Stop()
		}
		a.op.proxies.Delete(a.ID)
		a.mu.Unlock()
		// Abort expires transport I/O to release the protocol goroutines. Wait
		// for its writer before replacing that deadline: aznet.Close must be
		// able to send its FIN, but no interrupted DATA write may resume.
		if a.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), mux.DrainTimeout)
			if err := a.server.WaitWriter(ctx); err != nil {
				a.closeErr = err
			} else {
				a.closeErr = a.Conn.SetWriteDeadline(time.Now().Add(mux.DrainTimeout))
			}
			cancel()
		}
		a.closeErr = errors.Join(a.closeErr, a.Conn.Close())
		a.op.agents.CompareAndDelete(a.ID, a)
	})
	return a.closeErr
}

// Agents returns information about all connected agents.
func (o *Operator) Agents() []AgentInfo {
	var agents []AgentInfo
	o.agents.Range(func(key, value any) bool {
		agent := value.(*AgentConnection)
		var proxyPort string
		if val, ok := o.proxies.Load(key); ok {
			if addr := val.(*proxy.ProxyServer).ListenerAddr(); addr != nil {
				_, proxyPort, _ = net.SplitHostPort(addr.String())
			}
		}
		expiry, known := aznet.GetSessionExpiry(agent.Conn)
		if !known {
			expiry = time.Time{}
		}
		agents = append(agents, AgentInfo{AgentConnection: agent, ProxyPort: proxyPort, SessionExpiry: expiry})
		return true
	})
	return agents
}

// AgentIDs returns the IDs of all connected agents.
func (o *Operator) AgentIDs() []string {
	var ids []string
	o.agents.Range(func(key, _ any) bool {
		ids = append(ids, key.(string))
		return true
	})
	return ids
}

// HasAgent reports whether the agent is connected.
func (o *Operator) HasAgent(agentID string) bool {
	_, ok := o.agents.Load(agentID)
	return ok
}

// StartProxy starts the agent's local SOCKS listener on listenAddr. If another
// agent's proxy already uses that port, the next free one is chosen; the
// returned address is the one actually bound.
func (o *Operator) StartProxy(agentID, listenAddr string) (net.Addr, error) {
	if _, exists := o.proxies.Load(agentID); exists {
		return nil, ErrProxyRunning
	}
	val, ok := o.agents.Load(agentID)
	if !ok {
		return nil, ErrAgentNotFound
	}
	agent := val.(*AgentConnection)
	agent.mu.Lock()
	defer agent.mu.Unlock()
	if agent.closed {
		return nil, ErrAgentClosed
	}
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("parse listen address %q: %w", listenAddr, err)
	}
	portInt, _ := strconv.Atoi(port)
	for {
		portAvailable := true
		o.proxies.Range(func(_, value any) bool {
			if addr := value.(*proxy.ProxyServer).ListenerAddr(); addr != nil {
				if _, serverPort, _ := net.SplitHostPort(addr.String()); serverPort == port {
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
	listenAddr = net.JoinHostPort(host, port)
	agent.server.Start(listenAddr)
	addr := agent.server.ListenerAddr()
	if addr == nil {
		return nil, fmt.Errorf("failed to start proxy on %s", listenAddr)
	}
	o.proxies.Store(agentID, agent.server)
	return addr, nil
}

// StopProxy stops the agent's local SOCKS listener; the agent stays connected.
func (o *Operator) StopProxy(agentID string) error {
	val, exists := o.proxies.LoadAndDelete(agentID)
	if !exists {
		return ErrProxyNotRunning
	}
	val.(*proxy.ProxyServer).StopListening()
	return nil
}

// RemoveAgent disconnects the agent and stops its proxy. The agent is unlisted
// even when transport cleanup fails; that failure is returned.
func (o *Operator) RemoveAgent(agentID string) error {
	val, ok := o.agents.LoadAndDelete(agentID)
	if !ok {
		return ErrAgentNotFound
	}
	return val.(*AgentConnection).close()
}
