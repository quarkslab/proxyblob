package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/atsika/aznet"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"io"
	"net"
	"proxyblob/pkg/protocol"
	proxy "proxyblob/pkg/proxy/server"
	"sync"
	"sync/atomic"
	"time"
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
	// avoid a torn read of a multi-word time.Time. Use lastSeen()/setLastSeen.
	LastSeen atomic.Int64

	CreatedAt  time.Time // connection time
	generation *ListenerState
	mu         sync.Mutex
	closed     bool
	closeOnce  sync.Once
	closeErr   error
	server     *proxy.ProxyServer
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
	ProxyPort     string    // SOCKS port (if proxy is running)
	SessionExpiry time.Time // zero means unknown; supplied by aznet, never estimated
}

// AcceptAgentLoopForListener owns acceptance for exactly one generation.
func AcceptAgentLoopForListener(ctx context.Context, listenerID string, state *ListenerState) {
	acceptAgentLoop(ctx, listenerID, state, waitAcceptRetry)
}

func acceptAgentLoop(ctx context.Context, listenerID string, state *ListenerState, wait func(context.Context, time.Duration) bool) {
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
		info, err := readAgentIdentity(conn)
		if !cancelRead() || err != nil {
			conn.Close()
			continue
		}
		conn.SetReadDeadline(time.Time{})
		agent := &AgentConnection{ID: uuid.NewString(), Conn: conn, ListenerID: listenerID,
			Info: info, CreatedAt: time.Now(), generation: state}
		agent.setLastSeen(agent.CreatedAt)
		cfg, err := protocol.FlowConfigFromEnv()
		if err != nil {
			log.Error().Err(err).Msg("Invalid flow limits")
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
		connectedAgents.Store(agent.ID, agent)
		state.mu.Unlock()
		log.Info().Str("agent_id", agent.ID).Str("info", info).Str("listener_id", listenerID).Msg("Agent connected")
		// Read the session even before SOCKS starts: this consumes heartbeats and
		// detects disconnect without a competing reader or an idle-agent timer.
		agent.server.StartReceiving()
		go monitorAgent(agent)
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

// monitorAgent follows the actual session receiver, not listener or CLI activity.
func monitorAgent(agent *AgentConnection) {
	<-agent.server.Ctx.Done()
	if err := agent.close(); err != nil {
		log.Error().Err(err).Str("agent_id", agent.ID).Msg("Session cleanup failed")
	}
}

// ListAgents returns information about all connected agents.
func ListAgents() []AgentInfo {
	var agents []AgentInfo

	connectedAgents.Range(func(key, value interface{}) bool {
		agentID := key.(string)
		agent := value.(*AgentConnection)

		var proxyPort string
		if val, ok := runningProxies.Load(agentID); ok {
			if server, ok := val.(*proxy.ProxyServer); ok {
				if addr := server.ListenerAddr(); addr != nil {
					_, proxyPort, _ = net.SplitHostPort(addr.String())
				}
			}
		}

		expiry, known := aznet.GetSessionExpiry(agent.Conn)
		if !known {
			expiry = time.Time{}
		}
		agents = append(agents, AgentInfo{
			AgentConnection: agent,
			ProxyPort:       proxyPort,
			SessionExpiry:   expiry,
		})
		return true
	})

	return agents
}
