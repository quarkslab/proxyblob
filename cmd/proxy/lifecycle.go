package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/atsika/aznet"
	"github.com/rs/zerolog/log"
)

type listenerPhase uint8

const (
	running listenerPhase = iota
	stopping
	stopped
)

func (p listenerPhase) String() string { return [...]string{"running", "stopping", "stopped"}[p] }

type bootstrapListener interface {
	net.Listener
	ConnectionString() (string, error)
}

// Each state is one immutable generation. A replacement is allowed only after
// transport closure, acceptance exit and all agent cleanup have completed.
type ListenerState struct {
	Config     *ListenerConfig
	ListenAddr string
	StartedAt  time.Time
	mu         sync.Mutex
	phase      listenerPhase
	listener   bootstrapListener
	cancel     context.CancelFunc
	acceptDone chan struct{}
	done       chan struct{}
	closeErr   error
}

var listenerStartMu sync.Mutex
var listenAzure = aznet.Listen

const identityTimeout = 5 * time.Second

func (s *ListenerState) status() listenerPhase { s.mu.Lock(); defer s.mu.Unlock(); return s.phase }
func (s *ListenerState) IsRunning() bool       { return s.status() == running }
func (s *ListenerState) transport() bootstrapListener {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != running {
		return nil
	}
	return s.listener
}
func (s *ListenerState) beginStop() {
	s.mu.Lock()
	if s.phase != running {
		s.mu.Unlock()
		return
	}
	s.phase = stopping
	s.mu.Unlock()
	go func() {
		s.cancel()
		err := s.listener.Close()
		// A failed Close can leave backend work in flight. Return its bounded
		// result while retaining stopping ownership instead of waiting forever.
		if err == nil {
			<-s.acceptDone
		}
		connectedAgents.Range(func(_, value any) bool {
			agent := value.(*AgentConnection)
			if agent.generation == s {
				err = errors.Join(err, agent.close())
			}
			return true
		})
		// Close deliberately preserves bootstrap resources. Credentials may still
		// refer to this namespace; only its administrator may explicitly delete it.
		s.mu.Lock()
		s.closeErr = err
		if err != nil {
			log.Error().Err(err).Msg("Listener cleanup incomplete; restart blocked")
		}
		// Incomplete library cleanup can retain background ownership. Do not permit
		// name reuse after an error; restarting requires operator reconciliation.
		if err == nil {
			s.phase = stopped
		}
		close(s.done)
		s.mu.Unlock()
	}()
}
func (s *ListenerState) stop() error {
	s.beginStop()
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}
func (a *AgentConnection) close() error {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		if a.server != nil {
			a.server.Stop()
		}
		runningProxies.Delete(a.ID)
		a.mu.Unlock()
		a.closeErr = a.Conn.Close()
		connectedAgents.CompareAndDelete(a.ID, a)
	})
	return a.closeErr
}

// Lowercase alphanumeric names work for Blob, Queue and Table. Hash the exact
// configured name so punctuation/case normalization cannot alias listeners.
func bootstrapEndpoints(name string) (string, string) {
	hash := sha256.Sum256([]byte(name))
	suffix := fmt.Sprintf("%x", hash[:20])
	return "pbh" + suffix, "pbt" + suffix
}
func retryAccept(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
		return false
	}
	var classified interface{ Temporary() bool }
	return errors.As(err, &classified) && classified.Temporary()
}
func acceptBackoff(failures int) time.Duration {
	shift := min(max(failures-1, 0), 6)
	return min(100*time.Millisecond<<shift, 5*time.Second)
}

func waitAcceptRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
