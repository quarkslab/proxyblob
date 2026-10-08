package operator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"proxyblob/internal/bootstrap"

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
	op         *Operator
	mu         sync.Mutex
	phase      listenerPhase
	listener   bootstrapListener
	cancel     context.CancelFunc
	acceptDone chan struct{}
	done       chan struct{}
	closeErr   error
}

// ListenerInfo tracks listener metadata for display.
type ListenerInfo struct {
	ID        string    // listener ID/name
	Status    string    // "running" or "stopped"
	Protocol  string    // protocol scheme
	Address   string    // listen address
	StartedAt time.Time // when started (if running)
}

const identityTimeout = 5 * time.Second

// StartListener creates and starts an aznet listener for the named listener config.
func (o *Operator) StartListener(listenerID string) error {
	o.startMu.Lock()
	defer o.startMu.Unlock()
	if val, ok := o.listeners.Load(listenerID); ok {
		if val.(*ListenerState).status() != stopped {
			return fmt.Errorf("listener '%s' is already running", listenerID)
		}
	}

	var listenerConfig *ListenerConfig
	for i := range o.configs {
		if o.configs[i].Name == listenerID {
			listenerConfig = &o.configs[i]
			break
		}
	}
	if listenerConfig == nil {
		return fmt.Errorf("listener '%s' not found in configuration", listenerID)
	}
	if err := listenerConfig.Validate(); err != nil {
		return fmt.Errorf("invalid listener config: %v", err)
	}

	// Build the listen address using url.URL for proper escaping
	u, err := url.Parse(listenerConfig.Address)
	if err != nil {
		return fmt.Errorf("invalid address format: %v", err)
	}
	listenAddr := (&url.URL{
		Scheme: u.Scheme,
		User:   url.UserPassword(listenerConfig.StorageAccountName, listenerConfig.StorageAccountKey),
		Host:   u.Host,
		Path:   u.Path,
	}).String()

	log.Debug().Str("listener_id", listenerID).Msg("Starting aznet listener")

	// Keep aznet polling defaults; retry policy here applies only to failures.
	ctx, cancel := context.WithCancel(context.Background())
	handshake, token := bootstrap.Endpoints(listenerID)
	sessionDuration, _ := listenerConfig.sessionDuration() // validated above
	l, err := o.listen(listenerConfig.Driver, listenAddr, aznet.WithContext(ctx), aznet.WithEndpoints(handshake, token), aznet.WithSessionDuration(sessionDuration))
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

	state := &ListenerState{
		Config:     listenerConfig,
		ListenAddr: listenAddr,
		StartedAt:  time.Now(),
		op:         o,
		listener:   listener,
		cancel:     cancel,
		done:       make(chan struct{}),
		acceptDone: make(chan struct{}),
	}
	state.phase = running
	o.listeners.Store(listenerID, state)

	log.Info().Str("listener_id", listenerID).Str("driver", listenerConfig.Driver).Str("addr", listenerConfig.Address).Msg("Aznet listener started")

	go o.acceptAgents(ctx, listenerID, state)
	return nil
}

// StopListener waits for the captured generation's teardown, including acceptance.
func (o *Operator) StopListener(listenerID string) error {
	val, ok := o.listeners.Load(listenerID)
	if !ok {
		return fmt.Errorf("listener '%s' not found", listenerID)
	}
	return val.(*ListenerState).stop()
}

// ConnectionString issues a connection string for agents of a running listener,
// valid for expiry. Issuance does not change the listener's session policy.
func (o *Operator) ConnectionString(listenerID string, expiry time.Duration) (string, error) {
	val, ok := o.listeners.Load(listenerID)
	if !ok {
		return "", fmt.Errorf("listener '%s' not found", listenerID)
	}
	state := val.(*ListenerState)
	listener := state.transport()
	if !state.IsRunning() || listener == nil {
		return "", fmt.Errorf("listener '%s' is not running", listenerID)
	}
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

// Listeners returns information about all configured listeners.
func (o *Operator) Listeners() []ListenerInfo {
	var infos []ListenerInfo
	for _, listenerConfig := range o.configs {
		info := ListenerInfo{ID: listenerConfig.Name, Status: "stopped"}
		if val, ok := o.listeners.Load(listenerConfig.Name); ok {
			state := val.(*ListenerState)
			info.Status = state.status().String()
			if state.IsRunning() {
				info.Status = "running"
				info.StartedAt = state.StartedAt
				info.Protocol = state.Config.Driver
				info.Address = state.ListenAddr
			}
		}
		if info.Protocol == "" {
			info.Protocol = listenerConfig.Driver
		}
		infos = append(infos, info)
	}
	return infos
}

func (s *ListenerState) status() listenerPhase { s.mu.Lock(); defer s.mu.Unlock(); return s.phase }

// IsRunning reports whether this generation still accepts agents.
func (s *ListenerState) IsRunning() bool { return s.status() == running }

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
		s.op.agents.Range(func(_, value any) bool {
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
