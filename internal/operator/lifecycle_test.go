package operator

import (
	"context"
	"errors"
	"io"
	"net"
	"proxyblob/internal/bootstrap"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atsika/aznet"
)

type acceptResult struct {
	conn net.Conn
	err  error
}
type fakeListener struct {
	results  chan acceptResult
	closed   chan struct{}
	release  chan struct{}
	closes   atomic.Int32
	calls    atomic.Int32
	closeErr error
}

func newFakeListener() *fakeListener {
	return &fakeListener{results: make(chan acceptResult, 32), closed: make(chan struct{})}
}
func (l *fakeListener) Accept() (net.Conn, error) {
	l.calls.Add(1)
	select {
	case r := <-l.results:
		return r.conn, r.err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *fakeListener) Close() error {
	if l.closes.Add(1) == 1 {
		close(l.closed)
	}
	if l.release != nil {
		<-l.release
	}
	return l.closeErr
}
func (*fakeListener) Addr() net.Addr                    { return nil }
func (*fakeListener) ConnectionString() (string, error) { return "test", nil }

type countConn struct {
	net.Conn
	closes atomic.Int32
}

func (c *countConn) Close() error { c.closes.Add(1); return c.Conn.Close() }
func testGeneration(t *testing.T, o *Operator, id string, l *fakeListener) *ListenerState {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s := &ListenerState{Config: &ListenerConfig{Name: id}, op: o, listener: l, cancel: cancel, done: make(chan struct{}), acceptDone: make(chan struct{})}
	o.listeners.Store(id, s)
	go o.acceptAgents(ctx, id, s)
	t.Cleanup(func() { s.stop(); o.listeners.CompareAndDelete(id, s) })
	return s
}
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}
func connectAgent(t *testing.T, o *Operator, l *fakeListener) (*AgentConnection, net.Conn, *countConn) {
	t.Helper()
	c, peer := net.Pipe()
	counted := &countConn{Conn: c}
	l.results <- acceptResult{conn: counted}
	if _, err := peer.Write([]byte{0, 1, 'x'}); err != nil {
		t.Fatal(err)
	}
	var agent *AgentConnection
	eventually(t, func() bool {
		o.agents.Range(func(_, v any) bool {
			a := v.(*AgentConnection)
			if a.Conn == counted {
				agent = a
				return false
			}
			return true
		})
		return agent != nil
	})
	t.Cleanup(func() { peer.Close(); agent.close() })
	return agent, peer, counted
}
func TestStopRestartAndConcurrentClose(t *testing.T) {
	o := New(nil)
	l := newFakeListener()
	l.release = make(chan struct{})
	s := testGeneration(t, o, "race", l)
	_, _, conn := connectAgent(t, o, l)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := o.StopListener("race"); err != nil {
				t.Error(err)
			}
		}()
	}
	eventually(t, func() bool { return s.status() == stopping })
	if err := o.StartListener("race"); err == nil {
		t.Fatal("restart overlapped cleanup")
	}
	close(l.release)
	wg.Wait()
	if s.status() != stopped || l.closes.Load() != 1 || conn.closes.Load() != 1 {
		t.Fatalf("phase=%v listener closes=%d conn closes=%d", s.status(), l.closes.Load(), conn.closes.Load())
	}
	// Restart through the public entry point, retaining the previous generation.
	o.configs = []ListenerConfig{{Name: "race", Driver: "fake", Address: "https://example.invalid", StorageAccountName: "account", StorageAccountKey: "key"}}
	replacement := newFakeListener()
	o.listen = func(string, string, ...aznet.Option) (net.Listener, error) { return replacement, nil }
	if err := o.StartListener("race"); err != nil {
		t.Fatal(err)
	}
	next := mustState(t, o, "race")
	defer next.stop()
	agent, _, newConn := connectAgent(t, o, replacement)
	if err := s.stop(); err != nil {
		t.Fatal(err)
	}
	if newConn.closes.Load() != 0 {
		t.Fatal("old teardown closed replacement")
	}
	if _, ok := o.agents.Load(agent.ID); !ok {
		t.Fatal("replacement agent removed")
	}
}
func mustState(t *testing.T, o *Operator, id string) *ListenerState {
	t.Helper()
	v, ok := o.listeners.Load(id)
	if !ok {
		t.Fatal("missing listener")
	}
	return v.(*ListenerState)
}
func TestSessionDisconnectAndIdleAgent(t *testing.T) {
	o := New(nil)
	l := newFakeListener()
	testGeneration(t, o, "idle", l)
	agent, peer, c := connectAgent(t, o, l)
	// No SOCKS listener has started. Receiver must remain live and monitor EOF.
	if agent.server.ListenerAddr() != nil {
		t.Fatal("SOCKS started unexpectedly")
	}
	select {
	case <-agent.server.Ctx.Done():
		t.Fatal("idle agent reaped")
	case <-time.After(30 * time.Millisecond):
	}
	peer.Close()
	eventually(t, func() bool { _, ok := o.agents.Load(agent.ID); return !ok })
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); agent.close() }()
	}
	wg.Wait()
	if c.closes.Load() != 1 {
		t.Fatalf("duplicate close: %d", c.closes.Load())
	}
}

type classifiedFailure bool

func (classifiedFailure) Error() string     { return "accept failed" }
func (e classifiedFailure) Temporary() bool { return bool(e) }
func TestAcceptFailures(t *testing.T) {
	o := New(nil)
	for _, temporary := range []bool{false, true} {
		t.Run(map[bool]string{false: "permanent", true: "transient"}[temporary], func(t *testing.T) {
			l := newFakeListener()
			s := testGeneration(t, o, "failure", l)
			l.results <- acceptResult{err: classifiedFailure(temporary)}
			if !temporary {
				eventually(t, func() bool { return s.status() == stopped })
				if l.calls.Load() != 1 {
					t.Fatal("retried permanent error")
				}
				return
			}
			_, peer, _ := connectAgent(t, o, l)
			defer peer.Close()
			if !s.IsRunning() || l.calls.Load() < 2 {
				t.Fatal("transient failure did not recover")
			}
		})
	}
}
func TestAcceptBackoffCancellationAndBounds(t *testing.T) {
	o := New(nil)
	if acceptBackoff(1) != 100*time.Millisecond || acceptBackoff(20) != 5*time.Second {
		t.Fatal("unbounded backoff")
	}
	l := newFakeListener()
	s := testGeneration(t, o, "cancel", l)
	l.results <- acceptResult{err: classifiedFailure(true)}
	eventually(t, func() bool { return l.calls.Load() == 1 && len(l.results) == 0 })
	start := time.Now()
	s.stop()
	if time.Since(start) > time.Second {
		t.Fatal("backoff ignored cancellation")
	}
	if l.calls.Load() != 1 {
		t.Fatal("accept retried after cancellation")
	}
}
func TestCleanupErrorBlocksRestart(t *testing.T) {
	o := New(nil)
	l := newFakeListener()
	l.closeErr = errors.New("cleanup incomplete")
	s := testGeneration(t, o, "cleanup", l)
	if !errors.Is(s.stop(), l.closeErr) || !errors.Is(s.stop(), l.closeErr) {
		t.Fatal("lost cleanup result")
	}
	if s.status() != stopping {
		t.Fatal("reported stopped with outstanding ownership")
	}
	if o.StartListener("cleanup") == nil {
		t.Fatal("restart after incomplete cleanup")
	}
}

// Deadline-shortening preserves the production path while keeping this test fast.
type identityConn struct{ net.Conn }

func (c identityConn) SetReadDeadline(d time.Time) error {
	if !d.IsZero() {
		d = time.Now().Add(10 * time.Millisecond)
	}
	return c.Conn.SetReadDeadline(d)
}
func TestIdentityTimeout(t *testing.T) {
	o := New(nil)
	l := newFakeListener()
	s := testGeneration(t, o, "identity", l)
	c, peer := net.Pipe()
	defer peer.Close()
	l.results <- acceptResult{conn: identityConn{c}}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := peer.Read(b[:]); err != io.EOF {
		t.Fatalf("identity timeout did not close session: %v", err)
	}
	if !s.IsRunning() {
		t.Fatal("identity timeout stopped listener")
	}
}
func TestTwoListenersShareAccount(t *testing.T) {
	o := New(nil)
	a, b := bootstrap.Endpoints("one")
	c, d := bootstrap.Endpoints("two")
	if a == c || b == d || a == b {
		t.Fatal("bootstrap collision")
	}
	a2, b2 := bootstrap.Endpoints("one")
	if a != a2 || b != b2 {
		t.Fatal("namespace changed on restart")
	}
	l1, l2 := newFakeListener(), newFakeListener()
	s1 := testGeneration(t, o, "one", l1)
	testGeneration(t, o, "two", l2)
	_, _, _ = connectAgent(t, o, l1)
	agent, _, conn := connectAgent(t, o, l2)
	s1.stop()
	if conn.closes.Load() != 0 {
		t.Fatal("other listener session closed")
	}
	if _, ok := o.agents.Load(agent.ID); !ok {
		t.Fatal("other listener agent removed")
	}
}

// A backend may complete Accept after cancellation. The captured generation
// must reject that result even if the registry already contains a replacement.
type delayedListener struct {
	*fakeListener
	result  chan acceptResult
	entered chan struct{}
}

func (l *delayedListener) Accept() (net.Conn, error) {
	close(l.entered)
	r := <-l.result
	return r.conn, r.err
}
func TestStaleAcceptLoop(t *testing.T) {
	o := New(nil)
	for _, success := range []bool{true, false} {
		t.Run(map[bool]string{true: "connection", false: "failure"}[success], func(t *testing.T) {
			l := &delayedListener{fakeListener: newFakeListener(), result: make(chan acceptResult), entered: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			old := &ListenerState{op: o, listener: l, cancel: cancel, done: make(chan struct{}), acceptDone: make(chan struct{})}
			go o.acceptAgents(ctx, "stale", old)
			<-l.entered
			old.beginStop()
			nextListener := newFakeListener()
			next := testGeneration(t, o, "stale", nextListener)
			agent, _, replacementConn := connectAgent(t, o, nextListener)
			c, peer := net.Pipe()
			defer peer.Close()
			counted := &countConn{Conn: c}
			if success {
				l.result <- acceptResult{conn: counted}
			} else {
				c.Close()
				l.result <- acceptResult{err: classifiedFailure(false)}
			}
			old.stop()
			if !next.IsRunning() || replacementConn.closes.Load() != 0 {
				t.Fatal("stale acceptance stopped replacement")
			}
			if _, ok := o.agents.Load(agent.ID); !ok {
				t.Fatal("stale cleanup removed replacement agent")
			}
			if success && counted.closes.Load() != 1 {
				t.Fatal("late connection leaked")
			}
		})
	}
}
func TestStopDuringIdentity(t *testing.T) {
	o := New(nil)
	l := newFakeListener()
	s := testGeneration(t, o, "identity-stop", l)
	c, peer := net.Pipe()
	defer peer.Close()
	l.results <- acceptResult{conn: c}
	eventually(t, func() bool { return len(l.results) == 0 })
	done := make(chan struct{})
	go func() { s.stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop waited for identity timeout")
	}
}

func TestTransientAcceptBudget(t *testing.T) {
	o := New(nil)
	l := newFakeListener()
	for i := 0; i < 21; i++ {
		l.results <- acceptResult{err: classifiedFailure(true)}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &ListenerState{op: o, listener: l, cancel: cancel, done: make(chan struct{}), acceptDone: make(chan struct{})}
	var waits atomic.Int32
	go o.acceptAgentLoop(ctx, "budget", s, func(ctx context.Context, d time.Duration) bool {
		if d <= 0 || d > 5*time.Second {
			t.Error("invalid retry delay")
		}
		waits.Add(1)
		return ctx.Err() == nil
	})
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("retry budget not enforced")
	}
	if l.calls.Load() != 20 || waits.Load() != 19 || s.status() != stopped {
		t.Fatalf("calls=%d waits=%d phase=%v", l.calls.Load(), waits.Load(), s.status())
	}
}
