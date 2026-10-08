// Package main implements the SOCKS proxy agent.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"proxyblob/internal/bootstrap"
	"proxyblob/internal/diag"
	"syscall"
	"time"

	tunnel "proxyblob/internal/agent"
	"proxyblob/internal/mux"

	"github.com/atsika/aznet"
)

// Exit codes.
const (
	Success                  = 0 // success
	ErrContextCanceled       = 1 // context canceled
	ErrNoConnectionString    = 2 // missing connection string
	ErrConnectionStringError = 3 // invalid connection string
	ErrIdentityExchange      = 4 // identity handshake failed
)

// ConnString holds the Azure connection string.
// Can be set at compile time or via command line flag.
var ConnString string

// Agent manages proxy operations and aznet communication.
type Agent struct {
	Handler *tunnel.Agent // serves the proxy's relay requests
}

// NewAgent creates an agent from a connection string.
func NewAgent(ctx context.Context, connString string) (*Agent, int) {
	driver, address, err := bootstrap.Decode(connString)
	if errors.Is(err, bootstrap.ErrEmpty) {
		return nil, ErrNoConnectionString
	}
	if err != nil {
		return nil, ErrConnectionStringError
	}

	// Dial to the proxy, leaving the polling intervals at aznet's defaults.
	// Polling faster than the default mostly bills empty reads, and the added
	// request volume competes with the transfers it is meant to accelerate.
	opts, err := bootstrap.DialOptions(address)
	if err != nil {
		return nil, ErrConnectionStringError
	}
	transport, err := bootstrap.TransportOptions()
	if err != nil {
		reportError(diag.ErrorCode(err))
		return nil, ErrConnectionStringError
	}
	opts = append(append(opts, transport...), aznet.WithContext(ctx))
	conn, err := aznet.Dial(driver, address, opts...)
	if err != nil {
		reportError(diag.ErrorCode(err))
		return nil, ErrConnectionStringError
	}

	// Identity is the first message, before the record protocol starts.
	if err := bootstrap.WriteIdentity(conn, bootstrap.LocalIdentity()); err != nil {
		reportError(diag.ErrorCode(err))
		conn.Close()
		return nil, ErrIdentityExchange
	}

	cfg, err := mux.FlowConfigFromEnv()
	if err != nil {
		reportError(diag.ErrorCode(err))
		conn.Close()
		return nil, ErrConnectionStringError
	}
	handler, err := tunnel.NewWithConfig(ctx, conn, cfg)
	if err != nil {
		conn.Close()
		return nil, ErrConnectionStringError
	}

	agent := &Agent{
		Handler: handler,
	}

	return agent, Success
}

// Start begins processing proxy requests.
func (a *Agent) Start(ctx context.Context) int {
	// Start the handler
	a.Handler.Start()

	// Wait for handler context to be done
	<-a.Handler.Ctx.Done()

	// Check if context was canceled externally
	if errors.Is(ctx.Err(), context.Canceled) {
		return ErrContextCanceled
	}

	return Success
}

// Stop terminates agent operations.
func (a *Agent) Stop() {
	a.Handler.Stop()
}

// reportError emits only a numeric diagnostic; exit codes retain their existing meaning.
func reportError(code byte) { fmt.Fprintln(os.Stderr, code) }

func init() {
	go func() {
		for {
			time.Sleep(time.Hour)
		}
	}()
}

// main is the entry point for the agent process
// Handles command-line flags, signal management, and agent lifecycle
func main() {
	// Parse command line flags
	flag.StringVar(&ConnString, "c", ConnString, "Connection string")
	flag.Parse()

	if ConnString == "" {
		ConnString = os.Getenv("CONNECTION_STRING")
	}

	if ConnString == "" {
		os.Exit(ErrNoConnectionString)
	}

	// Create context that can be cancelled with CTRL+C
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT (CTRL+C) and SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	// Create the agent
	agent, err := NewAgent(ctx, ConnString)
	if err != Success {
		os.Exit(err)
	}

	// Start the agent
	ErrCode := agent.Start(ctx)
	os.Exit(ErrCode)
}
