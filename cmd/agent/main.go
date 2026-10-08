// Package main implements the SOCKS proxy agent.
package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"os/user"
	"strings"
	"syscall"
	"time"

	proxy "proxyblob/internal/agent"
	"proxyblob/pkg/protocol"

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

// MaxIdentityLen bounds the identity payload so the 2-byte length prefix can never
// disagree with the bytes that follow. Must match the proxy-side limit.
const MaxIdentityLen = 512

// ConnString holds the Azure connection string.
// Can be set at compile time or via command line flag.
var ConnString string

// Agent manages proxy operations and aznet communication.
type Agent struct {
	Handler *proxy.SocksHandler // SOCKS handler
}

// connectionOptions restores the bootstrap namespace advertised by the proxy.
// Older credentials without metadata retain aznet's default endpoints.
func connectionOptions(address string) ([]aznet.Option, error) {
	u, err := url.Parse(address)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	h, t := q.Get("proxyblob-handshake"), q.Get("proxyblob-token")
	if h == "" && t == "" {
		return nil, nil
	}
	if h == "" || t == "" || h == t || q.Get(h) == "" || q.Get(t) == "" {
		return nil, protocol.ErrBootstrapNamespace
	}
	return []aznet.Option{aznet.WithEndpoints(h, t)}, nil
}

// NewAgent creates an agent from a connection string.
func NewAgent(ctx context.Context, connString string) (*Agent, int) {
	// Decode and parse the connection string
	driver, address, errCode := ParseConnectionString(connString)
	if errCode != Success {
		return nil, errCode
	}

	// Dial to the proxy, leaving the polling intervals at aznet's defaults.
	// Polling faster than the default mostly bills empty reads, and the added
	// request volume competes with the transfers it is meant to accelerate.
	opts, err := connectionOptions(address)
	if err != nil {
		return nil, ErrConnectionStringError
	}
	opts = append(opts, aznet.WithContext(ctx))
	conn, err := aznet.Dial(driver, address, opts...)
	if err != nil {
		reportError(protocol.ErrorCode(err))
		return nil, ErrConnectionStringError
	}

	// Send identity (user@host) as the first message, before the record protocol starts.
	//
	// Wire format: 2-byte big-endian length N (1 <= N <= MaxIdentityLen), followed by
	// exactly N bytes of identity payload. The prefix and payload are emitted as a single
	// Write so no other writer can interleave bytes between them.
	//
	// FLAG DAY: this framing must match the reader in cmd/proxy/main.go
	// (AcceptAgentLoopForListener). There is no backward compatibility with the old
	// unframed exchange -- agent and proxy must be rebuilt and deployed together.
	identity := getIdentity()
	if len(identity) > MaxIdentityLen {
		identity = identity[:MaxIdentityLen]
	}
	identityFrame := make([]byte, 2+len(identity))
	binary.BigEndian.PutUint16(identityFrame[:2], uint16(len(identity)))
	copy(identityFrame[2:], identity)
	if _, err := conn.Write(identityFrame); err != nil {
		// Fatal for this connection: a partial identity leaves unconsumed bytes at the
		// head of the peer's stream, which would be parsed as a record header and
		// desynchronize framing permanently. Aborting is strictly safer than continuing.
		reportError(protocol.ErrorCode(err))
		conn.Close()
		return nil, ErrIdentityExchange
	}

	// Create SOCKS handler with direct connection (no transport wrapper)
	cfg, err := protocol.FlowConfigFromEnv()
	if err != nil {
		reportError(protocol.ErrorCode(err))
		conn.Close()
		return nil, ErrConnectionStringError
	}
	handler, err := proxy.NewSocksHandlerWithConfig(ctx, conn, cfg)
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
	a.Handler.Start("")

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

// ParseConnectionString extracts the driver and aznet address from a connection string.
func ParseConnectionString(connString string) (string, string, int) {
	// Check for empty string first
	if connString == "" {
		return "", "", ErrNoConnectionString
	}

	// Try to decode the base64 encoded string
	decoded, err := base64.RawStdEncoding.DecodeString(connString)
	if err != nil {
		return "", "", ErrConnectionStringError
	}

	// Parse the string - it might be in the new format: driver|address
	// or the old format: address (where driver is extracted from scheme)
	str := string(decoded)
	if strings.Contains(str, "|") {
		parts := strings.SplitN(str, "|", 2)
		return parts[0], parts[1], Success
	}

	return "", "", ErrConnectionStringError
}

// getIdentity returns "user@hostname" for agent identification.
func getIdentity() string {
	username := "unknown"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s@%s", username, hostname)
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
