// Command tunnel measures ProxyBlob end to end over a live aznet listener.
//
//	tunnel serve -config config.json -listener NAME [-mux proxyblob|yamux|smux] [-socks ADDR] [-metrics ADDR]
//	tunnel echo  -listen ADDR
//	tunnel probe -socks ADDR -target ADDR [-n N] [-size BYTES] [-interval DURATION]
//
// serve runs the proxy (aznet listener, SOCKS5 server) and the agent (aznet
// dialer) in one process with aznet's production polling and keep-alive
// defaults, like cmd/proxy and cmd/agent. It prints "READY" once SOCKS accepts
// connections and serves GET /metrics: cumulative SDK attempts by operation
// for both ends, so callers can difference snapshots around a workload.
// Drive bulk traffic through the SOCKS port (for example proxychains + iperf3);
// probe measures interactive round trips through the same port.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	socks "proxyblob/internal/agent"
	"proxyblob/internal/mux"
	proxy "proxyblob/internal/proxy"

	"github.com/atsika/aznet"
	"github.com/google/uuid"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: tunnel serve|echo|probe [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "echo":
		err = echo(os.Args[2:])
	case "probe":
		err = probe(os.Args[2:])
	default:
		err = fmt.Errorf("unknown mode %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "tunnel:", err)
		os.Exit(1)
	}
}

func listenAddress(path, name string) (driver, address string, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	var cfg struct {
		Listeners []struct {
			Name    string `json:"name"`
			Driver  string `json:"driver"`
			Address string `json:"address"`
			Account string `json:"storage_account"`
			Key     string `json:"storage_account_key"`
		} `json:"listeners"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return "", "", err
	}
	for _, l := range cfg.Listeners {
		if l.Name != name {
			continue
		}
		u, err := url.Parse(l.Address)
		if err != nil {
			return "", "", err
		}
		return l.Driver, (&url.URL{Scheme: u.Scheme, User: url.UserPassword(l.Account, l.Key), Host: u.Host, Path: u.Path}).String(), nil
	}
	return "", "", fmt.Errorf("listener %q not found", name)
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	config := fs.String("config", "config.json", "ProxyBlob config with listener keys")
	name := fs.String("listener", "", "listener name in the config")
	socksAddr := fs.String("socks", "127.0.0.1:1080", "SOCKS5 listen address")
	metricsAddr := fs.String("metrics", "127.0.0.1:1081", "metrics HTTP listen address")
	muxKind := fs.String("mux", "proxyblob", "multiplexer: proxyblob, yamux or smux (CONNECT only)")
	fs.Parse(args)

	driver, address, err := listenAddress(*config, *name)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	metrics := aznet.NewDefaultMetrics()
	// Unique bootstrap names keep concurrent or leftover runs apart.
	id := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	opts := []aznet.Option{aznet.WithContext(ctx), aznet.WithMetrics(metrics), aznet.WithEndpoints("h"+id, "t"+id)}

	started := time.Now()
	ln, err := aznet.Listen(driver, address, opts...)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	l := ln.(*aznet.Listener)
	defer func() {
		l.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		l.CleanupBootstrap(cleanup)
	}()
	connString, err := l.ConnectionString()
	if err != nil {
		return err
	}
	cfg, err := mux.FlowConfigFromEnv()
	if err != nil {
		return err
	}

	dialed := make(chan error, 1)
	var agentConn net.Conn
	go func() {
		var err error
		agentConn, err = aznet.Dial(driver, connString, opts...)
		dialed <- err
	}()
	proxyConn, err := l.Accept()
	if err != nil {
		return fmt.Errorf("accept: %w", err)
	}
	if err := <-dialed; err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer proxyConn.Close()
	defer agentConn.Close()

	stopped := make(chan error, 2)
	var socksListen net.Addr
	if *muxKind == "proxyblob" {
		agent, err := socks.NewSocksHandlerWithConfig(ctx, agentConn, cfg)
		if err != nil {
			return err
		}
		server, err := proxy.NewProxyServerWithConfig(ctx, proxyConn, cfg)
		if err != nil {
			return err
		}
		agent.Start("")
		server.Start(*socksAddr)
		defer agent.Stop()
		defer server.Stop()
		if socksListen = server.ListenerAddr(); socksListen == nil {
			return errors.New("SOCKS listener did not start")
		}
		go func() { <-server.Ctx.Done(); stopped <- errors.New("proxy stopped") }()
		go func() { <-agent.Ctx.Done(); stopped <- errors.New("agent stopped") }()
	} else {
		// ProxyBlob caps StreamWindow at 1 MiB; libraries may exceed it.
		window := cfg.StreamWindow
		if v := os.Getenv("MUX_STREAM_WINDOW"); v != "" {
			if _, err := fmt.Sscan(v, &window); err != nil {
				return err
			}
		}
		proxySession, err := newSession(*muxKind, proxyConn, true, window)
		if err != nil {
			return err
		}
		agentSession, err := newSession(*muxKind, agentConn, false, window)
		if err != nil {
			return err
		}
		ln, err := net.Listen("tcp", *socksAddr)
		if err != nil {
			return err
		}
		defer ln.Close()
		socksListen = ln.Addr()
		go muxProxy(proxySession, ln)
		go func() { stopped <- fmt.Errorf("agent session: %w", muxAgent(agentSession)) }()
	}

	routes := http.NewServeMux()
	routes.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		counts := map[string]int64{}
		var total int64
		for a, n := range metrics.RequestCounts() {
			key := fmt.Sprintf("%s %s %d", a.Operation, a.Method, a.StatusCode)
			if a.Retry {
				key += " retry"
			}
			counts[key] += n
			total += n
		}
		json.NewEncoder(w).Encode(map[string]any{
			"total": total, "requests": counts,
			"bytes_sent": metrics.GetBytesSent(), "bytes_received": metrics.GetBytesReceived(),
		})
	})
	ms := &http.Server{Addr: *metricsAddr, Handler: routes}
	go ms.ListenAndServe()
	defer ms.Close()

	fmt.Printf("READY driver=%s mux=%s socks=%s setup_ms=%d\n", driver, *muxKind, socksListen, time.Since(started).Milliseconds())
	select {
	case <-ctx.Done():
		return nil
	case err := <-stopped:
		return err
	}
}

func echo(args []string) error {
	fs := flag.NewFlagSet("echo", flag.ExitOnError)
	addr := fs.String("listen", "127.0.0.1:7007", "listen address")
	fs.Parse(args)
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() { defer c.Close(); io.Copy(c, c) }()
	}
}

// socksConnect performs a no-auth SOCKS5 CONNECT to an IPv4 target.
func socksConnect(proxyAddr, target string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host).To4()
	if ip == nil {
		return nil, errors.New("probe target must be IPv4")
	}
	var p uint16
	fmt.Sscan(port, &p)
	c, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		return nil, err
	}
	c.SetDeadline(time.Now().Add(60 * time.Second))
	req := []byte{5, 1, 0, 5, 1, 0, 1}
	req = append(req, ip...)
	req = binary.BigEndian.AppendUint16(req, p)
	if _, err := c.Write(req); err != nil {
		c.Close()
		return nil, err
	}
	var reply [2 + 10]byte
	if _, err := io.ReadFull(c, reply[:]); err != nil || reply[1] != 0 || reply[3] != 0 {
		c.Close()
		return nil, fmt.Errorf("SOCKS CONNECT failed: %v %v", reply, err)
	}
	c.SetDeadline(time.Time{})
	return c, nil
}

func probe(args []string) error {
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	socksAddr := fs.String("socks", "127.0.0.1:1080", "SOCKS5 address")
	target := fs.String("target", "127.0.0.1:7007", "echo target (IPv4)")
	n := fs.Int("n", 30, "round trips")
	size := fs.Int("size", 64, "bytes per round trip")
	interval := fs.Duration("interval", 200*time.Millisecond, "pause between round trips")
	fs.Parse(args)

	connectStart := time.Now()
	c, err := socksConnect(*socksAddr, *target)
	if err != nil {
		return err
	}
	defer c.Close()
	connect := time.Since(connectStart)
	payload := make([]byte, *size)
	got := make([]byte, *size)
	var rtts []time.Duration
	for i := range *n {
		binary.BigEndian.PutUint32(payload, uint32(i))
		start := time.Now()
		if _, err := c.Write(payload); err != nil {
			return err
		}
		if _, err := io.ReadFull(c, got); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(got) != uint32(i) {
			return errors.New("echo out of order")
		}
		rtts = append(rtts, time.Since(start))
		time.Sleep(*interval)
	}
	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })
	pct := func(p int) float64 { return float64(rtts[(p*len(rtts)+99)/100-1].Microseconds()) / 1000 }
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"connect_ms": float64(connect.Microseconds()) / 1000, "samples": len(rtts),
		"p50_ms": pct(50), "p95_ms": pct(95), "max_ms": pct(100),
	})
}
