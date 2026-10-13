//go:build !js

package bootstrap

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"
	"time"

	"proxyblob/internal/diag"

	"github.com/atsika/aznet"
	"golang.org/x/net/http/httpproxy"
)

// ProxyOptions makes the endpoint reach storage through the machine's egress
// proxy when it has one. The proxy is chosen for every new connection (a PAC
// file can answer differently per destination); the settings themselves are
// read once per process.
func ProxyOptions() []aznet.Option {
	return []aznet.Option{aznet.WithProxyFunc(resolveProxy)}
}

// proxyResolver applies the standard HTTPS_PROXY, HTTP_PROXY and NO_PROXY
// variables when any proxy is set there, and the operating system's settings
// (including PAC and WPAD) otherwise. Credentials ride in the proxy URL, as
// http://user:pass@host:port, and the scheme is chosen from the proxy's
// challenge. A nil result connects directly.
func resolveProxy(_ context.Context, target *url.URL) (*aznet.ProxyConfig, error) {
	proxy, err := proxyFor(target)
	if err != nil || proxy == nil {
		return nil, err
	}
	return &aznet.ProxyConfig{URL: proxy, Auth: aznet.ProxyAuto}, nil
}

func proxyFor(target *url.URL) (*url.URL, error) {
	if env := httpproxy.FromEnvironment(); env.HTTPProxy != "" || env.HTTPSProxy != "" {
		return env.ProxyFunc()(target)
	}
	return systemProxy(target)
}

// directTimeout bounds a direct dial to a relay target, as before targets
// could go through the proxy.
const directTimeout = 10 * time.Second

var directDialer = &net.Dialer{Timeout: directTimeout}

// DialTarget connects the agent to a target it relays to: through the
// machine's egress proxy when the settings name one for the target, as the
// tunnel itself does, and directly otherwise. A target the proxy refuses or
// cannot reach is then tried directly, so hosts that were reachable before
// (an internal network the proxy does not serve) still are.
func DialTarget(ctx context.Context, target string) (net.Conn, error) {
	pc, err := resolveProxy(ctx, relayURL(target))
	if err != nil || pc == nil {
		// Unreadable settings must not cut the agent off what it reached before.
		return dialDirect(ctx, target)
	}
	proxied := aznet.NewProxyDialer(func(context.Context, *url.URL) (*aznet.ProxyConfig, error) { return pc, nil }, nil)
	return withDirectFallback(
		func(ctx context.Context, target string) (net.Conn, error) {
			return proxied.DialContext(ctx, "tcp", target)
		},
		dialDirect,
	)(ctx, target)
}

func dialDirect(ctx context.Context, target string) (net.Conn, error) {
	return directDialer.DialContext(ctx, "tcp", target)
}

// relayURL is the URL a relay target is looked up as. SOCKS does not say what
// runs inside the connection, and every relayed connection is a CONNECT
// tunnel, so only port 80 counts as http; any other port is https and follows
// HTTPS_PROXY.
func relayURL(target string) *url.URL {
	scheme := "https"
	if _, port, err := net.SplitHostPort(target); err == nil && port == "80" {
		scheme = "http"
	}
	return &url.URL{Scheme: scheme, Host: target}
}

type dialFunc = func(ctx context.Context, target string) (net.Conn, error)

// withDirectFallback dials through proxied, then directly when the proxy
// itself refused or failed.
func withDirectFallback(proxied, direct dialFunc) dialFunc {
	return func(ctx context.Context, target string) (net.Conn, error) {
		conn, err := proxied(ctx, target)
		if err == nil {
			return conn, nil
		}
		perr := proxyError(err)
		if perr == nil || ctx.Err() != nil {
			// Not the proxy's answer: a direct dial, or one cut short, failed.
			return nil, err
		}
		conn, err = direct(ctx, target)
		if err == nil {
			return conn, nil
		}
		// A direct dial that times out says only that egress is filtered; the
		// proxy's answer is the better explanation then.
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil, perr
		}
		return nil, err
	}
}

// proxyError reduces a failure answered by the egress proxy to a numeric
// diagnostic, dropping the cause (it may name the proxy's credentials). It is
// nil for any other error.
func proxyError(err error) error {
	switch {
	case errors.Is(err, aznet.ErrProxyAuthRequired), errors.Is(err, aznet.ErrProxyAuthFailed),
		errors.Is(err, aznet.ErrProxyAuthUnsupported), errors.Is(err, aznet.ErrInvalidProxy):
		return diag.ErrProxyDenied
	case errors.Is(err, aznet.ErrProxyConnect):
		return diag.ErrProxyFailed
	}
	return nil
}

// pacURL is target as a browser would hand it to a PAC script: no default
// port, and a path, so patterns such as "https://*.corp/*" match.
func pacURL(target *url.URL) *url.URL {
	u := *target
	if (u.Scheme == "https" && u.Port() == "443") || (u.Scheme == "http" && u.Port() == "80") {
		u.Host = u.Hostname()
		if strings.Contains(u.Host, ":") {
			u.Host = "[" + u.Host + "]"
		}
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return &u
}
