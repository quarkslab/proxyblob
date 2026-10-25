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

// route is how the proxy settings say to reach a destination.
type route struct {
	// proxies are tried in order; none means a direct connection.
	proxies []*url.URL
	// byDefault marks proxies the settings give every destination they do not
	// single out (a static proxy, or a PAC file's catch-all rule) rather than an
	// answer about this one, so a direct connection is worth trying first.
	byDefault bool
}

// routeFor applies the standard HTTPS_PROXY, HTTP_PROXY and NO_PROXY variables
// when any proxy is set there, and the operating system's settings (including
// PAC and WPAD) otherwise. Credentials ride in the proxy URL, as
// http://user:pass@host:port, and the scheme is chosen from the proxy's
// challenge.
func routeFor(target *url.URL) (route, error) {
	if env := httpproxy.FromEnvironment(); env.HTTPProxy != "" || env.HTTPSProxy != "" {
		proxy, err := env.ProxyFunc()(target)
		if err != nil || proxy == nil {
			return route{}, err
		}
		return route{proxies: []*url.URL{proxy}, byDefault: true}, nil
	}
	return systemRoute(target)
}

// resolveProxy is the storage tunnel's choice: the first proxy of the route.
// A nil result connects directly.
func resolveProxy(_ context.Context, target *url.URL) (*aznet.ProxyConfig, error) {
	r, err := routeFor(target)
	if err != nil || len(r.proxies) == 0 {
		return nil, err
	}
	return proxyConfig(r.proxies[0]), nil
}

func proxyConfig(proxy *url.URL) *aznet.ProxyConfig {
	return &aznet.ProxyConfig{URL: proxy, Auth: aznet.ProxyAuto}
}

const (
	// setupTimeout bounds everything DialTarget tries for one target, so a dead
	// host releases its tunnel stream as soon as a direct dial would.
	setupTimeout = 10 * time.Second
	// directFirstTimeout is the share of setupTimeout a direct attempt gets
	// before the proxies, when they are only the settings' default.
	directFirstTimeout = 5 * time.Second
)

var directDialer net.Dialer // bounded by the caller's context

// DialTarget connects the agent to a target it relays to, within setupTimeout:
//   - directly when the settings name no proxy for it;
//   - through the proxies they name for this target specifically (a PAC rule),
//     then directly if those refuse or fail;
//   - directly first when the proxies are only the default, then through them
//     unless the target answered (a refused port needs no proxy), so internal
//     hosts are reached without the proxy and hosts behind it still are.
func DialTarget(ctx context.Context, target string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	r, err := routeFor(relayURL(target))
	if err != nil || len(r.proxies) == 0 {
		// Unreadable settings must not cut the agent off what it reached before.
		return dialDirect(ctx, target)
	}
	if r.byDefault {
		return directFirst(dialDirect, viaProxies(r.proxies))(ctx, target)
	}
	return proxiesFirst(viaProxies(r.proxies), dialDirect)(ctx, target)
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

// viaProxies tunnels through each proxy in turn, moving on only when one
// cannot be reached: a proxy that answered and refused speaks for the others.
func viaProxies(proxies []*url.URL) dialFunc {
	return func(ctx context.Context, target string) (net.Conn, error) {
		var err error
		for _, proxy := range proxies {
			pc := proxyConfig(proxy)
			d := aznet.NewProxyDialer(func(context.Context, *url.URL) (*aznet.ProxyConfig, error) { return pc, nil }, nil)
			var conn net.Conn
			conn, err = d.DialContext(ctx, "tcp", target)
			if err == nil || !errors.Is(err, aznet.ErrProxyUnreachable) || ctx.Err() != nil {
				return conn, err
			}
		}
		return nil, err
	}
}

// proxiesFirst dials through proxied, then directly when the proxy itself
// refused or failed.
func proxiesFirst(proxied, direct dialFunc) dialFunc {
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
		return nil, explain(err, perr)
	}
}

// directFirst dials directly for up to directFirstTimeout, then through
// proxied unless the target itself answered.
func directFirst(direct, proxied dialFunc) dialFunc {
	return func(ctx context.Context, target string) (net.Conn, error) {
		dctx, cancel := context.WithTimeout(ctx, directFirstTimeout)
		conn, err := direct(dctx, target)
		cancel()
		if err == nil || ctx.Err() != nil || diag.MapNetError(err) == diag.ErrConnectionRefused {
			return conn, err
		}
		conn, perr := proxied(ctx, target)
		if perr == nil {
			return conn, nil
		}
		if p := proxyError(perr); p != nil && ctx.Err() == nil {
			return nil, explain(err, p)
		}
		return nil, err
	}
}

// explain picks the better reason a target is out of reach: a direct dial
// that timed out says only that egress is filtered, so the proxy's answer
// explains more then.
func explain(direct, proxy error) error {
	var ne net.Error
	if errors.As(direct, &ne) && ne.Timeout() {
		return proxy
	}
	return direct
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
