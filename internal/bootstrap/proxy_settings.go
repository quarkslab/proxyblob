//go:build !js

package bootstrap

import (
	"net"
	"net/url"
	"path"
	"strings"
)

// parseProxyList reads WinHTTP's answer for a PAC file's
// "PROXY a:8080; PROXY b:8080": entries separated by semicolons or spaces.
// DIRECT entries are dropped, since the agent falls back to a direct
// connection anyway.
func parseProxyList(list string) []*url.URL {
	var proxies []*url.URL
	for _, entry := range strings.FieldsFunc(list, func(r rune) bool { return r == ';' || r == ' ' || r == '\t' }) {
		if strings.EqualFold(entry, "DIRECT") {
			continue
		}
		if proxy := parseProxy(entry); proxy != nil {
			proxies = append(proxies, proxy)
		}
	}
	return proxies
}

// parseProxy reads a proxy given as host:port or as a URL.
func parseProxy(s string) *url.URL {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return nil
	}
	return u
}

// bypassed applies an Internet Options bypass list (ProxyOverride): entries
// are wildcard patterns such as "10.*" or "*.corp.local", matched against
// the host and against host:port, and "<local>" matches any host without a
// dot. Loopback is never proxied, as with NO_PROXY.
func bypassed(target *url.URL, patterns []string) bool {
	host := strings.ToLower(target.Hostname())
	if ip := net.ParseIP(host); host == "localhost" || ip != nil && ip.IsLoopback() {
		return true
	}
	hostPort := host
	if port := target.Port(); port != "" {
		hostPort = net.JoinHostPort(host, port)
	}
	for _, p := range patterns {
		p = strings.ToLower(strings.TrimSpace(p))
		if i := strings.Index(p, "://"); i >= 0 {
			p = p[i+3:]
		}
		switch {
		case p == "":
		case p == "<local>":
			if !strings.Contains(host, ".") && !strings.Contains(host, ":") {
				return true
			}
		default:
			if ok, _ := path.Match(p, host); ok {
				return true
			}
			if ok, _ := path.Match(p, hostPort); ok {
				return true
			}
		}
	}
	return false
}
