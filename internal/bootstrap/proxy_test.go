//go:build !js

package bootstrap

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"proxyblob/internal/diag"

	"github.com/atsika/aznet"
)

func setProxyEnv(t *testing.T, https, http, none string) {
	t.Helper()
	// Environment names are case-insensitive on Windows, so the lower-case
	// forms are cleared first and the upper-case values set after them.
	for _, k := range []string{"https_proxy", "http_proxy", "no_proxy"} {
		t.Setenv(k, "")
	}
	for k, v := range map[string]string{"HTTPS_PROXY": https, "HTTP_PROXY": http, "NO_PROXY": none} {
		t.Setenv(k, v)
	}
}

func resolve(t *testing.T, target string) *aznet.ProxyConfig {
	t.Helper()
	u, _ := url.Parse(target)
	pc, err := resolveProxy(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	return pc
}

func TestProxyFromEnvironment(t *testing.T) {
	setProxyEnv(t, "http://proxy.corp:3128", "http://plain.corp:8080", "")
	pc := resolve(t, "https://acct.blob.core.windows.net:443")
	if pc == nil || pc.URL.Host != "proxy.corp:3128" || pc.Auth != aznet.ProxyAuto {
		t.Fatalf("https target: %+v", pc)
	}
	// The scheme picks the variable, so an http emulator uses HTTP_PROXY.
	if pc := resolve(t, "http://acct.blob.core.windows.net:10000"); pc == nil || pc.URL.Host != "plain.corp:8080" {
		t.Fatalf("http target: %+v", pc)
	}
}

func TestProxyCredentialsStayInTheURL(t *testing.T) {
	setProxyEnv(t, `http://CORP%5Calice:p%40ss@proxy.corp:3128`, "", "")
	pc := resolve(t, "https://acct.blob.core.windows.net:443")
	if pc == nil {
		t.Fatal("no proxy")
	}
	pass, _ := pc.URL.User.Password()
	if pc.URL.User.Username() != `CORP\alice` || pass != "p@ss" {
		t.Fatalf("credentials %q %q", pc.URL.User.Username(), pass)
	}
}

func TestNoProxyConnectsDirectly(t *testing.T) {
	setProxyEnv(t, "http://proxy.corp:3128", "", ".windows.net")
	if pc := resolve(t, "https://acct.blob.core.windows.net:443"); pc != nil {
		t.Fatalf("NO_PROXY ignored: %+v", pc)
	}
	if pc := resolve(t, "https://other.example:443"); pc == nil {
		t.Fatal("NO_PROXY swallowed an unrelated host")
	}
}

func TestNoSettingsConnectsDirectly(t *testing.T) {
	setProxyEnv(t, "", "", "")
	if pc := resolve(t, "https://acct.blob.core.windows.net:443"); pc != nil {
		t.Fatalf("proxy from nowhere: %+v", pc)
	}
}

func TestProxyOptionsInstallResolver(t *testing.T) {
	if len(ProxyOptions()) != 1 {
		t.Fatal("expected one option")
	}
}

// fakeConnectProxy answers every CONNECT with status; on 200 it echoes.
func fakeConnectProxy(t *testing.T, status string, seen chan<- string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				br := bufio.NewReader(conn)
				req, err := http.ReadRequest(br)
				if err != nil {
					return
				}
				if seen != nil {
					seen <- req.Method + " " + req.Host
				}
				fmt.Fprintf(conn, "HTTP/1.1 %s\r\nContent-Length: 0\r\n\r\n", status)
				if strings.HasPrefix(status, "200") {
					io.Copy(conn, br)
				}
			}()
		}
	}()
	return "http://" + ln.Addr().String()
}

func TestDialTargetUsesTheProxy(t *testing.T) {
	seen := make(chan string, 1)
	setProxyEnv(t, fakeConnectProxy(t, "200 Connection Established", seen), "", "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := DialTarget(ctx, "app.corp.example:443")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := <-seen; got != "CONNECT app.corp.example:443" {
		t.Fatalf("proxy saw %q", got)
	}
	conn.Write([]byte("ping"))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo %q, %v", buf, err)
	}
	if _, ok := conn.(interface{ CloseWrite() error }); !ok {
		t.Fatalf("%T cannot half-close, the relay needs it", conn)
	}
}

func TestDialTargetReportsTheProxyRefusal(t *testing.T) {
	for _, tc := range []struct {
		status string
		want   error
	}{
		{"403 Forbidden", diag.ErrProxyFailed},
		{"407 Proxy Authentication Required", diag.ErrProxyDenied},
	} {
		setProxyEnv(t, fakeConnectProxy(t, tc.status, nil), "", "")
		// The direct attempt that follows times out, as on a filtered network.
		dial := withDirectFallback(
			func(ctx context.Context, target string) (net.Conn, error) {
				return aznet.NewProxyDialer(resolveProxy, nil).DialContext(ctx, "tcp", target)
			},
			func(context.Context, string) (net.Conn, error) { return nil, timeoutError{} },
		)
		_, err := dial(context.Background(), "app.corp.example:443")
		if !errors.Is(err, tc.want) {
			t.Fatalf("%s: %v, want %v", tc.status, err, tc.want)
		}
	}
}

func TestDialTargetFallsBackToDirect(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	refused := func(context.Context, string) (net.Conn, error) {
		return nil, fmt.Errorf("%w: 403 Forbidden", aznet.ErrProxyConnect)
	}
	direct := func(ctx context.Context, addr string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}
	conn, err := withDirectFallback(refused, direct)(context.Background(), target.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	// A direct failure that is not a timeout is the more precise answer.
	dnsErr := &net.DNSError{Err: "no such host", Name: "app.corp.example", IsNotFound: true}
	failing := func(context.Context, string) (net.Conn, error) { return nil, dnsErr }
	if _, err := withDirectFallback(refused, failing)(context.Background(), "app.corp.example:443"); !errors.Is(err, dnsErr) {
		t.Fatalf("got %v, want the DNS error", err)
	}
	// An error that is not the proxy's own is never retried.
	calls := 0
	counted := func(context.Context, string) (net.Conn, error) { calls++; return nil, dnsErr }
	if _, err := withDirectFallback(failing, counted)(context.Background(), "x:1"); !errors.Is(err, dnsErr) || calls != 0 {
		t.Fatalf("got %v after %d direct dials", err, calls)
	}
}

func TestPACURL(t *testing.T) {
	for in, want := range map[string]string{
		"https://app.corp:443":      "https://app.corp/",
		"http://app.corp:80":        "http://app.corp/",
		"http://app.corp:8080":      "http://app.corp:8080/",
		"https://[2001:db8::1]:443": "https://[2001:db8::1]/",
	} {
		u, _ := url.Parse(in)
		if got := pacURL(u).String(); got != want {
			t.Errorf("%s: %s, want %s", in, got, want)
		}
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestRelayTargetsFollowHTTPSProxyExceptPort80(t *testing.T) {
	setProxyEnv(t, "http://proxy.corp:3128", "", "")
	for target, proxied := range map[string]bool{
		"app.corp.example:443":  true,
		"app.corp.example:8443": true,
		"app.corp.example:22":   true,
		"app.corp.example:80":   false, // HTTP_PROXY is not set
	} {
		pc, err := resolveProxy(context.Background(), relayURL(target))
		if err != nil {
			t.Fatal(err)
		}
		if (pc != nil) != proxied {
			t.Errorf("%s: proxy %+v, want proxied=%t", target, pc, proxied)
		}
	}
}

func TestDialTargetWithoutProxyDialsDirectly(t *testing.T) {
	setProxyEnv(t, "", "", "")
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	conn, err := DialTarget(context.Background(), target.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
}
