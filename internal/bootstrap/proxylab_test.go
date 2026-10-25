//go:build !js

package bootstrap

// The agent's proxy detection against the corporate lab in aznet's
// tests/proxylab (docker compose up -d --build there first):
//
//	AZNET_PROXYLAB=1 go test -v -count=1 -run TestProxyLab ./internal/bootstrap/

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/atsika/aznet"
)

const labAccount = "devstoreaccount1:Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq%2FK1SZFPTOtr%2FKBHBeksoGMGw%3D%3D"

// With AZNET_PROXYLAB_INSIDE=1 the test runs inside the lab network (aznet's
// tests/proxylab/run-linux.sh style), reaching everything by service name;
// otherwise it uses the ports published on localhost.
var labInside = os.Getenv("AZNET_PROXYLAB_INSIDE") != ""

// labProxy returns the address of the proxy published on hostPort, or its
// service name inside the network.
func labProxy(service string, hostPort int) string {
	if labInside {
		return service + ":3128"
	}
	return labHost() + ":" + strconv.Itoa(hostPort)
}

// labHost is where the lab's ports are published: this machine, or the one
// named by AZNET_PROXYLAB_HOST.
func labHost() string {
	if h := os.Getenv("AZNET_PROXYLAB_HOST"); h != "" {
		return h
	}
	return "localhost"
}

func labAzurite() string {
	if labInside {
		return "http://" + labAccount + "@azurite:10000/devstoreaccount1"
	}
	return "http://" + labAccount + "@" + labHost() + ":10000/devstoreaccount1"
}

func labConnString(t *testing.T) string {
	t.Helper()
	ln, err := aznet.Listen("azblob", labAzurite())
	if err != nil {
		t.Fatalf("listen (is the lab up?): %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	conn, err := ln.(*aznet.Listener).ConnectionString()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	// The proxies reach Azurite by its service name.
	return strings.Replace(conn, labHost()+":10000", "azurite:10000", 1)
}

// dialAgent dials the way cmd/agent does, with ProxyOptions as its only proxy
// configuration, and returns the dial error and the trace.
func dialAgent(t *testing.T, conn string) (error, string) {
	t.Helper()
	var mu sync.Mutex
	var trace bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	opts := append(ProxyOptions(), aznet.WithContext(ctx), aznet.WithProxyLog(func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		trace.WriteString(strings.TrimSpace(sprintf(format, args...)) + "\n")
	}))
	c, err := aznet.Dial("azblob", conn, opts...)
	if err == nil {
		defer c.Close()
		c.SetDeadline(time.Now().Add(20 * time.Second))
		if _, err = c.Write([]byte("agent")); err == nil {
			buf := make([]byte, 5)
			_, err = io.ReadFull(c, buf)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	return err, trace.String()
}

func TestProxyLab(t *testing.T) {
	if os.Getenv("AZNET_PROXYLAB") == "" {
		t.Skip("set AZNET_PROXYLAB=1 with the lab running")
	}
	conn := labConnString(t)
	t.Setenv("KRB5_CONFIG", "") // the lab has no Kerberos setup on this machine; NTLM or Basic must do

	// The machine's own proxy settings, with no environment variables: on
	// Windows Internet Options / PAC / WPAD. Point them at the lab first, e.g.
	// the proxy http://<lab host>:3129 (NTLM) or a PAC file, and run under
	// `runas /netonly /user:CORP\\alice` so the process has an identity.
	t.Run("system proxy settings", func(t *testing.T) {
		if os.Getenv("AZNET_PROXYLAB_SYSTEM") == "" {
			t.Skip("set AZNET_PROXYLAB_SYSTEM=1 after configuring the system proxy to a lab proxy")
		}
		setProxyEnv(t, "", "", "")
		err, trace := dialAgent(t, conn)
		t.Logf("trace:\n%s", trace)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(trace, "via ") || !strings.Contains(trace, "tunnel established") {
			t.Fatalf("the system proxy was not used:\n%s", trace)
		}
	})
	t.Run("no proxy configured connects directly", func(t *testing.T) {
		setProxyEnv(t, "", "", "")
		direct := conn
		if !labInside {
			direct = strings.Replace(conn, "azurite:10000", labHost()+":10000", 1)
		}
		err, trace := dialAgent(t, direct)
		t.Logf("trace:\n%s", trace)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(trace, "via ") {
			t.Fatal("went through a proxy")
		}
	})

	// Azurite is an http endpoint, so HTTP_PROXY applies; real storage on 443
	// uses HTTPS_PROXY, covered by the resolver tests.
	t.Run("credentials in HTTP_PROXY, proxy offering everything", func(t *testing.T) {
		setProxyEnv(t, "", "http://alice:Passw0rd!2024@"+labProxy("proxy-all", 3131), "")
		err, trace := dialAgent(t, conn)
		t.Logf("trace:\n%s", trace)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(trace, "Passw0rd") {
			t.Fatal("password in the trace")
		}
	})
	t.Run("domain user in HTTP_PROXY", func(t *testing.T) {
		setProxyEnv(t, "", `http://CORP%5Calice:Passw0rd%212024@`+labProxy("proxy-ntlm", 3129), "")
		err, trace := dialAgent(t, conn)
		t.Logf("trace:\n%s", trace)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("basic-only proxy", func(t *testing.T) {
		setProxyEnv(t, "", "http://bob:Passw0rd!2024@"+labProxy("proxy-basic", 3128), "")
		err, trace := dialAgent(t, conn)
		t.Logf("trace:\n%s", trace)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("wrong password is tried once", func(t *testing.T) {
		setProxyEnv(t, "", "http://alice:wrong@"+labProxy("proxy-all", 3131), "")
		err, trace := dialAgent(t, conn)
		t.Logf("trace:\n%s", trace)
		if !errors.Is(err, aznet.ErrProxyAuthFailed) {
			t.Fatalf("err %v", err)
		}
		if n := strings.Count(trace, ": via "); n != 1 {
			t.Fatalf("proxy tried %d times", n)
		}
	})
	t.Run("no credentials", func(t *testing.T) {
		setProxyEnv(t, "", "http://"+labProxy("proxy-all", 3131), "")
		err, trace := dialAgent(t, conn)
		t.Logf("trace:\n%s", trace)
		if err == nil {
			t.Fatal("dialed an authenticating proxy without credentials")
		}
	})
	t.Run("only HTTPS_PROXY set leaves the http emulator direct", func(t *testing.T) {
		setProxyEnv(t, "http://alice:Passw0rd!2024@"+labProxy("proxy-all", 3131), "", "")
		err, trace := dialAgent(t, conn)
		t.Logf("trace:\n%s", trace)
		assertDirect(t, err, trace)
	})
	t.Run("NO_PROXY bypasses the proxy", func(t *testing.T) {
		setProxyEnv(t, "", "http://alice:Passw0rd!2024@"+labProxy("proxy-all", 3131), "azurite")
		err, trace := dialAgent(t, conn)
		t.Logf("trace:\n%s", trace)
		assertDirect(t, err, trace)
	})
}

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

// assertDirect checks that the proxy was never contacted. Outside the lab
// network azurite does not resolve, so the direct dial fails (how it fails
// depends on the platform's resolver, and Windows can take until the
// deadline); inside it resolves and the direct dial succeeds.
func assertDirect(t *testing.T, err error, trace string) {
	t.Helper()
	if strings.Contains(trace, "via ") {
		t.Fatalf("went through the proxy:\n%s", trace)
	}
	if !strings.Contains(trace, "direct connection") {
		t.Fatalf("no direct connection in the trace:\n%s", trace)
	}
	switch {
	case labInside && err != nil:
		t.Fatalf("direct dial inside the network: %v", err)
	case !labInside && err == nil:
		t.Fatal("the direct dial to an unresolvable name succeeded")
	}
}
