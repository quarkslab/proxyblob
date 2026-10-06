package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/atsika/aznet"
	"github.com/rs/zerolog"
	proxy "proxyblob/pkg/proxy/server"
	socks "proxyblob/pkg/proxy/socks"
)

// No SDK error text, endpoint or credentials may reach stdout/stderr.
func liveCheck(step string, err error) {
	if err == nil {
		return
	}
	var response *azcore.ResponseError
	if errors.As(err, &response) {
		panic(fmt.Sprintf("%s: status=%d code=%s", step, response.StatusCode, response.ErrorCode))
	}
	panic(fmt.Sprintf("%s: error type %T", step, err))
}

// Handler Abort deliberately expires transport deadlines. aznet also bounds its
// best-effort FIN to 250ms. These delivery errors are expected on tunnel loss;
// independent catalog validation below still requires resource reclamation.
func liveClose(step string, err error) {
	if err == nil {
		return
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			liveClose(step, child)
		}
		return
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		liveClose(step, wrapped.Unwrap())
		return
	}
	if err == context.Canceled || err == context.DeadlineExceeded || err == os.ErrDeadlineExceeded || err == net.ErrClosed {
		fmt.Printf("CLOSE step=%s expected_abort_type=%T\n", step, err)
		return
	}
	liveCheck(step, err)
}

type liveConfig struct {
	Driver  string `json:"driver"`
	Address string `json:"address"`
	Account string `json:"storage_account"`
	Key     string `json:"storage_account_key"`
}

func liveAccount(driver string) (liveConfig, *url.URL) {
	raw, err := os.ReadFile("/config.json")
	liveCheck("read configuration", err)
	var config struct {
		Listeners []liveConfig `json:"listeners"`
	}
	liveCheck("parse configuration", json.Unmarshal(raw, &config))
	suffix := map[string]string{"azblob": ".blob.core.windows.net", "azqueue": ".queue.core.windows.net", "aztable": ".table.core.windows.net"}[driver]
	for _, c := range config.Listeners {
		u, err := url.Parse(c.Address)
		if err == nil && c.Driver == driver && suffix != "" && u.Scheme == "https" && strings.HasSuffix(u.Hostname(), suffix) && c.Account != "" && c.Key != "" {
			u.Path = ""
			u.RawQuery = ""
			u.Fragment = ""
			u.User = nil
			return c, u
		}
	}
	panic("no matching live Azure account-key configuration")
}
func liveOptions(ctx context.Context, prefix string, m *aznet.DefaultMetrics) []aznet.Option {
	return []aznet.Option{aznet.WithContext(ctx), aznet.WithEndpoints(prefix+"h", prefix+"t"), aznet.WithPrefixes(prefix+"q", prefix+"s"), aznet.WithSASExpiry(15 * time.Minute), aznet.WithConnectTimeout(90 * time.Second), aznet.WithAcceptPoll(100 * time.Millisecond), aznet.WithMetrics(m)}
}
func liveMetrics(role string, m *aznet.DefaultMetrics) {
	fmt.Printf("METRICS role=%s writes=%d reads=%d lists=%d deletes=%d sent=%d received=%d\n", role, m.GetWriteTransactionCount(), m.GetReadTransactionCount(), m.GetListTransactionCount(), m.GetDeleteTransactionCount(), m.GetBytesSent(), m.GetBytesReceived())
	for a, n := range m.RequestCounts() {
		fmt.Printf("HTTP role=%s driver=%s operation=%s method=%s status=%d retry=%t count=%d\n", role, a.Driver, a.Operation, a.Method, a.StatusCode, a.Retry, n)
	}
}
func liveRun(role string) {
	zerolog.SetGlobalLevel(zerolog.Disabled)
	driver, prefix := os.Getenv("LIVE_DRIVER"), os.Getenv("LIVE_PREFIX")
	if len(prefix) != 24 || !strings.HasPrefix(prefix, "pb") || strings.Trim(prefix[2:], "0123456789abcdef") != "" {
		panic("invalid isolated run prefix")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if role == "cleanup" {
		liveCleanup(ctx, driver, prefix)
		return
	}
	m := aznet.NewDefaultMetrics()
	defer liveMetrics(role, m)
	opts := liveOptions(ctx, prefix, m)
	if role == "proxy" {
		c, u := liveAccount(driver)
		u.User = url.UserPassword(c.Account, c.Key)
		listener, err := aznet.Listen(driver, u.String(), opts...)
		liveCheck("Azure listen", err)
		defer func() {
			closeErr := listener.Close()
			clean, done := context.WithTimeout(context.Background(), 60*time.Second)
			defer done()
			liveCheck("bootstrap cleanup", listener.(*aznet.Listener).CleanupBootstrap(clean))
			liveClose("listener close", closeErr)
		}()
		token, err := listener.(*aznet.Listener).ConnectionString()
		liveCheck("bootstrap SAS", err)
		liveCheck("write SAS file", os.WriteFile("/state/connection.tmp", []byte(token), 0600))
		liveCheck("publish complete SAS file", os.Rename("/state/connection.tmp", "/state/connection"))
		conn, err := listener.Accept()
		liveCheck("Azure accept", err)
		fmt.Println("PASS real Azure listener accepted session")
		server := proxy.NewProxyServer(ctx, conn)
		server.Start("0.0.0.0:1080")
		defer server.Stop()
		if server.ListenerAddr() == nil {
			panic("SOCKS listener did not start")
		}
		for {
			for i := 0; i < 3; i++ {
				name := fmt.Sprintf("/signals/control-close-%d", i)
				if raw, err := os.ReadFile(name); err == nil {
					assertRelayClosed(string(raw))
					liveCheck("acknowledge relay closure", os.Rename(name, name+".ok"))
				}
			}
			if raw, err := os.ReadFile("/signals/close-tunnel"); err == nil {
				liveClose("close active Azure tunnel", conn.Close())
				assertRelayClosed(string(raw))
				return
			}
			if _, err := os.Stat("/state/stop"); err == nil {
				return
			}
			select {
			case <-ctx.Done():
				panic("proxy test timeout")
			case <-server.Ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	if role == "agent" {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
					if _, err := os.Stat("/state/stop"); err == nil {
						cancel()
						return
					}
				}
			}
		}()
		bindPeerService()
		echo("udp", "[::]:19001")
		echo("udp6", "[::1]:19002")
		var token []byte
		for {
			var err error
			token, err = os.ReadFile("/state/connection")
			if err == nil {
				break
			}
			select {
			case <-ctx.Done():
				panic("SAS handoff timeout")
			case <-time.After(100 * time.Millisecond):
			}
		}
		conn, err := aznet.Dial(driver, string(token), opts...)
		liveCheck("Azure SAS dial", err)
		defer func() { liveClose("dialer close", conn.Close()) }()
		fmt.Println("PASS real Azure SAS dial completed")
		handler := socks.NewSocksHandler(ctx, conn)
		handler.Start("")
		defer handler.Stop()
		<-handler.Ctx.Done()
		return
	}
	panic("unknown live role")
}

// Rebinding the advertised port in the still-running proxy namespace proves
// that the relay socket was released, even if its dead tunnel could not reply.
func assertRelayClosed(address string) {
	addr, err := net.ResolveUDPAddr("udp4", address)
	liveCheck("relay address", err)
	deadline := time.Now().Add(10 * time.Second)
	for {
		socket, err := net.ListenUDP("udp4", addr)
		if err == nil {
			socket.Close()
			fmt.Println("PASS proxy independently rebound closed relay port")
			return
		}
		if time.Now().After(deadline) {
			panic("relay port remains bound after close")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Only resources inside this invocation's random prefix may be reclaimed.
// Independent SDK catalog queries verify absence after normal aznet Close.
func liveCleanup(ctx context.Context, driver, prefix string) {
	c, u := liveAccount(driver)
	var list func() []string
	var remove func(string) error
	switch driver {
	case "azblob":
		cred, err := azblob.NewSharedKeyCredential(c.Account, c.Key)
		liveCheck("blob credential", err)
		svc, err := blobservice.NewClientWithSharedKeyCredential(u.String(), cred, nil)
		liveCheck("blob admin", err)
		list = func() []string {
			var names []string
			p := svc.NewListContainersPager(&blobservice.ListContainersOptions{Prefix: &prefix})
			for p.More() {
				page, err := p.NextPage(ctx)
				liveCheck("blob catalog", err)
				for _, v := range page.ContainerItems {
					names = append(names, *v.Name)
				}
			}
			return names
		}
		remove = func(n string) error { _, err := svc.DeleteContainer(ctx, n, nil); return err }
	case "azqueue":
		cred, err := azqueue.NewSharedKeyCredential(c.Account, c.Key)
		liveCheck("queue credential", err)
		svc, err := azqueue.NewServiceClientWithSharedKeyCredential(u.String(), cred, nil)
		liveCheck("queue admin", err)
		list = func() []string {
			var names []string
			p := svc.NewListQueuesPager(&azqueue.ListQueuesOptions{Prefix: &prefix})
			for p.More() {
				page, err := p.NextPage(ctx)
				liveCheck("queue catalog", err)
				for _, v := range page.Queues {
					names = append(names, *v.Name)
				}
			}
			return names
		}
		remove = func(n string) error { _, err := svc.NewQueueClient(n).Delete(ctx, nil); return err }
	case "aztable":
		cred, err := aztables.NewSharedKeyCredential(c.Account, c.Key)
		liveCheck("table credential", err)
		svc, err := aztables.NewServiceClientWithSharedKey(u.String(), cred, nil)
		liveCheck("table admin", err)
		list = func() []string {
			var names []string
			filter := "TableName ge '" + prefix + "' and TableName lt '" + prefix + "z'"
			p := svc.NewListTablesPager(&aztables.ListTablesOptions{Filter: &filter})
			for p.More() {
				page, err := p.NextPage(ctx)
				liveCheck("table catalog", err)
				for _, v := range page.Tables {
					names = append(names, *v.Name)
				}
			}
			return names
		}
		remove = func(n string) error { _, err := svc.NewClient(n).Delete(ctx, nil); return err }
	}
	names := list()
	fmt.Printf("CLEANUP driver=%s prefix=%s residual_before=%d\n", driver, prefix, len(names))
	for _, n := range names {
		if !strings.HasPrefix(n, prefix) {
			panic("unsafe cleanup name")
		}
		liveCheck("remove test resource", remove(n))
		fmt.Printf("CLEANUP reclaimed=%s\n", n)
	}
	if len(list()) != 0 {
		panic("test Azure resources remain")
	}
	fmt.Printf("PASS independent Azure catalog residual_after=0 driver=%s\n", driver)
	if len(names) > 0 {
		panic("normal cleanup left resources; independent cleanup reclaimed them")
	}
}
