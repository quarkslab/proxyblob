package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"proxyblob/pkg/protocol"
	proxy "proxyblob/pkg/proxy/server"
	socks "proxyblob/pkg/proxy/socks"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestUDPAssociateUsesProxyEndpoint(t *testing.T) {
	a, b := net.Pipe()
	agent := socks.NewSocksHandler(context.Background(), b)
	proxy := proxy.NewProxyServer(context.Background(), a)
	agent.Start("")
	proxy.Start("127.0.0.1:0")
	defer agent.Stop()
	defer proxy.Stop()
	defer a.Close()
	defer b.Close()
	target, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	go func() {
		buf := make([]byte, 65535)
		for {
			n, addr, e := target.ReadFromUDP(buf)
			if e != nil {
				return
			}
			target.WriteToUDP(buf[:n], addr)
		}
	}()
	control, err := net.Dial("tcp", proxy.ListenerAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	control.SetDeadline(time.Now().Add(3 * time.Second))
	control.Write([]byte{5, 1, 0})
	auth := make([]byte, 2)
	if _, err = io.ReadFull(control, auth); err != nil {
		t.Fatal(err)
	}
	control.Write([]byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0})
	reply := make([]byte, 10)
	if _, err = io.ReadFull(control, reply); err != nil {
		t.Fatal(err)
	}
	if reply[1] != 0 || !net.IP(reply[4:8]).Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("UDP reply must name reachable proxy address: %v", reply)
	}
	client, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	relay := &net.UDPAddr{IP: net.IP(reply[4:8]), Port: int(binary.BigEndian.Uint16(reply[8:]))}
	payload := bytes.Repeat([]byte("udp"), 2000)
	packet := []byte{0, 0, 0, 1, 127, 0, 0, 1, 0, 0}
	binary.BigEndian.PutUint16(packet[8:], uint16(target.LocalAddr().(*net.UDPAddr).Port))
	packet = append(packet, payload...)
	client.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err = client.WriteToUDP(packet, relay); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 65535)
	n, _, err := client.ReadFromUDP(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:n], packet) {
		t.Fatalf("datagram changed: got %d want %d", n, len(packet))
	}
}

func udpTunnel(t *testing.T, listen string) (*proxy.ProxyServer, *socks.SocksHandler) {
	t.Helper()
	a, b := net.Pipe()
	agent := socks.NewSocksHandler(context.Background(), b)
	proxy := proxy.NewProxyServer(context.Background(), a)
	agent.Start("")
	proxy.Start(listen)
	if proxy.ListenerAddr() == nil {
		t.Fatal("proxy did not listen")
	}
	t.Cleanup(func() { proxy.Stop(); agent.Stop(); a.Close(); b.Close() })
	return proxy, agent
}

func udpAssociate(t *testing.T, proxy *proxy.ProxyServer, request []byte) (net.Conn, *net.UDPAddr) {
	t.Helper()
	control, err := net.Dial("tcp", proxy.ListenerAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { control.Close() })
	control.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err = control.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	var auth [2]byte
	if _, err = io.ReadFull(control, auth[:]); err != nil || auth != [2]byte{5, 0} {
		t.Fatalf("auth: %v %v", auth, err)
	}
	if request == nil {
		request = []byte{1, 0, 0, 0, 0, 0, 0}
	}
	if _, err = control.Write(append([]byte{5, 3, 0}, request...)); err != nil {
		t.Fatal(err)
	}
	var prefix [4]byte
	if _, err = io.ReadFull(control, prefix[:]); err != nil {
		t.Fatal(err)
	}
	if prefix[0] != 5 || prefix[1] != 0 || prefix[2] != 0 {
		t.Fatalf("reply: %v", prefix)
	}
	size := 6
	if prefix[3] == 4 {
		size = 18
	} else if prefix[3] != 1 {
		t.Fatalf("reply address type %d", prefix[3])
	}
	address := make([]byte, size)
	if _, err = io.ReadFull(control, address); err != nil {
		t.Fatal(err)
	}
	return control, &net.UDPAddr{IP: net.IP(address[:size-2]), Port: int(binary.BigEndian.Uint16(address[size-2:]))}
}

func udpSocket(t *testing.T, address string) *net.UDPConn {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	socket, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { socket.Close() })
	return socket
}

func udpEcho(t *testing.T, address string) *net.UDPConn {
	t.Helper()
	socket := udpSocket(t, address)
	go func() {
		buf := make([]byte, 65535)
		for {
			n, addr, err := socket.ReadFromUDP(buf)
			if err != nil {
				return
			}
			socket.WriteToUDP(buf[:n], addr)
		}
	}()
	return socket
}

func udpExchange(t *testing.T, client *net.UDPConn, relay, target *net.UDPAddr, domain bool, payload []byte) {
	t.Helper()
	address := socks.UDPAddress(target)
	if domain {
		address = append([]byte{3, 9}, []byte("localhost")...)
		address = binary.BigEndian.AppendUint16(address, uint16(target.Port))
	}
	packet := append([]byte{0, 0, 0}, address...)
	packet = append(packet, payload...)
	client.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := client.WriteToUDP(packet, relay); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 65535)
	n, from, err := client.ReadFromUDP(b)
	if err != nil {
		t.Fatal(err)
	}
	if domain {
		address, header, code := socks.ExtractUDPHeader(b[:n])
		actual, e := net.ResolveUDPAddr("udp", address)
		if code != 0 || e != nil || !actual.IP.IsLoopback() || actual.Port != target.Port || !bytes.Equal(b[header:n], payload) {
			t.Fatalf("domain reply: %v %v", address, e)
		}
		return
	}
	want := append([]byte{0, 0, 0}, socks.UDPAddress(target)...)
	want = append(want, payload...)
	if !bytes.Equal(b[:n], want) || !from.IP.Equal(relay.IP) || from.Port != relay.Port {
		t.Fatalf("UDP identity/source: got %d %v want %d %v", n, from, len(want), relay)
	}
}

func TestUDPMultipleClientsDestinationsAndFamilies(t *testing.T) {
	proxy, _ := udpTunnel(t, "127.0.0.1:0")
	v4 := udpEcho(t, "127.0.0.1:0").LocalAddr().(*net.UDPAddr)
	v6 := udpEcho(t, "[::1]:0").LocalAddr().(*net.UDPAddr)
	for i := 0; i < 3; i++ {
		_, relay := udpAssociate(t, proxy, nil)
		client := udpSocket(t, "127.0.0.1:0")
		for _, target := range []*net.UDPAddr{v4, v6} {
			udpExchange(t, client, relay, target, false, []byte{byte(i), 1, 2})
			udpExchange(t, client, relay, target, false, nil)
		}
		// localhost uses the agent resolver, with a dual-stack echo so either DNS
		// family is usable. The reply contains the resolved numeric source address.
		dual := udpEcho(t, "[::]:0")
		target, err := net.ResolveUDPAddr("udp", net.JoinHostPort("localhost", strconv.Itoa(dual.LocalAddr().(*net.UDPAddr).Port)))
		if err != nil {
			t.Fatal(err)
		}
		udpExchange(t, client, relay, target, true, []byte("domain"))
	}
}

func TestUDPIPv6ClientEndpoint(t *testing.T) {
	proxy, _ := udpTunnel(t, "[::1]:0")
	_, relay := udpAssociate(t, proxy, []byte{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	if !relay.IP.Equal(net.IPv6loopback) {
		t.Fatalf("unreachable IPv6 endpoint %v", relay)
	}
	udpExchange(t, udpSocket(t, "[::1]:0"), relay, udpEcho(t, "[::1]:0").LocalAddr().(*net.UDPAddr), false, []byte("ipv6 client"))
}

func TestUDPRejectsMalformedAndWrongSourcePorts(t *testing.T) {
	proxy, _ := udpTunnel(t, "127.0.0.1:0")
	client := udpSocket(t, "127.0.0.1:0")
	spoof := udpSocket(t, "127.0.0.1:0")
	control, relay := udpAssociate(t, proxy, socks.UDPAddress(client.LocalAddr().(*net.UDPAddr)))
	defer control.Close()
	target := udpEcho(t, "127.0.0.1:0").LocalAddr().(*net.UDPAddr)
	valid := append([]byte{0, 0, 0}, socks.UDPAddress(target)...)
	valid = append(valid, []byte("bad")...)
	// Wrong source must not claim even the first datagram.
	spoof.WriteToUDP(valid, relay)
	bad := append([]byte(nil), valid...)
	bad[0] = 1
	client.WriteToUDP(bad, relay)
	bad[0] = 0
	bad[2] = 1
	client.WriteToUDP(bad, relay)
	client.WriteToUDP([]byte{0, 0, 0, 1}, relay)
	udpExchange(t, client, relay, target, false, []byte("valid"))
	spoof.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	if _, _, err := spoof.ReadFromUDP(make([]byte, 100)); err == nil {
		t.Fatal("spoofed source received a reply")
	}
}

func TestUDPControlCloseReleasesEndpoint(t *testing.T) {
	proxy, agent := udpTunnel(t, "127.0.0.1:0")
	for i := 0; i < 20; i++ {
		control, relay := udpAssociate(t, proxy, nil)
		client := udpSocket(t, "127.0.0.1:0")
		udpExchange(t, client, relay, udpEcho(t, "127.0.0.1:0").LocalAddr().(*net.UDPAddr), false, []byte("before close"))
		control.Close()
		deadline := time.Now().Add(3 * time.Second)
		for {
			socket, err := net.ListenUDP("udp", relay)
			if err == nil {
				socket.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("control close leaked relay %v: %v", relay, err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		n := 0
		proxy.Connections.Range(func(_, _ any) bool { n++; return true })
		agent.Connections.Range(func(_, _ any) bool { n++; return true })
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leaked %d association streams", n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestUDPFiniteDestinationAndAssociationLimits(t *testing.T) {
	a, b := net.Pipe()
	cfg := protocol.DefaultFlowConfig()
	cfg.MaxStreams = 2
	cfg.UDPDestinations = 2
	agent, err := socks.NewSocksHandlerWithConfig(context.Background(), b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := proxy.NewProxyServerWithConfig(context.Background(), a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	agent.Start("")
	proxy.Start("127.0.0.1:0")
	defer agent.Stop()
	defer proxy.Stop()
	defer a.Close()
	defer b.Close()
	_, relay := udpAssociate(t, proxy, nil)
	client := udpSocket(t, "127.0.0.1:0")
	first := udpEcho(t, "127.0.0.1:0").LocalAddr().(*net.UDPAddr)
	second := udpEcho(t, "127.0.0.1:0").LocalAddr().(*net.UDPAddr)
	third := udpEcho(t, "127.0.0.1:0").LocalAddr().(*net.UDPAddr)
	udpExchange(t, client, relay, first, false, []byte("first"))
	udpExchange(t, client, relay, second, false, []byte("second"))
	packet := append([]byte{0, 0, 0}, socks.UDPAddress(third)...)
	packet = append(packet, 1)
	client.WriteToUDP(packet, relay)
	client.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	if _, _, err = client.ReadFromUDP(make([]byte, 100)); err == nil {
		t.Fatal("third destination exceeded configured limit")
	}
	udpExchange(t, client, relay, first, false, []byte("existing destination stays usable"))
	udpAssociate(t, proxy, nil)
	excess, err := net.Dial("tcp", proxy.ListenerAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer excess.Close()
	excess.SetDeadline(time.Now().Add(time.Second))
	excess.Write([]byte{5, 1, 0})
	if _, err = excess.Read(make([]byte, 2)); err == nil {
		t.Fatal("third association exceeded configured stream count")
	}
	udpExchange(t, client, relay, first, false, []byte("existing association stays usable"))
}

func TestUDPConcurrentTunnelAndControlTeardown(t *testing.T) {
	for i := 0; i < 10; i++ {
		proxy, agent := udpTunnel(t, "127.0.0.1:0")
		control, relay := udpAssociate(t, proxy, nil)
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(3)
			go func() { defer wg.Done(); control.Close() }()
			go func() { defer wg.Done(); proxy.Stop() }()
			go func() { defer wg.Done(); agent.Stop() }()
		}
		wg.Wait()
		deadline := time.Now().Add(time.Second)
		for {
			socket, err := net.ListenUDP("udp", relay)
			if err == nil {
				socket.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("racing teardown leaked socket %v", relay)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func TestUDPRejectsMismatchedAssociationSource(t *testing.T) {
	proxy, _ := udpTunnel(t, "127.0.0.1:0")
	control, err := net.Dial("tcp", proxy.ListenerAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	control.SetDeadline(time.Now().Add(time.Second))
	control.Write([]byte{5, 1, 0})
	var auth [2]byte
	_, err = io.ReadFull(control, auth[:])
	if err != nil {
		t.Fatal(err)
	}
	control.Write([]byte{5, 3, 0, 1, 192, 0, 2, 1, 0, 0})
	reply := make([]byte, 10)
	_, err = io.ReadFull(control, reply)
	if err != nil || reply[1] != 2 {
		t.Fatalf("mismatched source reply %v %v", reply, err)
	}
}

func TestUDPRejectsWrongSourceIP(t *testing.T) {
	spoof, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2)})
	if err != nil {
		t.Skipf("platform cannot bind alternate loopback address: %v", err)
	}
	defer spoof.Close()
	proxy, _ := udpTunnel(t, "127.0.0.1:0")
	_, relay := udpAssociate(t, proxy, nil)
	target := udpEcho(t, "127.0.0.1:0").LocalAddr().(*net.UDPAddr)
	packet := append([]byte{0, 0, 0}, socks.UDPAddress(target)...)
	packet = append(packet, 1)
	if _, err = spoof.WriteToUDP(packet, relay); err != nil {
		t.Fatal(err)
	}
	udpExchange(t, udpSocket(t, "127.0.0.1:0"), relay, target, false, []byte("legitimate first source"))
	spoof.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	if _, _, err = spoof.ReadFromUDP(make([]byte, 100)); err == nil {
		t.Fatal("wrong source IP received a reply")
	}
}

func TestUDPShortDomainSourceHints(t *testing.T) {
	proxy, _ := udpTunnel(t, "127.0.0.1:0")
	for _, domain := range []string{"a", "ab"} {
		address := append([]byte{3, byte(len(domain))}, []byte(domain)...)
		address = append(address, 0, 0)
		_, relay := udpAssociate(t, proxy, address)
		udpExchange(t, udpSocket(t, "127.0.0.1:0"), relay, udpEcho(t, "127.0.0.1:0").LocalAddr().(*net.UDPAddr), false, []byte("short source hint"))
	}
}
