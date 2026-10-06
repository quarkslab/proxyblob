// This harness runs in three isolated network namespaces. It exercises the
// production proxy/agent handlers over TCP, or real Azure with run-live.sh.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"proxyblob/pkg/protocol"
	proxy "proxyblob/pkg/proxy/server"
	socks "proxyblob/pkg/proxy/socks"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	watchdog := time.AfterFunc(12*time.Minute, func() { panic("topology harness exceeded 12-minute deadline") })
	defer watchdog.Stop()
	if len(os.Args) == 3 && os.Args[1] == "live" {
		liveRun(os.Args[2])
		return
	}
	switch os.Args[1] {
	case "proxy":
		listener, err := net.Listen("tcp", ":9000")
		must(err)
		for {
			conn, err := listener.Accept()
			must(err)
			server := proxy.NewProxyServer(context.Background(), conn)
			server.Start("0.0.0.0:1080")
			<-server.Ctx.Done()
			server.Stop()
			conn.Close()
		}
	case "agent":
		echo("udp", "[::]:19001")
		echo("udp6", "[::1]:19002")
		var conn net.Conn
		var err error
		for i := 0; i < 100; i++ {
			conn, err = net.Dial("tcp", "proxy-back:9000")
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		must(err)
		handler := socks.NewSocksHandler(context.Background(), conn)
		handler.Start("")
		<-handler.Ctx.Done()
		handler.Stop()
		conn.Close()
	case "client":
		client()
	default:
		panic("expected proxy, agent, or client")
	}
}
func echo(network, address string) {
	addr, err := net.ResolveUDPAddr(network, address)
	must(err)
	socket, err := net.ListenUDP(network, addr)
	must(err)
	go func() {
		buf := make([]byte, 65535)
		for {
			n, from, err := socket.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, err = socket.WriteToUDP(buf[:n], from)
			must(err)
		}
	}()
}
func associate() (net.Conn, *net.UDPAddr) {
	var control net.Conn
	var err error
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); {
		control, err = net.DialTimeout("tcp", "proxy-front:1080", time.Second)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	must(err)
	control.SetDeadline(time.Now().Add(60 * time.Second))
	_, err = control.Write([]byte{5, 1, 0})
	must(err)
	auth := make([]byte, 2)
	_, err = io.ReadFull(control, auth)
	must(err)
	if !bytes.Equal(auth, []byte{5, 0}) {
		panic("auth")
	}
	_, err = control.Write([]byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0})
	must(err)
	reply := make([]byte, 10)
	_, err = io.ReadFull(control, reply)
	must(err)
	if reply[1] != 0 || reply[3] != 1 {
		panic(fmt.Sprintf("UDP reply: %v", reply))
	}
	relay := &net.UDPAddr{IP: net.IP(reply[4:8]), Port: int(binary.BigEndian.Uint16(reply[8:]))}
	ips, err := net.LookupIP("proxy-front")
	must(err)
	if !relay.IP.Equal(ips[0]) {
		panic(fmt.Sprintf("relay %v is not proxy %v", relay, ips))
	}
	return control, relay
}
func signal(name, address string) {
	must(os.WriteFile(name+".tmp", []byte(address), 0600))
	must(os.Rename(name+".tmp", name))
}
func client() {
	// This reachable-only-from-back address proves Docker network separation.
	direct, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.ParseIP(os.Getenv("AGENT_IP")), Port: 19001})

	if err == nil {
		direct.SetDeadline(time.Now().Add(200 * time.Millisecond))
		direct.Write([]byte("must not reach agent"))
		if _, err = direct.Read(make([]byte, 100)); err == nil {
			panic("client unexpectedly reached agent directly")
		}
		direct.Close()
	}

	fmt.Println("PASS direct client-to-agent UDP is blocked across isolated networks")
	if os.Getenv("LIVE_TEST") == "1" {
		slow, relay := associate()
		defer slow.Close()
		unread, err := net.ListenUDP("udp4", &net.UDPAddr{})
		must(err)
		defer unread.Close()
		packet := append([]byte{0, 0, 0, 1, 127, 0, 0, 1, 74, 57}, make([]byte, 32768)...)
		unread.SetWriteDeadline(time.Now().Add(10 * time.Second))
		for i := 0; i < 100; i++ {
			_, err := unread.WriteToUDP(packet, relay)
			must(err)
		}
		fmt.Println("PRESSURE sent=100 payload=32768 unread_association=true; following healthy associations must progress")
	}
	controls := make([]net.Conn, 3)
	relays := make([]*net.UDPAddr, 3)
	for i := range controls {
		controls[i], relays[i] = associate()
		defer controls[i].Close()
	}
	for clientID := 0; clientID < 3; clientID++ {
		control, relay := controls[clientID], relays[clientID]
		socket, err := net.ListenUDP("udp4", &net.UDPAddr{})
		must(err)
		for _, kind := range []string{"ipv4", "ipv6", "domain"} {
			var address []byte
			port := 19001
			switch kind {
			case "ipv4":
				address = []byte{1, 127, 0, 0, 1}
			case "ipv6":
				address = append([]byte{4}, net.IPv6loopback...)
				port = 19002
			case "domain":
				address = append([]byte{3, 9}, []byte("localhost")...)
			}
			address = binary.BigEndian.AppendUint16(address, uint16(port))
			maxPayload := 65507 - 3 - len(address)
			if kind == "domain" {
				maxPayload = 65507 - 22
			}
			for _, size := range []int{0, 1, 16001, 32768, maxPayload} {
				payload := make([]byte, size)
				for i := range payload {
					payload[i] = byte(i % 251)
				}
				packet := append([]byte{0, 0, 0}, address...)
				packet = append(packet, payload...)
				start := time.Now()
				socket.SetDeadline(time.Now().Add(30 * time.Second))
				_, err = socket.WriteToUDP(packet, relay)
				must(err)
				buf := make([]byte, protocol.MaxDatagramSize)
				n, from, err := socket.ReadFromUDP(buf)
				must(err)
				source, header, code := socks.ExtractUDPHeader(buf[:n])
				if code != 0 || !bytes.Equal(buf[header:n], payload) || !from.IP.Equal(relay.IP) || from.Port != relay.Port {
					panic(fmt.Sprintf("datagram mismatch %s %d", kind, size))
				}
				addr, err := net.ResolveUDPAddr("udp", source)
				must(err)
				if !addr.IP.IsLoopback() || addr.Port != port {
					panic("reply source not destination")
				}
				fmt.Printf("ROUNDTRIP client=%d destination=%s payload=%d elapsed_ms=%d\n", clientID, kind, size, time.Since(start).Milliseconds())
			}
			fmt.Printf("PASS client=%d destination=%s through maximum complete SOCKS datagram\n", clientID, kind)
		}
		// Every header is validated after the valid client source was pinned.
		valid := []byte{0, 0, 0, 1, 127, 0, 0, 1, byte(19001 >> 8), byte(19001 & 255), 99}
		reject := func(packet []byte, sender *net.UDPConn) {
			_, err := sender.WriteToUDP(packet, relay)
			must(err)
			sender.SetReadDeadline(time.Now().Add(time.Second))
			if _, _, err := sender.ReadFromUDP(make([]byte, 100)); err == nil {
				panic("rejected datagram received a reply")
			}
		}
		malformed := append([]byte(nil), valid...)
		malformed[len(malformed)-1] = 98
		malformed[2] = 1
		reject(malformed, socket)
		malformed[2] = 0
		malformed[0] = 1
		reject(malformed, socket)
		reject([]byte{0, 0, 0, 4, 0}, socket)
		spoof, err := net.ListenUDP("udp4", &net.UDPAddr{})
		must(err)
		spoofPacket := append([]byte(nil), valid...)
		spoofPacket[len(spoofPacket)-1] = 97
		reject(spoofPacket, spoof)
		spoof.Close()
		// A good response following invalid packets proves the path stayed healthy.
		socket.SetDeadline(time.Now().Add(30 * time.Second))
		_, err = socket.WriteToUDP(valid, relay)
		must(err)
		healthy := make([]byte, 100)
		n, _, err := socket.ReadFromUDP(healthy)
		must(err)
		if !bytes.Equal(healthy[:n], valid) {
			panic("healthy probe mismatch or invalid packet arrived late")
		}
		fmt.Printf("PASS client=%d rejects FRAG/RSV/truncated headers and alternate source port; healthy traffic recovers\n", clientID)
		control.Close()
		time.Sleep(time.Second)
		if os.Getenv("LIVE_TEST") == "1" {
			name := fmt.Sprintf("/signals/control-close-%d", clientID)
			signal(name, relay.String())
			deadline := time.Now().Add(15 * time.Second)
			for {
				if _, err := os.Stat(name + ".ok"); err == nil {
					break
				}
				if time.Now().After(deadline) {
					panic("proxy did not verify relay port release")
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		reject(valid, socket)
		fmt.Printf("PASS client=%d relay stops after control close\n", clientID)
		socket.Close()
	}
	if os.Getenv("LIVE_TEST") == "1" {
		control, relay := associate()
		defer control.Close()
		socket, err := net.ListenUDP("udp4", &net.UDPAddr{})
		must(err)
		defer socket.Close()
		signal("/signals/close-tunnel", relay.String())
		control.SetReadDeadline(time.Now().Add(30 * time.Second))
		if _, err := control.Read(make([]byte, 1)); err != io.EOF {
			panic("control did not close on tunnel loss")
		}
		socket.SetDeadline(time.Now().Add(time.Second))
		_, err = socket.WriteToUDP([]byte{0, 0, 0, 1, 127, 0, 0, 1, 74, 57, 99}, relay)
		must(err)
		if _, _, err := socket.ReadFromUDP(make([]byte, 100)); err == nil {
			panic("relay survived tunnel loss")
		}
		fmt.Println("PASS open association control/UDP relay close on Azure tunnel loss")
	}
	fmt.Println("PASS topology UDP round trips, reply addressing, malformed input, maximum sizes, multiple clients/destinations and TCP teardown")
}
