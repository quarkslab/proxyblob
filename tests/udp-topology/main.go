// This harness runs in three isolated network namespaces. It exercises the
// production proxy/agent handlers over a TCP tunnel; it does not emulate Azure.
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
	for i := 0; i < 100; i++ {
		control, err = net.Dial("tcp", "proxy-front:1080")
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	must(err)
	control.SetDeadline(time.Now().Add(10 * time.Second))
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
	for clientID := 0; clientID < 3; clientID++ {
		control, relay := associate()
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
				socket.SetDeadline(time.Now().Add(5 * time.Second))
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
			}
			fmt.Printf("PASS client=%d destination=%s through maximum complete SOCKS datagram\n", clientID, kind)
		}
		// Every header must be validated, even after a valid source was learned.
		malformed := []byte{0, 0, 1, 1, 127, 0, 0, 1, byte(19001 >> 8), byte(19001 & 255), 99}
		socket.WriteToUDP(malformed, relay)
		socket.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if _, _, err = socket.ReadFromUDP(make([]byte, 100)); err == nil {
			panic("fragment relayed")
		}
		control.Close()
		time.Sleep(100 * time.Millisecond)
		valid := append([]byte(nil), malformed...)
		valid[2] = 0
		socket.WriteToUDP(valid, relay)
		socket.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if _, _, err = socket.ReadFromUDP(make([]byte, 100)); err == nil {
			panic("association survived TCP close")
		}
		socket.Close()
	}
	fmt.Println("PASS topology UDP round trips, reply addressing, malformed input, maximum sizes, multiple clients/destinations and TCP teardown")
}
