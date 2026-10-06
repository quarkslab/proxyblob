package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	socks "proxyblob/pkg/proxy/socks"
)

// A test-only destination in the agent namespace initiates the reverse BIND
// connection. Its control connection also crosses SOCKS and the Azure tunnel.
func bindPeerService() {
	listener, err := net.Listen("tcp", "127.0.0.1:19003")
	must(err)
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				must(c.SetDeadline(time.Now().Add(30 * time.Second)))
				line, err := bufio.NewReader(io.LimitReader(c, 128)).ReadString('\n')
				must(err)
				addr, err := net.ResolveTCPAddr("tcp", line[:len(line)-1])
				must(err)
				if !addr.IP.IsLoopback() {
					panic("BIND test peer must stay in agent namespace")
				}
				peer, err := net.DialTCP("tcp", nil, addr)
				must(err)
				defer peer.Close()
				must(peer.SetDeadline(time.Now().Add(30 * time.Second)))
				payload, err := io.ReadAll(io.LimitReader(peer, 1<<20))
				must(err)
				_, err = peer.Write(payload)
				must(err)
				must(peer.CloseWrite())
			}()
		}
	}()
}

func bindRequest(command byte, address []byte) net.Conn {
	var c net.Conn
	var err error
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); {
		c, err = net.DialTimeout("tcp", "proxy-front:1080", time.Second)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	must(err)
	must(c.SetDeadline(time.Now().Add(30 * time.Second)))
	_, err = c.Write([]byte{5, 1, 0})
	must(err)
	var auth [2]byte
	_, err = io.ReadFull(c, auth[:])
	must(err)
	if auth != [2]byte{5, 0} {
		panic("BIND test auth failed")
	}
	_, err = c.Write(append([]byte{5, command, 0}, address...))
	must(err)
	return c
}
func bindReply(c net.Conn) *net.TCPAddr {
	var header [4]byte
	_, err := io.ReadFull(c, header[:])
	must(err)
	if header[0] != 5 || header[1] != 0 || header[2] != 0 {
		panic(fmt.Sprintf("BIND reply %v", header))
	}
	size := 4
	if header[3] == 4 {
		size = 16
	} else if header[3] != 1 {
		panic("BIND reply address type")
	}
	addr := make([]byte, size+2)
	_, err = io.ReadFull(c, addr)
	must(err)
	return &net.TCPAddr{IP: net.IP(addr[:size]), Port: int(binary.BigEndian.Uint16(addr[size:]))}
}
func bindClient() {
	for _, host := range []string{"127.0.0.1", "::1", "localhost"} {
		address := socks.UDPAddress(&net.UDPAddr{IP: net.ParseIP(host)})
		if host == "localhost" {
			address = append([]byte{3, 9}, []byte(host)...)
			address = append(address, 0, 0)
		}
		client := bindRequest(socks.Bind, address)
		bound := bindReply(client)
		if !bound.IP.IsLoopback() || bound.Port == 0 {
			panic("BIND first reply is not agent loopback")
		}
		// The same loopback address in the isolated client namespace has no listener.
		direct, err := net.DialTimeout("tcp", bound.String(), time.Second)
		if err == nil {
			direct.Close()
			panic("client directly reached agent BIND endpoint")
		}
		trigger := bindRequest(socks.Connect, []byte{1, 127, 0, 0, 1, 74, 59}) // 19003
		bindReply(trigger)
		_, err = fmt.Fprintln(trigger, bound.String())
		must(err)
		peer := bindReply(client)
		if !peer.IP.IsLoopback() || peer.Port == 0 {
			panic("BIND second reply is not accepted peer")
		}
		payload := bytes.Repeat([]byte("BIND bytes over Azure\x00\xff"), 4096)
		_, err = client.Write(payload)
		must(err)
		must(client.(*net.TCPConn).CloseWrite())
		got, err := io.ReadAll(io.LimitReader(client, int64(len(payload)+1)))
		must(err)
		if !bytes.Equal(got, payload) {
			panic("BIND payload/ordered EOF mismatch")
		}
		client.Close()
		trigger.Close()
		fmt.Printf("PASS BIND destination=%s bytes=%d first_reply=true second_reply=true half_close=true direct_path_blocked=true\n", host, len(payload))
	}
}
