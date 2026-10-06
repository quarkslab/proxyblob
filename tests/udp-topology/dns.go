package main

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	socks "proxyblob/pkg/proxy/socks"
)

// dnsClient uses the public resolver only through the SOCKS UDP association.
// Its container has no Internet egress; a direct-query negative control must
// fail with no route or a timeout before the same query can pass the tunnel.
func dnsClient() {
	resolver := &net.UDPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 53}
	control, relay := associate()
	defer control.Close()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{})
	must(err)
	defer socket.Close()
	for _, tc := range []struct {
		name string
		kind dnsmessage.Type
		code dnsmessage.RCode
	}{
		{"example.com.", dnsmessage.TypeA, dnsmessage.RCodeSuccess},
		{"example.com.", dnsmessage.TypeAAAA, dnsmessage.RCodeSuccess},
		{"proxyblob-dns-check.invalid.", dnsmessage.TypeA, dnsmessage.RCodeNameError},
	} {
		var nonce [2]byte
		_, err := rand.Read(nonce[:])
		must(err)
		id := binary.BigEndian.Uint16(nonce[:])
		question := dnsmessage.Question{Name: dnsmessage.MustNewName(tc.name), Type: tc.kind, Class: dnsmessage.ClassINET}
		query, err := (&dnsmessage.Message{
			Header:    dnsmessage.Header{ID: id, RecursionDesired: true},
			Questions: []dnsmessage.Question{question},
		}).Pack()
		must(err)

		direct, err := net.DialUDP("udp4", nil, resolver)
		if err == nil {
			must(direct.SetDeadline(time.Now().Add(time.Second)))
			_, err = direct.Write(query)
			if err == nil {
				_, err = direct.Read(make([]byte, 4096))
			}
			direct.Close()
		}
		var networkError net.Error
		if !errors.Is(err, syscall.ENETUNREACH) && !errors.Is(err, syscall.EHOSTUNREACH) &&
			!(errors.As(err, &networkError) && networkError.Timeout()) {
			panic(fmt.Sprintf("DNS direct path must be unreachable or time out, got %v", err))
		}
		fmt.Printf("PASS direct DNS blocked name=%s type=%s\n", tc.name, tc.kind)

		// RSV, FRAG, ATYP, resolver IPv4 address, destination port, DNS wire query.
		packet := append([]byte{0, 0, 0, 1, 1, 1, 1, 1, 0, 53}, query...)
		start := time.Now()
		must(socket.SetDeadline(time.Now().Add(30 * time.Second)))
		_, err = socket.WriteToUDP(packet, relay)
		must(err)
		buf := make([]byte, 65535)
		n, from, err := socket.ReadFromUDP(buf)
		must(err)
		source, header, code := socks.ExtractUDPHeader(buf[:n])
		if code != 0 || source != resolver.String() || !from.IP.Equal(relay.IP) || from.Port != relay.Port {
			panic("DNS reply did not come from the proxy relay with the resolver source address")
		}
		var response dnsmessage.Message
		must(response.Unpack(buf[header:n]))
		if response.ID != id || !response.Response || response.OpCode != 0 || response.Truncated || response.RCode != tc.code ||
			len(response.Questions) != 1 || response.Questions[0] != question {
			panic(fmt.Sprintf("unexpected DNS response for %s %s", tc.name, tc.kind))
		}
		answers := 0
		for _, answer := range response.Answers {
			if answer.Header.Name != question.Name || answer.Header.Class != dnsmessage.ClassINET || answer.Header.Type != tc.kind {
				continue
			}
			switch body := answer.Body.(type) {
			case *dnsmessage.AResource:
				fmt.Printf("DNS answer name=%s A=%s\n", tc.name, net.IP(body.A[:]))
				answers++
			case *dnsmessage.AAAAResource:
				fmt.Printf("DNS answer name=%s AAAA=%s\n", tc.name, net.IP(body.AAAA[:]))
				answers++
			}
		}
		if tc.code == dnsmessage.RCodeSuccess && answers == 0 {
			panic("DNS success without a matching address answer")
		}
		if tc.code == dnsmessage.RCodeNameError && len(response.Answers) != 0 {
			panic("DNS NXDOMAIN unexpectedly contained answers")
		}
		fmt.Printf("PASS DNS resolver=%s name=%s type=%s rcode=%s answers=%d elapsed_ms=%d\n",
			resolver, tc.name, tc.kind, response.RCode, answers, time.Since(start).Milliseconds())
	}
}
