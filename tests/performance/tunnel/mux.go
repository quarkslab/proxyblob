package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/xtaci/smux"
)

// Library multiplexers replace pkg/protocol for SOCKS5 CONNECT only, so the
// tunnel's multiplexing can be compared with ProxyBlob's on identical traffic.
// Windows follow ProxyBlob's StreamWindow and frames its 32 KiB default. Library keep-alives are off: aznet already pings the session.

type session interface {
	open() (net.Conn, error)
	accept() (net.Conn, error)
}

type yamuxSession struct{ *yamux.Session }

func (s yamuxSession) open() (net.Conn, error)   { return s.Open() }
func (s yamuxSession) accept() (net.Conn, error) { return s.Accept() }

type smuxSession struct{ *smux.Session }

func (s smuxSession) open() (net.Conn, error)   { return s.OpenStream() }
func (s smuxSession) accept() (net.Conn, error) { return s.AcceptStream() }

func newSession(kind string, conn net.Conn, client bool, window int) (session, error) {
	switch kind {
	case "yamux":
		cfg := yamux.DefaultConfig()
		cfg.EnableKeepAlive = false
		cfg.MaxStreamWindowSize = uint32(window)
		cfg.ConnectionWriteTimeout = time.Minute
		cfg.LogOutput = io.Discard
		var s *yamux.Session
		var err error
		if client {
			s, err = yamux.Client(conn, cfg)
		} else {
			s, err = yamux.Server(conn, cfg)
		}
		return yamuxSession{s}, err
	case "smux":
		cfg := smux.DefaultConfig()
		cfg.Version = 2
		cfg.KeepAliveDisabled = true
		cfg.MaxFrameSize = 32 << 10
		cfg.MaxStreamBuffer = window
		cfg.MaxReceiveBuffer = 64 << 20 // ProxyBlob's tunnel window
		var s *smux.Session
		var err error
		if client {
			s, err = smux.Client(conn, cfg)
		} else {
			s, err = smux.Server(conn, cfg)
		}
		return smuxSession{s}, err
	}
	return nil, fmt.Errorf("unknown mux %q", kind)
}

// muxProxy serves SOCKS5 CONNECT (no auth) and forwards each client over a new
// stream whose first bytes name the target; the agent replies with one status.
func muxProxy(s session, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer c.Close()
			target, err := socksRequest(c)
			if err != nil {
				return
			}
			st, err := s.open()
			if err == nil {
				_, err = st.Write(append([]byte{byte(len(target))}, target...))
			}
			var status [1]byte
			if err == nil {
				_, err = io.ReadFull(st, status[:])
			}
			if err != nil || status[0] != 0 {
				c.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
				if st != nil {
					st.Close()
				}
				return
			}
			c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
			pipe(c, st)
		}()
	}
}

func muxAgent(s session) error {
	for {
		st, err := s.accept()
		if err != nil {
			return err
		}
		go func() {
			var n [1]byte
			if _, err := io.ReadFull(st, n[:]); err != nil {
				st.Close()
				return
			}
			target := make([]byte, n[0])
			if _, err := io.ReadFull(st, target); err != nil {
				st.Close()
				return
			}
			c, err := net.DialTimeout("tcp", string(target), 30*time.Second)
			if err != nil {
				st.Write([]byte{1})
				st.Close()
				return
			}
			defer c.Close()
			st.Write([]byte{0})
			pipe(c, st)
		}()
	}
}

// pipe copies both ways, propagating each EOF as a half-close.
func pipe(a, b net.Conn) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		io.Copy(dst, src)
		if hc, ok := dst.(interface{ CloseWrite() error }); ok {
			hc.CloseWrite()
		} else if _, ok := dst.(*yamux.Stream); ok {
			dst.Close() // yamux Close is a half-close; reads continue until the peer's FIN
		}
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	<-done
	<-done
	b.Close()
}

func socksRequest(c net.Conn) (string, error) {
	c.SetDeadline(time.Now().Add(30 * time.Second))
	defer c.SetDeadline(time.Time{})
	var hdr [2]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil || hdr[0] != 5 {
		return "", errors.New("not SOCKS5")
	}
	if _, err := io.ReadFull(c, make([]byte, hdr[1])); err != nil {
		return "", err
	}
	c.Write([]byte{5, 0})
	var req [4]byte
	if _, err := io.ReadFull(c, req[:]); err != nil || req[1] != 1 {
		c.Write([]byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0})
		return "", errors.New("only CONNECT")
	}
	var host string
	switch req[3] {
	case 1, 4:
		ip := make([]byte, map[byte]int{1: 4, 4: 16}[req[3]])
		if _, err := io.ReadFull(c, ip); err != nil {
			return "", err
		}
		host = net.IP(ip).String()
	case 3:
		var n [1]byte
		if _, err := io.ReadFull(c, n[:]); err != nil {
			return "", err
		}
		name := make([]byte, n[0])
		if _, err := io.ReadFull(c, name); err != nil {
			return "", err
		}
		host = string(name)
	default:
		return "", errors.New("bad address type")
	}
	var port [2]byte
	if _, err := io.ReadFull(c, port[:]); err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(port[:])))), nil
}
