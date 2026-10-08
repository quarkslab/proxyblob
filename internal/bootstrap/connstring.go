// Package bootstrap is what the proxy and an agent agree on before the tunnel
// is multiplexed: the connection string the agent is given, the aznet dial
// options it implies, and the identity frame the agent sends first.
package bootstrap

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"proxyblob/internal/mux"

	"github.com/atsika/aznet"
)

var (
	// ErrEmpty reports a missing connection string.
	ErrEmpty = errors.New("empty connection string")
	// ErrInvalid reports a connection string that is not base64("driver|address").
	ErrInvalid = errors.New("invalid connection string")
)

// Query keys naming the listener's bootstrap resources. aznet does not infer
// endpoint names from the credential query keys, so the proxy carries them.
const (
	handshakeKey = "proxyblob-handshake"
	tokenKey     = "proxyblob-token"
)

// Endpoints returns a listener's bootstrap namespace: the aznet handshake and
// token resource names, derived from the listener name so restarts reuse them.
func Endpoints(listener string) (handshake, token string) {
	hash := sha256.Sum256([]byte(listener))
	suffix := fmt.Sprintf("%x", hash[:20])
	return "pbh" + suffix, "pbt" + suffix
}

// Encode builds the connection string an agent receives: the aznet address
// with the bootstrap namespace attached, prefixed by the driver name and
// base64-encoded as base64("driver|address").
func Encode(driver, address, handshake, token string) (string, error) {
	u, err := url.Parse(address)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set(handshakeKey, handshake)
	q.Set(tokenKey, token)
	u.RawQuery = q.Encode()
	return base64.RawStdEncoding.EncodeToString([]byte(driver + "|" + u.String())), nil
}

// Decode splits a connection string into the aznet driver and address.
func Decode(connString string) (driver, address string, err error) {
	if connString == "" {
		return "", "", ErrEmpty
	}
	decoded, err := base64.RawStdEncoding.DecodeString(connString)
	if err != nil {
		return "", "", ErrInvalid
	}
	driver, address, ok := strings.Cut(string(decoded), "|")
	if !ok {
		return "", "", ErrInvalid
	}
	return driver, address, nil
}

// DialOptions restores the bootstrap namespace carried by address. Addresses
// without one keep aznet's default endpoints.
func DialOptions(address string) ([]aznet.Option, error) {
	u, err := url.Parse(address)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	h, t := q.Get(handshakeKey), q.Get(tokenKey)
	if h == "" && t == "" {
		return nil, nil
	}
	if h == "" || t == "" || h == t || q.Get(h) == "" || q.Get(t) == "" {
		return nil, mux.ErrBootstrapNamespace
	}
	return []aznet.Option{aznet.WithEndpoints(h, t)}, nil
}
