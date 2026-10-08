// Package socks5 is the SOCKS5 wire format (RFC 1928): protocol constants,
// addresses and the UDP request header.
package socks5

// SOCKS protocol versions.
const (
	Version5 byte = 0x05 // SOCKS Protocol Version 5
)

// Authentication methods as defined in RFC 1928.
const (
	NoAuth              byte = 0x00 // No authentication required
	GSSAPI              byte = 0x01 // GSSAPI
	UsernamePassword    byte = 0x02 // Username/Password (RFC 1929)
	NoAcceptableMethods byte = 0xFF // No acceptable methods
)

// SOCKS5 commands that clients may request.
const (
	Connect      byte = 0x01 // Establish TCP/IP stream connection
	Bind         byte = 0x02 // Listen for incoming TCP connection
	UDPAssociate byte = 0x03 // Set up UDP relay
)

// Address types for target addresses.
const (
	IPv4   byte = 0x01 // IPv4 address (4 bytes)
	Domain byte = 0x03 // Domain name (variable length)
	IPv6   byte = 0x04 // IPv6 address (16 bytes)
)

// Reply codes sent from server to client.
const (
	Succeeded               byte = 0x00 // Request granted
	GeneralFailure          byte = 0x01 // General failure
	ConnectionNotAllowed    byte = 0x02 // Connection not allowed by ruleset
	NetworkUnreachable      byte = 0x03 // Network unreachable
	HostUnreachable         byte = 0x04 // Host unreachable
	ConnectionRefused       byte = 0x05 // Connection refused by destination
	TTLExpired              byte = 0x06 // TTL expired
	CommandNotSupported     byte = 0x07 // Command not supported
	AddressTypeNotSupported byte = 0x08 // Address type not supported
)

// Buffer size limits.
const (
	MaxSocksHeaderSize = 262   // Maximum size of SOCKS header in bytes
	MaxUDPPacketSize   = 65535 // Maximum size of UDP datagram in bytes
)
