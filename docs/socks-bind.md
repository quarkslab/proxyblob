# Native BIND and SOCKS command replies

Native agents implement SOCKS5 BIND over the existing version-3 tunnel. Following
[RFC 1928 section 6](https://www.rfc-editor.org/rfc/rfc1928.html), the agent sends
one reply after opening a TCP listener and another after accepting the expected
peer. Both contain the actual socket address family and port. The accepted
conversation uses the same bounded forwarding and directional EOF as CONNECT.
No aznet driver or tunnel wire-format change is required.

The request names the expected **remote peer**, not the local listen address.
A literal IP must match the peer; a domain is resolved once and the resulting
IPs form the allowlist. A nonzero port must match. An unspecified IP allows any
peer of that family; port zero allows any source port. Unexpected peers are
closed and the listener continues waiting within the original timeout.

For a specified peer the agent chooses its local interface using the route to
that peer, without sending a probe packet, and binds an ephemeral TCP port there.
For an unspecified peer it chooses an up, non-loopback, non-link-local interface
of that family. Multi-homed deployments should specify the expected peer so the
route determines the interface. The first reply advertises that concrete local
address. NAT mapping and firewall reachability must be arranged by deployment;
the agent does not discover public addresses or open firewall ports.

Resolution, listener setup and peer waiting share a two-minute default timeout,
configurable for embedded callers with `WithBindTimeout` on the SOCKS handler
constructor. This is a finite rendezvous allowance, not a measured performance
limit and not an idle timeout on accepted traffic. Client EOF before acceptance
cancels the rendezvous; clients should wait for the second reply before sending
payload or half-closing. After acceptance, directional EOF preserves the opposite
response direction. Client/tunnel cancellation releases the listener and any
accepted socket. Timeout returns a failure second reply if the first was sent.

WASM explicitly returns command-not-supported and closes gracefully without
allocating a host listener or socket. CONNECT and UDP remain supported. No new
host API is required. Failed method negotiation returns exactly `05 ff`;
command failures return a SOCKS reply before graceful stream closure. CONNECT
success reports its actual IPv4 or IPv6 local endpoint. NoAuth is unchanged;
this does not claim support for every authentication method in RFC 1928.

## Validation

Native regression tests cover both BIND replies, IPv4/IPv6/domain requests,
peer-port rejection, timeout, control/tunnel closure, repeated listener release,
byte identity and half-close response. The Bun harness executes WASM BIND
rejection with zero host allocations alongside existing socket tests.

`GOWORK=off tests/udp-topology/run.sh` exercises separate client/proxy/agent
namespaces. A test-only service in the agent namespace initiates the reverse
connection; its instruction also travels through SOCKS CONNECT. Agent loopback
listeners cannot be reached directly from the client namespace.

For real Azure and independent resource cleanup, opt in with:

```sh
LIVE_BIND=1 AZNET_LIVE_CONFIG=/absolute/path/to/config.json tests/udp-topology/run-live.sh azblob
```

Use `azqueue` or `aztable` for the other drivers. The BIND cases run before the
existing UDP matrix. This is native runtime validation; WASM-over-Azure and
public/NAT inbound reachability are not implied.
