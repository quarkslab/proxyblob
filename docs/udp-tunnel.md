# UDP associations through the tunnel

SOCKS UDP ASSOCIATE now binds a UDP socket at the **proxy**, on the local IP of
its accepted TCP control connection. The SOCKS reply names that IP and the new
UDP port. The agent owns a separate target-facing UDP socket. Complete SOCKS UDP
headers and payloads cross the existing multiplexed tunnel in both directions;
there is no direct client-to-agent UDP path and no aznet Driver change.

Deploy proxy and agent together: the tunnel handshake now requires **version 3**.
Versions 1, 2, empty legacy handshakes, and future unsupported versions fail with
an explicit negotiation error. TCP DATA, CREDIT and directional EOF semantics
remain unchanged. Native BIND and negotiation corrections are described in [SOCKS BIND](socks-bind.md).
Additional authentication remains outside this effort. It does not claim
full SOCKS5 conformance.

## Wire records and ownership

Records keep the existing command/UUID/length header. NEW/ACK still carry a
4-byte version and 8-byte receive window, both big-endian. Added commands:

| Command | Payload |
| --- | --- |
| 7, UDP ASSOCIATE | Client's SOCKS source hint: ATYP, address, 2-byte port |
| 8, UDP READY | SOCKS REP byte followed by proxy bind ATYP/address/port; failure uses an all-zero IPv4 bind address |
| 9, DATAGRAM | One complete SOCKS UDP packet: RSV(2), FRAG, ATYP, address, port, payload |

The agent retains SOCKS authentication and command negotiation. It first binds
its target socket, asks the proxy for a relay, then returns the proxy's result to
the client. An association shares its TCP logical-stream UUID; a repeated setup
is rejected. TCP EOF, full close, handler cancellation and tunnel loss dispose
both UDP sockets, destination state and queued datagrams. Pending socket I/O and
DNS resolve are interrupted. Setup failures return a SOCKS failure before close.

## Source checks, addresses and packet limits

Following [RFC 1928 sections 6 and 7](https://www.rfc-editor.org/rfc/rfc1928.html),
the proxy only accepts UDP from the TCP peer's IP. A supplied nonzero source port
must match; otherwise the first well-formed packet pins the source port. A
nonzero literal source hint that differs from the TCP peer is rejected. Domain
source hints never override the TCP peer's IP and need no proxy-side DNS.
Every packet must have zero RSV and zero FRAG; fragmented and malformed packets
are dropped. Fragment reassembly is not implemented.

Destination IPv4, IPv6 and domain fields survive the tunnel. DNS runs at the
agent. Replies carry the actual numeric source IP/port, including IPv6. Only
previously contacted resolved IP/port pairs may reply. Entries idle for one
minute expire; new destinations at capacity are dropped while existing active
destinations remain usable.

The supported maximum is **65,507 bytes for the entire SOCKS UDP packet**.
Subtract 10 bytes for an IPv4 header, 22 for IPv6, or `7 + domain length` for a
domain. Replies may need a larger header than requests: reserve 22 bytes when a
domain can resolve to IPv6. Oversized replies are dropped whole. Relay reads use
65,535-byte buffers, so larger OS packets are rejected without forwarding a
truncated prefix. Adapter callers with smaller buffers receive `io.ErrShortBuffer`.
Native OS UDP limits and path MTUs can be lower: the test macOS host rejects a
36 KiB send, while isolated Linux tests exercise the full supported size. IP
fragmentation remains subject to the OS/network; SOCKS FRAG is unsupported.

The returned address is the proxy's TCP-local interface address. Deployments
behind NAT must provide reachability to that interface and ephemeral UDP ports;
there is no external advertised-address or NAT port-mapping configuration here.

## Finite resources and overload

Association count shares `PROXYBLOB_MAX_STREAMS` (default 128) and the existing
TCP receive reservation. Additional per-association limits are configurable on
both endpoints through `FlowConfig` or decimal environment variables:

| Variable | Default | Scope |
| --- | ---: | --- |
| `PROXYBLOB_UDP_QUEUE_BYTES` | 262144 | Incoming queued datagram payload bytes |
| `PROXYBLOB_UDP_QUEUE_PACKETS` | 64 | Incoming queued datagram records |
| `PROXYBLOB_UDP_DESTINATIONS` | 64 | Resolved target IP/port entries at the agent |

The 256 KiB default admits four maximum-sized packets, or 64 smaller records;
regressions send 100 maximum packets to a stalled consumer and verify exactly
four complete packets remain while healthy TCP still progresses. At 128 active
associations, these queues add at most 32 MiB per endpoint to the TCP rings.
Destination tests enforce the configured limit while existing destinations keep
working. These are bounded-memory choices, not throughput-tuning claims.

Outgoing datagrams share the fair per-stream DATA scheduler, its stream/tunnel
byte budgets and its 64-record-per-stream cap, including in-flight writes. UDP
uses **no TCP receive credit** and never waits for outbound queue capacity:
excess packets are dropped whole. Incoming queue saturation, destination limits,
and JS host send refusal also drop whole packets. A packet larger than a
configured stream or batch budget is dropped. Control records retain separate
bounded admission; control payloads now reach 259 bytes. TCP remains reliable
and pauses at its credit-backed bounds. Fatal UDP socket errors are reported and
close the association; a fatal tunnel write fails the tunnel.

Socket buffers, bounded JS host queues, one receive scratch buffer and one
working packet per direction are additional memory. Defaults do not constitute
a whole-process heap cap. The v2 JS adapter separately retains at most 256 KiB /
64 incoming packets; the reference host caps outstanding sends at 64 packets
(each at most 65,507 bytes). Production hosts must supply equivalent bounds.

## Platform evidence and reproduction

`GOWORK=off tests/udp-topology/run.sh` builds native handlers and runs client,
proxy and agent in separate Docker network namespaces. Two internal networks
connect client→proxy and proxy→agent; direct client→agent UDP is checked to be
unreachable. IPv4, IPv6, agent-side domain resolution, zero-length payloads,
16/32 KiB and maximum-sized packets, multiple clients/destinations, response
addresses, malformed FRAG and TCP teardown are exercised. The script also runs
native socket regressions under Linux, including a spoofed alternate source IP,
finite limits and racing teardown. Resources created by the script are removed.
This validates a real TCP-carried multiplexed tunnel, not Azure storage behavior.

The [Bun 1.4.2 host harness](js-socket-host.md) executes the real Go/WASM SOCKS
handler and multiplexing against real dual-stack UDP echo sockets, including
IPv4, IPv6, domains, zero-length packets and control EOF. Its protocol peer
models proxy setup; the separated namespace test covers the production native
proxy. Node tests cover deterministic JS adapter cases. Neither compilation nor
these harnesses establish production runner/browser or WebSocket compatibility.

WASM literal UDP destinations retain socket host v2. Domains additionally
require the optional `UDPResolve(host, onIP, onError): Handle` extension, using
the same immediate, idempotent disposal/callback-detachment contract. A missing
extension is an explicit unsupported-host resolution error; such packets are
dropped. Validate and deploy the production resolver and dual-stack socket
capabilities before promising parity. No production deployment is part of this
change.

## Opt-in live Azure validation

`AZNET_LIVE_CONFIG=/absolute/path/to/config.json tests/udp-topology/run-live.sh azblob`
uses real aznet `Listen`/`Dial`, bootstrap SAS, session credentials and Azure
storage for the tunnel. Repeat with `azqueue` and `aztable`. The configuration
must contain an HTTPS Azure account-key listener for the selected driver. The
script defaults to the committed module dependency with `GOWORK=off -mod=readonly`.
For source integration, set `PROXYBLOB_LIVE_GOWORK=/absolute/go.work` explicitly;
record its selected revision separately from published-dependency results.
It creates only random, invocation-prefixed bootstrap/session resources and
removes them on exit. No existing listener namespace is used. Run only against
an account where these temporary resource operations are authorized.

The client network is internal; proxy and agent have separate Azure HTTPS
network access. A direct client-to-agent UDP probe must fail. Only the proxy
and cleanup container receive the read-only configuration mount; the agent
receives a private bootstrap-SAS file. The client receives neither. No endpoint,
credential, SAS or raw SDK error is logged. Logs contain payload sizes,
round-trip latency and aznet's sanitized per-operation HTTP request counters.
The temporary handoff files and Docker resources are removed on exit.

The client exercises three simultaneous associations across IPv4, IPv6 and
agent-resolved domains, including zero payload and the full 65,507-byte SOCKS
packet bound. Invalid RSV/FRAG, truncated headers and a different UDP source
port must not elicit replies; a following byte-identical valid probe must
succeed. A separate association sends 100 32-KiB payloads without reading while
healthy associations progress. This is a pressure/liveness check, not a claim
about exact drop counts, OS buffering or a measured memory ceiling. Closing
control connections and an active Azure tunnel must close their client relays;
the still-running proxy independently rebinds each advertised UDP port to prove
the socket was released.

aznet's 250-ms best-effort FIN or the handler's deliberate deadline interruption
can report an expected abort/deadline at tunnel shutdown. Those results are
reported separately from successful datagram delivery. Unexpected close errors
fail the run. Normal teardown must leave no resources under the invocation's
prefix: an independent Azure catalog query checks this, reclaims any leftovers,
and fails if it had to reclaim anything. This verifies storage cleanup, not
private goroutine/map counts. Live native results do not establish WASM/browser
networking support or full SOCKS conformance.
