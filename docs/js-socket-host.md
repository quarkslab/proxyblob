# JS socket host contract and Bun runtime tests

The WASM SOCKS adapters require `globalThis.ProxyBlobSocketHostVersion = 2`.
Legacy hosts are rejected with `errors.ErrUnsupported` before callbacks are
allocated. This is a coordinated host change: update and validate the production
runner before deploying the new WASM agent. The runner is not in this repository;
passing these tests does not establish production browser or runner compatibility.

## Lifetime and scheduling

The setup/data/error callback arguments remain; TCP adds a writable notification:

```typescript
TCPDial(host, port, onConnect, onData, onClose, onError, onWritable): TCPHandle
UDPListen(onBind, onData, onError): Handle

interface Handle { dispose(): void }
interface TCPHandle extends Handle { read(maxBytes: number): void }
```

Both factories return an immediate handle, including during DNS, connect, or
bind. Factories and handle/socket methods must not throw. Report failures via
`onError(message)`. Callbacks must run asynchronously, never inline within a
factory, handle method, or socket method. A successful setup callback occurs at
most once. Setup errors and TCP close-before-connect are terminal. A Go setup
operation is canceled by handler or logical-stream closure and times out after
10 seconds. No callback can block waiting for Go I/O.

`dispose()` is idempotent and synchronously detaches **all** callback references
before returning. It cancels pending work and fully closes owned sockets.
Queued events and late completions consult the detached state; a late-created
resource is closed without invoking Go. Disposal must not invoke callbacks.
Go calls `js.Func.Release` only after this barrier, once per callback. A host
that advertises v2 without meeting this contract is unsupported.

For TCP, `onConnect(socket)` exposes `write(Uint8Array): number | "would-block"` and `end(): void`.
`write` reports the actual accepted byte count, including short or zero writes;
acceptance is not an acknowledgement of remote delivery. A full finite write
queue returns `"would-block"` without admitting bytes. The host must subsequently
invoke `onWritable()` when capacity returns (or report an error). Go waits without
holding its state mutex, wakes on readiness/close/error, and sends chunks of at
most 64 KiB. A numeric short count remains an explicit short-write error.
The harness bounds its write queue at 256 KiB; ordinary congestion waits instead
of resetting the logical stream. `end()` sends local FIN
after admitted writes and leaves reads alive. `onClose()` means **peer EOF**,
ordered after all preceding data; it leaves writes alive. Neither FIN is resource
disposal. The owner must eventually call `Close`, including after bidirectional
EOF. Fatal errors dispose immediately and unblock pending I/O with an error.

## Receive and memory bounds

TCP begins with zero delivery credit. `read(maxBytes)` authorizes exactly one
nonempty `onData(socket, Uint8Array)` of at most that length, or peer EOF/error.
Only one read may be outstanding. Partial chunks consume the entire grant;
there is no accumulated credit. Go requests at most 64 KiB and retains at most
one chunk, with no second pipe or goroutine queue. It grants again only after the
reader consumes the chunk. Oversized or uncredited data is a host protocol
violation, rejected before copying. Normal slow readers stop receiving credits;
they are not reset or silently truncated.

Hosts must also bound their own queues and stop socket reads when out of receive
capacity. The Bun harness uses a 64 KiB Readable high-water mark, plus at most one
512 KiB native receive event: a conservative 576 KiB host receive allowance.
Together with Go's 64 KiB chunk, retained receive payload is at most 640 KiB per
TCP connection, excluding kernel socket buffers and transient runtime copies.
`PullSocket` suppresses automatic stream prefetch and only resumes native reads
when the host queue is empty and Go has requested data. Bun's ordinary
`Socket.read()` otherwise resumes native input even when buffered bytes remain.
The harness checks the bound; a violation fails tests rather than relaxing it.

UDP preserves datagrams and source addresses through
`onData(socket, Uint8Array, port, address)`.
`onBind(socket, localPort)` exposes `send(data, port, address): boolean`;
false means the datagram was not admitted. Go accepts up to 65,507 bytes per
packet and queues at most 64 packets / 256 KiB, dropping excess packets before
copying (UDP is lossy). The host adds no receive queue and bounds pending sends
at 64 datagrams. Read deadlines can be moved or cleared while a read is blocked;
close/error interrupts it. TCP read/write deadlines remain explicitly unsupported.

## Running the tests

Use **Bun 1.4.2**, Go, and Node (for the supplementary Node runner). The harness
pins Bun 1.4.2 because earlier Bun 1.3.14 failed real half-close regressions.
There is no FFI, private file-descriptor access, or POSIX-specific workaround.
A different Bun version requires rerunning the socket contract tests before
changing the pin. The runtime runner prints the executable and toolchain used.

From the repository root:

```sh
bun install --cwd tests/js-host --frozen-lockfile
bun run --cwd tests/js-host typecheck
bun test tests/js-host
GOWORK=off bun run tests/js-host/run.ts
```

The last command builds the **actual Go test binary for js/wasm**, dynamically
loads `$(go env GOROOT)/lib/wasm/wasm_exec.js` from that same selected toolchain,
and executes it under Bun. It never vendors a shim or assumes a fixed GOROOT.
For local source integration, repeat with an explicit `GOWORK=/absolute/go.work`
containing this checkout and the chosen aznet source. The committed aznet pin is
unchanged; standalone builds use `-mod=readonly`.

The Go tests cover setup success/error/timeout, close-before-connect, late
completion suppression, callback release, teardown, receive violations, bounded
UDP delivery, deadline interruption, and half-close/write-count behavior.
A diagnostic fixture checks released callback IDs against the real Go runtime
registry after host detachment. Those diagnostic references are never dispatched
as socket events. The Node runtime can execute these same deterministic cases:

```sh
GOWORK=off GOOS=js GOARCH=wasm go test -mod=readonly \
  -exec "$(GOWORK=off go env GOROOT)/lib/wasm/go_js_wasm_exec" ./pkg/proxy/socks
```

Bun additionally creates real loopback TCP/UDP peers: 4 MiB deterministic byte
identity through a stalled reader, a healthy concurrent peer, EOF ordering,
both half-close directions, a paused destination receiving a 4 MiB write, and UDP echo. TypeScript tests exercise pending
cancellation, late/failed bind, refused connections, slow grants, and actual resource
closure. End-of-run counters require zero live handles, callback references,
and owned sockets. Go runtime timer handles are terminated only after owned
socket cleanup and temporary-output removal.

These tests exercise actual TCP/UDP resources, **not WebSockets**. A production
host using a WebSocket bridge must implement the same disposal barrier for its
WebSocket, queued messages, and connection-attempt callbacks, and validate its
own finite queues and backpressure before rollout. No WebSocket cleanup claim
is made here. UDP tunneling and SOCKS BIND changes are outside this work.

Bun API references: [TCP](https://bun.sh/docs/runtime/networking/tcp),
[Node compatibility](https://bun.sh/docs/runtime/nodejs-compat), and the pinned
[Bun socket implementation](https://github.com/oven-sh/bun/blob/bun-v1.4.2/src/js/node/net.ts).
