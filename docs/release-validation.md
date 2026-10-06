# Stabilization validation — 2026-10-06

This records the validated source, not a deployment or a tagged release.
ProxyBlob source revision: `6493d6329a62dc3a1ff5a2f0ca3823b5926ea46b`.
The final documentation change does not alter executable source or dependencies.
aznet source: `7abcd80a7a2820d69e55deb1fc62f8b5a601f4be`, published as
`v0.0.0-20261006132211-7abcd80a7a28` and integrated by
[PR #23](https://github.com/quarkslab/proxyblob/pull/23).
A fresh module-cache download verified all 78 published source files against
that merged revision. No local `replace` is committed.

## Dependency and platform matrix

Both repositories were tested with `GOWORK=off -mod=readonly` and with an explicit
workspace selecting the above source checkouts. ProxyBlob and workspace checks
used Go 1.26.4 on darwin/arm64. Standalone aznet selected Go 1.26.5 through its
`toolchain` directive. Docker runtime cases used linux/arm64. Bun was 1.4.2;
the supplementary Node runtime was 26.3.0.

| Check | Published / standalone | Explicit workspace | Evidence scope |
|---|---|---|---|
| Both repositories, `go test -race -count=1 ./...` | Pass | Pass | Native correctness and race regressions |
| Both repositories, `go vet ./...` | Pass | Pass | Static analysis |
| Proxy `make`, native agent/proxy and WASM agent | Pass | Pass | Compilation |
| aznet native and `GOOS=js GOARCH=wasm go build ./...` | Pass | Pass | Compilation |
| aznet `AZNET_AZURITE=1` race suite, Azurite 3.34.0 | Pass | Pass | Supported Blob/Queue/Table emulator contracts |
| Actual Go/WASM SOCKS binary under Bun 1.4.2 | Pass | Pass | Real TCP/UDP sockets, bounded queues, byte identity, half-close, cancellation, zero owned handles/sockets/callbacks |
| Bun TypeScript typecheck and seven host tests | Pass | — | Host contract and socket lifecycle |
| Go/WASM adapter tests under Node | Pass | — | Deterministic adapter runtime, not production host networking |
| Separated Linux client/proxy/agent topology and native socket regressions | Pass | — | No direct client-agent route; BIND/UDP and teardown over a TCP-carried tunnel |
| Native live Azure Blob, Queue and Table topology | Pass | — | Actual storage-backed tunnels; matrix below |
| aznet live deletion-window and Table-reclamation tests with race detection | Pass | — | Real service responses and retry/reclamation invariants |

Azurite checks include connection ordering/half-close, Blob rollover, batched
session lifecycle, Queue token capacity/expiry and Table reclamation. Emulator
limitations remain; the live Table tests separately exercise account-key and SAS
receivers, successful 100-row batches, response-loss reconciliation, missing-row
rollback, ciphertext-identical retries and retention of the newest receipt.
Deletion-window tests require the actual service-specific 409 response, preserved
error wrapping and eventual resource-name reuse for all three drivers.

## Native live Azure results

Each driver used independently named temporary resources and the committed
published dependency. Direct client-to-agent and client-to-public-DNS access was
blocked by the topology. The application traffic therefore had to cross SOCKS
and the Azure tunnel.

| Driver | BIND IPv4/IPv6/domain | DNS A/AAAA/NXDOMAIN | UDP echo matrix | Independent residual resources |
|---|---|---|---|---|
| Blob | 3 pass, 94,208 bytes each, both replies and half-close | 3 pass | 45 pass | 0 before and after cleanup check |
| Queue | 3 pass, 94,208 bytes each, both replies and half-close | 3 pass | 45 pass | 0 before and after cleanup check |
| Table | 3 pass, 94,208 bytes each, both replies and half-close | 3 pass | 45 pass | 0 before and after cleanup check |

UDP covers three concurrent clients and IPv4/IPv6/domain destinations, empty
payloads through the maximum supported complete SOCKS packet, malformed/source
rejection, a stalled association beside healthy traffic, and relay release on
control/tunnel close. These tests verify packet identity and liveness; they do
not establish a loss rate, whole-process memory ceiling or full SOCKS conformance.
BIND uses reachable agent-local peers, not public inbound NAT traversal.

## Reproduction

From the ProxyBlob checkout, with Bun 1.4.2 on PATH:

```sh
GOWORK=off go test -mod=readonly -race -count=1 ./...
GOWORK=off go vet -mod=readonly ./...
GOWORK=off GOFLAGS=-mod=readonly make TOKEN= CONN_STRING=
bun install --cwd tests/js-host --frozen-lockfile
bun run --cwd tests/js-host typecheck
bun test tests/js-host
GOWORK=off bun tests/js-host/run.ts
GOWORK=off tests/udp-topology/run.sh
```

Create an explicit workspace outside either checkout with `go work init
/absolute/proxyblob /absolute/aznet`; repeat tests, vet, builds and the Bun runner
with `GOWORK=/absolute/go.work`. Check `go list -m github.com/atsika/aznet` before
using results as dependency-integration evidence.

In the aznet checkout, with Azurite services on localhost ports 10000–10002:

```sh
GOWORK=off AZNET_AZURITE=1 go test -mod=readonly -race -count=1 ./...
GOWORK=off go vet -mod=readonly ./...
GOWORK=off go build -mod=readonly ./...
GOWORK=off GOOS=js GOARCH=wasm go build -mod=readonly ./...
```

Live tests require explicitly authorized Azure test accounts in a local config.
They create and delete isolated temporary resources; no credentials belong in
logs or repository artifacts. From ProxyBlob, repeat for `azqueue` and `aztable`:

```sh
LIVE_BIND=1 LIVE_DNS=1 AZNET_LIVE_CONFIG=/absolute/config.json tests/udp-topology/run-live.sh azblob
```

From aznet:

```sh
GOWORK=off AZNET_LIVE_CONFIG=/absolute/config.json go test -mod=readonly -race -count=1 -run '^TestLive(BootstrapDeletionWindow|TableReclamation)$' -v ./...
```

The measurements and their limits are recorded separately in
[performance](performance.md). Raw logs and the exact validation scripts remain
local planning evidence; credentials are not published.

## Rollout requirements and remaining limits

- Drain existing tunnels and upgrade matching proxy/agent binaries together.
  Wire version 3 rejects older peers during logical-stream setup.
- WASM hosts must implement socket host v2, plus `UDPResolve` for domain UDP.
  Actual Bun socket tests do not validate a production browser/WebSocket bridge
  or WASM-over-Azure deployment. Run those host-specific checks before rollout.
- Validate proxy UDP and agent BIND advertised-address reachability for the
  deployment's firewall/NAT arrangement. No public-address discovery is provided.
- Session authorization expires independently of bootstrap credentials; no
  automatic renewal is implemented. Multi-day expiry/soak behavior and sustained
  production workloads were not established by this run.
- CLI agent removal clears local selection; finite FIN/cleanup failure is still
  possible. Check remote process termination and resource cleanup under the
  deployment's failure conditions. In-flight delivery must not be inferred from
  an abort or timeout.
- Native Windows runtime was not tested. Compilation and the Linux/macOS tests
  do not imply validation of every operating system or JavaScript runtime.
- Proxy-owned `ErrToString` remains in place. Restoring numeric diagnostic
  consumption and auditing application-owned agent strings is a separate open
  follow-up, not a completed property of this release.
