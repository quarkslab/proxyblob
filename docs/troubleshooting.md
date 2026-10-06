# Troubleshooting

Start by identifying which step failed: build, storage listener, agent registration, local SOCKS startup, or the application request. Record proxy/agent revisions, driver and toolchain. Redact account keys, generated connection strings and SAS query parameters before sharing output.

## Build fails

Build from the current checkout using the committed module dependency:

```sh
go version
GOWORK=off GOFLAGS=-mod=readonly make
```

The Makefile builds packages, not a single `main.go`; building one file omits supporting files. `GOWORK=off` prevents an unrelated workspace from selecting a sibling aznet checkout. If Go requests a newer toolchain, allow toolchain selection or install the requested version. Preserve the first actual compiler error when reporting a problem.

## Listener cannot start

Verify the configured listener **name**, driver and matching endpoint. Listeners do not start automatically. For Azurite, wait for all services to be ready and use the pinned invocation with `--skipApiVersionCheck` in [getting started](getting-started.md). For Azure, check account credentials and authorized network access; anonymous Blob access is unnecessary.

A listener left in `stopping` after a cleanup error deliberately blocks reuse. Reconcile the outstanding session/resources before restarting it.

## Agent is quiet or exits

A healthy native agent is normally quiet. Look for **Agent connected** at the proxy, then run `agent ls`. If it exits, inspect its process exit status and any numeric diagnostic:

| Exit status | Meaning |
|---|---|
| 0 | Normal completion |
| 1 | External context cancellation |
| 2 | Missing connection string |
| 3 | Parsing, dialing or handler-configuration failure |
| 4 | Identity exchange failure |

Exit statuses and diagnostic codes are separate namespaces. [Numeric diagnostics](agent-errors.md) lists the latter. Descriptions are proxy-owned; the agent no longer prints raw SDK/JS causes. A native or WASM binary still contains Go/runtime/dependency strings.

Generate a fresh complete connection string from a running listener. Both processes need access to its endpoint. A localhost URL works only when the agent can reach storage at its own localhost. Confirm matching proxy/agent versions and the WASM host contract when applicable.

## SOCKS request fails

Run `agent ls`, select the full agent ID and run `agent start`. Use the actual **Proxy started** port. `curl --socks5-hostname` resolves the destination through the agent; `--socks5` can resolve it locally. The agent must be able to reach the target and resolve its hostname.

A target such as `127.0.0.1:8000` refers to the agent's loopback. Keep the SOCKS endpoint on proxy loopback unless your deployment controls access: only NoAuth is implemented.

For UDP, use a SOCKS UDP-capable client and keep the TCP association alive. For BIND, ensure the peer can reach the returned agent address; public/NAT mapping is not automatic. See the [UDP](udp-tunnel.md) and [BIND](socks-bind.md) guides for protocol-specific cases.

## Traffic is slow

Record the driver, workload, concurrency, payload size, region and approximate latency/throughput. Compare repeated runs with the [measured baseline](performance.md). Blob's directional lock fix removes one source of contention, but polling, request latency, batching and service throttling remain. Do not increase buffers without measuring memory and healthy-stream progress.

## Removal or shutdown reports an error

Wait for important application transfers to finish before `agent stop` or `agent rm`. A forced drain or timeout does not prove delivery or remote resource deletion. Agent removal clears local selection even when cleanup reports an error; inspect the remote process and owned storage resources separately.

`listener stop` retains bootstrap discovery resources by design. Remove only the retired namespace's resources, after all users stop. For the disposable emulator tutorial, stopping its unmounted `--rm` container removes its data; persistent volumes do not disappear on restart.
