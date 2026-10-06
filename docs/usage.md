# Everyday usage

The **storage listener** accepts agent tunnels. The **agent's SOCKS endpoint** accepts local applications. Starting one does not start the other. Use [getting started](getting-started.md) for the complete sequence.

## Manage listeners and agents

Run these inside the proxy prompt:

| Command | Result |
|---|---|
| `listener ls` | Show configured listeners and state |
| `listener start NAME` | Start a configured listener and select it |
| `listener select NAME` | Choose the listener used by `new` |
| `new --listener NAME --duration 168h` | Generate credentials for joining that running listener |
| `agent ls` | Show connected agents and issued session expiry |
| `agent select FULL_ID` | Select the agent for subsequent commands |
| `agent start --listen 127.0.0.1:1080` | Start its local SOCKS endpoint |
| `agent stop` | Stop its SOCKS endpoint and active logical streams; retain the agent tunnel |
| `agent rm` | Remove the selected agent and tunnel; clear selection |
| `listener stop NAME` | Stop acceptance and close that listener's sessions |

Use `help`, `help agent` and `help listener` for the installed binary's options. A listener already owned by incomplete cleanup cannot be restarted until the failure is reconciled. Check the reported SOCKS port instead of assuming 1080 when running multiple agents.

## Use applications through SOCKS

For HTTP and HTTPS:

```sh
curl --socks5-hostname 127.0.0.1:1080 https://example.com/
```

For an OpenSSH/SFTP client, use a ProxyCommand or a SOCKS-capable wrapper appropriate to your platform. SFTP normally uses TCP CONNECT, not BIND. The [active FTP example](active-ftp-bind.md) exercises real BIND with tnftp and Dante.

A UDP application must support SOCKS5 UDP ASSOCIATE, keep its TCP control connection open, and send complete SOCKS UDP packets to the relay returned by the proxy. Plain `dig` against port 1080 does not perform that handshake. The [UDP/DNS harness](udp-tunnel.md#opt-in-live-azure-validation) provides a repeatable DNS-over-SOCKS test and documents its Azure opt-in.

BIND accepts an incoming peer at the agent. The first SOCKS reply reports the listening address; the second reports the accepted peer. Wait for the second reply before sending data. See [BIND behavior and reachability](socks-bind.md).

## Authorization lifetimes

| Setting | Controls | Default |
|---|---|---|
| `new --duration` | Bootstrap credential validity for joining | 168 hours |
| Listener `session_duration` in config | Authorization issued to newly accepted sessions | 24 hours |
| `agent ls` expiry | Actual issued session-token expiration in UTC | `unknown` if metadata is unavailable |

Changing configuration does not renew an existing session. Bootstrap expiry alone does not terminate established sessions. There is no automatic credential renewal. Expiry is informational and not a liveness promise; credentials may fail for other reasons.

## Build or deploy an agent

```sh
make agent
make wasm
```

An optional `TOKEN='<generated-connection-string>'` embeds the bearer credential in the output binary. Use it only when that artifact and build command are protected. Otherwise provide `-c` or `CONNECTION_STRING` at runtime.

Native proxy/agent builds need compatible tunnel protocol versions. For WASM, load `wasm_exec.js` from the exact Go toolchain used to build the binary and implement socket host v2. The [Bun 1.4.2 harness](js-socket-host.md) is a development/test host, not a ready-made production runner.

## Limits and performance

TCP senders pause when their reserved receive credit is exhausted. UDP queues instead drop complete packets under overload. Increasing windows can increase memory without improving a storage-latency bottleneck.

Configure the finite environment limits described in [flow control](flow-control.md) and [UDP tunneling](udp-tunnel.md). [Measured performance](performance.md) records concurrency, heap samples, latency and request counts. Keep those measurements separate from your own region, account and workload.

Before deployment, use the [release validation and rollout checklist](release-validation.md). In particular, native Azure tests do not prove production WASM-host behavior, and BIND does not arrange inbound firewall/NAT reachability.

## Logging

The default `info` level shows normal operation without per-stream socket-closure messages. Use `./proxy -c config.json --log-level debug` to collect those diagnostics, or set `log_level` in the configuration. See [logging levels](logging.md) for precedence and severity.
