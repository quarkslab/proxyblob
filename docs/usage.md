# Usage

A storage listener accepts agents. Each connected agent can provide a SOCKS endpoint on the proxy machine. See [setup](getting-started.md) for the initial connection.

## Commands

Run these inside the proxy prompt:

| Command | Action |
|---|---|
| `listener ls` | List configured listeners |
| `listener start NAME` | Start and select a listener |
| `listener select NAME` | Select the listener used by `new` |
| `new --listener NAME --duration 168h` | Generate an agent connection string |
| `agent ls` | List agents and their session expiry |
| `agent select FULL_ID` | Select an agent |
| `agent start --listen 127.0.0.1:1080` | Start its local SOCKS endpoint |
| `agent stop` | Stop local SOCKS service and its active connections |
| `agent rm` | Disconnect and remove the selected agent |
| `listener stop NAME` | Stop the listener and disconnect its agents |

Use `help`, `help agent` or `help listener` for available options. Check the reported SOCKS port when running several agents: if another ProxyBlob endpoint uses 1080, the next port may be selected.

## Applications

Set your application's SOCKS5 proxy to `127.0.0.1:1080`, or the port reported at startup. Enable proxy-side hostname resolution if the application offers it.

```sh
curl --noproxy "" --socks5-hostname 127.0.0.1:1080 https://example.com/
```

SSH/SFTP needs a SOCKS-capable wrapper or a suitable `ProxyCommand`. UDP needs a client that supports SOCKS5 UDP ASSOCIATE; sending plain DNS requests to port 1080 will not work. Native agents also support BIND, provided the incoming peer can reach the agent's advertised address. WASM agents support TCP and UDP, but not BIND.

## Agent credentials

Pass the generated connection string with `-c`, or set `CONNECTION_STRING` in the agent's environment. To embed it when building:

```sh
make agent TOKEN='<connection-string>'
```

The resulting binary contains the credential; keep it private.

`new --duration` controls how long the string permits new connections (default: seven days). Listener configuration can set `"session_duration": "24h"`, the default authorization lifetime for a new session. `agent ls` shows the issued expiry. Changing these settings does not renew existing sessions; reconnect with fresh credentials when needed.

## Agents behind a proxy

A native agent reaches storage through the machine's HTTP proxy when there is one. It uses the standard `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` variables, and on Windows, when none of them is set, the system proxy settings, including a PAC file or automatic detection. The settings are read when the agent starts; restart it to pick up a change. A PAC file is still consulted for each new connection.

Put credentials in the proxy URL, with special characters percent-encoded:

```sh
HTTPS_PROXY='http://CORP%5Calice:p%40ssw0rd@proxy.corp:3128' ./agent -c '<connection-string>'
```

The authentication scheme is chosen from what the proxy offers: Negotiate (Kerberos), then NTLM, then Basic. Without credentials in the URL:

- **Windows** uses the logged-on user for Negotiate and for NTLM.
- **Linux and macOS** use a Kerberos ticket from the file cache named by `KRB5CCNAME` (default `/tmp/krb5cc_<uid>`). The macOS default cache is not readable; run `kinit` with `KRB5CCNAME=FILE:/path`. With a password in the URL, the agent logs in using `krb5.conf` (`KRB5_CONFIG`), or finds the KDC through DNS when you write `user@REALM`.

The connections the agent opens for SOCKS clients (CONNECT) follow the same settings. SOCKS does not say what protocol a connection carries, so a target on port 80 is looked up as `http://host` (and follows `HTTP_PROXY`), and a target on any other port as `https://host:port` (and follows `HTTPS_PROXY`). What the agent then does depends on the answer:

- No proxy for the target (no settings, `NO_PROXY` or the Windows bypass list, or a PAC file answering `DIRECT`): a direct connection.
- A proxy a PAC file names for this target in particular: that proxy, then a direct connection if it refuses or fails.
- A proxy that is only the default (`HTTPS_PROXY`, a static Windows proxy, or the proxy a PAC file gives every host it does not single out): a direct connection first, for up to 5 seconds, then the proxy, unless the target refused the connection. Internal hosts are reached without bothering the proxy, and hosts only reachable through it still are, after that delay.

When a PAC file lists several proxies, the next one is tried only if one cannot be reached. Each CONNECT gets 10 seconds in all, so a dead host frees its stream as quickly as it did without a proxy. When everything fails, the SOCKS reply names the most useful answer: `connection not allowed` (2) when the proxy rejected the agent's credentials, `general failure` (1) when it refused or failed the tunnel, and otherwise the direct connection's error. Most proxies only tunnel to port 443. BIND and UDP never use the proxy.

Only HTTP and HTTPS proxies are supported, not SOCKS. WASM agents use the host runtime's networking and its own proxy settings.

## WASM agent with Bun

Install Bun **1.4 or later**, which the included host requires. From the repository root:

```sh
make wasm
bun run examples/bun/agent.ts ./agent.wasm -c '<connection-string>'
```

The [Bun example](../examples/bun/agent.ts) loads the agent and provides its TCP/UDP sockets. Keep Go installed and use the same toolchain for building and launching; the launcher loads that toolchain's `wasm_exec.js` automatically. `CONNECTION_STRING` also works here instead of `-c`.

## Logging

Normal operation uses `info`. For connection diagnostics:

```sh
./proxy -c config.json --log-level debug
```

You can also set `"log_level": "debug"` at the top level of `config.json`. The startup flag overrides configuration. Available levels are `trace`, `debug`, `info`, `warn` and `error`.

Use `info` for interactive operation: `warn` and `error` also hide confirmations and generated connection strings. Common socket closures appear at `debug`; unexpected failures remain warnings.

## Upgrading

Stop active sessions and replace both proxy and agent with builds from the same release. Older tunnel versions are incompatible. If using WASM, update the Bun example files with the agent as well.
