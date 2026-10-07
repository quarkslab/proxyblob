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

## WASM agent with Bun

Install Bun **1.4.2**, which the included host requires. From the repository root:

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
