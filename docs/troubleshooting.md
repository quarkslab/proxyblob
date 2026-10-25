# Troubleshooting

## Build fails

Build from the repository root with `make`, rather than compiling an individual Go file. Allow Go to download the toolchain requested by dependencies.

## Listener or agent does not connect

- Check the listener name, driver, endpoint and account credentials in `config.json`.
- Run `listener start NAME` before generating a connection string with `new`.
- Make sure both proxy and agent can reach the storage endpoint. A localhost URL only works if storage is reachable on the agent's own machine.
- Try a fresh, complete connection string and matching proxy/agent builds.
- If the machine reaches the internet through a proxy, see [Agents behind a proxy](usage.md#agents-behind-a-proxy). Check `HTTPS_PROXY`, `NO_PROXY` and the credentials in the proxy URL.

The agent is normally quiet. Look for **Agent connected** on the proxy and check `agent ls`. If the agent exits, status `2` means missing credentials, `3` means connection/setup failure, and `4` means agent registration failed. Numeric messages printed by the agent are separate diagnostic codes; include them when reporting a problem.

## SOCKS requests fail

Select an agent, run `agent start`, and use the port shown by **Proxy started**. Check that the agent can reach the destination. Use `curl --socks5-hostname` to resolve hostnames from the agent's network. `curl -v` prints the SOCKS reply code: `(6)` TTL expired after about 10 seconds means the agent's direct connection was silently dropped, typically on a network that only lets traffic out through a proxy the agent does not know about (set `HTTPS_PROXY`); `(4)` means the name did not resolve or the host is unreachable from the agent.

**Client refused: no free tunnel stream** (error 45) means every stream of the session was in use for 30 seconds. The log line counts them by phase: `negotiating` (SOCKS greeting), `opening` (waiting for the agent to accept the stream), `awaiting_reply` (the agent is connecting to the target), `forwarding`, and among those `half_closed`. Mostly `awaiting_reply` means targets that do not answer: lower the scanner's thread count or its timeout. With proxychains, set `tcp_read_time_out` above 10000 so it waits for the agent's answer instead of leaving the stream behind.

For UDP, use a SOCKS5 UDP-capable client. BIND requires a native agent and a reachable incoming address; it does not configure firewall or NAT rules.

## Transfers are slow

Azure request latency and the selected storage service affect speed. Compare the same transfer with Blob, Queue and Table using several runs. Check whether the delay is in connection setup or sustained transfer, and whether it also occurs when accessing the destination directly from the agent.

When reporting a slowdown, include the driver, native or WASM agent, approximate download/upload speed, and whether one or several connections were active.

## Errors appear after a transfer

Applications may close extra connections when a download or speed test ends. Common socket-closure messages are available at `debug`. A repeated warning, timeout or interrupted transfer is worth investigating:

```sh
./proxy -c config.json --log-level debug
```

## Stop or removal fails

Wait for important transfers to finish before stopping an agent. If cleanup fails, check the remote agent and the listener's Azure resources before retrying. A listener still marked `stopping` cannot be restarted until cleanup completes.

`listener stop` retains shared discovery resources. Delete only resources belonging to a retired listener, after all its users have stopped.

When sharing logs, include the ProxyBlob version and driver, and redact account keys and connection strings.
