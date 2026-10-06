# Proxy logging

The proxy displays `info` and higher by default. Set the minimum level in your existing configuration:

```json
"log_level": "info"
```

Or override it for one run:

```sh
./proxy -c config.json --log-level debug
```

Precedence is startup flag, then configuration, then `info`. Supported levels, from most detailed to least, are `trace`, `debug`, `info`, `warn`, and `error`. Invalid values are rejected at startup.

| Level | Use |
| --- | --- |
| Trace | Most detailed logging, including lower levels; currently no dedicated trace events |
| Debug | Stream cancellation, reset, broken pipe and disconnected-socket diagnostics (codes 59–62), with connection IDs |
| Info | Normal operation, agent connections and command confirmations |
| Warn | Unknown forwarding failures, timeouts, protocol problems and ambiguous legacy code 10 |
| Error | Operations that could not be completed |

Use `info` for everyday interactive operation and `debug` when collecting stream diagnostics. `warn` and `error` also hide informational command output, including generated connection strings. Debug-level socket errors are not proof of successful delivery: applications can cancel connections intentionally or unexpectedly. They remain available for investigation rather than being discarded.

This controls proxy logging only. Agent diagnostics remain numeric. No event rate limiting is applied.
