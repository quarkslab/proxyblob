# Numeric agent diagnostics

Application-owned error descriptions live in the proxy's `ErrToString` map in
`pkg/proxy/server/errors.go`. Shared protocol and agent code use numeric values.
`protocol.Error` implements `error` with a decimal representation and retains
sentinel identity through `errors.Is` and wrappers. `ErrorCode` classifies an error
without formatting it; unrecognized SDK/host errors become code 22.

The proxy decodes incoming nonzero CLOSE codes and local protocol diagnostics.
Unknown values retain their numeric code and are described as “unknown protocol
error”. Successful CLOSE does not produce an error log. The existing proxy-side
“Agent connected” message remains.

The agent's local diagnostics contain decimal codes, without SDK URLs, SAS values,
JS exception strings, panic values, stacks or destination printouts. Failures before
a tunnel exists cannot be reported remotely. Existing process exit statuses remain
separate from protocol diagnostic codes:

| Agent exit status | Meaning |
|---|---|
| 0 | Normal completion |
| 1 | External context cancellation |
| 2 | Missing connection string |
| 3 | Connection string, dialing or handler configuration failure |
| 4 | Identity exchange failure |

Existing wire error values 0–40 remain unchanged. Local diagnostic values are:

| Code | Description |
|---|---|
| 41 / 42 | Incomplete packet / malformed framing |
| 43 / 44 | Unsupported tunnel version / invalid receive credit |
| 45 / 46 | Stream reservation exhausted / datagram dropped at finite queue limit |
| 47 / 48 | Invalid flow configuration / invalid BIND timeout |
| 49 / 50 | JS host contract violation / unsupported JS host |
| 51 | Receive-loop panic |
| 52 / 53 / 54 | Write drain / delivery drain / peer-close drain failure |
| 55 | Incomplete bootstrap namespace |
| 56 / 57 / 58 | No BIND peer addresses / no DNS addresses / no usable wildcard BIND interface |

These additions are local diagnostic values, not new wire records. Protocol
version 3, commands, framing and SOCKS reply values are unchanged. A SOCKS setup
failure still sends its standard reply followed by graceful zero-code CLOSE, so
the reply drains to the client. It does not become a nonzero CLOSE that discards
accepted data. Such failures are visible to the SOCKS client; this change does not
add a separate remote diagnostic channel or parse SOCKS replies at the proxy.

`BaseHandler.OnError` is an optional local callback configured before ReceiveLoop
starts. The proxy supplies the description logger; without it, the handler emits
only a decimal code locally. Callbacks must be concurrency-safe and return promptly.
UDP destination-resolution failures remain per-datagram drops without a new
per-packet log stream.

This is not a stringless-binary guarantee. Go runtime, standard library, aznet/SDK
and other dependency strings remain, as do identifiers, flag help and host adapter
implementation text. The boundary is application-owned Go diagnostic explanations
and raw error output from agent paths. JS host capability failures remain
inspectable with `errors.Is(err, errors.ErrUnsupported)`.
