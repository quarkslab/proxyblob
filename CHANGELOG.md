# Changelog

**ProxyBlob v2.4 - 09/10/2026:**

- Faster bulk transfers: about 3.5× on Blob and Table and 1.8× on Queue, with about 75% fewer storage requests per MiB on Blob.
- New connections take two tunnel round trips instead of three; malformed or unsupported SOCKS requests are answered by the proxy without tunnel traffic.
- Lower latency on an idle tunnel for most storage backends.
- Tuning: added `PROXYBLOB_MAX_STREAM_WINDOW`, `PROXYBLOB_WRITE_BUFFER` and `PROXYBLOB_WRITE_CHUNKS`; removed `PROXYBLOB_BATCH_BYTES`.
- Reorganized the code by role under `internal/` (operator, proxy, agent, relay, mux, SOCKS5 codec): SOCKS5 is now handled entirely by the proxy, and the agent only relays connections.
- v2.4 proxies and agents cannot connect to v2.3 ones: update both, and the Bun example files for WASM agents, at the same time. See the [upgrade notes](docs/usage.md#upgrading).

**ProxyBlob v2.3 - 07/10/2026:**

- Improved stream delivery, throughput and connection cleanup.
- Added native SOCKS5 BIND support and UDP forwarding through the storage tunnel.
- Improved WASM socket handling and added a Bun agent example.
- Added agent session-expiry display and configurable proxy log levels.
- Improved listener feedback and connection error reporting.
- Simplified setup and usage documentation, and reorganized code and tests.
- Upgrade proxy and agent together; WASM users should also update the host. See the [upgrade notes](docs/usage.md#upgrading).

**ProxyBlob v2.2 - 03/08/2026:**

- Dropped the vendored `azure-sdk-for-go` submodule; the WASM fix it carried is now upstream ([Azure/azure-sdk-for-go#26748](https://github.com/Azure/azure-sdk-for-go/pull/26748))
- Azure SDK dependencies bumped and resolved from upstream, so a plain `git clone` is enough to build

**ProxyBlob v2.1 (WASM agent) - 19/03/2026:**

- WebAssembly agent build target (`agent.wasm`) for deployment in JavaScript runtimes (Bun, Node.js, etc.)
- Build tag separation for JS/native network and UDP code
- Refactored UDP relay into a shared, platform-agnostic layer

**ProxyBlob v2 (aznet boosted) - 16/02/2026:**

- Complete architecture rewrite using aznet networking layer
- Multiple Azure Storage backends (Blob, Queue, Table Storage)
- Last seen timestamps for connected agents
- Enhanced connection management and lifecycle
- Better error handling and recovery mechanisms
- Improved polling efficiency with adaptive intervals
- Automatic connection cleanup and resource management
- Significantly higher throughput and lower latency
- Cost optimization through backend selection
- Multi-listener configuration support

**ProxyBlob public release - 29/04/2025:**

- SOCKS5 protocol (CONNECT and UDP ASSOCIATE)
- Reverse client-server architecture
- Azure Blob Storage communication
- Interactive CLI with auto-completion
- Multi-agent management
- Local or remote proxy server
- Container-based communication
- Connection string authentication
- Error handling
- Azurite support for local development

