# Changelog

**Unreleased stabilization:**

- Reserved receive credit, bounded fair scheduling, and ordered half-close.
- Native BIND and cloud-tunneled UDP with explicit overload limits.
- Session authorization expiry display and bounded lifecycle cleanup.
- Socket host v2 and actual Bun 1.4.2 WASM runtime validation.
- Published aznet directional Blob I/O fix and measured performance baselines.
- Coordinated wire-version-3 rollout required; see the [upgrade notes](docs/release-validation.md).

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

