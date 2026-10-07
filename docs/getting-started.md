# Setup

Run the proxy on your machine and the agent on the machine that should reach your destinations. Both need access to the same Azure Storage account. Run shell commands from the repository root; commands marked **Proxy prompt** go inside the interactive proxy.

## Build

Install Git, Make and Go with automatic toolchain selection enabled:

```sh
git clone https://github.com/quarkslab/proxyblob.git
cd proxyblob
make
```

This produces `proxy`, `agent` and `agent.wasm`. Use `make proxy agent` if you only need native binaries. Bun is needed only for the [WASM example](usage.md#wasm-agent-with-bun).

## Configure storage

Create `config.json` on the proxy machine:

```json
{
  "listeners": [{
    "name": "demo",
    "driver": "azblob",
    "address": "https://YOUR_ACCOUNT.blob.core.windows.net",
    "storage_account": "YOUR_ACCOUNT",
    "storage_account_key": "YOUR_ACCOUNT_KEY"
  }]
}
```

Replace the account name and key with your own values. Keep this file private. The agent receives a generated connection string and does not need the configuration file.

Choose the driver and matching endpoint:

| Driver | Address |
|---|---|
| `azblob` | `https://YOUR_ACCOUNT.blob.core.windows.net` |
| `azqueue` | `https://YOUR_ACCOUNT.queue.core.windows.net` |
| `aztable` | `https://YOUR_ACCOUNT.table.core.windows.net` |

The [example configuration](../example_config.json) shows how to define multiple listeners.

<details>
<summary>Try locally with Azurite instead</summary>

Run the storage emulator in another terminal:

```sh
docker run --rm --name proxyblob-demo \
  -p 127.0.0.1:10000:10000 -p 127.0.0.1:10001:10001 -p 127.0.0.1:10002:10002 \
  mcr.microsoft.com/azure-storage/azurite:3.34.0 \
  azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0 --skipApiVersionCheck
```

For the Blob example, use these values in `config.json`:

```json
{
  "listeners": [{
    "name": "demo",
    "driver": "azblob",
    "address": "http://127.0.0.1:10000/devstoreaccount1",
    "storage_account": "devstoreaccount1",
    "storage_account_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  }]
}
```

This is Azurite's public development key. Run both proxy and agent on this machine and wait for the emulator to be ready before continuing. Stop it afterward with `docker stop proxyblob-demo`.

</details>

## Start the proxy

```sh
./proxy -c config.json
```

**Proxy prompt:**

```text
listener start demo
new
```

Copy the complete `connection_string` value from the output, without the log prefix. Keep it private.

## Connect an agent

On the agent machine:

```sh
./agent -c '<connection-string>'
```

Replace the placeholder with the generated value. The proxy displays **Agent connected** when registration succeeds.

**Proxy prompt:**

```text
agent ls
agent select <full-agent-id>
agent start --listen 127.0.0.1:1080
```

Use the full ID from `agent ls`, or tab completion. Check the **Proxy started** message for the port in use.

## Use the SOCKS proxy

On the proxy machine:

```sh
curl --noproxy "" --fail --show-error --max-time 60 \
  --socks5-hostname 127.0.0.1:1080 https://example.com/
```

You should receive the Example Domain page. Hostnames are resolved by the agent. You can also point a browser or another SOCKS5-capable application at `127.0.0.1:1080`.

For a destination such as `127.0.0.1:8000`, loopback refers to the agent's machine.

## Stop

**Proxy prompt:**

```text
agent stop
agent rm
listener stop demo
exit
```

`agent stop` stops local SOCKS service; `agent rm` disconnects the selected agent. A successful listener stop prints **Listener stopped**. Shared listener discovery resources remain in Azure; remove them only when that listener is retired and all its users have stopped.

See [usage](usage.md) for more commands or [troubleshooting](troubleshooting.md) if a step fails.
