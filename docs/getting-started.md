# Your first SOCKS connection

This walkthrough starts one native proxy and one native agent. Begin with local Azurite to verify the workflow, or use your Azure account in step 2. Run shell commands from the repository root unless a step says otherwise. Commands inside the ProxyBlob prompt are marked separately.

## 1. Build the binaries

Install Git, Make and Go with automatic toolchain selection enabled. The module declares Go 1.25; the recorded validation uses Go 1.26.4. The selected dependencies may require a newer toolchain than the module's declared minimum. Docker is needed only for the local emulator and container-based tests.

```sh
git clone https://github.com/quarkslab/proxyblob.git
cd proxyblob
make
```

Expected files: `proxy`, `agent`, `agent.wasm`. The default build uses the committed published aznet dependency; no sibling checkout or workspace is required. To build only native binaries, use `make proxy agent`.

Bun is not needed to run the native binaries. The optional WASM test host uses Bun 1.4.2; see [WASM setup](js-socket-host.md).

## 2. Configure storage

Choose **one** of the following paths. The configuration stays on the proxy machine. The agent will receive a narrower generated connection string.

### Local Azurite

In another terminal, start a disposable emulator and wait for the services to report ready:

```sh
docker run --rm --name proxyblob-demo \
  -p 127.0.0.1:10000:10000 -p 127.0.0.1:10001:10001 -p 127.0.0.1:10002:10002 \
  mcr.microsoft.com/azure-storage/azurite:3.34.0 \
  azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0 --skipApiVersionCheck
```

Create `config.json` in the repository root with this **public emulator credential**:

```json
{
  "listeners": [{
    "name": "demo",
    "driver": "azblob",
    "session_duration": "24h",
    "address": "http://127.0.0.1:10000/devstoreaccount1",
    "storage_account": "devstoreaccount1",
    "storage_account_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
  }]
}
```

Run both proxy and agent on this machine for this local walkthrough. A generated URL containing `127.0.0.1` refers to the agent's own machine, so it cannot reach your emulator from another host without a reachable endpoint.

### Azure

Use an account with the chosen storage service and authorized HTTPS access from **both** proxy and agent. Standard general-purpose v2 supports Blob, Queue and Table. Premium Blob and anonymous public Blob access are not required. See Microsoft's [account types](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview).

Create `config.json`, replacing the three account placeholders:

```json
{
  "listeners": [{
    "name": "demo",
    "driver": "azblob",
    "session_duration": "24h",
    "address": "https://YOUR_ACCOUNT.blob.core.windows.net",
    "storage_account": "YOUR_ACCOUNT",
    "storage_account_key": "YOUR_ACCOUNT_KEY"
  }]
}
```

Protect the file and keep it out of commits. `example_config.json` shows multiple listeners, but contains placeholders; it is not ready to run unchanged. To choose a different service, change both fields:

| Driver | Azure address | Local address |
|---|---|---|
| `azblob` | `https://ACCOUNT.blob.core.windows.net` | `http://127.0.0.1:10000/devstoreaccount1` |
| `azqueue` | `https://ACCOUNT.queue.core.windows.net` | `http://127.0.0.1:10001/devstoreaccount1` |
| `aztable` | `https://ACCOUNT.table.core.windows.net` | `http://127.0.0.1:10002/devstoreaccount1` |

## 3. Start the proxy and its storage listener

In the proxy terminal:

```sh
./proxy -c config.json
```

At the interactive prompt, enter these commands without copying the prompt itself:

```text
listener ls
listener start demo
new --duration 168h
```

`demo` is the configured listener name, not a network address. Starting it also selects it. `new` prints a `connection_string` value: copy only that complete value, without the log prefix. Treat it as a bearer secret. The duration controls when new agents can join; it is separate from the session's `24h` authorization.

## 4. Start the native agent

In a second terminal, on the machine that should reach destination services:

```sh
./agent -c '<paste-the-generated-connection-string>'
```

The angle-bracket text is a placeholder; replace it completely. Alternatively, set `CONNECTION_STRING` through your environment and run `./agent`. The agent is normally quiet. Successful registration is shown by **Agent connected** in the proxy terminal.

## 5. Open the local SOCKS endpoint

Back in the proxy prompt:

```text
agent ls
agent select <full-agent-id>
agent start --listen 127.0.0.1:1080
```

Copy the full ID from the agent list or use completion. The prompt then shows its short ID. Check the **Proxy started** log for the actual port: another ProxyBlob SOCKS endpoint already using 1080 can cause selection of the next port.

## 6. Make a request

In a third shell on the proxy machine, substituting the reported port if needed:

```sh
curl --noproxy "" --fail --show-error --max-time 60 --socks5-hostname 127.0.0.1:1080 https://example.com/
```

Expected result: the Example Domain HTML page. `--socks5-hostname` sends the target hostname to the agent for resolution. This tests a TCP CONNECT conversation; it is not a UDP DNS test.

If the agent has no public Internet access, use an HTTP service it can reach. For a fully local demonstration, run `python3 -m http.server 8000 --bind 127.0.0.1` on the agent machine, then request `http://127.0.0.1:8000/` through the same SOCKS command. Here the target loopback address means the **agent** machine.

## 7. Stop the demo

After the request completes, use the proxy prompt:

```text
agent stop
agent rm
listener stop demo
exit
```

`agent stop` stops local SOCKS service while retaining the tunnel; `agent rm` removes the selected agent and its tunnel. The selection should clear. `listener stop demo` stops acceptance and closes its remaining sessions. A successful stop prints **Listener stopped** at the default log level. Inspect any cleanup failure instead of assuming pending traffic was delivered.

For the disposable local emulator:

```sh
docker stop proxyblob-demo
```

Listener shutdown intentionally retains shared bootstrap resources. The CLI does not delete that namespace; an administrator may remove its owned resources only after every user stops. Crashes and failed cleanup also require reconciliation. Do not delete an entire Azure account to clean up a single listener.

Continue with [everyday usage](usage.md) or [troubleshooting](troubleshooting.md).
