// Builds with the selected go toolchain, then loads that toolchain's own shim.
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createRequire } from "node:module";
import { installHost, stats } from "./host";
import { startPeers } from "./peers";

const root = resolve(import.meta.dir, "../..");
const output = await mkdtemp(join(tmpdir(), "proxyblob-wasm-"));
async function command(args: string[], env = process.env) {
  const child = Bun.spawn(args, {
    cwd: root,
    env,
    stdout: "pipe",
    stderr: "inherit",
  });
  const text = await new Response(child.stdout).text();
  if ((await child.exited) !== 0) throw new Error(`${args[0]} failed`);
  return text.trim();
}
try {
  const goroot = await command(["go", "env", "GOROOT"]);
  console.log(
    `Runtime: ${process.execPath} Bun ${Bun.version}; ${await command(["go", "version"])}; GOROOT=${goroot}`,
  );
  const wasm = join(output, "socks.test.wasm");
  await command(
    ["go", "test", "-mod=readonly", "-c", "-o", wasm, "./pkg/proxy/socks"],
    { ...process.env, GOOS: "js", GOARCH: "wasm" },
  );
  const require = createRequire(import.meta.url);
  Object.assign(globalThis, { require, fs: require("node:fs") });
  await import(join(goroot, "lib/wasm/wasm_exec.js"));
  installHost();
  const peers = await startPeers();
  Object.assign(globalThis, {
    ProxyBlobTestPeers: peers.ports,
    ProxyBlobHostStats: () => ({ ...stats }),
  });
  // The Go constructor is installed by Go's unmodified runtime shim.
  const go = new (globalThis as any).Go();
  go.argv = [wasm, "-test.v", "-test.timeout=40s", ...process.argv.slice(2)];
  go.env = process.env;
  let exitCode = 0;
  go.exit = (code: number) => {
    exitCode = code;
  };
  const watchdog = setTimeout(() => {
    console.error("WASM host watchdog expired");
    process.exit(1);
  }, 60_000);
  try {
    const result = await WebAssembly.instantiate(
      await readFile(wasm),
      go.importObject,
    );
    await go.run(result.instance);
    for (let i = 0; stats.sockets && i < 100; i++) await Bun.sleep(10);
    if (stats.active || stats.callbacks || stats.sockets)
      throw new Error(`host resources leaked: ${JSON.stringify(stats)}`);
    console.log("Host resource/queue counters:", JSON.stringify(stats));
    process.exitCode = exitCode;
  } finally {
    await peers.close();
    clearTimeout(watchdog);
  }
} finally {
  await rm(output, { recursive: true, force: true });
}

// Go's shim can leave runtime timer handles after go.exit. All owned test
// sockets and temporary outputs have been closed before terminating the runner.
process.exit(process.exitCode ?? 0);
