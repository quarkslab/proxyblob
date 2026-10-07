// Run an agent built with `make wasm`, using the same Go toolchain's shim.
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { installHost } from "./host";

const root = resolve(import.meta.dir, "../..");
const toolchain = Bun.spawn(["go", "env", "GOROOT"], {
  cwd: root,
  stdout: "pipe",
  stderr: "inherit",
});
const goroot = (await new Response(toolchain.stdout).text()).trim();
if ((await toolchain.exited) !== 0) throw new Error("Cannot locate Go runtime");

const require = createRequire(import.meta.url);
Object.assign(globalThis, { require, fs: require("node:fs") });
await import(join(goroot, "lib/wasm/wasm_exec.js"));
installHost();

const wasm = resolve(process.argv[2] ?? join(root, "agent.wasm"));
const go = new (globalThis as any).Go();
go.argv = [wasm, ...process.argv.slice(3)];
go.env = process.env;
let exitCode = 0;
go.exit = (code: number) => {
  exitCode = code;
};
const { instance } = await WebAssembly.instantiate(
  await readFile(wasm),
  go.importObject,
);
await go.run(instance);
process.exit(exitCode);
