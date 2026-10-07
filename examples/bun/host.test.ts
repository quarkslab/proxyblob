import { Socket as DatagramSocket, createSocket } from "node:dgram";
import { test, expect } from "bun:test";
import {
  TCPDial,
  UDPListen,
  UDPResolve,
  stats,
  CHUNK_BYTES,
  HOST_READ_BYTES,
  type TCPHandle,
  type Handle,
} from "./host";
import { startPeers } from "../../tests/wasm/peers";

async function drained() {
  for (let i = 0; stats.sockets && i < 100; i++) await Bun.sleep(5);
  expect(stats.sockets).toBe(0);
  expect(stats.callbacks).toBe(0);
  expect(stats.active).toBe(0);
}

test("pending TCP and UDP disposal detaches queued callbacks exactly once", async () => {
  const baseline = stats.disposed;
  let called = 0;
  for (let i = 0; i < 100; i++) {
    const tcp = TCPDial(
      "127.0.0.1",
      "1",
      () => called++,
      () => called++,
      () => called++,
      () => called++,
      () => called++,
    );
    const udp = UDPListen(
      () => called++,
      () => called++,
      () => called++,
    );
    tcp.dispose();
    tcp.dispose();
    udp.dispose();
    udp.dispose();
  }
  await Bun.sleep(10);
  expect(called).toBe(0);
  expect(stats.disposed - baseline).toBe(200);
  await drained();
});

test("dispose during UDP bind closes a late socket without calling Go", async () => {
  let called = 0;
  const handle = UDPListen(
    () => called++,
    () => called++,
    () => called++,
  );
  // The bind-start microtask runs first; dispose precedes listening delivery.
  queueMicrotask(() => handle.dispose());
  await Bun.sleep(10);
  expect(called).toBe(0);
  await drained();
});

test("real TCP refused connection is terminal and disposable", async () => {
  // Acquire then release an ephemeral port to produce a loopback refusal.
  const peers = await startPeers();
  const port = peers.ports.response;
  await peers.close();
  let handle: TCPHandle;
  await new Promise<void>((resolve, reject) => {
    handle = TCPDial(
      "127.0.0.1",
      String(port),
      () => reject(new Error("unexpected connect")),
      () => reject(new Error("unexpected data")),
      () => reject(new Error("unexpected EOF")),
      () => {
        handle.dispose();
        resolve();
      },
      () => {},
    );
  });
  await drained();
});

test("Bun host preserves a paused real stream, bounded chunks, and EOF ordering", async () => {
  const peers = await startPeers();
  let handle: TCPHandle | undefined;
  try {
    const chunks: Buffer[] = [];
    let delivered = 0;
    await new Promise<void>((resolve, reject) => {
      handle = TCPDial(
        "127.0.0.1",
        String(peers.ports.bulk),
        () => {
          // Let the OS/host queue fill while zero receive credit is granted.
          setTimeout(() => {
            try {
              expect(delivered).toBe(0);
              handle!.read(CHUNK_BYTES);
            } catch (err) {
              reject(err);
            }
          }, 25);
        },
        (_socket, bytes) => {
          try {
            expect(bytes.length).toBeLessThanOrEqual(CHUNK_BYTES);
            chunks.push(Buffer.from(bytes));
            delivered += bytes.length;
            // Delay every grant to exercise backpressure after partial host reads.
            setTimeout(() => handle!.read(CHUNK_BYTES), 1);
          } catch (err) {
            reject(err);
          }
        },
        resolve,
        (message) => reject(new Error(message)),
        () => {},
      );
    });
    const bytes = Buffer.concat(chunks);
    expect(bytes.length).toBe(4 * 1024 * 1024);
    for (let i = 0; i < bytes.length; i++)
      if (bytes[i] !== i % 251) throw new Error(`byte mismatch at ${i}`);
    expect(stats.maxTCPReadable).toBeLessThanOrEqual(HOST_READ_BYTES);
  } finally {
    handle?.dispose();
    await peers.close();
  }
  await drained();
});

test("real UDP socket disposal closes the resource", async () => {
  let handle: Handle;
  await new Promise<void>((resolve) => {
    handle = UDPListen(
      () => {
        handle.dispose();
        resolve();
      },
      () => {},
      () => {},
    );
  });
  await drained();
});

test("real UDP bind failure closes the created socket", async () => {
  const occupied = createSocket("udp4");
  await new Promise<void>((resolve) => occupied.bind(0, "127.0.0.1", resolve));
  const original = DatagramSocket.prototype.bind;
  const port = occupied.address().port;
  DatagramSocket.prototype.bind = function (this: DatagramSocket) {
    return original.call(this, { port, address: "127.0.0.1" });
  };
  let handle: Handle | undefined;
  try {
    const message = await new Promise<string>((resolve, reject) => {
      handle = UDPListen(
        () => reject(new Error("unexpected bind")),
        () => {},
        (message) => {
          handle!.dispose();
          resolve(message);
        },
      );
    });
    expect(message).toContain("EADDRINUSE");
    await drained();
  } finally {
    DatagramSocket.prototype.bind = original;
    handle?.dispose();
    await new Promise<void>((resolve) => occupied.close(resolve));
  }
});


test("UDP DNS resolves and pending disposal detaches callbacks", async () => {
  let calls = 0;
  const pending = UDPResolve("localhost", () => calls++, () => calls++);
  pending.dispose(); pending.dispose();
  await Bun.sleep(10);
  expect(calls).toBe(0);
  await new Promise<void>((resolve, reject) => {
    const handle = UDPResolve("localhost", (ip) => {
      expect(ip === "127.0.0.1" || ip === "::1").toBe(true);
      handle.dispose(); resolve();
    }, (message) => { handle.dispose(); reject(new Error(message)); });
  });
  await drained();
});
