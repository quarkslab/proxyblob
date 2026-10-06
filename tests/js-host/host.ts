import { Socket } from "node:net";
import { Duplex } from "node:stream";
import { createSocket, type Socket as DatagramSocket } from "node:dgram";

if (Bun.version !== "1.4.2")
  throw new Error(
    "This socket harness is validated on Bun 1.4.2; revalidate before changing the runtime pin",
  );

export const CHUNK_BYTES = 64 * 1024;
// Bun 1.4.2 uses a 512 KiB native receive event. Readable may overshoot its
// 64 KiB high-water mark by that one event before pausing the socket.
export const HOST_READ_BYTES = CHUNK_BYTES + 512 * 1024;
export const WRITE_BYTES = 256 * 1024;
export interface Handle {
  dispose(): void;
}
export interface TCPHandle extends Handle {
  read(maxBytes: number): void;
}
export interface TCPSocket {
  write(data: Uint8Array): number;
  end(): void;
}
export interface UDPSocket {
  send(data: Uint8Array, port: number, address: string): boolean;
}
type TCPCallbacks = {
  connect(socket: TCPSocket): void;
  data(socket: TCPSocket, data: Uint8Array): void;
  eof(): void;
  error(message: string): void;
};
type UDPCallbacks = {
  bind(socket: UDPSocket, port: number): void;
  data(
    socket: UDPSocket,
    data: Uint8Array,
    port: number,
    address: string,
  ): void;
  error(message: string): void;
};

// These counters concern resources this harness owns, not browser/WebSocket
// resources. They let runtime tests assert cleanup and receive bounds.
export const stats = {
  active: 0,
  sockets: 0,
  callbacks: 0,
  disposed: 0,
  tcpDelivered: 0,
  maxTCPReadable: 0,
  maxTCPChunk: 0,
  maxUDPInFlight: 0,
};

// Bun's Socket.read() resumes native input even while bytes remain buffered.
// Suppress automatic reads and grant native input only when the existing host
// buffer is empty. The Readable high-water mark still bounds one native event.
class PullSocket extends Socket {
  override read(size?: number): Buffer | null {
    return Duplex.prototype.read.call(this, size);
  }
  override _read(_size: number) {}
  requestInput() {
    super._read(CHUNK_BYTES);
  }
}

export function TCPDial(
  host: string,
  port: string,
  connect: TCPCallbacks["connect"],
  data: TCPCallbacks["data"],
  eof: TCPCallbacks["eof"],
  error: TCPCallbacks["error"],
): TCPHandle {
  let callbacks: TCPCallbacks | undefined = { connect, data, eof, error };
  // A Socket exists before DNS/connect starts, so destroy cancels pending I/O.
  const options = { allowHalfOpen: true, highWaterMark: CHUNK_BYTES };
  const socket = new PullSocket(options);

  let requested = 0;
  let connected = false;
  let eofSent = false;
  let peerEOF = false;

  let writeEnded = false;
  stats.active++;
  stats.sockets++;
  stats.callbacks += 4;
  const facade: TCPSocket = {
    write(bytes) {
      if (!callbacks || socket.destroyed || writeEnded) return 0;
      if (bytes.length > WRITE_BYTES - socket.writableLength) return 0;
      try {
        socket.write(bytes);
        return bytes.length;
      } catch (err) {
        queueMicrotask(() => callbacks?.error(String(err)));
        return 0;
      }
    },
    end() {
      if (callbacks && !writeEnded) {
        writeEnded = true;
        queueMicrotask(() => {
          if (callbacks) {
            try {
              socket.end();
            } catch (err) {
              callbacks.error(String(err));
            }
          }
        });
      }
    },
  };
  // Scheduling prevents Go callback reentrancy from methods called with a Go
  // mutex held. Each scheduled task consults the detachable callback table.
  let scheduled = false;
  function schedule() {
    if (scheduled || !callbacks) return;
    scheduled = true;
    queueMicrotask(() => {
      scheduled = false;
      pump();
    });
  }
  function pump() {
    if (!callbacks || !connected) return;
    stats.maxTCPReadable = Math.max(
      stats.maxTCPReadable,
      socket.readableLength,
    );
    if (socket.readableLength > HOST_READ_BYTES) {
      callbacks.error("Bun receive bound violated");
      return;
    }
    if (requested && socket.readableLength) {
      const limit = requested;
      requested = 0;
      const bytes: Buffer | null = socket.read(
        Math.min(limit, socket.readableLength),
      );
      if (bytes) {
        requested = 0;
        stats.tcpDelivered += bytes.length;
        stats.maxTCPChunk = Math.max(stats.maxTCPChunk, bytes.length);
        callbacks?.data(facade, bytes);
      }
    }
    if (callbacks && requested && !socket.readableLength && !peerEOF)
      socket.requestInput();
    if (callbacks && peerEOF && !socket.readableLength && !eofSent) {
      eofSent = true;
      callbacks.eof();
    }
  }
  socket.on("readable", () => {
    socket.pause();
    schedule();
  });
  socket.on("end", () => {
    peerEOF = true;
    schedule();
  });
  socket.on("connect", () => {
    if (!callbacks) {
      socket.destroy();
      return;
    }
    socket.pause();
    connected = true;
    callbacks.connect(facade);
    schedule();
  });
  socket.on("error", (err) => callbacks?.error(err.message));
  socket.on("close", (hadError) => {
    stats.sockets--;
    if (callbacks && !peerEOF && !hadError)
      callbacks.error("TCP socket closed before EOF");
  });
  const handle: TCPHandle = {
    read(maxBytes) {
      if (!callbacks) return;
      if (
        requested ||
        !Number.isInteger(maxBytes) ||
        maxBytes < 1 ||
        maxBytes > CHUNK_BYTES
      ) {
        queueMicrotask(() => callbacks?.error("invalid receive credit"));
        return;
      }
      requested = maxBytes;
      schedule();
    },
    dispose() {
      if (!callbacks) return;
      callbacks = undefined;
      requested = 0;
      stats.active--;
      stats.callbacks -= 4;
      stats.disposed++;
      socket.destroy();
    },
  };
  // No synchronous setup callbacks or retained callbacks on a thrown setup.
  queueMicrotask(() => {
    if (!callbacks) return;
    try {
      socket.connect({ host, port: Number(port) });
    } catch (err) {
      callbacks?.error(String(err));
    }
  });
  return handle;
}

export function UDPListen(
  bind: UDPCallbacks["bind"],
  data: UDPCallbacks["data"],
  error: UDPCallbacks["error"],
): Handle {
  let callbacks: UDPCallbacks | undefined = { bind, data, error };
  let socket: DatagramSocket | undefined;
  let bound = false;
  let inFlight = 0;
  stats.active++;
  stats.callbacks += 3;
  const facade: UDPSocket = {
    send(bytes, port, address) {
      if (
        !callbacks ||
        !socket ||
        !bound ||
        bytes.length > 65507 ||
        inFlight >= 64
      )
        return false;
      inFlight++;
      stats.maxUDPInFlight = Math.max(stats.maxUDPInFlight, inFlight);
      try {
        socket.send(bytes, port, address, (err) => {
          inFlight--;
          if (err) callbacks?.error(err.message);
        });
        return true;
      } catch (err) {
        inFlight--;
        queueMicrotask(() => callbacks?.error(String(err)));
        return false;
      }
    },
  };
  const handle: Handle = {
    dispose() {
      if (!callbacks) return;
      callbacks = undefined;
      stats.active--;
      stats.callbacks -= 3;
      stats.disposed++;
      // Bind starts only in the scheduled task below. A pending bind's listening
      // callback closes the late socket without ever calling into Go.
      if (bound) socket?.close();
    },
  };
  queueMicrotask(() => {
    if (!callbacks) return;
    try {
      socket = createSocket("udp4");
      stats.sockets++;
      socket.on("close", () => {
        stats.sockets--;
      });
      socket.on("error", (err) => callbacks?.error(err.message));
      socket.on("message", (bytes, peer) =>
        callbacks?.data(facade, bytes, peer.port, peer.address),
      );
      socket.on("listening", () => {
        bound = true;
        if (!callbacks) {
          socket?.close();
          return;
        }
        callbacks.bind(facade, socket!.address().port);
      });
      socket.bind(0, "127.0.0.1");
    } catch (err) {
      callbacks?.error(String(err));
    }
  });
  return handle;
}

export function installHost() {
  Object.assign(globalThis, {
    ProxyBlobSocketHostVersion: 2,
    TCPDial,
    UDPListen,
  });
}
