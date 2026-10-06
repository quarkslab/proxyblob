import { createServer, type Socket, type Server } from "node:net";
import { createSocket } from "node:dgram";

// Actual loopback peers. No host callbacks are mocked in these cases.
export async function startPeers() {
  const sockets = new Set<Socket>();
  const servers: Server[] = [];
  async function listen(handler: (socket: Socket) => void) {
    const server = createServer({ allowHalfOpen: true }, (socket) => {
      sockets.add(socket);
      socket.on("close", () => sockets.delete(socket));
      socket.on("error", () => {});
      handler(socket);
    });
    servers.push(server);
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    return (server.address() as { port: number }).port;
  }
  const response = await listen((socket) => {
    const chunks: Buffer[] = [];
    socket.on("data", (bytes) => chunks.push(Buffer.from(bytes)));
    socket.on("end", () => {
      socket.write(Buffer.concat(chunks));
      socket.end();
    });
  });
  const slowSink = await listen((socket) => {
    const chunks: Buffer[] = [];
    socket.pause();
    socket.on("data", (bytes) => chunks.push(Buffer.from(bytes)));
    socket.on("end", () => socket.end(Buffer.concat(chunks)));
    setTimeout(() => socket.resume(), 100);
  });
  const peerFIN = await listen((socket) => {
    socket.write("peer-fin");
    socket.end();
    socket.on("data", () => {});
  });
  const bulk = await listen((socket) => {
    let sent = 0;
    const size = 4 * 1024 * 1024;
    function pump() {
      while (sent < size) {
        const bytes = Buffer.alloc(Math.min(16 * 1024, size - sent));
        for (let i = 0; i < bytes.length; i++) bytes[i] = (sent + i) % 251;
        sent += bytes.length;
        if (!socket.write(bytes)) return;
      }
      socket.end();
    }
    socket.on("drain", pump);
    pump();
  });
  const udp = createSocket("udp4");
  udp.on("message", (bytes, peer) => udp.send(bytes, peer.port, peer.address));
  await new Promise<void>((resolve) => udp.bind(0, "127.0.0.1", resolve));
  return {
    ports: { response, peerFIN, bulk, slowSink, udp: udp.address().port },
    async close() {
      for (const socket of sockets) socket.destroy();
      await Promise.all(
        servers.map(
          (server) =>
            new Promise<void>((resolve) => server.close(() => resolve())),
        ),
      );
      await new Promise<void>((resolve) => udp.close(resolve));
    },
  };
}
