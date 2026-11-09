import type { Server } from "node:http";

export async function listen(server: Server, port = 0, host = "127.0.0.1"): Promise<string> {
  await new Promise<void>((resolve, reject) => {
    const error = (failure: Error): void => { server.off("listening", ready); reject(failure); };
    const ready = (): void => { server.off("error", error); resolve(); };
    server.once("error", error);
    server.once("listening", ready);
    server.listen(port, host);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Expected TCP address");
  return `http://${host}:${address.port}`;
}

export async function close(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close(error => error ? reject(error) : resolve());
    server.closeAllConnections();
  });
}
