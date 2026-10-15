// @effect-diagnostics nodeBuiltinImport:off -- Owned local Unix socket exercises only the passive native client.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeNet from "node:net";
import * as Effect from "effect/Effect";
import { connect, type BridgeClient } from "./pitako/bridge/client.js";
import { capabilities, failure, isRequest, success, type ErrorCode, type ResumeLease, type Request } from "./pitako/bridge/protocol.js";
import { frame, frames } from "./pitako/framing.js";

/** Authenticated wire fixture, not a broker/controller or provider runtime. Every socket is joined on release. */
export const workerWireFixture = (owner: BridgeClient["owner"], ownedDirectory?: string) => Effect.acquireRelease(
  Effect.promise(async () => {
    const directory = ownedDirectory ?? NodeFS.mkdtempSync(NodePath.join(NodeFS.realpathSync(NodeOS.tmpdir()), "pw-wire-"));
    const socketPath = NodePath.join(directory, "b.sock");
    const descriptor = NodePath.join(directory, "descriptor.json");
    const token = "a".repeat(64);
    const sockets = new Set<NodeNet.Socket>();
    const joins: Array<Promise<void>> = [];
    const calls: string[] = [];
    const policy = {
      resumeError: undefined as ErrorCode | undefined, malformed: false,
      subscribeError: false,
      reply: undefined as ((request: Request) => unknown) | undefined,
    };
    const server = NodeNet.createServer((socket) => {
      sockets.add(socket);
      joins.push(new Promise<void>((resolve) => socket.once("close", () => { sockets.delete(socket); resolve(); })));
      const receive = frames((value) => {
        if (!isRequest(value)) throw new Error("Invalid fixture request");
        calls.push(value.method);
        if (value.method === "connect") {
          if (value.params.token !== token || value.params.expectedSessionId !== owner.coordinatorSessionId)
            throw new Error("Unauthenticated fixture client");
          socket.write(frame(value.params.resumeToken && policy.resumeError
            ? failure(value, policy.resumeError)
            : success(value, { capabilities, lease: {
                clientId: value.clientId, resumeToken: "b".repeat(64),
              } })));
        } else if (policy.malformed) {
          socket.write("not-json\n");
        } else if (value.method === "subscribe") {
          socket.write(frame(policy.subscribeError ? failure(value, "unsupported_method") :
            success(value, { subscriptionId: "subscription", snapshot: { owner, revision: 1, items: [], cursor: null } })));
        } else if (value.method === "unsubscribe") {
          socket.write(frame(success(value, { subscriptionId: value.params.subscriptionId })));
        } else if (policy.reply) {
          socket.write(frame(success(value, policy.reply(value))));
        } else if (value.method === "workers.list") {
          socket.write(frame(success(value, { owner, revision: 1, items: [], cursor: null })));
        } else throw new Error(`Unexpected fixture method: ${value.method}`);
      });
      socket.on("data", receive);
      socket.on("error", () => socket.destroy());
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(socketPath, resolve);
    });
    NodeFS.chmodSync(socketPath, 0o600);
    // @effect-diagnostics-next-line preferSchemaOverJson:off -- Native descriptor fixture, consumed by the passive client's own boundary validation.
    NodeFS.writeFileSync(descriptor, JSON.stringify({
      version: 2, transport: "unix", socket: socketPath, token, owner, capabilities,
    }), { mode: 0o600 });
    const clients = new Set<BridgeClient>();
    const open = async (resume?: ResumeLease) => {
      const client = await connect(descriptor, { expectedSessionId: owner.coordinatorSessionId,
        ...(resume ? { resume } : {}) });
      clients.add(client);
      return client;
    };
    const client = await open();
    return { client, open, descriptor, calls, policy, sockets, joinSockets: () => Promise.all(joins), async close() {
      await Promise.all([...clients].map((client) => client.disconnect()));
      for (const socket of sockets) socket.destroy();
      await Promise.all(joins);
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
      NodeFS.unlinkSync(descriptor);
      if (ownedDirectory === undefined) NodeFS.rmdirSync(directory);
    } };
  }),
  (fixture) => Effect.promise(() => fixture.close()),
);
