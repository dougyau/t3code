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
export const workerWireFixture = (owner: BridgeClient["owner"], existingDirectory?: string) => Effect.acquireRelease(
  Effect.promise(async () => {
    const directory = existingDirectory ?? NodeFS.mkdtempSync(NodePath.join(NodeFS.realpathSync(NodeOS.tmpdir()), "pw-wire-"));
    const socketPath = NodePath.join(directory, "b.sock");
    const descriptor = NodePath.join(directory, "descriptor.json");
    const token = "a".repeat(64);
    const sockets = new Set<NodeNet.Socket>();
    const joins: Array<Promise<void>> = [];
    const calls: string[] = [];
    const requests: Request[] = [];
    const policy = {
      resumeError: undefined as ErrorCode | undefined, malformed: false,
      roster: { owner, revision: 1, cursor: null, items: [] } as unknown,
      pageError: undefined as ErrorCode | undefined,
      detail: undefined as unknown,
      result: undefined as unknown,
      activity: undefined as unknown,
    };
    let notify: ((revision: number) => void) | undefined;
    const server = NodeNet.createServer((socket) => {
      sockets.add(socket);
      joins.push(new Promise<void>((resolve) => socket.once("close", () => { sockets.delete(socket); resolve(); })));
      // Invalidation may close the client after writing unsubscribe, before its reply arrives.
      socket.on("error", (error) => {
        if ("code" in error && (error.code === "EPIPE" || error.code === "ECONNRESET")) socket.destroy();
        else throw error;
      });
      const receive = frames((value) => {
        if (!isRequest(value)) throw new Error("Invalid fixture request");
        calls.push(value.method);
        requests.push(value);
        if (value.method === "connect") {
          if (value.params.token !== token || value.params.expectedSessionId !== owner.coordinatorSessionId)
            throw new Error("Unauthenticated fixture client");
          socket.write(frame(value.params.resumeToken && policy.resumeError
            ? failure(value, policy.resumeError)
            : success(value, { capabilities, grants: [], lease: {
                clientId: value.clientId, resumeToken: "b".repeat(64), highWater: 7,
              } })));
        } else if (policy.malformed) {
          socket.write("not-json\n");
        } else if (value.method === "workers.list") {
          socket.write(frame(policy.pageError ? failure(value, policy.pageError) : success(value, policy.roster)));
        } else if (value.method === "subscribe") {
          socket.write(frame(success(value, { subscriptionId: "subscription", snapshot: policy.roster })));
          notify = (revision) => socket.write(frame({
            version: 1, method: "changed", owner, clientId: value.clientId, subscriptionId: "subscription", revision,
            refreshRequired: true,
          }));
        } else if (value.method === "unsubscribe") {
          socket.write(frame(success(value, { subscriptionId: value.params.subscriptionId })));
        } else if (value.method === "workers.detail" || value.method === "workers.result" || value.method === "workers.activity") {
          const page = value.method === "workers.detail" ? policy.detail
            : value.method === "workers.result" ? policy.result : policy.activity;
          socket.write(frame(success(value, page)));
        } else if (value.method === "request.status") {
          socket.write(frame(success(value, { state: "not_admitted", sequence: value.params.sequence })));
        } else if (value.method === "input" || value.method === "cancel") {
          socket.write(frame(success(value, { state: "pending", sequence: value.params.sequence })));
        } else throw new Error(`Unexpected fixture method: ${value.method}`);
      });
      socket.on("data", receive);
    });
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(socketPath, resolve);
    });
    NodeFS.chmodSync(socketPath, 0o600);
    // @effect-diagnostics-next-line preferSchemaOverJson:off -- Native descriptor fixture, consumed by the passive client's own boundary validation.
    NodeFS.writeFileSync(descriptor, JSON.stringify({
      version: 1, transport: "unix", socket: socketPath, token, owner,
      grants: ["history", "route", "control"], capabilities,
    }), { mode: 0o600 });
    const clients = new Set<BridgeClient>();
    const open = async (resume?: ResumeLease) => {
      const client = await connect(descriptor, { expectedSessionId: owner.coordinatorSessionId,
        ...(resume ? { resume } : {}) });
      clients.add(client);
      return client;
    };
    const client = await open();
    return { client, open, calls, requests, policy, sockets,
      notify: (revision: number) => {
        if (!notify) throw new Error("No subscription");
        notify(revision);
      },
      joinSockets: () => Promise.all(joins), async close() {
      await Promise.all([...clients].map((client) => client.disconnect()));
      for (const socket of sockets) socket.destroy();
      await Promise.all(joins);
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
      NodeFS.rmSync(directory, { recursive: true });
    } };
  }),
  (fixture) => Effect.promise(() => fixture.close()),
);
