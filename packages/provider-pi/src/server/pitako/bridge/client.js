import { randomUUID } from "node:crypto";
import { connect as openSocket } from "node:net";
import { constants, openSync, fstatSync, readFileSync, closeSync, lstatSync, realpathSync } from "node:fs";
import path from "node:path";
import { frame, frames } from "../framing.js";
import { version, isDescriptor, isReply, isRequest, isNotification, sameOwner, outstandingRequests, queueBytes, replyBytes, string } from "./protocol.js";

export class BridgeError extends Error {
  constructor(code, message) { super(message); this.name = "BridgeError"; this.code = code; }
}
function readDescriptor(file, expectedSessionId) {
  if (!path.isAbsolute(file) || typeof expectedSessionId !== "string" || !expectedSessionId.length)
    throw new BridgeError("invalid_descriptor", "An absolute descriptor path and explicit expected session ID are required");
  const root = path.dirname(file), directory = lstatSync(root);
  if (!process.getuid || !directory.isDirectory() || realpathSync(root) !== root ||
      directory.uid !== process.getuid() || (directory.mode & 0o777) !== 0o700)
    throw new BridgeError("invalid_descriptor", "Bridge descriptor directory is not private and same-user owned");
  const fd = openSync(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const info = fstatSync(fd);
    if (!info.isFile() || info.uid !== process.getuid() || (info.mode & 0o777) !== 0o600 || info.size > 16 * 1024)
      throw new BridgeError("invalid_descriptor", "Bridge descriptor is not a bounded private same-user file");
    const value = JSON.parse(readFileSync(fd, "utf8"));
    if (!isDescriptor(value) || value.owner.coordinatorSessionId !== expectedSessionId ||
        path.dirname(value.socket) !== root)
      throw new BridgeError("invalid_descriptor", "Bridge descriptor version, transport or expected owner is invalid");
    const socket = lstatSync(value.socket);
    if (!socket.isSocket() || socket.uid !== process.getuid() || (socket.mode & 0o777) !== 0o600)
      throw new BridgeError("invalid_descriptor", "Bridge socket is not private and same-user owned");
    return value;
  } finally { closeSync(fd); }
}

/** Explicit connect only. No automatic discovery, reconnect, grants, Pi imports or startup. */
export async function connect(descriptorPath, { expectedSessionId, resume } = {}) {
  let descriptor;
  try { descriptor = readDescriptor(descriptorPath, expectedSessionId); }
  catch (error) {
    if (error instanceof BridgeError) throw error;
    throw new BridgeError("invalid_descriptor", "Cannot read a valid private bridge descriptor");
  }
  if (resume !== undefined && (!resume || !string(resume.clientId, 128) ||
      typeof resume.resumeToken !== "string" || !/^[a-f0-9]{64}$/.test(resume.resumeToken)))
    throw new BridgeError("invalid_request", "Invalid logical client resume lease");
  const clientId = resume?.clientId ?? randomUUID(), pending = new Map(), subscriptions = new Map();
  const socket = openSocket(descriptor.socket);
  let active = true, sequence = 0;
  const closed = new Promise(resolve => socket.once("close", resolve));
  const fail = error => {
    active = false;
    for (const { reject, timer } of pending.values()) { clearTimeout(timer); reject(error); }
    pending.clear();
    for (const subscription of subscriptions.values()) subscription.active = false;
    subscriptions.clear();
    socket.destroy();
  };
  socket.on("error", () => fail(new BridgeError("disconnected", "Bridge transport failed; explicit connect required")));
  socket.on("close", () => fail(new BridgeError("disconnected", "Bridge disconnected; explicit connect required")));
  const receive = frames(value => {
    if (value?.method === "changed") {
      if (!isNotification(value) || !sameOwner(value.owner, descriptor.owner) || value.clientId !== clientId ||
          Buffer.byteLength(JSON.stringify(value)) + 1 > replyBytes)
        throw new BridgeError("invalid_reply", "Invalid bridge notification");
      const subscription = subscriptions.get(value.subscriptionId);
      if (!subscription) throw new BridgeError("invalid_reply", "Unknown bridge subscription");
      if (value.revision <= subscription.revision) throw new BridgeError("invalid_reply", "Nonmonotonic bridge revision");
      const gap = value.revision !== subscription.revision + 1;
      subscription.revision = value.revision;
      if (subscription.active) {
        try { subscription.listener(Object.freeze({ ...value, owner: Object.freeze({ ...value.owner }), gap })); }
        catch { /* Host callbacks cannot break parsing or another pending reply. */ }
      }
      return;
    }
    const request = pending.get(value?.id);
    if (!request || !isReply(value, request.method) || !sameOwner(value.owner, descriptor.owner) ||
        value.clientId !== clientId || Buffer.byteLength(JSON.stringify(value)) + 1 > replyBytes)
      throw new BridgeError("invalid_reply", "Invalid bridge reply");
    pending.delete(value.id); clearTimeout(request.timer);
    if (value.ok) request.resolve(value.result);
    else request.reject(new BridgeError(value.error.code, value.error.message));
  }, { label: "Agent bridge", fatalUtf8: true });
  socket.on("data", chunk => { try { receive(chunk); } catch {
    fail(new BridgeError("invalid_reply", "Invalid bridge framing or reply"));
  } });
  const request = (method, params, accepted) => {
    if (!active) return Promise.reject(new BridgeError("disconnected", "Bridge disconnected; explicit connect required"));
    if (pending.size >= outstandingRequests) return Promise.reject(new BridgeError("busy", "Bridge request limit exceeded"));
    const envelope = { version, id: String(++sequence), clientId, owner: descriptor.owner, method, params };
    if (!isRequest(envelope)) return Promise.reject(new BridgeError("invalid_request", "Invalid bridge request"));
    let data;
    try { data = frame(envelope, "Agent bridge"); }
    catch { return Promise.reject(new BridgeError("invalid_request", "Complete bridge request exceeds its frame budget")); }
    if (socket.writableLength + Buffer.byteLength(data) > queueBytes) {
      fail(new BridgeError("disconnected", "Bridge outbound queue exceeded"));
      return Promise.reject(new BridgeError("disconnected", "Bridge outbound queue exceeded"));
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => fail(new BridgeError("disconnected", "Bridge request timed out; explicit connect required")), 15_000);
      timer.unref();
      pending.set(envelope.id, { method, resolve: result => { accepted?.(result); resolve(result); }, reject, timer });
      socket.write(data);
    });
  };
  let lease;
  try {
    const connected = await request("connect", { token: descriptor.token, expectedSessionId,
      ...(resume === undefined ? {} : { resumeToken: resume.resumeToken }) });
    lease = Object.freeze(connected.lease);
  } catch (error) {
    fail(error); await closed; throw error;
  }
  return Object.freeze({
    owner: Object.freeze({ ...descriptor.owner }),
    capabilities: Object.freeze(descriptor.capabilities),
    lease,
    list: (after, revision) => request("workers.list", { ...(after === undefined ? {} : { after }),
      ...(revision === undefined ? {} : { revision }) }),
    async subscribe(listener) {
      if (typeof listener !== "function") throw new BridgeError("invalid_request", "A subscription listener is required");
      const state = { active: true, listener, revision: 0 };
      // Install on the synchronous reply boundary before a coalesced following hint.
      const initial = await request("subscribe", {}, result => {
        state.revision = result.snapshot.revision;
        subscriptions.set(result.subscriptionId, state);
      });
      let disposal;
      return Object.freeze({
        subscriptionId: initial.subscriptionId, snapshot: initial.snapshot,
        get active() { return state.active; },
        refresh: () => request("workers.list", {}),
        dispose() {
          if (disposal) return disposal;
          state.active = false;
          // Keep a tombstone until the unsubscribe reply, for already-written hints.
          disposal = active ? request("unsubscribe", { subscriptionId: initial.subscriptionId }).then(() => {
            subscriptions.delete(initial.subscriptionId);
          }).catch(async error => {
            // If unsubscribe cannot be admitted, disconnect owns listener release.
            fail(error); await closed; throw error;
          }) : Promise.resolve();
          return disposal;
        },
      });
    },
    detail: (id, cursor) => request("workers.detail", { id, ...(cursor === undefined ? {} : { cursor }) }),
    requests: (id, cursor) => request("workers.requests", { id, ...(cursor === undefined ? {} : { cursor }) }),
    activity: (ref, cursor) => request("workers.activity", { ref, ...(cursor === undefined ? {} : { cursor }) }),
    result: (ref, cursor) => request("workers.result", { ref, ...(cursor === undefined ? {} : { cursor }) }),
    historyList: (options = {}) => request("history.list", options),
    historyRead: options => request("history.read", options),
    routeRead: (id, offset) => request("route.read", { id, ...(offset === undefined ? {} : { offset }) }),
    grant: (grant, acknowledged, selection) => request("view.grant", { grant, acknowledged,
      ...(selection === undefined ? {} : { selection }) }),
    // The caller chooses each mutation sequence. No reconnect, resubmission or later decision is inferred.
    input: (selection, input, sequence) => request("input", { selection, input, sequence }),
    cancel: (selection, sequence) => request("cancel", { selection, sequence }),
    requestStatus: sequence => request("request.status", { sequence }),
    async disconnect() {
      fail(new BridgeError("disconnected", "Bridge client disconnected"));
      await closed;
    },
  });
}
