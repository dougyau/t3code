// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Native boundary owns inode checks and one bounded readiness deadline; no polling.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  PiNativeWorkerRecord,
  PiNativeWorkerLaunch,
  type PiNativeWorkerSummary,
  type PiNativeWorkerCheckpoint,
  type PiNativeWorkerBinding,
} from "@t3tools/contracts";
import type {
  ProviderNativeWorkersPort,
  ProviderNativeWorkerBatch,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { connect, BridgeError, type BridgeClient, type BridgeSubscription } from "./pitako/bridge/client.js";
import type {
  PublicWorkerDetail, PublicRecord, PublicFragment, PublicConversationPage, WorkerReadReference,
} from "./pitako/bridge/protocol.js";
import { sameOwner, errors as nativeErrors } from "./pitako/bridge/protocol.js";

const RECORD_LIMIT = 4 * 1024 * 1024;
const decodeRecord = Schema.decodeUnknownSync(PiNativeWorkerRecord);
const sameLaunch = Schema.toEquivalence(PiNativeWorkerLaunch);
const decodeJsonRecord = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const decodeFragmentUpsert = Schema.decodeUnknownSync(Schema.Struct({
  op: Schema.Literal("upsert"), sequence: Schema.NullOr(Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))),
  record: Schema.Unknown,
}));
const encodeJsonRecord = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
export class WorkerSourceError extends Schema.TaggedError<WorkerSourceError>()(
  "WorkerSourceError", { code: Schema.String },
) {}
const errorOf = (cause: unknown) =>
  new WorkerSourceError({ code: cause instanceof BridgeError ? cause.code : "source_failed" });
export function classifyPiWorkerFailure(cause: unknown, phase: "request" | "acquisition" = "request") {
  if (!(cause instanceof BridgeError)) {
    // @effect-diagnostics-next-line instanceOfSchema:off -- Carrier identity rejects forged code-shaped objects.
    if (phase !== "acquisition" || !(cause instanceof WorkerSourceError)) return { kind: "unclassified" as const };
  }
  const code = cause.code;
  if (Object.hasOwn(nativeErrors, code)) return { kind: "coded" as const, code };
  if (code === "disconnected" || code === "invalid_reply") return { kind: "transport" as const, code };
  return { kind: "unclassified" as const };
}

/** One incomplete record. Its restart manifest contains no partial transcript bytes. */
export class NativeRecordAssembler {
  private incomplete: {
    manifest: NonNullable<PiNativeWorkerCheckpoint["fragment"]>;
    chunks: Array<Buffer>;
    bytes: number;
  } | undefined;
  get fragment() { return this.incomplete?.manifest; }

  accept(
    change: PublicConversationPage["changes"][number],
    page: Pick<PublicConversationPage, "epoch" | "ref">,
    cursor: string | null,
  ): PiNativeWorkerRecord | undefined {
    if ("kind" in change && change.kind === "fragment") {
      const fragment: PublicFragment = change;
      if (!fragment.id || !page.epoch ||
        !Number.isSafeInteger(fragment.binding) || fragment.binding < 0 ||
        !Number.isSafeInteger(fragment.revision) || fragment.revision < 1 ||
        !Number.isSafeInteger(fragment.byteOffset) || fragment.byteOffset < 0 ||
        (fragment.sequence !== null && (!Number.isSafeInteger(fragment.sequence) || fragment.sequence < 0)))
        throw new BridgeError("invalid_record", "Invalid native fragment identity");
      if (
        fragment.encoding !== "base64-json" ||
        !Number.isSafeInteger(fragment.totalBytes) || fragment.totalBytes <= 0 ||
        fragment.totalBytes > RECORD_LIMIT
      ) throw new BridgeError("record_oversized", "Native record exceeds capture bound");
      if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(fragment.data))
        throw new BridgeError("invalid_record", "Invalid native record base64");
      const byteLength = fragment.data.length / 4 * 3 -
        (fragment.data.endsWith("==") ? 2 : fragment.data.endsWith("=") ? 1 : 0);
      if (byteLength === 0 || fragment.byteOffset + byteLength > fragment.totalBytes)
        throw new BridgeError("invalid_record", "Native fragment exceeds declared record size");
      const bytes = Buffer.from(fragment.data, "base64");
      if (bytes.length === 0 || bytes.toString("base64") !== fragment.data)
        throw new BridgeError("invalid_record", "Empty or noncanonical native fragment");
      if (!this.incomplete) {
        if (fragment.byteOffset !== 0) throw new BridgeError("invalid_record", "Missing fragment start");
        this.incomplete = {
          manifest: {
            id: fragment.id, binding: fragment.binding, revision: fragment.revision,
            epoch: page.epoch, sequence: fragment.sequence, totalBytes: fragment.totalBytes, cursor,
          },
          chunks: [], bytes: 0,
        };
      }
      const partial = this.incomplete;
      const first = partial.manifest;
      if (
        first.id !== fragment.id || first.binding !== fragment.binding ||
        first.revision !== fragment.revision || first.epoch !== page.epoch ||
        first.sequence !== fragment.sequence || first.totalBytes !== fragment.totalBytes ||
        fragment.byteOffset !== partial.bytes || partial.bytes + bytes.length > first.totalBytes
      ) throw new BridgeError("invalid_record", "Native fragment identity or offset changed");
      partial.chunks.push(bytes);
      partial.bytes += bytes.length;
      if (partial.bytes !== first.totalBytes) return undefined;
      // Fatal UTF-8: replacement characters must not silently repair corrupt records.
      const text = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(partial.chunks, partial.bytes));
      const decoded = decodeFragmentUpsert(decodeJsonRecord(text));
      if (decoded.sequence !== first.sequence)
        throw new BridgeError("invalid_record", "Decoded fragment sequence changed");
      const record = normalizeRecord(decoded.record, page, first.sequence);
      if (record.provenance.id !== first.id || record.provenance.binding !== first.binding || record.provenance.revision !== first.revision)
        throw new BridgeError("invalid_record", "Decoded fragment identity changed");
      this.incomplete = undefined;
      return record;
    }
    if (this.incomplete) throw new BridgeError("invalid_record", "Interleaved native record fragments");
    if (!("record" in change)) throw new BridgeError("invalid_record", "Unknown native change");
    return toNativeRecord(change.record, page, change.sequence);
  }
}

function toNativeRecord(record: PublicRecord, page: Pick<PublicConversationPage, "epoch" | "ref">, sequence: number | null) {
  if (Buffer.byteLength(encodeJsonRecord({ sequence, op: "upsert", record })) > RECORD_LIMIT)
    throw new BridgeError("record_oversized", "Native complete record exceeds capture bound");
  const { kind, payload, ...fields } = record;
  return decodeRecord({
    kind, payload,
    ...(kind === "tool" && record.parentToolCallId !== undefined
      ? { parentToolCallId: record.parentToolCallId } : {}),
    provenance: {
      ...fields,
      instanceId: page.ref.instanceId,
      owner: {
        processId: page.ref.processId, coordinatorSessionId: page.ref.coordinatorSessionId, epoch: page.ref.epoch,
      },
      captureEpoch: page.epoch, sequence,
    },
  });
}

function normalizeRecord(value: unknown, page: Pick<PublicConversationPage, "epoch" | "ref">, sequence: number | null) {
  if (typeof value !== "object" || value === null)
    throw new BridgeError("invalid_record", "Invalid native record JSON");
  return decodeRecord({
    ...value, provenance: {
      ...value, instanceId: page.ref.instanceId, owner: page.ref,
      captureEpoch: page.epoch, sequence,
    },
  });
}

function matches(launch: PiNativeWorkerLaunch, detail: PublicWorkerDetail) {
  const worker = detail.worker;
  return worker.linkage === "tool" &&
    worker.source === launch.source && worker.instanceId === launch.instanceId &&
    worker.coordinatorSessionId === launch.coordinatorSessionId && worker.toolCallId === launch.toolCallId &&
    (launch.source !== "team_assign" ||
      (worker.assignmentId === launch.assignmentId && worker.roleId === launch.roleId));
}
function sameReference(a: WorkerReadReference, b: WorkerReadReference) {
  return sameOwner(a, b) && a.instanceId === b.instanceId && a.source === b.source &&
    a.assignmentId === b.assignmentId && a.native?.generation === b.native?.generation &&
    a.native?.historyId === b.native?.historyId && a.native?.sessionId === b.native?.sessionId;
}
const unavailableSummary = (launch: PiNativeWorkerLaunch, gap: PiNativeWorkerSummary["gaps"][number]): PiNativeWorkerSummary => ({
  launch, capture: "incomplete", availability: "unavailable", gaps: [gap],
});

/** Passive protocol-2 source only; no controller, route, history, grant, or public inspector port. */
export class PiWorkerBridge implements ProviderNativeWorkersPort {
  readonly directory: string | undefined;
  readonly launchArgs: ReadonlyArray<string>;
  private readonly directoryIdentity: NodeFS.Stats | undefined;
  private client: BridgeClient | undefined;
  private subscription: BridgeSubscription | undefined;
  private generation = 0;
  private closed = false;
  private readonly work = new Set<Promise<unknown>>();
  private readonly listeners = new Set<() => void>();
  private readinessCancel: (() => void) | undefined;
  private binding: { sessionId: string | undefined; task: Promise<void> } | undefined;
  private state: "connecting" | "ready" | "unavailable" | "stale_owner" = "connecting";
  private launchFailed: boolean;

  constructor(platform: string, launchFailed = false) {
    this.launchFailed = launchFailed;
    let directory: string | undefined;
    if (!launchFailed && platform !== "win32") {
      try {
        directory = NodeFS.mkdtempSync(NodePath.join(NodeFS.realpathSync(NodeOS.tmpdir()), "t3-pw-"));
        NodeFS.chmodSync(directory, 0o700);
        if (Buffer.byteLength(NodePath.join(directory, "b-000000000000.sock")) > 103) {
          NodeFS.rmdirSync(directory);
          directory = undefined;
        }
      } catch { directory = undefined; }
    }
    this.directory = directory;
    this.directoryIdentity = directory === undefined ? undefined : NodeFS.lstatSync(directory);
    this.launchArgs = directory === undefined ? [] : [`--pitako-agent-bridge=${directory}`];
    if (directory === undefined) this.state = "unavailable";
  }
  get ownerGeneration() { return this.generation; }
  get owner() { return this.state === "ready" ? this.client?.owner : undefined; }
  readonly hints = Stream.callback<void>(
    (queue) => Effect.acquireRelease(
      Effect.sync(() => {
        const listener = () => { Queue.offerUnsafe(queue, undefined); };
        this.listeners.add(listener);
        listener();
        return listener;
      }),
      (listener) => Effect.sync(() => { this.listeners.delete(listener); }),
    ),
    { bufferSize: 1, strategy: "sliding" },
  );
  private hint() { for (const listener of this.listeners) listener(); }
  private track<A>(promise: Promise<A>): Promise<A> {
    this.work.add(promise);
    void promise.then(() => this.work.delete(promise), () => this.work.delete(promise));
    return promise;
  }
  private async descriptorReady() {
    const directory = this.directory;
    if (!directory) throw new BridgeError("bridge_unavailable", "Native worker source unavailable");
    await new Promise<void>((resolve, reject) => {
      let done = false;
      const watcher = NodeFS.watch(directory, () => check());
      const timer = setTimeout(() => finish(new BridgeError("bridge_not_ready", "Native worker descriptor unavailable")), 3_000);
      const finish = (error?: Error) => {
        if (done) return;
        done = true;
        clearTimeout(timer); watcher.close(); this.readinessCancel = undefined;
        if (error) reject(error); else resolve();
      };
      const check = () => { if (NodeFS.existsSync(NodePath.join(directory, "descriptor.json"))) finish(); };
      this.readinessCancel = () => finish(new BridgeError("stale_owner", "Native owner invalidated"));
      watcher.on("error", () => finish(new BridgeError("bridge_unavailable", "Native readiness failed")));
      check();
    });
  }
  /** Correlated stdio get_state session ID is authoritative, never sessionFile/descriptor. */
  readonly bind = (sessionId: string | undefined) => Effect.promise(async () => {
    if (this.launchFailed || this.closed || !this.directory) return;
    if (this.binding && this.binding.sessionId === sessionId) return this.binding.task;
    if (this.binding) await this.invalidateNative();
    if (this.client?.owner.coordinatorSessionId === sessionId) return;
    if (this.client) await this.invalidateNative();
    const generation = this.generation;
    const directory = this.directory;
    const task = this.track((async () => {
      try {
        if (!sessionId) throw new BridgeError("missing_session_id", "Pi returned no session ID");
        await this.descriptorReady();
        const client = await connect(NodePath.join(directory, "descriptor.json"), { expectedSessionId: sessionId });
        if (generation !== this.generation || this.closed) { await client.disconnect(); return; }
        this.client = client;
        if (!["workers.detail.public-v1", "workers.activity.public-v1", "workers.result.public-v1"]
          .every((capability) => client.capabilities.some((value) => value === capability))) {
          this.state = "unavailable";
        } else {
          this.state = "ready";
          // A subscription is only an optimization. A publicDetail probe never depends on it.
          try { this.subscription = await client.subscribe(() => this.hint()); } catch { /* Refresh on demand. */ }
        }
      } catch {
        if (generation === this.generation && !this.closed) this.state = "unavailable";
      } finally { this.hint(); }
    })());
    this.binding = { sessionId, task };
    try { await task; } finally { if (this.binding?.task === task) this.binding = undefined; }
  });
  private async withClient<A>(use: (client: BridgeClient) => Promise<A>): Promise<A> {
    const generation = this.generation;
    const client = this.client;
    if (!client || this.closed || this.state !== "ready")
      throw new BridgeError(this.launchFailed ? "bridge_launch_failed" : "bridge_unavailable", "Native worker source unavailable");
    return this.track((async () => {
      const result = await use(client);
      if (generation !== this.generation || this.closed) throw new BridgeError("stale_owner", "Native owner changed");
      return result;
    })());
  }
  private async detail(client: BridgeClient, launch: PiNativeWorkerLaunch) {
    const detail = await client.publicDetail(launch.instanceId);
    if ("state" in detail) return undefined;
    if (!matches(launch, detail) || !sameOwner(client.owner, detail.ref) ||
      detail.ref.instanceId !== launch.instanceId || detail.ref.source !== launch.source ||
      (launch.source === "team_assign" && detail.ref.assignmentId !== launch.assignmentId))
      throw new BridgeError("provenance_mismatch", "Native public launch provenance differs");
    return detail;
  }
  private summary(launch: PiNativeWorkerLaunch, detail: PublicWorkerDetail): PiNativeWorkerSummary {
    return {
      launch, roleId: detail.worker.roleId, acceptedAt: detail.worker.acceptedAt,
      brief: detail.workbrief.text, briefComplete: detail.nextBrief === null,
      selected: detail.selected, activated: detail.activated,
      execution: detail.lifecycle.execution, result: detail.lifecycle.result,
      capture: detail.lifecycle.capture, availability: "ready", gaps: detail.captureGaps ?? [],
    };
  }
  readonly reconcile = (launch: PiNativeWorkerLaunch) => Effect.tryPromise({
    try: async () => {
      try {
        return await this.withClient(async (client) => {
          const detail = await this.detail(client, launch);
          return detail ? this.summary(launch, detail) : unavailableSummary(launch, "unavailable");
        });
      } catch (cause) {
        return unavailableSummary(launch, cause instanceof BridgeError && cause.code === "provenance_mismatch"
          ? "provenance_mismatch" : this.state === "stale_owner" ? "stale_owner" : "unavailable");
      }
    }, catch: errorOf,
  });

  readonly read = (launch: PiNativeWorkerLaunch, previous?: PiNativeWorkerCheckpoint) => Effect.tryPromise({
    try: () => this.withClient(async (client): Promise<ProviderNativeWorkerBatch> => {
      const detail = await this.detail(client, launch);
      if (!detail) throw new BridgeError("unavailable", "Native public detail unavailable");
      const bindings: Array<PiNativeWorkerBinding> = [];
      const references = new Map<number, WorkerReadReference>();
      let metadata = detail;
      const metadataCursors = new Set<string>();
      while (true) {
        for (const { ref, ...binding } of metadata.bindings) {
          if (!sameOwner(ref, client.owner) || ref.instanceId !== launch.instanceId ||
            ref.source !== launch.source || ref.assignmentId !== detail.ref.assignmentId ||
            ref.native?.generation !== binding.generation ||
            ref.native.historyId !== binding.historyId || ref.native.sessionId !== binding.sessionId ||
            references.has(binding.generation))
            throw new BridgeError("provenance_mismatch", "Native binding reference differs");
          bindings.push(binding);
          references.set(binding.generation, ref);
        }
        if (!metadata.nextBindings) break;
        const cursorKey = encodeJsonRecord(metadata.nextBindings);
        if (metadataCursors.has(cursorKey)) throw new BridgeError("invalid_cursor", "Repeated native binding cursor");
        metadataCursors.add(cursorKey);
        const next = await client.publicDetail(launch.instanceId, metadata.nextBindings);
        if ("state" in next || !matches(launch, next) || !sameReference(detail.ref, next.ref))
          throw new BridgeError("provenance_mismatch", "Native binding metadata changed identity");
        metadata = next;
      }
      const compatible = previous !== undefined &&
        sameOwner(previous.owner, client.owner) && previous.captureEpoch === (detail.captureEpoch ?? null) &&
        sameLaunch(previous.launch, launch);
      const bindingCursors = compatible ? [...previous.bindingCursors] : [];
      const unread = bindings.find((binding) => !bindingCursors.some((saved) => saved.generation === binding.generation));
      const continuing = bindingCursors.find((saved) => !saved.omission && (saved.more || saved.fragment));
      const importable = bindings.filter((binding) =>
        !bindingCursors.some((saved) => saved.generation === binding.generation && saved.omission));
      const selectedBinding = (continuing && references.has(continuing.generation) ? continuing.generation : undefined) ?? unread?.generation ??
        importable.find((binding) => binding.state === "active")?.generation ?? importable.at(-1)?.generation ?? null;
      const missingBindings = previous !== undefined && sameOwner(previous.owner, client.owner)
        ? previous.bindings.filter((saved) => !references.has(saved.generation)) : [];
      bindings.push(...missingBindings.map((saved) => ({ ...saved, state: "historical" as const })));
      const saved = bindingCursors.find((binding) => binding.generation === selectedBinding);
      const checkpoint: { -readonly [Key in keyof PiNativeWorkerCheckpoint]: PiNativeWorkerCheckpoint[Key] } = {
        launch, owner: client.owner, ownerGeneration: this.generation,
        captureEpoch: detail.captureEpoch ?? null, bindings,
        bindingGeneration: selectedBinding, bindingCursors,
        cursor: saved?.fragment?.cursor ?? saved?.cursor ?? null,
        cut: saved?.cut ?? 0, recovery: saved?.recovery ?? true,
        capture: detail.lifecycle.capture, finalSequence: detail.lifecycle.finalSequence ?? null,
        gaps: [...(detail.captureGaps ?? []), ...bindingCursors.flatMap((saved) => saved.omission ? [saved.omission] : []),
          ...(previous && !compatible ? ["resync_required" as const] : [])],
      };
      if (missingBindings.length > 0) {
        checkpoint.gaps = [...new Set([...checkpoint.gaps, "pruned" as const])];
      }
      const records: Array<PiNativeWorkerRecord> = [];
      const assembler = new NativeRecordAssembler();
      const cursors = new Set<string | null>();
      let more = false;
      let omission: "invalid_record" | "record_oversized" | undefined;
      const addGap = (gap: PiNativeWorkerCheckpoint["gaps"][number]) => {
        checkpoint.gaps = [...new Set([...checkpoint.gaps, gap])];
      };
      let cursor = checkpoint.cursor;
      const reference = selectedBinding === null ? undefined : references.get(selectedBinding);
      try {
        if (reference) do {
          if (cursors.has(cursor)) throw new BridgeError("invalid_record", "Repeated native conversation cursor");
          cursors.add(cursor);
          const page = await client.publicActivity(reference, cursor ?? undefined, checkpoint.recovery);
          if (page.state !== "ready" && page.state !== "resync_required") {
            addGap(page.state === "stale_owner" ? "stale_owner" : page.state === "evicted" ? "evicted" : "unavailable");
            checkpoint.capture = "incomplete";
            more = false;
            break;
          }
          if (!sameReference(page.ref, reference))
            throw new BridgeError("invalid_record", "Native conversation owner differs");
          if (page.state === "resync_required") {
            more = !(previous?.recovery && previous.cursor === null &&
              previous.captureEpoch === page.epoch && previous.gaps.includes("resync_required"));
            addGap("resync_required");
            checkpoint.captureEpoch = page.epoch;
            checkpoint.recovery = true;
            checkpoint.cursor = null;
            if (!more) checkpoint.capture = "incomplete";
            break;
          }
          if (checkpoint.captureEpoch !== null && page.epoch !== checkpoint.captureEpoch)
            throw new BridgeError("invalid_record", "Native conversation epoch differs");
          checkpoint.captureEpoch = page.epoch;
          checkpoint.cut = page.cut;
          if (page.capture) {
            checkpoint.capture = page.capture.state;
            checkpoint.finalSequence = page.capture.finalSequence;
          }
          for (const gap of page.gaps) addGap(gap);
          if (page.binding) {
            const pageBinding = page.binding;
            const index = bindings.findIndex((binding) => binding.generation === pageBinding.generation);
            if (index < 0) throw new BridgeError("invalid_record", "Unknown native binding");
            if (pageBinding.generation !== selectedBinding ||
              pageBinding.historyId !== reference.native?.historyId || pageBinding.sessionId !== reference.native?.sessionId)
              throw new BridgeError("invalid_record", "Native page selection changed binding");
            bindings[index] = pageBinding;
          }
          for (const change of page.changes) {
            const record = assembler.accept(change, page, cursor);
            if (record) {
              if (record.provenance.binding !== selectedBinding)
                throw new BridgeError("invalid_record", "Record has unknown native binding");
              records.push(record);
            }
          }
          more = page.more;
          if (assembler.fragment) {
            checkpoint.fragment = assembler.fragment;
            checkpoint.cursor = assembler.fragment.cursor;
            if (!page.next) { addGap("not_captured"); checkpoint.capture = "incomplete"; more = false; break; }
          } else {
            delete checkpoint.fragment;
            checkpoint.cursor = page.next;
            if (!page.more) checkpoint.recovery = false;
          }
          cursor = page.next;
        } while (assembler.fragment !== undefined && cursor !== null);
      } catch (cause) {
        omission = cause instanceof BridgeError && cause.code === "record_oversized" ? "record_oversized" : "invalid_record";
        addGap(omission);
        if (assembler.fragment) {
          checkpoint.fragment = assembler.fragment;
          checkpoint.cursor = assembler.fragment.cursor;
        }
        checkpoint.capture = "incomplete";
        more = false;
      }
      if (selectedBinding !== null) {
        const next = {
          generation: selectedBinding, cursor: checkpoint.cursor, cut: checkpoint.cut, recovery: checkpoint.recovery, more,
          ...(checkpoint.fragment ? { fragment: checkpoint.fragment } : {}),
          ...(omission ? { omission } : {}),
        };
        const index = bindingCursors.findIndex((binding) => binding.generation === selectedBinding);
        if (index < 0) bindingCursors.push(next); else bindingCursors[index] = next;
      }
      more ||= bindings.some((binding) => references.has(binding.generation) &&
        !bindingCursors.some((saved) => saved.generation === binding.generation));
      if (bindingCursors.some((saved) => saved.omission)) checkpoint.capture = "incomplete";
      if (!reference && bindings.length === 0) addGap("pending");
      let result: ProviderNativeWorkerBatch["result"];
      if (detail.lifecycle.result === "available") {
        let nativeResult = await client.publicResult(detail.ref);
        if (nativeResult.state === "ready" && Buffer.byteLength(nativeResult.text.text) <= RECORD_LIMIT) {
          if (!sameReference(nativeResult.ref, detail.ref))
            throw new BridgeError("provenance_mismatch", "Native result owner differs");
          let text = nativeResult.text.text;
          let size = Buffer.byteLength(text);
          let complete = nativeResult.nextText === null;
          const offsets = new Set<number>();
          while (nativeResult.state === "ready" && nativeResult.nextText !== null) {
            const expectedOffset = nativeResult.text.nextOffset;
            const cursor = nativeResult.nextText;
            if (expectedOffset === null || offsets.has(expectedOffset)) {
              addGap("invalid_record"); break;
            }
            offsets.add(expectedOffset);
            const next = await client.publicResult(detail.ref, cursor);
            if (next.state !== "ready" || !sameReference(next.ref, detail.ref) || next.text.offset !== expectedOffset) {
              addGap("invalid_record"); break;
            }
            size += Buffer.byteLength(next.text.text);
            if (size > RECORD_LIMIT) { addGap("record_oversized"); break; }
            text += next.text.text;
            nativeResult = next;
            complete = next.nextText === null;
          }
          result = { status: nativeResult.status, text, complete };
        } else {
          addGap(nativeResult.state === "ready" ? "record_oversized" : "unavailable");
          checkpoint.capture = "incomplete";
        }
      }
      return { launch, summary: { ...this.summary(launch, detail),
          capture: checkpoint.capture, gaps: checkpoint.gaps }, expectedCheckpoint: previous,
        checkpoint, records, bindings, ...(result ? { result } : {}), more };
    }), catch: errorOf,
  });

  /** No roster admission: the server supplies its committed launch set. */
  readonly hasPendingWork = (admitted: ReadonlyArray<PiNativeWorkerLaunch>) => Effect.tryPromise({
    try: () => this.withClient(async (client) => {
      for (const launch of admitted) {
        const detail = await this.detail(client, launch);
        if (detail && (detail.lifecycle.execution === "accepted" || detail.lifecycle.execution === "running" ||
          detail.lifecycle.capture === "opening" || detail.lifecycle.capture === "open")) return true;
      }
      return false;
    }), catch: errorOf,
  }).pipe(Effect.orElseSucceed(() => false));
  readonly invalidate = Effect.promise(() => this.invalidateNative());
  private async invalidateNative() {
    this.generation++;
    this.state = "stale_owner";
    this.readinessCancel?.();
    const subscription = this.subscription;
    this.subscription = undefined;
    const disposal = subscription?.dispose().catch(() => undefined);
    const client = this.client;
    this.client = undefined;
    if (client) await client.disconnect();
    await Promise.allSettled([...(disposal ? [disposal] : []), ...this.work]);
    this.hint();
  }
  readonly close = Effect.sync(() => { this.closed = true; }).pipe(Effect.andThen(this.invalidate));
  /** Remove only an unchanged, empty owned directory after orderly process exit. */
  cleanupDirectory(orderlyExit: boolean) {
    if (!orderlyExit || !this.directory || !this.directoryIdentity) return;
    try {
      const current = NodeFS.lstatSync(this.directory);
      if (current.dev === this.directoryIdentity.dev && current.ino === this.directoryIdentity.ino &&
        !current.isSymbolicLink() && NodeFS.readdirSync(this.directory).length === 0) NodeFS.rmdirSync(this.directory);
    } catch { /* Retain uncertain resources; never delete another owner. */ }
  }
}
