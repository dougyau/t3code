// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Owned native fixtures and decoded public wire witnesses only.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import { assert, describe, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import type { PiNativeWorkerCheckpoint, PiNativeWorkerRecord } from "@t3tools/contracts";
import type { ProviderNativeWorkerBatch } from "@t3tools/provider-core/server/ProviderAdapter";
import type { BridgeClient } from "./pitako/bridge/client.js";
import type { PublicBinding, PublicChange, PublicConversationPage, PublicRecord, PublicWorkerDetail, WorkerReadReference } from "./pitako/bridge/protocol.js";
import { nativeIntegration } from "./workerBridge.native.fixture.ts";

const source = NodeProcess.env.T3_PITAKO_NATIVE_TEST_ROOT;
const evidence = NodeProcess.env.T3_PITAKO_NATIVE_EVIDENCE;
const witnesses = Schema.decodeUnknownSync(Schema.Array(Schema.Struct({
  sessionId: Schema.String, file: Schema.String, head: Schema.NullOr(Schema.String),
  entries: Schema.Array(Schema.Struct({
    id: Schema.String, parentId: Schema.NullOr(Schema.String), type: Schema.String,
    message: Schema.optional(Schema.Struct({
      role: Schema.String, toolCallId: Schema.optional(Schema.String), toolName: Schema.optional(Schema.String),
    })),
  })),
})));
const probe = Schema.decodeUnknownSync(Schema.Struct({
  coordinatorCalls: Schema.Number,
  rows: Schema.Array(Schema.Struct({ resultTaken: Schema.Boolean })),
}));

async function detail(client: BridgeClient, id: string) {
  const value = await client.publicDetail(id);
  if (!("bindings" in value)) throw Error(value.reason);
  return value;
}

/** Assemble the producer's complete immutable UTF-8 upsert before interpreting its payload. */
async function available(client: BridgeClient, ref: WorkerReadReference, copies: Map<string, PublicRecord>,
  cursor?: string, recovery = false, afterFirst?: (page: PublicConversationPage) => Promise<void>,
  afterUpsert?: (record: PublicRecord, page: PublicConversationPage, receipt: {
    pageNumber: number; changeNumber: number; producerRecord: PublicRecord; argumentRetentionInjected: boolean;
  }) => void) {
  const fragments = new Map<string, { bytes: number; chunks: Buffer[] }>();
  const present = new Set<string>();
  const pages = [];
  let fragmented = false;
  for (;;) {
    const page = await client.publicActivity(ref, cursor, recovery);
    if (!("changes" in page)) throw Error(JSON.stringify(page));
    pages.push(page);
    if (pages.length === 1) await afterFirst?.(page);
    if (page.state === "resync_required") return { page, pages, present, fragmented };
    for (const [index, change] of page.changes.entries()) {
      let record: PublicRecord;
      if ("kind" in change) {
        fragmented = true;
        const key = `${change.id}:${change.revision}`;
        const part = fragments.get(key) ?? { bytes: 0, chunks: [] };
        assert.equal(change.byteOffset, part.bytes);
        const bytes = Buffer.from(change.data, "base64");
        part.chunks.push(bytes);
        part.bytes += bytes.length;
        fragments.set(key, part);
        if (part.bytes < change.totalBytes) continue;
        assert.equal(part.bytes, change.totalBytes);
        const decoded: PublicChange = JSON.parse(Buffer.concat(part.chunks).toString("utf8"));
        assert.equal(decoded.op, "upsert");
        record = decoded.record;
        assert.equal(record.id, change.id);
        assert.equal(record.revision, change.revision);
        fragments.delete(key);
      } else record = change.record;
      present.add(record.id);
      const old = copies.get(record.id);
      if (!old || old.revision < record.revision) {
        const producerRecord = record;
        // Recovery's omission is not a request to discard previously observed live arguments.
        if (old?.kind === "tool" && record.kind === "tool" &&
          old.payload.argsState === "available" && record.payload.argsState === "not_captured")
          record = { ...record, payload: { ...record.payload, args: old.payload.args, argsState: "available" } };
        copies.set(record.id, record);
        afterUpsert?.(record, page, { pageNumber: pages.length, changeNumber: index + 1,
          producerRecord, argumentRetentionInjected: producerRecord !== record });
      }
    }
    if (!page.more) {
      assert.equal(fragments.size, 0);
      if (!recovery) assert.isNotNull(page.next);
      return { page, pages, present, fragmented };
    }
    assert.isNotNull(page.next);
    cursor = page.next!;
  }
}

function selectedTodo(metadata: { bindings: readonly PublicBinding[] }, copies: Map<string, PublicRecord>) {
  const binding = metadata.bindings[0];
  const head = binding?.todoHead;
  if (!head) return { state: "unsupported" as const };
  if (head.state === "unavailable") return { state: "unavailable" as const };
  if (head.sourceEntryId === null) return { state: "absent" as const };
  const record = [...copies.values()].find(record =>
    record.binding === binding.generation && record.nativeEntryId === head.sourceEntryId &&
    record.kind === "tool" && record.payload.todoSnapshot);
  if (!record || record.kind !== "tool") return { state: "unavailable" as const };
  return { state: "ready" as const, sourceEntryId: head.sourceEntryId, tasks: record.payload.todoSnapshot!.tasks };
}

function bridgeTodo(batch: ProviderNativeWorkerBatch, copies: Map<string, PiNativeWorkerRecord>) {
  const binding = batch.bindings[0];
  const head = binding?.todoHead;
  if (!head) return { state: "unsupported" as const };
  if (head.state === "unavailable") return { state: "unavailable" as const };
  if (head.sourceEntryId === null) return { state: "absent" as const };
  const record = [...copies.values()].find(record =>
    record.provenance.binding === binding.generation &&
    record.provenance.nativeEntryId === head.sourceEntryId && record.kind === "tool" &&
    record.payload.todoSnapshot);
  if (!record || record.kind !== "tool") return { state: "unavailable" as const };
  return { state: "ready" as const, sourceEntryId: head.sourceEntryId, tasks: record.payload.todoSnapshot!.tasks };
}

describe.skipIf(source === undefined)("T3 public conversation from the real native Pitako producer", () => {
  it.live("decodes launch, native TODO ancestry, immutable fragments, loss and independent closure", () =>
    nativeIntegration(false, (fixture) => Effect.gen(function* () {
      const { client, workers, launches } = fixture;
      const save = (name: string, value: unknown) => {
        if (evidence) NodeFS.writeFileSync(NodePath.join(evidence, name), JSON.stringify(value, null, 2));
      };
      (yield* Effect.promise(() => fixture.wait("coordinator-waiting.json")));
      const early = (yield* Effect.promise(() => client.list()));
      assert.equal(early.items.length, 2);
      const bridgeReads: unknown[] = [];
      const bridgeCopies = new Map<string, PiNativeWorkerRecord>();
      const backgroundLaunch = launches.find(launch => launch.source === "agent_spawn")!;
      assert.isDefined(backgroundLaunch);
      let checkpoint: PiNativeWorkerCheckpoint | undefined;
      const consume = (phase: string, recovery = false,
        received?: (record: PiNativeWorkerRecord, batch: ProviderNativeWorkerBatch) => void) => Effect.gen(function* () {
        if (recovery && checkpoint) checkpoint = { ...checkpoint, cursor: null, recovery: true,
          bindingCursors: [], bindingGeneration: null };
        let batch: ProviderNativeWorkerBatch;
        do {
          batch = (yield* workers.read(backgroundLaunch, checkpoint));
          assert.deepEqual(batch.launch, backgroundLaunch);
          assert.deepEqual(batch.checkpoint.owner, workers.owner);
          assert.notInclude(batch.checkpoint.gaps, "invalid_record");
          assert.notInclude(batch.checkpoint.gaps, "record_oversized");
          for (const record of batch.records) {
            const old = bridgeCopies.get(record.provenance.id);
            if (!old || old.provenance.revision < record.provenance.revision) {
              bridgeCopies.set(record.provenance.id, record);
              received?.(record, batch);
            }
          }
          bridgeReads.push({ phase, batch, selected: bridgeTodo(batch, bridgeCopies) });
          checkpoint = batch.checkpoint;
          if (phase === "lost-cursor") break;
        } while (batch.more);
        save("current-bridge-reads.json", bridgeReads);
        return batch;
      });
      for (const item of early.items) {
        const launch = (yield* Effect.promise(() => detail(client, item.id)));
        assert.deepEqual(launch.bindings, []);
        assert.equal(launch.lifecycle.capture, "opening");
        assert.equal(selectedTodo(launch, new Map()).state, "unsupported");
        assert.equal(launch.worker.toolCallId, item.source === "agent_spawn" ? "spawn-background" : "assign-team");
        assert.equal(launch.worker.linkage, "tool");
        const input = launches.find(value => value.instanceId === item.id ||
          ("assignmentId" in value && value.assignmentId === item.id))!;
        assert.isDefined(input);
        const summary = (yield* workers.reconcile(input));
        save(`current-bridge-early-${item.source}.json`, { input, summary, producer: launch });
        assert.equal(summary.capture, "opening");
        assert.equal(summary.availability, "ready");
        assert.deepEqual(summary.launch, input);
      }
      const earlyBridge = (yield* consume("early-launch"));
      assert.deepEqual(earlyBridge.bindings, []);
      assert.equal(bridgeTodo(earlyBridge, bridgeCopies).state, "unsupported");
      save("early-launch.json", (yield* Effect.promise(() => Promise.all(early.items.map(item => detail(client, item.id))))));
      fixture.signal("launch");
      (yield* Effect.promise(() => fixture.wait("step-1.json")));
      (yield* Effect.promise(() => fixture.wait("team-waiting.json")));
      const background = early.items.find(item => item.source === "agent_spawn")!;
      const team = early.items.find(item => item.source === "team_assign")!;
      const initial = (yield* Effect.promise(() => detail(client, background.id)));
      const ref = initial.bindings[0]!.ref;
      const copies = new Map<string, PublicRecord>();
      let read = (yield* Effect.promise(() => available(client, ref, copies)));
      assert.equal(bridgeTodo((yield* consume("initial-binding")), bridgeCopies).state, "absent");
      assert.equal(selectedTodo(initial, copies).state, "absent");
      assert.deepEqual(initial.bindings[0]!.todoHead, { state: "ready", sourceEntryId: null });
      const todoEntries: string[] = [];
      let liveB: PublicRecord | undefined;
      let bridgeB: PiNativeWorkerRecord | undefined;
      for (let step = 1; step <= 6; step++) {
        fixture.signal(`step-${step}`);
        const native = witnesses((yield* Effect.promise(() => fixture.wait(`step-${step + 1}.json`))));
        const nativeResult = native[0]!.entries.find(entry =>
          entry.message?.role === "toolResult" && entry.message.toolCallId === `native-tool-${step}`)!;
        assert.isDefined(nativeResult);
        read = (yield* Effect.promise(() => available(client, ref, copies, read.page.next!, false, undefined, (record, page, receipt) => {
          if (step !== 6 || record.nativeEntryId !== nativeResult.id) return;
          const selected = selectedTodo({ bindings: page.binding ? [page.binding] : [] }, copies);
          assert.deepEqual(page.binding?.todoHead, { state: "ready", sourceEntryId: nativeResult.id });
          assert.equal(selected.state === "ready" && selected.sourceEntryId, nativeResult.id);
          liveB = record;
          save("live-b-before-recovery.json", { receipt: { observation: 1, phase: "live-step-6", ...receipt },
            ref, record, binding: page.binding, selected, native });
        })));
        const metadata = (yield* Effect.promise(() => detail(client, background.id)));
        const selected = selectedTodo(metadata, copies);
        const consumerBatch = (yield* consume(`live-step-${step}`, false, (record, batch) => {
          if (step !== 6 || record.provenance.nativeEntryId !== nativeResult.id) return;
          bridgeB = record;
          const selection = bridgeTodo(batch, bridgeCopies);
          assert.equal(selection.state === "ready" && selection.sourceEntryId, nativeResult.id);
          save("current-bridge-live-b.json", { phase: "live-step-6", record, batch, selection, native });
        }));
        const consumerSelection = bridgeTodo(consumerBatch, bridgeCopies);
        assert.deepEqual(consumerSelection, selected);
        assert.equal(selected.state, "ready");
        if (selected.state !== "ready") throw Error("Selected native TODO unavailable");
        assert.equal(selected.sourceEntryId, nativeResult.id);
        todoEntries.push(nativeResult.id);
        const tasks = selected.tasks;
        if (step === 1) assert.deepEqual(tasks.map(task => task.id), [1]);
        if (step === 2) {
          assert.deepEqual(tasks.map(task => task.id), [1, 2]);
          assert.deepEqual(tasks.map(task => task.identityEntryId), [todoEntries[0], todoEntries[1]]);
        }
        if (step === 3 || step === 4) {
          assert.deepEqual(tasks.map(task => task.status), ["deleted", "pending"]);
          assert.equal(tasks[0]!.identityEntryId, todoEntries[0]);
        }
        if (step === 4) {
          const tool = [...copies.values()].find(record => record.nativeEntryId === nativeResult.id)!;
          if (tool.kind !== "tool") throw Error("Filtered native TODO result missing");
          assert.notInclude(JSON.stringify(tool.payload.result), "first native task");
          assert.equal(tasks[0]!.subject, "first native task");
        }
        if (step === 5) {
          assert.deepEqual(tasks, []);
          assert.isNotNull(selected.sourceEntryId);
        }
        if (step === 6) {
          assert.deepEqual(tasks.map(task => task.id), [1]);
          assert.equal(tasks[0]!.identityEntryId, todoEntries[5]);
          assert.notEqual(tasks[0]!.identityEntryId, todoEntries[0]);
        }
        save(`todo-${step}.json`, { metadata, selected, native });
        if (step === 2) {
          const frozenCopies = new Map<string, PublicRecord>();
          let laterEntry: string | undefined;
          const frozen = (yield* Effect.promise(() => available(client, ref, frozenCopies, undefined, true, async page => {
            assert.isTrue(page.more);
            fixture.signal("step-3");
            const later = witnesses(await fixture.wait("step-4.json"));
            laterEntry = later[0]!.entries.find(entry => entry.message?.toolCallId === "native-tool-3")!.id;
          })));
          assert.equal(frozen.page.state, "ready");
          assert.isFalse([...frozenCopies.values()].some(record => record.nativeEntryId === laterEntry));
          assert.isTrue(frozen.pages.every(page =>
            page.binding?.todoHead?.state === "ready" && page.binding.todoHead.sourceEntryId === todoEntries[1]));
          save("frozen-cut.json", { pages: frozen.pages, laterEntry, current: (yield* Effect.promise(() => detail(client, background.id))) });
        }
      }
      fixture.signal("step-7");
      (yield* Effect.promise(() => fixture.wait("tool-entered.json")));
      read = (yield* Effect.promise(() => available(client, ref, copies, read.page.next!)));
      const bridgeLive = (yield* consume("live-fragments"));
      const bridgeLiveTool = [...bridgeCopies.values()].find(record =>
        record.kind === "tool" && record.provenance.toolCallId === "native-tool-7");
      assert.equal(bridgeLiveTool?.kind === "tool" && bridgeLiveTool.payload.argsState, "available");
      assert.equal(bridgeLiveTool?.kind === "tool" && bridgeLiveTool.payload.status, "updated");
      assert.equal(bridgeLiveTool?.provenance.nativeEntryId, null);
      assert.equal(bridgeLive.summary.capture, "open");
      assert.isTrue(read.fragmented);
      assert.isAbove(read.pages.length, 1);
      const liveTool = [...copies.values()].find(record => record.toolCallId === "native-tool-7" && record.kind === "tool")!;
      if (liveTool.kind !== "tool") throw Error("Native tool not captured");
      assert.equal(liveTool.payload.status, "updated");
      assert.isNull(liveTool.nativeEntryId);
      assert.isUndefined(liveTool.payload.todoSnapshot);
      assert.equal(liveTool.payload.argsState, "available");
      save("live-fragments.json", read.pages);
      const beforeReads = (yield* Effect.promise(() => fixture.snapshot()));
      assert.isTrue(JSON.stringify(beforeReads).includes('"resultTaken":false'));
      (yield* Effect.promise(() => client.publicResult(ref)));
      assert.deepEqual((yield* Effect.promise(() => fixture.snapshot())), beforeReads);
      const lostCursor = read.page.next!;
      (yield* Effect.promise(() => fixture.command({ id: "burst", action: "burst" })));
      const lost = (yield* Effect.promise(() => client.publicActivity(ref, lostCursor)));
      assert.equal(lost.state, "resync_required");
      save("lost.json", lost);
      const bridgeResync = (yield* consume("lost-cursor"));
      assert.include(bridgeResync.checkpoint.gaps, "resync_required");
      const fresh = (yield* Effect.promise(() => available(client, ref, copies)));
      assert.equal(fresh.page.state, "ready");
      const missing = selectedTodo((yield* Effect.promise(() => detail(client, background.id))), new Map());
      assert.equal(missing.state, "unavailable");
      if (!liveB) throw Error("Live B receipt not observed");
      const consumedB = liveB;
      assert.strictEqual(copies.get(consumedB.id), consumedB);
      const priorA = [...copies.values()].find(record => record.nativeEntryId === todoEntries[0])!;
      assert.isDefined(priorA);
      let recoveredAReceived = false;
      const recovery = (yield* Effect.promise(() => available(client, ref, copies, undefined, true, undefined, (record, page, receipt) => {
        if (record.nativeEntryId !== todoEntries[0]) return;
        const selected = selectedTodo({ bindings: page.binding ? [page.binding] : [] }, copies);
        save("recovered-a-at-receipt.json", { receipt: { observation: 2, phase: "recovery", ...receipt },
          ref, record, priorA, liveB: consumedB, retainedB: copies.get(consumedB.id), binding: page.binding, selected });
        assert.isFalse(recoveredAReceived);
        assert.equal(record.id, priorA.id);
        assert.isTrue(record.recovered);
        assert.isAbove(record.revision, priorA.revision);
        assert.strictEqual(copies.get(record.id), record);
        assert.strictEqual(copies.get(consumedB.id), consumedB);
        assert.deepEqual(page.binding?.todoHead, { state: "ready", sourceEntryId: consumedB.nativeEntryId });
        assert.equal(selected.state === "ready" && selected.sourceEntryId, consumedB.nativeEntryId);
        assert.deepEqual(selected.state === "ready" && selected.tasks, consumedB.kind === "tool" && consumedB.payload.todoSnapshot?.tasks);
        recoveredAReceived = true;
      })));
      assert.isTrue(recoveredAReceived);
      assert.isDefined(bridgeB);
      const consumedBridgeB = bridgeB!;
      const bridgePriorA = [...bridgeCopies.values()].find(record =>
        record.provenance.nativeEntryId === todoEntries[0])!;
      let bridgeRecoveredA = false;
      const bridgeRecovery = (yield* consume("recovered-a-after-selected-b", true, (record, batch) => {
        if (record.provenance.nativeEntryId !== todoEntries[0]) return;
        bridgeRecoveredA = true;
        assert.isTrue(record.provenance.recovered);
        assert.equal(record.provenance.id, bridgePriorA.provenance.id);
        assert.isAbove(record.provenance.revision, bridgePriorA.provenance.revision);
        assert.strictEqual(bridgeCopies.get(consumedBridgeB.provenance.id), consumedBridgeB);
        const selected = bridgeTodo(batch, bridgeCopies);
        assert.equal(selected.state === "ready" && selected.sourceEntryId, todoEntries[5]);
        assert.deepEqual(selected.state === "ready" && selected.tasks,
          consumedBridgeB.kind === "tool" && consumedBridgeB.payload.todoSnapshot?.tasks);
        save("current-bridge-recovered-a.json", { phase: "recovered-a-after-selected-b",
          record, priorA: bridgePriorA, consumedB: consumedBridgeB, batch, selected,
          injection: null });
      }));
      assert.isTrue(bridgeRecoveredA);
      assert.equal(bridgeTodo(bridgeRecovery, bridgeCopies).state, "ready");
      assert.equal(recovery.page.state, "ready");
      assert.isTrue(recovery.pages.every(page =>
        page.binding?.todoHead?.state === "ready" && page.binding.todoHead.sourceEntryId === todoEntries[5]));
      assert.isTrue(recovery.pages.some(page => page.gaps.includes("not_persisted")));
      const recoveredSelection = selectedTodo((yield* Effect.promise(() => detail(client, background.id))), copies);
      assert.equal(recoveredSelection.state, "ready");
      if (recoveredSelection.state === "ready") assert.equal(recoveredSelection.sourceEntryId, todoEntries[5]);
      // Only IDs in the completed available cut or recovered here have comparable order.
      const sortable = new Set([...fresh.present, ...recovery.present]);
      const ordered = [...copies.values()].filter(record => sortable.has(record.id)).sort((a, b) => a.order - b.order);
      assert.isTrue(ordered.every(record => Number.isSafeInteger(record.order)));
      assert.equal(new Set(ordered.map(record => record.id)).size, ordered.length);
      assert.isTrue(ordered.every(record => !("sequence" in record)));
      const recoveredEarlier = [...copies.values()].find(record => record.nativeEntryId === todoEntries[0])!;
      assert.isTrue(recoveredEarlier.recovered);
      assert.isNull(recoveredEarlier.nativeOrder);
      // Receipt-time assertions above prove recovered A cannot replace the already consumed live B.
      assert.equal(recoveredEarlier.nativeEntryId, todoEntries[0]);
      assert.equal(selectedTodo((yield* Effect.promise(() => detail(client, background.id))), copies).state, "ready");
      assert.equal(recoveredSelection.state === "ready" && recoveredSelection.sourceEntryId, todoEntries[5]);
      save("recovery.json", { pages: recovery.pages, sortable: [...sortable], ordered });
      fixture.signal("tool-end");
      const appendedNative = witnesses((yield* Effect.promise(() => fixture.wait("step-8.json"))))[0]!;
      const appended = appendedNative.entries.find(entry => entry.message?.toolCallId === "native-tool-7")!.id;
      assert.equal(recoveredEarlier.parentEntryId, appendedNative.entries
        .find(entry => entry.id === todoEntries[0])!.parentId);
      fixture.signal("step-8");
      (yield* Effect.promise(() => fixture.wait("evict-tool-entered.json")));
      (yield* Effect.promise(() => fixture.command({ id: "evict-prior", action: "burst" })));
      const producerCopies = new Map<string, PublicRecord>();
      const completedRecovery = (yield* Effect.promise(() => available(client, ref, producerCopies, undefined, true)));
      assert.equal(completedRecovery.page.state, "ready");
      const uncaptured = [...producerCopies.values()].find(record => record.nativeEntryId === appended);
      if (!uncaptured || uncaptured.kind !== "tool")
        throw Error("Appended native result not recovered");
      assert.equal(uncaptured.payload.argsState, "not_captured");
      (yield* Effect.promise(() => available(client, ref, copies, undefined, true)));
      const keptArgs = copies.get(liveTool.id)!;
      if (keptArgs.kind !== "tool") throw Error("Stored native tool missing");
      assert.equal(keptArgs.payload.argsState, "available");
      assert.deepEqual(keptArgs.payload.args, liveTool.payload.args);
      save("not-captured.json", { producer: uncaptured, stored: keptArgs });
      fixture.signal("evict-tool-end");
      (yield* Effect.promise(() => fixture.wait("step-9.json")));
      fixture.signal("step-9");
      fixture.signal("team-end");
      (yield* Effect.promise(() => fixture.wait("disposal.json")));
      const duringClose = (yield* Effect.promise(() => detail(client, background.id)));
      assert.equal(duringClose.lifecycle.capture, "open");
      assert.equal(duringClose.lifecycle.execution, "completed");
      assert.equal(((yield* Effect.promise(() => client.publicResult(duringClose.ref)))).state, "pending");
      save("independent-close.json", duringClose);
      const bridgeClosing = (yield* consume("execution-completed-capture-open"));
      assert.equal(bridgeClosing.summary.execution, "completed");
      assert.equal(bridgeClosing.summary.capture, "open");
      assert.isUndefined(bridgeClosing.result);
      assert.isTrue((yield* workers.hasPendingWork([backgroundLaunch])));
      let resolveTerminal: (value: PublicWorkerDetail) => void = () => {};
      let rejectTerminal: (error: unknown) => void = () => {};
      const terminalChanged = new Promise<PublicWorkerDetail>((resolve, reject) => {
        resolveTerminal = resolve; rejectTerminal = reject;
      });
      const reads = new Set<Promise<void>>();
      const subscription = (yield* Effect.promise(() => client.subscribe(() => {
        const pending = detail(client, background.id).then(value => {
          if (value.lifecycle.capture === "closed" && value.lifecycle.execution === "completed")
            resolveTerminal(value);
        }, rejectTerminal);
        reads.add(pending);
        void pending.finally(() => reads.delete(pending));
      })));
      let resolveFollowReady: () => void = () => {};
      let resolveBridgeClosed: () => void = () => {};
      const followReady = new Promise<void>(resolve => { resolveFollowReady = resolve; });
      const bridgeClosed = new Promise<void>(resolve => { resolveBridgeClosed = resolve; });
      let followObservations = 0;
      const follow = yield* Effect.forkScoped(workers.hints.pipe(Stream.runForEach(() =>
        workers.reconcile(backgroundLaunch).pipe(Effect.tap(summary => Effect.sync(() => {
          followObservations++;
          if (summary.capture === "open") resolveFollowReady();
          if (summary.capture === "closed" && summary.execution === "completed") resolveBridgeClosed();
        }))))));
      let terminal: PublicWorkerDetail;
      try {
        (yield* Effect.promise(() => followReady));
        fixture.signal("close-capture");
        terminal = (yield* Effect.promise(() => terminalChanged));
        (yield* Effect.promise(() => bridgeClosed));
      } finally {
        (yield* Effect.promise(() => subscription.dispose()));
        (yield* Effect.promise(() => Promise.all(reads)));
        (yield* Fiber.interrupt(follow));
      }
      assert.isAtLeast(followObservations, 2);
      save("current-bridge-follow.json", { followObservations, joined: true });
      assert.equal(terminal.lifecycle.capture, "closed");
      assert.equal(terminal.lifecycle.execution, "completed");
      const beforeResult = (yield* Effect.promise(() => fixture.snapshot()));
      const result = (yield* Effect.promise(() => client.publicResult(terminal.ref)));
      assert.equal(result.state, "ready");
      assert.deepEqual((yield* Effect.promise(() => client.publicResult(terminal.ref))), result);
      const after = (yield* Effect.promise(() => fixture.snapshot()));
      assert.equal(probe(after).coordinatorCalls, probe(beforeReads).coordinatorCalls);
      assert.deepEqual(after, beforeResult);
      const bridgeTerminal = (yield* consume("terminal-result"));
      assert.equal(bridgeTerminal.summary.execution, "completed");
      assert.equal(bridgeTerminal.summary.capture, "closed");
      assert.deepEqual(bridgeTerminal.result, { status: "completed", complete: true,
        text: "native result 😀".repeat(200) });
      assert.deepEqual((yield* Effect.promise(() => fixture.snapshot())), beforeResult);
      assert.isTrue(probe(after).rows.every(row => !row.resultTaken));
      save("terminal.json", { terminal, result, after, team: (yield* Effect.promise(() => detail(client, team.id))) });
      const native = witnesses((yield* Effect.promise(() => fixture.wait("closed.json"))));
      if (evidence) {
        for (const [index, worker] of native.entries()) {
          NodeFS.copyFileSync(worker.file, NodePath.join(evidence, `native-worker-${index}.jsonl`));
        }
        NodeFS.copyFileSync(NodePath.join(fixture.root, "fixture.mjs"), NodePath.join(evidence, "coordinator-fixture.mjs"));
        NodeFS.copyFileSync(NodePath.join(fixture.root, "agent/extensions/worker.js"), NodePath.join(evidence, "worker-fixture.mjs"));
      }
      // Loss of this fixture-owned persisted source invalidates recovery, never clears its TODOs.
      const cut = (yield* Effect.promise(() => client.publicActivity(ref, undefined, true)));
      if (!("changes" in cut)) throw Error("Native recovery cut unavailable");
      assert.isNotNull(cut.next);
      const file = native[0]!.file;
      NodeFS.renameSync(file, file + ".held");
      try {
        const pruned = (yield* Effect.promise(() => client.publicActivity(ref, cut.next!, true)));
        if (!("changes" in pruned)) throw Error("Native recovery gap unavailable");
        assert.equal(pruned.state, "resync_required");
        assert.include(pruned.gaps, "pruned");
        const unavailable = (yield* Effect.promise(() => detail(client, background.id)));
        assert.equal(selectedTodo(unavailable, copies).state, "unavailable");
        assert.deepEqual(unavailable.bindings[0]!.todoHead, { state: "unavailable" });
        const bridgePruned = (yield* consume("pruned-source", true));
        assert.include(bridgePruned.checkpoint.gaps, "pruned");
        assert.equal(bridgeTodo(bridgePruned, bridgeCopies).state, "unavailable");
        save("unavailable-source.json", { pruned, unavailable });
      } finally {
        NodeFS.renameSync(file + ".held", file);
      }
    })),
  );
});
