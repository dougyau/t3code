// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Offline native fixture, with independently witnessed Team admission.
import * as NodePath from "node:path";
import * as NodeFS from "node:fs";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { nativeIntegration } from "./workerBridge.native.fixture.ts";
import { extensionSource } from "./workerBridge.native.shared.ts";
import {
  architectureConfig,
  architectureCoordinator,
  architectureGoal,
  architectureWorker,
} from "./architectureBridge.native.fixture.ts";
import type {
  ArchitectureRecord,
  ArchitectureRef,
  WorkerReadCursor,
} from "./pitako/bridge/protocol.js";
import { isArchitectureRecord } from "./pitako/bridge/protocol.js";
import type { BridgeClient } from "./pitako/bridge/client.js";
import { readArchitectures } from "./architectureBridge.ts";

const goal = "# Encargo 🧠\r\n\nKnown native admission";
const ToolResults = Schema.Struct({
  messages: Schema.Array(Schema.Unknown),
});
const StartResult = Schema.Struct({
  role: Schema.Literal("toolResult"),
  toolName: Schema.Literal("architecture_start"),
  isError: Schema.Literal(false),
  details: Schema.Struct({
    roomId: Schema.String,
    instanceId: Schema.String,
    memberId: Schema.String,
    assignmentId: Schema.String,
  }),
});
const Admission = Schema.Struct({
  assignments: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      instanceId: Schema.String,
      memberId: Schema.String,
      task: Schema.String,
      ordinal: Schema.Number,
    }),
  ),
});
const decodeToolResults = Schema.decodeUnknownEffect(ToolResults);
const decodeAdmission = Schema.decodeUnknownEffect(Admission);

async function readRoom(client: BridgeClient, ref: ArchitectureRef) {
  const records: ArchitectureRecord[] = [];
  const fragments = new Map<string, Buffer[]>();
  let cursor: string | undefined;
  let cut: number | undefined;
  let snapshotId: string | undefined;
  let capture;
  do {
    const page = await client.readArchitecture(ref, cursor);
    assert.equal(page.state, "ready", JSON.stringify(page));
    if (page.state !== "ready") throw Error("Native room unavailable");
    cut ??= page.cut;
    snapshotId ??= page.snapshotId;
    assert.equal(page.cut, cut);
    assert.equal(page.snapshotId, snapshotId);
    capture = page.capture;
    for (const record of page.records) {
      if (record.kind !== "fragment") {
        assert.isTrue(isArchitectureRecord(record));
        records.push(record);
        continue;
      }
      const parts = fragments.get(record.recordId) ?? [];
      assert.equal(
        record.byteOffset,
        parts.reduce((size, part) => size + part.length, 0),
      );
      const part = Buffer.from(record.data, "base64");
      parts.push(part);
      fragments.set(record.recordId, parts);
      if (record.byteOffset + part.length === record.totalBytes) {
        const restored: unknown = JSON.parse(Buffer.concat(parts).toString("utf8"));
        assert.isTrue(isArchitectureRecord(restored));
        if (!isArchitectureRecord(restored)) throw Error("Invalid native fragment");
        records.push(restored);
        fragments.delete(record.recordId);
      }
    }
    cursor = page.next ?? undefined;
  } while (cursor);
  assert.equal(fragments.size, 0);
  return { cut, capture, records };
}

function coordinator(root: string, pitako: string) {
  return extensionSource(root, pitako, true, false, true)
    .replace(
      "import { teamEvaluationForSession }",
      "import { teamAssignments, teamEvaluationForSession }",
    )
    .replace(
      "streamSimple(model) {",
      `streamSimple(model, input) {
      writeFileSync(root + "/coordinator-input.json", JSON.stringify({ messages: input.messages.filter(m => m.role === "toolResult") }));`,
    )
    .replace(
      "const state = { coordinatorCalls",
      `const assignments = evaluation ? teamAssignments(evaluation).map(a => ({
      id: a.id, memberId: a.memberId, instanceId: a.instanceId, task: a.task, ordinal: a.ordinal
    })) : [];
    const state = { assignments, coordinatorCalls`,
    )
    .replace(
      '{ type: "toolCall", id: "spawn-background", name: "agent_spawn", arguments:',
      '{ type: "toolCall", id: "architecture-start", name: "architecture_start", arguments:',
    )
    .replace(
      /role: "researcher", task: "Offline background fixture"[\s\S]*?task: "Offline Team fixture" \} \}/,
      `goal: ${JSON.stringify(goal)} } }`,
    );
}

// This first interoperability checkpoint deliberately holds worker runtime
// creation at the existing "launch" gate. Cleanup may release it, so it must
// still use an entirely offline provider and never fall back to a paid model.
function worker(pitako: string) {
  return `
import { createAssistantMessageEventStream } from ${JSON.stringify(NodePath.join(pitako, "node_modules/@earendil-works/pi-ai/dist/utils/event-stream.js"))};
export default function(pi) {
  pi.registerProvider("t3-worker", {
    api: "openai-responses", baseUrl: "http://127.0.0.1", apiKey: "fixture",
    models: [{ id: "controlled", name: "Offline architecture", compat: { supportsOpenAIGrammarTools: true },
      reasoning: false, input: ["text"], contextWindow: 1000000, maxTokens: 128,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }],
    streamSimple(model) {
      const message = { role: "assistant", content: [{ type: "text", text: "Fixture cleanup only" }],
        api: model.api, provider: model.provider, model: model.id, stopReason: "stop", timestamp: Date.now(),
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      const stream = createAssistantMessageEventStream();
      queueMicrotask(() => { stream.push({ type: "done", reason: "stop", message }); stream.end(message); });
      return stream;
    }
  });
}
`;
}

describe.skipIf(process.env.T3_PITAKO_NATIVE_TEST_ROOT === undefined)(
  "real architecture producer through makePiAdapterV2",
  () => {
    it.live(
      "exports independent admissions, failures, replacement, support, dissent and settled closure across two rooms",
      () =>
        nativeIntegration(
          false,
          (fixture) =>
            Effect.gen(function* () {
              yield* Effect.promise(() => fixture.wait("coordinator-waiting.json"));
              fixture.signal("launch");
              yield* Effect.promise(() =>
                Promise.race([
                  fixture.wait("frontier.json"),
                  fixture.wait("submission-error.json").then((value) => {
                    throw Error(JSON.stringify(value));
                  }),
                ]),
              );
              yield* Effect.promise(() => fixture.wait("provisional-other.json"));
              const input = yield* decodeToolResults(
                yield* Effect.promise(() => fixture.wait("coordinator-input.json")),
              ).pipe(Effect.orDie);
              const starts = input.messages.filter(Schema.is(StartResult));
              assert.equal(starts.length, 2, JSON.stringify(input));
              const catalog = yield* Effect.promise(() => fixture.client.listArchitectures());
              assert.equal(catalog.state, "ready");
              if (catalog.state !== "ready") throw Error("Missing native catalog");
              for (const item of catalog.items) assert.notProperty(item, "mode");
              assert.sameMembers(
                catalog.items.map((item) => item.ref.roomId),
                starts.map((start) => start.details.roomId),
              );
              const table = catalog.items.find(
                (item) => item.ref.roomId === starts[0]!.details.roomId,
              )!;
              const consultation = catalog.items.find(
                (item) => item.ref.roomId === starts[1]!.details.roomId,
              )!;
              const before = yield* Effect.promise(() => readRoom(fixture.client, table.ref));
              const otherBefore = yield* Effect.promise(() =>
                readRoom(fixture.client, consultation.ref),
              );
              assert.notEqual(
                before.records.find((record) => record.kind === "room")?.id,
                otherBefore.records.find((record) => record.kind === "room")?.id,
              );
              assert.isFalse(
                otherBefore.records.some(
                  (record) => record.kind === "publication" && record.message.stage === "synthesis",
                ),
                "Provisional submit is not a settled publication",
              );
              const members = before.records.filter((record) => record.kind === "member");
              assert.equal(members.filter((member) => member.function === "specialist").length, 3);
              assert.equal(members.filter((member) => member.function === "support").length, 1);
              const failed = members.find(
                (member) =>
                  member.mandate.source === "composition" &&
                  member.mandate.text === "Fails before contribution",
              )!;
              assert.equal(failed.execution, "failed");
              assert.isFalse(
                before.records.some(
                  (record) =>
                    record.kind === "publication" && record.message.memberId === failed.id,
                ),
              );
              let original = "";
              let briefCursor: WorkerReadCursor | undefined;
              do {
                const detail = yield* Effect.promise(() =>
                  fixture.client.publicDetail(table.director.instanceId, briefCursor),
                );
                if (!("workbrief" in detail)) throw Error("Missing root WorkBrief");
                assert.equal(detail.workbrief.source, "original WorkBrief");
                assert.equal(detail.workbrief.offset, original.length);
                original += detail.workbrief.text;
                briefCursor = detail.nextBrief ?? undefined;
              } while (briefCursor);
              assert.equal(original, architectureGoal);
              fixture.signal("contributions");
              yield* Effect.promise(() => fixture.wait("provisional-table.json"));
              const provisional = yield* Effect.promise(() => readRoom(fixture.client, table.ref));
              assert.isFalse(provisional.records.some((record) => record.kind === "outcome"));
              assert.isFalse(
                provisional.records.some(
                  (record) => record.kind === "publication" && record.message.stage === "synthesis",
                ),
              );
              fixture.signal("synthesis");
              yield* Effect.promise(() => fixture.wait("director-disposal.json"));
              const held = yield* Effect.promise(() => readRoom(fixture.client, table.ref));
              // The director has not disposed: publication may be settled, but outcome
              // and release must remain separate from the provisional receipt.
              assert.isFalse(held.records.some((record) => record.kind === "outcome"));
              fixture.signal("close-capture");
              yield* Effect.promise(() => fixture.wait("capture-closed.json"));
              const final = yield* Effect.promise(() => readRoom(fixture.client, table.ref));
              const otherFinal = yield* Effect.promise(() =>
                readRoom(fixture.client, consultation.ref),
              );
              const imported = yield* Effect.promise(() => readArchitectures(fixture.client, 0));
              assert.equal(imported.length, 2);
              for (const batch of imported) {
                const expected = batch.roomId === table.ref.roomId ? final : otherFinal;
                assert.deepEqual(batch.records, expected.records);
                assert.equal(batch.capture, "closed");
                assert.isEmpty(batch.gaps);
              }
              assert.deepEqual(
                imported.find((batch) => batch.roomId === table.ref.roomId)?.original,
                {
                  id: "original",
                  text: architectureGoal,
                  source: "original WorkBrief",
                  complete: true,
                },
              );
              const snapshot = yield* Effect.promise(() => fixture.snapshot());
              const known = (yield* decodeAdmission(snapshot).pipe(Effect.orDie)).assignments;
              const evidence = process.env.T3_PITAKO_NATIVE_EVIDENCE;
              if (evidence)
                NodeFS.writeFileSync(
                  NodePath.join(evidence, "full-native-observation.json"),
                  JSON.stringify(
                    {
                      catalog,
                      before,
                      otherBefore,
                      provisional,
                      held,
                      final,
                      otherFinal,
                      imported,
                      known,
                    },
                    null,
                    2,
                  ),
                );
              const observed = [...final.records, ...otherFinal.records].filter(
                (record) => record.kind === "member",
              );
              assert.sameDeepMembers(
                observed.map((member) => [
                  member.assignmentId,
                  member.id,
                  member.instanceId,
                  member.ordinal,
                ]),
                known.map((assignment) => [
                  assignment.id,
                  assignment.memberId,
                  assignment.instanceId,
                  assignment.ordinal,
                ]),
              );
              for (const member of observed) {
                const assignment = known.find((item) => item.id === member.assignmentId)!;
                assert.deepEqual(
                  member.mandate,
                  member.function === "director"
                    ? { source: "root-workbrief", instanceId: assignment.instanceId }
                    : {
                        source:
                          member.function === "specialist" ? "composition" : "TeamAssignment.task",
                        text: assignment.task,
                      },
                );
                assert.equal(member.workerRef?.instanceId, member.instanceId);
              }
              const finalMembers = final.records.filter((record) => record.kind === "member");
              assert.equal(
                new Set(finalMembers.map((member) => member.id)).size,
                finalMembers.length,
              );
              const finalFailed = finalMembers.find((member) => member.id === failed.id)!;
              assert.deepInclude(finalFailed, {
                assignmentId: failed.assignmentId,
                instanceId: failed.instanceId,
                function: "specialist",
                mandate: failed.mandate,
                execution: "failed",
                state: "closed",
              });
              // Stage failure already closed this member, before recomposition.
              assert.notProperty(finalFailed, "supersededByCompositionId");
              const healthy = members.filter(
                (member) =>
                  member.mandate.source === "composition" &&
                  ["Healthy one", "Healthy two"].includes(member.mandate.text),
              );
              assert.sameDeepMembers(
                healthy.map((member) => member.mandate),
                [
                  { source: "composition", text: "Healthy one" },
                  { source: "composition", text: "Healthy two" },
                ],
              );
              const replacements = finalMembers.filter(
                (member) =>
                  member.mandate.source === "composition" && member.mandate.text === "Replacement",
              );
              assert.lengthOf(replacements, 1);
              const replacement = replacements[0]!;
              assert.equal(replacement.function, "specialist");
              assert.isNotEmpty(replacement.compositionMessageId);
              const outgoing = finalMembers.filter(
                (member) => member.supersededByCompositionId !== undefined,
              );
              assert.sameMembers(
                outgoing.map((member) => member.id),
                healthy.map((member) => member.id),
              );
              for (const member of outgoing) {
                assert.equal(member.function, "specialist");
                assert.equal(member.state, "closed");
                assert.equal(member.supersededByCompositionId, replacement.compositionMessageId);
              }
              const publications = final.records.filter((record) => record.kind === "publication");
              assert.isTrue(
                publications.some(
                  (record) =>
                    record.message.id === replacement.compositionMessageId &&
                    record.message.body.kind === "composition",
                ),
              );
              const dissent = publications.filter(
                (record) =>
                  record.message.body.kind === "contribution" &&
                  record.message.body.objections.length,
              );
              assert.lengthOf(dissent, 5, "Real protocol must settle all authored dissent");
              for (const contribution of dissent) {
                assert.isTrue(
                  finalMembers.some((member) => member.id === contribution.message.memberId),
                );
                if (contribution.message.body.kind === "contribution") {
                  assert.lengthOf(contribution.message.body.objections, 1);
                  for (const objection of contribution.message.body.objections) {
                    assert.equal(objection.argument, "A genuine dissent");
                    assert.notEqual(objection.memberId, contribution.message.memberId);
                    assert.isTrue(
                      publications.some(
                        (record) =>
                          record.message.id === objection.messageId &&
                          record.message.memberId === objection.memberId,
                      ),
                    );
                  }
                }
              }
              for (const record of publications) {
                assert.isNotEmpty(record.message.id);
                assert.isNotEmpty(record.message.turnId);
                assert.isNotEmpty(record.message.attemptId);
                assert.isNotEmpty(record.message.snapshotId);
              }
              for (const room of [final, otherFinal]) {
                assert.equal(room.capture?.state, "closed");
                assert.deepEqual(
                  room.records.find((record) => record.kind === "room")?.lifecycle.closure,
                  { state: "released", members: "released", director: "released" },
                );
              }
              const outcome = final.records.find((record) => record.kind === "outcome");
              assert.equal(outcome?.status, "failed");
              assert.equal(outcome?.content.kind, "synthesis");
              if (outcome?.content.kind === "synthesis")
                assert.equal(outcome.content.body.status, "partial");
              assert.equal(
                otherFinal.records.find((record) => record.kind === "outcome")?.status,
                "completed",
              );
            }),
          {
            coordinator: architectureCoordinator,
            worker: architectureWorker,
            config: architectureConfig,
            launches: 0,
          },
        ),
    );

    it.live(
      "exposes the real architecture_start admission through the passive catalog before runtime creation",
      () =>
        nativeIntegration(
          false,
          (fixture) =>
            Effect.gen(function* () {
              assert.includeMembers(
                [...fixture.client.capabilities],
                ["architecture.list", "architecture.read", "architecture.public-v1"],
              );
              yield* Effect.promise(() => fixture.wait("coordinator-waiting.json"));
              const input = yield* decodeToolResults(
                yield* Effect.promise(() => fixture.wait("coordinator-input.json")),
              ).pipe(Effect.orDie);
              const start = input.messages.find(Schema.is(StartResult));
              assert.isDefined(
                start,
                "The real architecture_start must succeed before passive discovery",
              );
              const actual = start!.details;
              const snapshot = yield* Effect.promise(() => fixture.snapshot());
              const known = (yield* decodeAdmission(snapshot).pipe(Effect.orDie)).assignments;
              assert.deepEqual(known, [
                {
                  id: actual.assignmentId,
                  memberId: actual.memberId,
                  instanceId: actual.instanceId,
                  ordinal: 1,
                  task: goal,
                },
              ]);
              const workers = yield* Effect.promise(() => fixture.client.list());
              assert.isTrue(workers.items.some((row) => row.id === actual.instanceId));
              const catalog = yield* Effect.promise(() => fixture.client.listArchitectures());
              const room = yield* Effect.promise(() =>
                fixture.client.readArchitecture({
                  kind: "architecture",
                  ...fixture.client.owner,
                  roomId: actual.roomId,
                }),
              );
              // Keep the actual wire response in the failure, not an inferred room or
              // a roster compared with itself. A fresh cut rules out continuation reuse.
              const freshCatalog = yield* Effect.promise(() => fixture.client.listArchitectures());
              assert.equal(catalog.state, "ready");
              assert.equal(freshCatalog.state, "ready");
              assert.isTrue(
                catalog.state === "ready" &&
                  catalog.items.some((item) => item.ref.roomId === actual.roomId),
                JSON.stringify({ start, known, workers, catalog, freshCatalog, room }),
              );
              assert.equal(room.state, "ready");
              if (room.state !== "ready") return;
              const members = room.records.filter((record) => record.kind === "member");
              assert.deepEqual(
                members.map((member) => [member.assignmentId, member.id, member.instanceId]),
                known.map((assignment) => [
                  assignment.id,
                  assignment.memberId,
                  assignment.instanceId,
                ]),
              );
              assert.deepEqual(members[0]?.conversationRefs, []);
            }),
          {
            coordinator,
            worker,
            launches: 0,
            config:
              `[model_policies.architect.primary]\nmodel = "t3-worker/controlled"\nreasoning = "off"\n` +
              `[architecture]\ncandidates = [{ model = "t3-worker/controlled" }]\n`,
          },
        ),
    );
  },
);
