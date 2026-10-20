import { assert, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ProviderArchitectureCatalogLoss } from "@t3tools/provider-core/server/ProviderAdapter";
import { PiArchitectureBody, PiArchitectureRecord } from "@t3tools/contracts";
import { readArchitectures } from "./architectureBridge.ts";
import { PiWorkerBridge } from "./workerBridge.ts";
import { workerWireFixture } from "./workerBridge.wire.fixture.ts";
import type {
  ArchitectureRecord,
  ArchitectureRead,
  ArchitectureRef,
  ArchitectureCatalog,
  Capability,
} from "./pitako/bridge/protocol.js";
import { isPublicArchitectureBody } from "./pitako/bridge/protocol.js";

const owner = { processId: "process", coordinatorSessionId: "coordinator", epoch: "epoch" };
const isArchitectureCatalogLoss = Schema.is(ProviderArchitectureCatalogLoss);
const isRecord = Schema.is(PiArchitectureRecord);
const decodeBody = Schema.decodeUnknownSync(PiArchitectureBody);
const ref = (roomId: string): ArchitectureRef => ({ ...owner, kind: "architecture", roomId });
const record: ArchitectureRecord = {
  kind: "control",
  id: "unicode",
  order: 1,
  action: "context",
  text: "café 🧭 中文",
  author: { coordinatorSessionId: owner.coordinatorSessionId, toolCallId: "context" },
  application: { state: "pending" },
};
const lifecycle = {
  stage: "composition",
  round: 0,
  protocolState: "running",
  outcome: "pending",
  closure: { state: "pending", members: "pending", director: "pending" },
} as const;
const room = {
  kind: "room",
  id: "current",
  origin: {
    coordinatorSessionId: owner.coordinatorSessionId,
    tool: "architecture_start",
    toolCallId: "start",
    memberId: "director",
    assignmentId: "assignment",
    instanceId: "director",
  },
  goalSource: { kind: "worker-workbrief", instanceId: "director" },
  lifecycle,
  pendingDoubtIds: [],
} as const;
const composition = {
  kind: "composition",
  snapshotId: "snapshot",
  reason: "Retain the specialist and add independent evidence",
  specialists: [
    { kind: "retain", memberId: "existing", mandate: "Review boundaries", reason: "Continuity" },
    { kind: "add", candidateRef: "candidate", mandate: "Check evidence", reason: "Independence" },
  ],
} as const;
const member = {
  kind: "member",
  id: "existing",
  assignmentId: "specialist-assignment",
  instanceId: "specialist-instance",
  roleId: "architect",
  function: "specialist",
  ordinal: 1,
  admittedAt: 1,
  mandate: { source: "composition", text: "Review boundaries" },
  execution: "running",
  conversationRefs: [],
  compositionMessageId: "publication",
  compositionChoiceIndex: 0,
  selection: { compositionMessageId: "publication", choiceIndex: 0 },
  state: "active",
} as const;
const publication = {
  kind: "publication",
  id: "publication",
  message: {
    id: "publication",
    ordinal: 1,
    roomId: room.id,
    assignmentId: "assignment",
    memberId: "director",
    author: "director",
    stage: "composition",
    snapshotId: "snapshot",
    turnId: "turn",
    attemptId: "attempt",
    body: composition,
    native: { sessionId: "session", toolCallId: "submit", persistence: "unconfirmed" },
    doubts: [],
  },
} as const;
const bodyCases = [
  { body: composition },
  { body: { ...composition, reason: "The director needs no specialists", specialists: [] } },
  {
    mode: "table",
    body: {
      kind: "composition",
      snapshotId: "historical",
      specialists: [
        { candidateRef: "old-candidate", mandate: "Old mandate", reason: "Old reason" },
      ],
    },
  },
  {
    mode: "consultation",
    body: { kind: "answer", answer: "Historical answer", evidence: [], limits: [], doubts: [] },
  },
  {
    mode: "consultation",
    body: { kind: "consultation-plan", resolutions: [], support: [] },
  },
] as const;
function client() {
  const calls: string[] = [];
  const capabilities: Capability[] = [
    "architecture.list",
    "architecture.read",
    "architecture.public-v1",
  ];
  return {
    calls,
    owner,
    capabilities,
    listArchitectures: async (cursor?: string) => {
      calls.push(`catalog:${cursor ?? "start"}`);
      return {
        format: "architecture-public-v1" as const,
        state: "ready" as const,
        owner,
        snapshotId: "catalog",
        cut: 2,
        next: cursor ? null : "opaque-catalog-token",
        items: [
          {
            ref: ref(cursor ? "second" : "first"),
            mode: "table" as const,
            director: { instanceId: "director", memberId: "director", assignmentId: "assignment" },
            lifecycle: {
              stage: null,
              round: 0,
              protocolState: "running" as const,
              outcome: "pending" as const,
              closure: {
                state: "pending" as const,
                members: "pending" as const,
                director: "pending" as const,
              },
            },
          },
        ],
      };
    },
    readArchitecture: async (
      target: ArchitectureRef,
      cursor?: string,
    ): Promise<ArchitectureRead> => {
      calls.push(`${target.roomId}:${cursor ?? "start"}`);
      const bytes = Buffer.from(JSON.stringify(record));
      // Split inside a multi-byte character, not between JS characters.
      const split = bytes.indexOf(Buffer.from("🧭")) + 1;
      const start = cursor ? split : 0;
      return {
        format: "architecture-public-v1",
        state: "ready",
        ref: target,
        snapshotId: target.roomId,
        cut: 3,
        capture: { state: "open", finalCut: null, gaps: [] },
        records: [
          {
            kind: "fragment",
            recordId: record.id,
            encoding: "base64-json",
            byteOffset: start,
            totalBytes: bytes.length,
            data: bytes.subarray(start, cursor ? undefined : split).toString("base64"),
          },
        ],
        next: cursor ? null : "opaque-room-token",
      };
    },
    publicDetail: async (): Promise<never> => {
      throw Error("No room record in this fixture");
    },
  };
}

describe("Architecture passive cut reader", () => {
  it.effect.each(bodyCases)(
    "reads current and historical rooms through the authenticated standalone decoder: %j",
    (example) =>
      Effect.gen(function* () {
        const wire = yield* workerWireFixture(owner);
        const mode = "mode" in example ? { mode: example.mode } : {};
        const records = [
          { ...room, ...mode },
          member,
          {
            ...publication,
            message: { ...publication.message, stage: example.body.kind, body: example.body },
          },
        ];
        wire.policy.reply = (request) =>
          request.method === "architecture.list"
            ? {
                format: "architecture-public-v1",
                state: "ready",
                owner,
                snapshotId: "catalog",
                cut: 2,
                next: null,
                items: [
                  {
                    ref: ref(room.id),
                    director: {
                      instanceId: "director",
                      memberId: "director",
                      assignmentId: "assignment",
                    },
                    lifecycle,
                    ...mode,
                  },
                ],
              }
            : request.method === "architecture.read"
              ? {
                  format: "architecture-public-v1",
                  state: "ready",
                  ref: ref(room.id),
                  snapshotId: "snapshot",
                  cut: 2,
                  capture: { state: "open", finalCut: null, gaps: [] },
                  records,
                  next: null,
                }
              : { state: "unavailable", reason: "not_captured" };
        const catalog = yield* Effect.promise(() => wire.client.listArchitectures());
        assert.equal(catalog.state, "ready");
        if (catalog.state !== "ready") throw Error("Expected catalog");
        if ("mode" in example) assert.equal(catalog.items[0]!.mode, example.mode);
        else assert.notProperty(catalog.items[0], "mode");
        const batches = yield* Effect.promise(() => readArchitectures(wire.client, 7));
        assert.lengthOf(batches, 1);
        assert.deepEqual(batches[0]!.records, records);
        assert.equal(batches[0]!.capture, "open");
        assert.isEmpty(batches[0]!.gaps);
        if (!("mode" in example)) assert.notProperty(batches[0]!.records[0], "mode");
      }).pipe(Effect.scoped),
  );
  it.effect.each(["invalid", null, 3])(
    "rejects malformed present catalog and room mode: %j",
    (mode) =>
      Effect.gen(function* () {
        const wire = yield* workerWireFixture(owner);
        wire.policy.reply = (request) =>
          request.method === "architecture.list"
            ? {
                format: "architecture-public-v1",
                state: "ready",
                owner,
                snapshotId: "catalog",
                cut: 2,
                next: null,
                items: [
                  {
                    ref: ref(room.id),
                    mode,
                    director: {
                      instanceId: "director",
                      memberId: "director",
                      assignmentId: "assignment",
                    },
                    lifecycle,
                  },
                ],
              }
            : {
                format: "architecture-public-v1",
                state: "ready",
                ref: ref(room.id),
                snapshotId: "snapshot",
                cut: 2,
                next: null,
                capture: { state: "open", finalCut: null, gaps: [] },
                records: [{ ...room, mode }],
              };
        yield* Effect.promise(() =>
          expect(wire.client.listArchitectures()).rejects.toMatchObject({ code: "invalid_reply" }),
        );
        // An invalid reply closes its client. A second authenticated client tests the room boundary.
        const next = yield* Effect.promise(() => wire.open());
        yield* Effect.promise(() =>
          expect(next.readArchitecture(ref(room.id))).rejects.toMatchObject({
            code: "invalid_reply",
          }),
        );
        assert.isFalse(isRecord({ ...room, mode }));
      }).pipe(Effect.scoped),
  );
  it.each([
    { ...composition, reason: "" },
    { ...composition, reason: null },
    { ...composition, reason: undefined },
    { ...composition, specialists: [{ candidateRef: "candidate", mandate: "M", reason: "R" }] },
    { kind: "composition", snapshotId: "s", specialists: composition.specialists },
    { kind: "composition", snapshotId: "s", specialists: [] },
    { ...composition, specialists: Array.from({ length: 6 }, () => composition.specialists[0]) },
    {
      ...composition,
      specialists: [{ kind: "retain", candidateRef: "wrong-id", mandate: "M", reason: "R" }],
    },
    { ...composition, specialists: [{ kind: "add", candidateRef: "", mandate: "M", reason: "R" }] },
  ])("does not reinterpret malformed current composition as historical: %j", (body) => {
    assert.isFalse(isPublicArchitectureBody(body));
    assert.throws(() => decodeBody(body));
  });
  it("enumerates catalog before sequential rooms and decodes complete UTF-8 fragments", async () => {
    const source = client();
    const batches = await readArchitectures(source, 7);
    assert.deepEqual(source.calls, [
      "catalog:start",
      "catalog:opaque-catalog-token",
      "first:start",
      "first:opaque-room-token",
      "second:start",
      "second:opaque-room-token",
    ]);
    assert.equal(batches.length, 2);
    for (const batch of batches) {
      assert.deepEqual(batch.records, [record]);
      assert.equal(batch.capture, "open");
      assert.equal(batch.checkpoint.ownerGeneration, 7);
    }
  });
  it.each(["stale_owner", "stale_cursor", "unavailable"] as const)(
    "retains a confirmed record and marks %s incomplete",
    async (state) => {
      const source = client();
      source.readArchitecture = async (target, cursor) =>
        cursor
          ? state === "unavailable"
            ? { state, reason: "not_captured" }
            : { state }
          : {
              format: "architecture-public-v1",
              state: "ready",
              ref: target,
              snapshotId: target.roomId,
              cut: 3,
              capture: { state: "open", finalCut: null, gaps: [] },
              records: [record],
              next: "opaque",
            };
      for (const batch of await readArchitectures(source, 1)) {
        assert.deepEqual(batch.records, [record]);
        assert.equal(batch.capture, "incomplete");
        assert.include(batch.gaps, state);
      }
    },
  );
  it("rejects changed catalog cuts rather than importing an empty roster", async () => {
    const source = client();
    const list = source.listArchitectures;
    source.listArchitectures = async (cursor) => ({
      ...(await list(cursor)),
      cut: cursor ? 99 : 2,
    });
    await expect(readArchitectures(source, 1)).rejects.toThrow("Architecture catalog cut changed");
  });
  it("reports first-page unavailability without inventing room records or a source cut", async () => {
    const source = client();
    source.readArchitecture = async () => ({
      state: "unavailable",
      reason: "snapshot_oversized",
    });
    const batches = await readArchitectures(source, 7);
    assert.equal(batches.length, 2);
    for (const batch of batches) {
      assert.isEmpty(batch.records);
      assert.isUndefined(batch.original);
      assert.equal(batch.capture, "incomplete");
      assert.deepEqual(batch.gaps, ["unavailable"]);
      assert.deepEqual(batch.checkpoint, {
        owner,
        ownerGeneration: 7,
        snapshotId: "",
        cut: 0,
      });
    }
  });
  it.each([false, true])(
    "reports catalog loss without importing partial discoveries, after a page=%s",
    async (afterPage) => {
      const source = client();
      const list = source.listArchitectures;
      const limited = {
        ...source,
        listArchitectures: async (cursor?: string): Promise<ArchitectureCatalog> => {
          if (afterPage && !cursor) return list(cursor);
          source.calls.push(`catalog:${cursor ?? "start"}`);
          return { state: "unavailable", reason: "snapshot_oversized" };
        },
      };
      await expect(readArchitectures(limited, 7)).rejects.toMatchObject({
        catalogGap: "catalog_unavailable:snapshot_oversized",
        owner,
        ownerGeneration: 7,
      });
      assert.deepEqual(source.calls, [
        "catalog:start",
        ...(afterPage ? ["catalog:opaque-catalog-token"] : []),
      ]);
    },
  );
  it.effect.each([false, true])(
    "preserves catalog loss through the authenticated worker port, after a page=%s",
    (afterPage) =>
      Effect.gen(function* () {
        const bridge = new PiWorkerBridge("linux");
        yield* Effect.addFinalizer(() => Effect.sync(() => bridge.cleanupDirectory(true)));
        const wire = yield* workerWireFixture(owner, bridge.directory);
        yield* Effect.addFinalizer(() => bridge.close);
        wire.policy.reply = (request) => {
          if (request.method !== "architecture.list")
            throw Error("An incomplete catalog must not read any room");
          return afterPage && request.params.cursor === undefined
            ? {
                state: "ready",
                format: "architecture-public-v1",
                owner,
                snapshotId: "catalog",
                cut: 5,
                items: [],
                next: "next-catalog-page",
              }
            : { state: "unavailable", reason: "snapshot_oversized" };
        };
        yield* bridge.bind(owner.coordinatorSessionId);
        const loss = yield* bridge.readArchitectures().pipe(Effect.flip);
        if (!isArchitectureCatalogLoss(loss)) throw Error("Catalog loss was normalized away");
        assert.equal(loss.catalogGap, "catalog_unavailable:snapshot_oversized");
        assert.deepEqual(loss.owner, owner);
        assert.equal(loss.ownerGeneration, bridge.ownerGeneration);
      }).pipe(Effect.scoped),
  );
  it("degrades without the complete optional capability group", async () => {
    const source = client();
    source.capabilities.pop();
    assert.deepEqual(await readArchitectures(source, 1), []);
    assert.isEmpty(source.calls);
  });
});
