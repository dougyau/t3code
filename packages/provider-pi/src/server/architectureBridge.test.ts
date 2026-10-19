import { assert, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ProviderArchitectureCatalogLoss } from "@t3tools/provider-core/server/ProviderAdapter";
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

const owner = { processId: "process", coordinatorSessionId: "coordinator", epoch: "epoch" };
const isArchitectureCatalogLoss = Schema.is(ProviderArchitectureCatalogLoss);
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
