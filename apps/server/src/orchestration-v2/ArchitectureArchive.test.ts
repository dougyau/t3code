import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
  type PiArchitectureReadResult,
  type PiArchitectureRecord,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as SqlClient from "effect/sql/SqlClient";
import * as ArchitectureArchive from "./ArchitectureArchive.ts";
import * as ArchitectureQuery from "./ArchitectureQuery.ts";
import {
  nativeSubagentFixture,
  nativeWorkerFixture,
} from "../../../../packages/client-runtime/src/testing/nativeWorker.ts";

it.effect.each([undefined, "table", "consultation"] as const)(
  "reads archived history after closing and reopening the database without a native source, mode=%s",
  (mode) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-architecture-history-" });
      const threadId = ThreadId.make("coordinator");
      const lifecycle = {
        stage: "waiting",
        round: 1,
        protocolState: "running",
        outcome: "pending",
        closure: { state: "pending", members: "pending", director: "pending" },
      } as const;
      const archive: ArchitectureArchive.ArchitectureArchive = {
        summary: {
          tableId: "table",
          threadId,
          providerInstanceId: ProviderInstanceId.make("pi"),
          coordinatorSessionId: "native",
          roomId: "room",
          ...(mode === undefined ? {} : { mode }),
          participants: 0,
          lifecycle,
          capture: "open",
          observation: "stale",
          gaps: [],
        },
        checkpoint: {
          owner: { processId: "gone", epoch: "gone", coordinatorSessionId: "native" },
          ownerGeneration: 3,
          snapshotId: "snapshot",
          cut: 42,
        },
        providerSessionId: ProviderSessionId.make("session"),
        parentProviderThreadId: ProviderThreadId.make("parent"),
        original: {
          id: "original",
          text: "  # exacto\n🧭 café 中文\n",
          source: "original WorkBrief",
          complete: true,
        },
        records: [
          {
            kind: "room",
            id: "room",
            ...(mode === undefined ? {} : { mode }),
            origin: {
              coordinatorSessionId: "native",
              tool: "architecture_start",
              toolCallId: "start",
              instanceId: "director",
              memberId: "director",
              assignmentId: "assignment",
            },
            goalSource: { kind: "worker-workbrief", instanceId: "director" },
            lifecycle,
            pendingDoubtIds: [],
          },
          {
            kind: "publication",
            id: "composition",
            message: {
              id: "composition",
              ordinal: 1,
              roomId: "room",
              memberId: "director",
              assignmentId: "assignment",
              author: "director",
              stage: "composition",
              snapshotId: "snapshot",
              turnId: "turn",
              attemptId: "attempt",
              native: { sessionId: "session", toolCallId: "submit", persistence: "unconfirmed" },
              doubts: [],
              body: {
                kind: "composition",
                snapshotId: "snapshot",
                reason: "The director can answer independently",
                specialists: [],
              },
            },
          },
        ],
      };
      const layer = () => NodeSqliteClient.layer({ filename: `${directory}/history.sqlite` });
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* ArchitectureArchive.setup(sql);
          yield* ArchitectureArchive.write(sql, archive);
        }).pipe(Effect.provide(layer())),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const loaded = yield* ArchitectureArchive.read(sql, threadId);
          assert.deepEqual(loaded, [archive]);
          const details = yield* ArchitectureQuery.read(
            { threadId, kind: "records", tableId: "table" },
            loaded,
            [],
          );
          if (details.kind !== "records") throw Error("Expected records");
          if (mode === undefined) {
            assert.notProperty(details.summary, "mode");
            assert.notProperty(details.items[0], "mode");
          } else {
            assert.equal(details.summary.mode, mode);
            assert.deepInclude(details.items[0], { mode });
          }
          const composition = yield* ArchitectureQuery.read(
            { threadId, kind: "text", tableId: "table", textId: "publication:composition" },
            loaded,
            [],
          );
          if (composition.kind !== "text") throw Error("Expected composition");
          assert.deepEqual(JSON.parse(composition.text).body, {
            kind: "composition",
            snapshotId: "snapshot",
            reason: "The director can answer independently",
            specialists: [],
          });
          const result = yield* ArchitectureQuery.read(
            { threadId, kind: "text", tableId: "table", textId: "original" },
            loaded,
            [],
          );
          assert.equal(result.kind, "text");
          if (result.kind === "text") {
            assert.equal(result.text, archive.original!.text);
            assert.isTrue(result.complete);
          }
          const tables = yield* ArchitectureQuery.read({ threadId, kind: "tables" }, loaded, []);
          if (tables.kind === "tables") {
            assert.equal(tables.items[0]?.observation, "stale");
            assert.equal(tables.items[0]?.lifecycle.protocolState, "running");
          }
          assert.isEmpty(
            yield* ArchitectureArchive.read(sql, ThreadId.make("another-coordinator")),
          );
          const member = {
            kind: "member",
            id: "specialist",
            assignmentId: "task",
            instanceId: "instance",
            roleId: "architect",
            function: "specialist",
            ordinal: 1,
            admittedAt: 1,
            execution: "completed",
            mandate: { source: "TeamAssignment.task", text: "Public mandate" },
            conversationRefs: [],
          } satisfies PiArchitectureRecord;
          let current = {
            ...archive,
            records: [
              member,
              ...Array.from({ length: 44 }, (_, i): PiArchitectureRecord => ({
                kind: "failure",
                id: `failure-${i}`,
                phase: "turn",
                error: "Captured failure",
              })),
            ],
            original: {
              ...archive.original!,
              text: "x".repeat(4095) + "🧭 café 中文\n".repeat(1400),
            },
          };
          const worker = nativeWorkerFixture({
            launch: {
              ...nativeWorkerFixture().launch,
              providerInstanceId: archive.summary.providerInstanceId,
              coordinatorSessionId: archive.summary.coordinatorSessionId,
              instanceId: "instance",
            },
            selected: { model: "selected", requestedModel: "requested" },
            activated: { attemptId: "attempt", model: "activated", reasoning: "high" },
          });
          let workers = [nativeSubagentFixture({ nativeWorker: worker })];
          const save = () => ArchitectureArchive.write(sql, current);
          const read = (input: Parameters<typeof ArchitectureQuery.read>[0]) =>
            ArchitectureArchive.read(sql, threadId).pipe(
              Effect.flatMap((rows) => ArchitectureQuery.read(input, rows, workers)),
            );
          const recapture = () => {
            current = {
              ...current,
              checkpoint: {
                ...current.checkpoint,
                cut: current.checkpoint.cut + 1,
                snapshotId: `recapture-${current.checkpoint.cut + 1}`,
                owner: { ...current.checkpoint.owner, epoch: `epoch-${current.checkpoint.cut}` },
              },
            };
            return save();
          };
          yield* save();
          const input = { kind: "records", threadId, tableId: "table" } as const;
          const first = yield* read(input);
          assert.equal(first.kind, "records");
          if (first.kind !== "records" || !first.next) return yield* Effect.die("Expected pages");
          const ids = first.items.map((item) => item.id);
          let next: string | null = first.next;
          while (next) {
            yield* recapture();
            const result: PiArchitectureReadResult = yield* read({ ...input, cursor: next });
            if (result.kind !== "records") return yield* Effect.die("Expected records");
            ids.push(...result.items.map((item) => item.id));
            next = result.next;
          }
          assert.deepEqual(ids, [
            "specialist",
            ...Array.from({ length: 44 }, (_, i) => `failure-${i}`),
          ]);
          const textInput = {
            kind: "text",
            threadId,
            tableId: "table",
            textId: "original",
          } as const;
          const start = yield* read(textInput);
          if (start.kind !== "text" || start.nextOffset === null)
            return yield* Effect.die("Expected text pages");
          assert.equal(start.text.length, 4095);
          let literal = start.text;
          let offset: number | null = start.nextOffset;
          while (offset !== null) {
            yield* recapture();
            const result: PiArchitectureReadResult = yield* read({
              ...textInput,
              offset,
              revision: start.revision,
            });
            if (result.kind !== "text") return yield* Effect.die("Expected text");
            literal += result.text;
            offset = result.nextOffset;
          }
          assert.equal(literal, current.original.text);
          const continuation = { ...textInput, offset: start.nextOffset, revision: start.revision };
          const reject = (query: Parameters<typeof read>[0]) =>
            read(query).pipe(
              Effect.flip,
              Effect.tap((error) =>
                Effect.sync(() => assert.equal(error._tag, "ArchitectureQueryError")),
              ),
            );
          // A different text resource, table, thread or section cannot reuse the token.
          yield* reject({ ...continuation, textId: "member:specialist" });
          yield* reject({ ...input, cursor: `${start.revision}:20` });
          yield* reject({ kind: "tables", threadId, cursor: first.next });
          yield* ArchitectureArchive.write(sql, {
            ...current,
            summary: { ...current.summary, tableId: "other" },
          });
          yield* reject({ ...input, tableId: "other", cursor: first.next });
          yield* reject({ ...continuation, tableId: "other" });
          yield* reject({ ...continuation, threadId: ThreadId.make("elsewhere") });
          yield* reject({ ...input, cursor: first.next.replace(/:\d+$/, ":99999") });
          yield* reject({ ...textInput, offset: 1 });
          // Unrelated content does not pin an original-text revision.
          current = {
            ...current,
            records: [
              ...current.records,
              { kind: "failure", id: "new", phase: "turn", error: "new" },
            ],
          };
          yield* save();
          yield* reject({ ...input, cursor: first.next });
          yield* read(continuation);
          current = {
            ...current,
            original: { ...current.original, text: `${current.original.text}changed` },
          };
          yield* save();
          yield* reject(continuation);
          current = {
            ...current,
            original: {
              ...current.original,
              text: literal,
              source: "invocation task",
              complete: false,
            },
          };
          yield* save();
          yield* reject(continuation);
          const beforeModel = yield* read(input);
          if (beforeModel.kind !== "records" || !beforeModel.next)
            return yield* Effect.die("Expected pages");
          const beforeMember = beforeModel.items[0];
          if (beforeMember?.kind !== "member") return yield* Effect.die("Expected member");
          workers = [
            nativeSubagentFixture({
              nativeWorker: {
                ...worker,
                activated: { attemptId: "next", model: "new-activation", reasoning: "low" },
              },
            }),
          ];
          yield* reject({ ...input, cursor: beforeModel.next });
          const afterModel = yield* read(input);
          if (afterModel.kind !== "records") return yield* Effect.die("Expected records");
          assert.deepEqual(afterModel.items[0], {
            ...beforeMember,
            model: { selected: "selected", activated: "new-activation" },
          });
          const beforeCapture = afterModel.next!;
          current = {
            ...current,
            summary: { ...current.summary, capture: "incomplete", gaps: ["missed interval"] },
          };
          yield* save();
          yield* reject({ ...input, cursor: beforeCapture });
        }).pipe(Effect.provide(layer())),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
