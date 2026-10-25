import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import { ChildProcessSpawner } from "effect/process";

import * as PiAdapterTestkit from "../Adapters/PiAdapterV2.testkit.ts";
import { PiOrchestratorReplayHarness } from "../Adapters/PiAdapterV2.testkit.ts";
import * as EffectWorker from "../EffectWorker.ts";
import * as Orchestrator from "../Orchestrator.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./ProviderReplayHarness.ts";
import { itRunsReplayFixtures, runReplayFixture } from "./ProviderReplayFixtures.ts";

// JSONL transport only: the adapter, run subscriptions and SQLite ingestor remain real.
const makeForegroundTransport = Effect.gen(function* () {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const prompts = yield* Queue.unbounded<void>();
  const modelPrompts: string[] = [];
  const emit = (record: unknown) =>
    Queue.offer(stdout, new TextEncoder().encode(`${JSON.stringify(record)}\n`)).pipe(
      Effect.asVoid,
    );
  let buffer = "";
  const spawner = ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(999_999_999),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((chunk: Uint8Array) =>
          Effect.gen(function* () {
            buffer += new TextDecoder().decode(chunk);
            while (buffer.includes("\n")) {
              const newline = buffer.indexOf("\n");
              const request = JSON.parse(buffer.slice(0, newline)) as Record<string, unknown>;
              buffer = buffer.slice(newline + 1);
              if (request.type === "prompt") {
                modelPrompts.push(String(request.message));
                yield* Queue.offer(prompts, undefined);
              }
              if (typeof request.id !== "string") continue;
              yield* emit({
                type: "response",
                id: request.id,
                command: request.type,
                success: true,
                data:
                  request.type === "get_state"
                    ? {
                        thinkingLevel: "high",
                        isStreaming: false,
                        isCompacting: false,
                        steeringMode: "one-at-a-time",
                        followUpMode: "one-at-a-time",
                        sessionFile: "/fake/foreground.jsonl",
                        sessionId: "00000000-0000-4000-8000-000000000002",
                        autoCompactionEnabled: true,
                        messageCount: 0,
                        pendingMessageCount: 0,
                      }
                    : request.type === "get_available_models"
                      ? { models: [] }
                      : request.type === "get_entries"
                        ? { entries: [], leafId: null }
                        : request.type === "get_messages"
                          ? { messages: [] }
                          : {},
              });
            }
          }),
        ),
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    ),
  );
  return { emit, prompts, spawner, modelPrompts, loseTransport: Queue.end(stdout) };
});

it.effect.each(["handoff", "cancel-wake", "lose-wake", "empty"] as const)(
  "persists retained foreground artifacts on R1 before its delivery fence releases after R2 owns the thread, %s",
  (disposition) =>
    Effect.gen(function* () {
      const transport = yield* makeForegroundTransport;
      const offers =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const transportEnded = yield* Deferred.make<void>();
      const ownerLost = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      const sourceReleased = yield* Deferred.make<void>();
      const secondPersisted = yield* Deferred.make<void>();
      let expectedOutput = "final retained output";
      const starts = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2TurnInput>();
      const rootEvents: ProviderAdapter.ProviderAdapterV2Event[] = [];
      let original: ProviderAdapter.ProviderAdapterV2TurnInput | undefined;
      let terminalSent = false;
      let acceptedFence = false;
      let artifactsPersistedBeforeFence = false;
      let rejectedAcksRetainedDelivery = true;
      let readChild: (() => Effect.Effect<boolean>) | undefined;
      let readLaunch: (() => Effect.Effect<boolean>) | undefined;
      let originalLaunch:
        | Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn_item.updated" }>
        | undefined;
      let session: ProviderAdapter.ProviderAdapterV2SessionRuntime | undefined;
      const registryLayer = Layer.effect(
        ProviderAdapterRegistry.ProviderAdapterRegistryV2,
        Effect.gen(function* () {
          const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistryV2;
          return {
            ...registry,
            get: (instanceId: ProviderInstanceId) =>
              registry.get(instanceId).pipe(
                Effect.map((adapter) => ({
                  ...adapter,
                  openSession: (input: ProviderAdapter.ProviderAdapterV2OpenSessionInput) =>
                    adapter.openSession(input).pipe(
                      Effect.map((runtime) => {
                        session = runtime;
                        return {
                          ...runtime,
                          startTurn: (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
                            runtime.startTurn(turn).pipe(
                              Effect.tap(() => {
                                original ??= turn;
                                return Queue.offer(starts, turn);
                              }),
                            ),
                          events: runtime.events.pipe(
                            Stream.tap((event) =>
                              Effect.sync(() => {
                                rootEvents.push(event);
                                if (
                                  disposition === "empty" &&
                                  originalLaunch === undefined &&
                                  event.type === "turn_item.updated" &&
                                  event.turnItem.type === "dynamic_tool" &&
                                  event.turnItem.status === "completed"
                                )
                                  originalLaunch = event;
                              }),
                            ),
                            Stream.ensuring(Deferred.succeed(transportEnded, undefined)),
                          ),
                          hasPendingEventDeliveryForRun: (runId: RunId) =>
                            Effect.gen(function* () {
                              const pending = yield* runtime.hasPendingEventDeliveryForRun!(runId);
                              if (terminalSent && runId === original?.runId && !pending)
                                yield* Deferred.succeed(released, undefined);
                              return pending;
                            }),
                          hasOpenEventSourceForRun: (runId: RunId) =>
                            Effect.gen(function* () {
                              const open = yield* (
                                runtime.hasOpenEventSourceForRun?.(runId) ?? Effect.succeed(false)
                              );
                              if (
                                terminalSent &&
                                runId === original?.runId &&
                                !open &&
                                !(yield* runtime.hasPendingEventDeliveryForRun!(runId))
                              )
                                yield* Deferred.succeed(sourceReleased, undefined);
                              return open;
                            }),
                          acknowledgeEvent: (
                            event: ProviderAdapter.ProviderAdapterV2Event,
                            runId: RunId,
                          ) =>
                            Effect.gen(function* () {
                              if (
                                runId === original?.runId &&
                                event.type === "provider_thread.updated" &&
                                event.providerThread.id === original.providerThread.id &&
                                event.providerThread.lastRunOrdinal === 2
                              )
                                yield* Deferred.succeed(ownerLost, undefined);
                              if (
                                terminalSent &&
                                runId === original?.runId &&
                                ((event.type === "node.updated" &&
                                  event.node.threadId !== original.threadId &&
                                  event.node.status === "completed") ||
                                  (disposition === "empty" &&
                                    event.type === "turn_item.updated" &&
                                    event !== originalLaunch &&
                                    event.turnItem.id === originalLaunch?.turnItem.id)) &&
                                (yield* runtime.hasPendingEventDeliveryForRun!(runId))
                              ) {
                                // Reads occur inside the real accepted-persistence ACK, before releasing the adapter fence.
                                const persistedBeforeAck = yield* (
                                  (disposition === "empty" ? readLaunch?.() : readChild?.()) ??
                                    Effect.succeed(false)
                                );
                                yield* runtime.acknowledgeEvent!(event, RunId.make("foreign"));
                                yield* runtime.acknowledgeEvent!({ ...event }, runId);
                                rejectedAcksRetainedDelivery &&=
                                  yield* runtime.hasPendingEventDeliveryForRun!(runId);
                                yield* runtime.acknowledgeEvent!(event, runId);
                                if (!(yield* runtime.hasPendingEventDeliveryForRun!(runId))) {
                                  artifactsPersistedBeforeFence = persistedBeforeAck;
                                  acceptedFence = true;
                                  if (expectedOutput === "second valid retained output") {
                                    assert.isTrue(
                                      persistedBeforeAck,
                                      "second capture artifacts must commit before its original exact ACK",
                                    );
                                    yield* Deferred.succeed(secondPersisted, undefined);
                                  }
                                }
                                yield* runtime.acknowledgeEvent!(event, runId);
                              } else yield* runtime.acknowledgeEvent!(event, runId);
                            }),
                        };
                      }),
                    ),
                })),
              ),
          };
        }),
      ).pipe(
        Layer.provide(
          PiAdapterTestkit.layer({
            scenario: "foreground-owner-handoff",
            binaryPath: "pi",
            launchArgs: "",
            spawner: Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, transport.spawner),
          }).pipe(
            Layer.provide(
              Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
                offer: (offer) => Queue.offer(offers, offer).pipe(Effect.asVoid),
                take: Effect.never,
              }),
            ),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("pi-foreground-owner-handoff");
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = ThreadId.make("thread:pi-foreground-owner-handoff");
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runCollect,
            Effect.forkScoped,
          );
        const dispatch = (ordinal: number) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`dispatch:${ordinal}`),
            threadId,
            messageId: MessageId.make(`message:${ordinal}`),
            text: `turn ${ordinal}`,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:pi-foreground-owner-handoff"),
          title: "Pi foreground",
          modelSelection: { instanceId: ProviderInstanceId.make("pi"), model: "default" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* dispatch(1);
        yield* worker.drain();
        const r1 = yield* Queue.take(starts);
        yield* Queue.take(transport.prompts);
        yield* transport.emit({
          type: "tool_execution_start",
          toolCallId: "retained-call",
          toolName: "subagent",
          args: {},
        });
        const callback = (status: string, finalOutput: string, summaries = false) => ({
          type: "tool_execution_update",
          toolCallId: "retained-call",
          toolName: "subagent",
          partialResult: {
            details: {
              runId: "retained-native",
              mode: "single",
              results: [
                {
                  index: 0,
                  agent: "scout",
                  task: "inspect",
                  progress: { status },
                  finalOutput,
                  ...(summaries
                    ? {
                        exitCode: 0,
                        toolCalls: [
                          { text: "read final.ts", expandedText: "read final.ts (1-20)" },
                        ],
                      }
                    : {}),
                },
              ],
            },
          },
        });
        if (disposition === "empty") {
          yield* transport.emit({
            type: "tool_execution_end",
            toolCallId: "retained-call",
            toolName: "subagent",
            result: {
              details: {
                mode: "single",
                runId: "empty-native",
                asyncId: "empty-native",
                results: [],
              },
            },
          });
          yield* transport.emit({
            type: "extension_ui_request",
            method: "setWidget",
            widgetKey: "t3-subagents",
            widgetLines: [
              "PI_SUBAGENT_ASYNC_JSON:" +
                JSON.stringify({
                  kind: "pi-subagents.async-status-snapshot",
                  version: 1,
                  runs: [{ id: "empty-native", kind: "subagent", state: "complete", children: [] }],
                }),
            ],
          });
          const settled = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === r1.runId &&
              event.payload.status === "completed",
          );
          yield* transport.emit({ type: "agent_settled" });
          yield* Fiber.join(settled);
          yield* worker.drain();
          yield* dispatch(2);
          yield* worker.drain();
          yield* Queue.take(starts);
          yield* Queue.take(transport.prompts);
          yield* Deferred.await(ownerLost);
          assert.isDefined(originalLaunch);
          readLaunch = () =>
            orchestrator.getThreadProjection(threadId).pipe(
              Effect.orDie,
              Effect.map((projection) => {
                const item = projection.turnItems.find(
                  (item) => item.id === originalLaunch!.turnItem.id,
                );
                return (
                  item?.status === "completed" &&
                  item.runId === r1.runId &&
                  item.type === "dynamic_tool" &&
                  projection.subagents.length === 0
                );
              }),
            );
          assert.isTrue(yield* session!.hasOpenEventSourceForRun!(r1.runId));
          terminalSent = true;
          yield* TestClock.adjust("30 minutes");
          assert.isTrue(
            yield* Deferred.isDone(sourceReleased),
            "Empty original owner must wake its real ingestion consumer without R2 traffic",
          );
          assert.isTrue(acceptedFence);
          assert.isTrue(artifactsPersistedBeforeFence);
          assert.isTrue(rejectedAcksRetainedDelivery);
          const replay = rootEvents
            .filter(
              (event) =>
                event.type === "turn_item.updated" &&
                event.turnItem.id === originalLaunch!.turnItem.id,
            )
            .at(-1);
          assert.notStrictEqual(replay, originalLaunch);
          assert.deepEqual(replay, originalLaunch);
          const parent = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(
            parent.providerThreads.find((thread) => thread.id === r1.providerThread.id)
              ?.lastRunOrdinal,
            2,
          );
          assert.deepEqual(parent.subagents, []);
          assert.isFalse(yield* session!.hasOpenEventSourceForRun!(r1.runId));
          assert.isFalse(yield* session!.hasPendingEventDeliveryForRun!(r1.runId));
          assert.deepEqual(transport.modelPrompts, ["turn 1", "turn 2"]);
          assert.equal(rootEvents.filter((event) => event.type === "turn.terminal").length, 1);
          return;
        }
        const childStarted = yield* watch(
          (event) => event.type === "subagent.updated" && event.payload.status === "running",
        );
        yield* transport.emit(callback("running", "initial progress"));
        yield* Fiber.join(childStarted);
        yield* transport.emit({
          type: "tool_execution_end",
          toolCallId: "retained-call",
          toolName: "subagent",
          result: {
            details: {
              runId: "retained-native",
              mode: "single",
              results: [
                {
                  index: 0,
                  agent: "scout",
                  task: "inspect",
                  detached: true,
                  exitCode: -2,
                  progress: { status: "detached" },
                  finalOutput: "Detached for intercom coordination before task completion.",
                },
              ],
            },
          },
        });
        const settled = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === r1.runId &&
            event.payload.status === "completed",
        );
        yield* transport.emit({ type: "agent_settled" });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* dispatch(2);
        yield* worker.drain();
        const r2 = yield* Queue.take(starts);
        yield* Queue.take(transport.prompts);
        yield* Deferred.await(ownerLost);
        yield* transport.emit({
          ...callback("completed", "foreign call", true),
          toolCallId: "foreign-call",
        });
        const foreignRun = callback("completed", "foreign run", true);
        yield* transport.emit({
          ...foreignRun,
          partialResult: {
            details: {
              ...foreignRun.partialResult.details,
              runId: "foreign-native",
            },
          },
        });
        const continued = yield* watch(
          (event) =>
            event.type === "subagent.updated" && event.payload.progress === "R1 after owner loss",
        );
        yield* transport.emit(callback("running", "R1 after owner loss"));
        yield* Fiber.join(continued);
        const parent = yield* orchestrator.getThreadProjection(threadId);
        const child = parent.subagents.find((child) => child.runId === r1.runId)!;
        assert.isDefined(child);
        assert.equal(
          parent.providerThreads.find((thread) => thread.id === r1.providerThread.id)
            ?.lastRunOrdinal,
          2,
        );
        assert.equal(child.status, "running");
        readChild = () =>
          orchestrator.getThreadProjection(child.childThreadId!).pipe(
            Effect.orDie,
            Effect.map(
              (projection) =>
                projection.turnItems.some(
                  (item) =>
                    item.type === "system_notice" && item.message.includes("read final.ts (1-20)"),
                ) &&
                projection.messages.some(
                  (message) => message.role === "assistant" && message.text === expectedOutput,
                ) &&
                projection.turnItems.some(
                  (item) => item.type === "assistant_message" && item.text === expectedOutput,
                ),
            ),
          );
        let wake: ProviderContinuationRequests.ProviderContinuationRequest | undefined;
        if (disposition !== "handoff") {
          const r2Settled = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === r2.runId &&
              event.payload.status === "completed",
          );
          yield* transport.emit({ type: "agent_settled" });
          yield* Fiber.join(r2Settled);
          yield* transport.emit({ type: "agent_start" });
          wake = yield* Queue.take(offers);
          yield* transport.emit({ type: "message_start", message: { role: "assistant" } });
          yield* transport.emit({
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", delta: "unadmitted wake output" },
          });
        }
        terminalSent = true;
        yield* transport.emit(callback("completed", "final retained output", true));
        yield* Deferred.await(released);
        const persisted = yield* orchestrator.getThreadProjection(child.childThreadId!);
        assert.isTrue(
          persisted.messages.some(
            (message) => message.role === "assistant" && message.text === "final retained output",
          ),
          "final assistant must persist before R1 releases",
        );
        assert.isTrue(
          persisted.turnItems.some(
            (item) => item.type === "assistant_message" && item.text === "final retained output",
          ),
        );
        assert.isTrue(
          persisted.turnItems.some(
            (item) =>
              item.type === "system_notice" && item.message.includes("read final.ts (1-20)"),
          ),
          "compact tool summary must persist before R1 releases",
        );
        const finalParent = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          finalParent.subagents.map((subagent) => [
            subagent.id,
            subagent.runId,
            subagent.status,
            subagent.result,
          ]),
          [[child.id, r1.runId, "completed", "final retained output"]],
        );
        assert.equal(finalParent.runs.length, 2);
        assert.isFalse(persisted.turnItems.some((item) => item.runId === r2.runId));
        assert.deepEqual(transport.modelPrompts, ["turn 1", "turn 2"]);
        assert.isTrue(
          acceptedFence,
          "release must follow the exact accepted original-run fence ACK",
        );
        assert.isTrue(
          artifactsPersistedBeforeFence,
          "all final artifacts must already be in SQLite before the releasing ACK",
        );
        assert.isTrue(
          rejectedAcksRetainedDelivery,
          "foreign-run and cloned ACKs must not release delivery",
        );
        assert.isFalse(yield* session!.hasPendingEventDeliveryForRun!(r1.runId));
        assert.isDefined(
          session!.hasOpenEventSourceForRun,
          "Admitted original history needs its original persistence consumer",
        );
        assert.isTrue(yield* session!.hasOpenEventSourceForRun!(r1.runId));
        expectedOutput = "second valid retained output";
        yield* transport.emit(callback("completed", expectedOutput, true));
        yield* Deferred.await(secondPersisted);
        const second = yield* orchestrator.getThreadProjection(child.childThreadId!);
        assert.isTrue(
          second.messages.some((message) => message.text === "second valid retained output"),
        );
        assert.isTrue(
          second.turnItems.some(
            (item) =>
              item.type === "assistant_message" && item.text === "second valid retained output",
          ),
        );
        if (wake !== undefined) {
          if (disposition === "cancel-wake") yield* wake.clearIfCurrent!();
          else yield* transport.loseTransport;
          yield* Deferred.await(transportEnded);
          let admitted = false;
          yield* wake.dispatchIfCurrent!(
            Effect.sync(() => {
              admitted = true;
            }),
          );
          assert.isFalse(admitted);
          const retainedParent = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(retainedParent.runs.length, 2);
          assert.equal(
            retainedParent.subagents.find((subagent) => subagent.id === child.id)?.status,
            "completed",
          );
          assert.equal(
            retainedParent.subagents.find((subagent) => subagent.id === child.id)?.result,
            expectedOutput,
          );
          assert.isFalse(
            retainedParent.messages.some((message) =>
              message.text.includes("unadmitted wake output"),
            ),
          );
          assert.isFalse(yield* session!.hasPendingEventDeliveryForRun!(r1.runId));
          assert.deepEqual(transport.modelPrompts, ["turn 1", "turn 2"]);
          return;
        }
        yield* TestClock.adjust("30 minutes");
        yield* Deferred.await(sourceReleased);
        assert.isFalse(yield* session!.hasOpenEventSourceForRun!(r1.runId));
        assert.isFalse(yield* session!.hasPendingEventDeliveryForRun!(r1.runId));
        yield* transport.emit(callback("completed", "after admission closed", true));
        const r2Barrier = yield* watch(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.type === "system_notice" &&
            event.payload.message === "after closed barrier",
        );
        yield* transport.emit({
          type: "extension_ui_request",
          method: "notify",
          message: "after closed barrier",
        });
        yield* Fiber.join(r2Barrier);
        const afterClosed = yield* orchestrator.getThreadProjection(child.childThreadId!);
        assert.isFalse(
          afterClosed.messages.some((message) => message.text === "after admission closed"),
        );
        assert.isFalse(yield* session!.hasPendingEventDeliveryForRun!(r1.runId));
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).providerThreads.find(
            (thread) => thread.id === r1.providerThread.id,
          )?.lastRunOrdinal,
          2,
        );
        assert.notEqual(r1.runId, r2.runId);
        assert.isFalse(
          rootEvents.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.runId === r2.runId &&
              event.turnItem.type === "dynamic_tool",
          ),
        );
        assert.equal(rootEvents.filter((event) => event.type === "turn.terminal").length, 1);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "foreground-owner-handoff" },
            registryLayer,
          ),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

describe("Pi replay fixtures", () => {
  itRunsReplayFixtures("pi", (input) =>
    runReplayFixture({ ...input, harness: PiOrchestratorReplayHarness }),
  );
});
