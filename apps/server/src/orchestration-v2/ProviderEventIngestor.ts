import * as NodeCrypto from "@effect/platform-node-shared/NodeCrypto";
import * as NodeUtil from "node:util";
import {
  NodeId,
  CommandId,
  OrchestrationV2DomainEvent,
  OrchestrationV2StoredEvent,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2Run,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
  type ModelSelection,
  type RuntimeMode,
  type ProviderInteractionMode,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RawEventId,
  RunAttemptId,
  RunId,
  ThreadId,
  MessageId,
  TurnItemId,
  type PiNativeWorkerCheckpoint,
  type PiNativeWorkerSummary,
  type PiArchitectureSummary,
  ProviderDriverKind,
  type PiChecklistCapture,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { AgentScope } from "@t3tools/shared/AgentScope";
import { isPiChecklistPlan } from "@t3tools/shared/checklist";
import { normalizePiTodoSelection } from "@t3tools/shared/piTodo";
import { getModelSelectionStringOptionValue, modelSelectionsEqual } from "@t3tools/shared/model";
import * as AnalyticsService from "../telemetry/AnalyticsService.ts";
import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderFailureTurnItem } from "@t3tools/provider-core/server/failure";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import { stripUnservedToolOutputImageBytes } from "./toolOutputImageBytes.ts";
import {
  makeSubagentChildThread,
  makeSubagentConversationArtifacts,
} from "@t3tools/provider-core/server/subagentProjection";

const encodeNativeWorkerIdentity = Schema.encodeSync(
  Schema.fromJsonString(Schema.Tuple([ProviderInstanceId, Schema.String, Schema.String])),
);
const decodeNativeTodoSnapshot = Schema.decodeUnknownOption(
  Schema.Struct({
    format: Schema.Literal("rpiv-todo-v1"),
    tasks: Schema.Array(
      Schema.Struct({
        id: Schema.Number,
        identityEntryId: Schema.String,
        subject: Schema.String,
        status: Schema.Literals(["pending", "in_progress", "completed", "deleted"]),
      }),
    ),
  }),
);

export class ProviderEventNormalizeError extends Schema.TaggedError<ProviderEventNormalizeError>()(
  "ProviderEventNormalizeError",
  {
    providerSessionId: ProviderSessionId,
    threadId: ThreadId,
    providerEvent: ProviderAdapter.ProviderAdapterV2Event,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to normalize provider event ${this.providerEvent.type} for thread ${this.threadId}.`;
  }
}

export class ProviderEventPublishError extends Schema.TaggedError<ProviderEventPublishError>()(
  "ProviderEventPublishError",
  {
    providerSessionId: ProviderSessionId,
    eventCount: Schema.Number,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to publish ${this.eventCount} normalized provider event(s).`;
  }
}

export const ProviderEventIngestorV2Error = Schema.Union([
  ProviderEventNormalizeError,
  ProviderEventPublishError,
]);
export type ProviderEventIngestorV2Error = typeof ProviderEventIngestorV2Error.Type;

export interface ProviderTurnAnalyticsContext {
  readonly modelSelection: ModelSelection;
  readonly runtimeMode?: RuntimeMode;
  readonly interactionMode?: ProviderInteractionMode;
}

export class ProviderTurnAnalytics extends Context.Reference<{
  readonly record: (properties: Readonly<Record<string, unknown>>) => Effect.Effect<void>;
}>("t3/orchestration-v2/ProviderTurnAnalytics", {
  defaultValue: () => ({ record: () => Effect.void }),
}) {}

export const layerAnalytics = Layer.effect(
  ProviderTurnAnalytics,
  Effect.gen(function* () {
    const analytics = yield* AnalyticsService.AnalyticsService;
    return {
      record: (properties: Readonly<Record<string, unknown>>) =>
        analytics.record("provider.turn.completed", properties),
    };
  }),
);

function providerTurnAnalyticsProperties(input: {
  readonly driver: ProviderAdapter.ProviderAdapterV2Event["driver"];
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly context?: ProviderTurnAnalyticsContext;
}): Readonly<Record<string, unknown>> {
  const usage = input.providerTurn.turnTokenUsage;
  const modelSelection = input.context?.modelSelection;
  const effort = modelSelection
    ? (getModelSelectionStringOptionValue(modelSelection, "reasoningEffort") ??
      getModelSelectionStringOptionValue(modelSelection, "effort"))
    : undefined;
  return {
    provider: input.driver,
    terminalStatus: input.providerTurn.status,
    usageStatus: usage?.usageStatus ?? "unavailable",
    usageScope: usage?.usageScope ?? "main_agent",
    ...(usage ? { hasSubagents: usage.hasSubagents } : {}),
    ...(usage?.inputTokens === undefined ? {} : { inputTokens: usage.inputTokens }),
    ...(usage?.cachedInputTokens === undefined
      ? {}
      : { cachedInputTokens: usage.cachedInputTokens }),
    ...(usage?.cacheCreationTokens === undefined
      ? {}
      : { cacheCreationTokens: usage.cacheCreationTokens }),
    ...(usage?.outputTokens === undefined ? {} : { outputTokens: usage.outputTokens }),
    ...(usage?.reasoningTokens === undefined ? {} : { reasoningTokens: usage.reasoningTokens }),
    ...(modelSelection ? { model: modelSelection.model, mixedModels: false } : {}),
    ...(effort ? { effort } : {}),
    ...(input.context?.runtimeMode ? { runtimeMode: input.context.runtimeMode } : {}),
    ...(input.context?.interactionMode ? { interactionMode: input.context.interactionMode } : {}),
    ...(input.providerTurn.startedAt && input.providerTurn.completedAt
      ? {
          durationMs: Math.max(
            0,
            DateTime.toEpochMillis(input.providerTurn.completedAt) -
              DateTime.toEpochMillis(input.providerTurn.startedAt),
          ),
        }
      : {}),
  };
}

type TodoListPlan = Extract<OrchestrationV2PlanArtifact, { readonly kind: "todo_list" }>;

function withPlanStepDurations(
  plan: TodoListPlan,
  previous: TodoListPlan | undefined,
  occurredAt: DateTime.Utc,
): TodoListPlan {
  const occurredAtIso = DateTime.formatIso(occurredAt);
  const occurredAtMs = DateTime.toEpochMillis(occurredAt);
  const previousById = new Map(previous?.steps.map((step) => [step.id, step]));
  // Provider step IDs may be positional. Changed text must not inherit another task's timing.
  const previousStep = (step: TodoListPlan["steps"][number]) => {
    const prior = previousById.get(step.id);
    return prior?.text === step.text ? prior : undefined;
  };
  const hasNewCompletion = plan.steps.some((step) => {
    const prior = previousStep(step);
    return step.status === "completed" && prior?.status !== "completed";
  });
  let fallbackCompletionConsumed = false;

  return {
    ...plan,
    steps: plan.steps.map((step) => {
      const prior = previousStep(step);
      const baseStep = { id: step.id, text: step.text, status: step.status };
      if (step.status === "completed") {
        if (prior?.status === "completed") {
          return {
            ...baseStep,
            ...(prior.durationMs === undefined ? {} : { durationMs: prior.durationMs }),
          };
        }
        const durationAnchorAt =
          prior?.status === "running" || !fallbackCompletionConsumed
            ? prior?.durationAnchorAt
            : occurredAtIso;
        fallbackCompletionConsumed = true;
        const anchorMs =
          durationAnchorAt === undefined ? occurredAtMs : Date.parse(durationAnchorAt);
        const durationMs = Number.isFinite(anchorMs) ? Math.max(0, occurredAtMs - anchorMs) : 0;
        return {
          ...baseStep,
          ...(durationMs > 0 ? { durationMs } : {}),
        };
      }
      if (step.status === "running") {
        return {
          ...baseStep,
          durationAnchorAt:
            prior?.status === "running" ? (prior.durationAnchorAt ?? occurredAtIso) : occurredAtIso,
        };
      }
      return {
        ...baseStep,
        durationAnchorAt:
          hasNewCompletion || prior?.status !== "pending"
            ? occurredAtIso
            : (prior.durationAnchorAt ?? occurredAtIso),
      };
    }),
  };
}

/** Settled runs that carried conversation to the provider. */
const CONVERSATION_RUN_STATUSES = new Set<OrchestrationV2Run["status"]>([
  "completed",
  "interrupted",
  "failed",
]);

/**
 * Settled runs on a provider thread that a native rewind left off the active
 * branch. A run whose provider turn has a strong native ref is judged by that
 * ref. A run without one cannot be located, so it falls with everything after
 * the last run still on the branch.
 */
function runsOffNativeBranch(input: {
  readonly projection: Pick<OrchestrationV2ThreadProjection, "runs" | "attempts" | "providerTurns">;
  readonly providerThreadId: ProviderThreadId;
  readonly retainedNativeTurnIds: ReadonlySet<string>;
  readonly reportingRunId: RunId | undefined;
}): ReadonlyArray<OrchestrationV2Run> {
  const { attempts, providerTurns, runs } = input.projection;
  const nativeTurnId = (run: OrchestrationV2Run) => {
    const attempt = attempts.find((candidate) => candidate.id === run.activeAttemptId);
    if (attempt === undefined) return null;
    const ref = providerTurns.find(
      (turn) => turn.id === attempt.providerTurnId || turn.runAttemptId === attempt.id,
    )?.nativeTurnRef;
    return ref?.strength === "strong" ? ref.nativeId : null;
  };
  const settled = runs.flatMap((run) =>
    run.providerThreadId === input.providerThreadId &&
    run.id !== input.reportingRunId &&
    CONVERSATION_RUN_STATUSES.has(run.status)
      ? [{ run, nativeTurnId: nativeTurnId(run) }]
      : [],
  );
  const lastRetainedOrdinal = settled.reduce(
    (last, { run, nativeTurnId }) =>
      nativeTurnId !== null && input.retainedNativeTurnIds.has(nativeTurnId)
        ? Math.max(last, run.ordinal)
        : last,
    0,
  );
  return settled
    .filter(({ run, nativeTurnId }) =>
      nativeTurnId === null
        ? run.ordinal > lastRetainedOrdinal
        : !input.retainedNativeTurnIds.has(nativeTurnId),
    )
    .map(({ run }) => run);
}

export interface ProviderEventIngestInput {
  readonly providerSessionId: ProviderSessionId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly commandId?: CommandId;
  readonly threadId: ThreadId;
  readonly runId?: RunId;
  readonly nodeId?: NodeId;
  readonly rawEventId?: RawEventId;
  readonly event: ProviderAdapter.ProviderAdapterV2Event;
  readonly analyticsContext?: ProviderTurnAnalyticsContext;
  readonly nativeWorkerAdmission?: {
    readonly summary: PiNativeWorkerSummary;
    readonly checkpoint: PiNativeWorkerCheckpoint;
    readonly isConnected?: Effect.Effect<boolean>;
  };
}

// Full Architecture text is paged from the archive, never copied into shell summaries.
const architecturePreview = (text: string) => text.slice(0, 512).replace(/[\uD800-\uDBFF]$/u, "");

export interface ProviderEventIngestorV2Shape {
  readonly ingestArchitecture: (input: {
    readonly threadId: ThreadId;
    readonly providerInstanceId: ProviderInstanceId;
    readonly providerSessionId: ProviderSessionId;
    readonly parentProviderThreadId: ProviderThreadId;
    readonly parentNodeId: NodeId;
    readonly batch: ProviderAdapter.ProviderArchitectureBatch;
    readonly isConnected: Effect.Effect<boolean>;
  }) => ReturnType<EventSink.EventSinkV2Shape["writeArchitecture"]>;
  readonly ingestNativeWorker: (
    batch: ProviderAdapter.ProviderNativeWorkerBatch,
    isConnected?: Effect.Effect<boolean>,
  ) => ReturnType<EventSink.EventSinkV2Shape["writeNativeWorker"]>;
  readonly normalize: (
    input: ProviderEventIngestInput,
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2DomainEvent>, ProviderEventIngestorV2Error>;
  readonly ingestNormalized: (
    input: ProviderEventIngestInput & {
      /**
       * Atomically reject mutable provider state emitted by an attempt that
       * lost ownership while the adapter event was in flight.
       */
      readonly writeIfRunCurrent?: {
        readonly runId: RunId;
        readonly activeAttemptId: RunAttemptId;
        readonly expectedStatus: OrchestrationV2Run["status"];
      };
      /**
       * Atomically reject provider-thread snapshots from an attempt that no
       * longer owns the run or from a run that no longer owns the thread.
       */
      readonly writeIfProviderThreadOwner?: {
        readonly providerThreadId: ProviderThreadId;
        readonly runId: RunId;
        readonly activeAttemptId: RunAttemptId;
        readonly expectedLastRunOrdinal: number;
      };
    },
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2StoredEvent>, ProviderEventIngestorV2Error>;
}

export class ProviderEventIngestorV2 extends Context.Service<
  ProviderEventIngestorV2,
  ProviderEventIngestorV2Shape
>()("t3/orchestration-v2/ProviderEventIngestor/ProviderEventIngestorV2") {}

function compactUndefined<T extends Record<string, unknown>>(record: T): T {
  return Object.fromEntries(Object.entries(record).filter(([, value]) => value !== undefined)) as T;
}

const decodeDomainEvent = Schema.decodeUnknownEffect(OrchestrationV2DomainEvent);

export const layer: Layer.Layer<
  ProviderEventIngestorV2,
  never,
  | EventSink.EventSinkV2
  | IdAllocator.IdAllocatorV2
  | ProjectionStore.ProjectionStoreV2
  | ThreadCommandExecutor.ThreadCommandExecutor
> = Layer.effect(
  ProviderEventIngestorV2,
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const idAllocator = yield* IdAllocator.IdAllocatorV2;
    const threadCommands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
    const analytics = yield* ProviderTurnAnalytics;
    const agentScope = yield* AgentScope;
    const completedTurnAnalytics = new Set<string>();

    const makeDomainEvent = (
      input: Omit<ProviderEventIngestInput, "event"> & {
        readonly event: Pick<ProviderAdapter.ProviderAdapterV2Event, "driver">;
      },
      payloadInput: {
        readonly type: OrchestrationV2DomainEvent["type"];
        readonly payload: OrchestrationV2DomainEvent["payload"];
        readonly threadId?: ThreadId;
        readonly runId?: RunId | null;
        readonly nodeId?: NodeId | null;
        readonly occurredAt?: DateTime.Utc;
      },
    ) =>
      Effect.gen(function* () {
        const threadId = payloadInput.threadId ?? input.threadId;
        const eventId = yield* idAllocator.allocate.event({
          threadId,
          providerSessionId: input.providerSessionId,
        });
        const occurredAt = payloadInput.occurredAt ?? (yield* DateTime.now);
        return yield* decodeDomainEvent(
          compactUndefined({
            id: eventId,
            type: payloadInput.type,
            threadId,
            runId: payloadInput.runId ?? input.runId,
            nodeId: payloadInput.nodeId ?? input.nodeId,
            driver: input.event.driver,
            providerInstanceId: input.providerInstanceId,
            rawEventId: input.rawEventId,
            occurredAt,
            payload: payloadInput.payload,
          }),
        );
      });

    const dismissNativeUserInputs = Effect.fn("ProviderEventIngestor.dismissNativeUserInputs")(
      function* (
        input: ProviderEventIngestInput,
        providerTurnId: ProviderTurnId,
        threadId = input.threadId,
      ) {
        const pending = yield* projections.getPendingNativeUserInputs(threadId, providerTurnId);
        const now = yield* DateTime.now;
        const events: Array<OrchestrationV2DomainEvent> = [];
        for (const request of pending.runtimeRequests) {
          events.push(
            yield* makeDomainEvent(input, {
              type: "runtime-request.updated",
              threadId,
              nodeId: request.nodeId,
              payload: { ...request, status: "cancelled", resolvedAt: now },
            }),
          );
        }
        for (const node of pending.nodes) {
          events.push(
            yield* makeDomainEvent(input, {
              type: "node.updated",
              threadId,
              nodeId: node.id,
              runId: node.runId,
              payload: { ...node, status: "cancelled", completedAt: now },
            }),
          );
        }
        for (const item of pending.turnItems) {
          events.push(
            yield* makeDomainEvent(input, {
              type: "turn-item.updated",
              threadId,
              nodeId: item.nodeId,
              runId: item.runId,
              payload: { ...item, status: "cancelled", completedAt: now, updatedAt: now },
            }),
          );
        }
        return events;
      },
    );

    /**
     * A native subagent's thread starts on the parent's model when the
     * provider names the real one later (a Claude agent file's model arrives
     * with the subagent's first reply). Clients read the thread's model and
     * effort, so move the thread to the reported selection, or to the reported
     * model alone when the provider reports no selection. Thread commands
     * rewrite the whole thread row under the thread's lock, so this read and
     * write take it too.
     */
    const syncSubagentThreadModel = Effect.fn("ProviderEventIngestor.syncSubagentThreadModel")(
      function* (input: ProviderEventIngestInput, subagent: OrchestrationV2Subagent) {
        const { childThreadId, model } = subagent;
        if (
          subagent.origin !== "provider_native" ||
          childThreadId === null ||
          (model === null && subagent.modelSelection === undefined)
        ) {
          return [];
        }
        const reportedSelection = ({ modelSelection }: { modelSelection: ModelSelection }) =>
          subagent.modelSelection ??
          (model === null || model === modelSelection.model
            ? modelSelection
            : // The parent's options belong to the parent's model.
              { instanceId: modelSelection.instanceId, model });
        const staleThread = projections.getThread(childThreadId).pipe(
          Effect.map((thread) =>
            modelSelectionsEqual(thread.modelSelection, reportedSelection(thread)) ? null : thread,
          ),
          Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
        );
        // Nearly every update already matches; only a mismatch takes the lock.
        if ((yield* staleThread) === null) return [];
        return yield* threadCommands.withLock(
          childThreadId,
          Effect.gen(function* () {
            const thread = yield* staleThread;
            if (thread === null) return [];
            const now = yield* DateTime.now;
            const event = yield* makeDomainEvent(input, {
              type: "thread.model-selection-updated",
              threadId: thread.id,
              payload: {
                ...thread,
                modelSelection: reportedSelection(thread),
                updatedAt: now,
              },
              occurredAt: now,
            });
            return yield* eventSink.write({ events: [event] });
          }),
        );
      },
    );

    /**
     * The provider rewound its conversation outside T3. Retire the runs that
     * fell off its active branch with the same run, root-node, and checkpoint
     * updates CheckpointRollbackService writes for a T3 revert. The run
     * reporting the rewind performed it and stays, so it keeps lastRunOrdinal.
     */
    const rollBackRewoundRuns = Effect.fn("ProviderEventIngestor.rollBackRewoundRuns")(function* (
      input: ProviderEventIngestInput,
      rewind: {
        readonly threadId: ThreadId;
        readonly providerThreadId: ProviderThreadId;
        readonly retainedNativeTurnIds: ReadonlySet<string>;
      },
    ) {
      const projection = yield* projections.getThreadRecords(rewind.threadId, [
        "runs",
        "attempts",
        "providerTurns",
        "nodes",
        "checkpoints",
      ]);
      const runs = runsOffNativeBranch({
        projection,
        providerThreadId: rewind.providerThreadId,
        retainedNativeTurnIds: rewind.retainedNativeTurnIds,
        reportingRunId: input.runId,
      });
      if (runs.length === 0) return [];
      const now = yield* DateTime.now;
      const eventId = () =>
        idAllocator.allocate.event({
          threadId: rewind.threadId,
          providerSessionId: input.providerSessionId,
        });
      const runIds = new Set(runs.map((run) => run.id));
      const events: Array<OrchestrationV2DomainEvent> = [];
      for (const checkpoint of projection.checkpoints) {
        if (checkpoint.status !== "ready" || checkpoint.runId === null) continue;
        if (!runIds.has(checkpoint.runId)) continue;
        events.push({
          id: yield* eventId(),
          type: "checkpoint.captured",
          threadId: rewind.threadId,
          runId: checkpoint.runId,
          nodeId: checkpoint.nodeId,
          providerInstanceId: input.providerInstanceId,
          occurredAt: now,
          payload: { ...checkpoint, status: "stale" },
        });
      }
      for (const run of runs) {
        const rootNode = projection.nodes.find((node) => node.id === run.rootNodeId);
        events.push({
          id: yield* eventId(),
          type: "run.updated",
          threadId: rewind.threadId,
          runId: run.id,
          ...(rootNode === undefined ? {} : { nodeId: rootNode.id }),
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: { ...run, status: "rolled_back", completedAt: now },
        });
        if (rootNode !== undefined) {
          events.push({
            id: yield* eventId(),
            type: "node.updated",
            threadId: rewind.threadId,
            runId: run.id,
            nodeId: rootNode.id,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...rootNode, status: "rolled_back", completedAt: now },
          });
        }
      }
      return events;
    });

    const normalize: ProviderEventIngestorV2Shape["normalize"] = (input) =>
      Effect.gen(function* () {
        switch (input.event.type) {
          case "app_thread.created": {
            const offered = input.event.appThread;
            const existing = yield* projections.getThread(offered.id).pipe(
              Effect.catchTags({
                ProjectionStoreThreadNotFoundError: () => Effect.succeed(null),
              }),
            );
            if (
              existing !== null &&
              existing.title === offered.title &&
              modelSelectionsEqual(existing.modelSelection, offered.modelSelection)
            )
              return [];
            const now = yield* DateTime.now;
            return [
              yield* makeDomainEvent(input, {
                type: existing === null ? "thread.created" : "thread.metadata-updated",
                threadId: offered.id,
                payload:
                  existing === null
                    ? offered
                    : {
                        ...existing,
                        title: offered.title,
                        modelSelection: offered.modelSelection,
                        updatedAt: now,
                      },
                occurredAt: now,
              }),
            ];
          }
          case "provider_session.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "provider-session.updated",
                payload: input.event.providerSession,
              }),
            ];
          case "provider_thread.updated": {
            const threadId = input.event.providerThread.appThreadId ?? input.threadId;
            const updated = yield* makeDomainEvent(input, {
              type: "provider-thread.updated",
              threadId,
              payload: input.event.providerThread,
            });
            return input.event.retainedNativeTurnIds === undefined
              ? [updated]
              : [
                  updated,
                  ...(yield* rollBackRewoundRuns(input, {
                    threadId,
                    providerThreadId: input.event.providerThread.id,
                    retainedNativeTurnIds: new Set(input.event.retainedNativeTurnIds),
                  })),
                ];
          }
          case "provider_turn.updated":
            return [
              ...(["completed", "interrupted", "failed", "cancelled"].includes(
                input.event.providerTurn.status,
              )
                ? yield* dismissNativeUserInputs(
                    input,
                    input.event.providerTurn.id,
                    input.event.threadId,
                  )
                : []),
              yield* makeDomainEvent(input, {
                type: "provider-turn.updated",
                ...(input.event.threadId === undefined ? {} : { threadId: input.event.threadId }),
                payload: input.event.providerTurn,
                nodeId: input.event.providerTurn.nodeId,
              }),
            ];
          case "node.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "node.updated",
                threadId: input.event.node.threadId,
                payload: input.event.node,
                runId: input.event.node.runId,
                nodeId: input.event.node.id,
              }),
            ];
          case "subagent.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "subagent.updated",
                threadId: input.event.subagent.threadId,
                payload: input.event.subagent,
                runId: input.event.subagent.runId,
                nodeId: input.event.subagent.id,
              }),
            ];
          case "message.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "message.updated",
                threadId: input.event.message.threadId,
                payload: input.event.message,
                runId: input.event.message.runId,
                nodeId: input.event.message.nodeId,
              }),
            ];
          case "turn_item.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "turn-item.updated",
                threadId: input.event.turnItem.threadId,
                payload: stripUnservedToolOutputImageBytes(input.event.turnItem),
                runId: input.event.turnItem.runId,
                nodeId: input.event.turnItem.nodeId,
              }),
            ];
          case "runtime_request.updated":
            return [
              yield* makeDomainEvent(input, {
                type: "runtime-request.updated",
                ...(input.event.threadId === undefined ? {} : { threadId: input.event.threadId }),
                payload: input.event.runtimeRequest,
                nodeId: input.event.runtimeRequest.nodeId,
              }),
            ];
          case "plan.updated": {
            const occurredAt = yield* DateTime.now;
            const plan = input.event.plan;
            const previous =
              plan.kind === "todo_list" && !isPiChecklistPlan(plan)
                ? yield* projections.getPlan(plan.threadId, plan.id)
                : undefined;
            const payload =
              plan.kind === "todo_list" && !isPiChecklistPlan(plan)
                ? withPlanStepDurations(
                    plan,
                    previous?.kind === "todo_list" ? previous : undefined,
                    occurredAt,
                  )
                : plan;
            return [
              yield* makeDomainEvent(input, {
                type: "plan.updated",
                threadId: plan.threadId,
                payload,
                runId: plan.runId,
                nodeId: plan.nodeId,
                occurredAt,
              }),
            ];
          }
          case "turn.terminal": {
            const dismissed = yield* dismissNativeUserInputs(input, input.event.providerTurnId);
            if (input.event.status !== "failed") {
              return dismissed;
            }
            const occurredAt = yield* DateTime.now;
            // A turn that failed because systemd killed the agent's scope for
            // memory would otherwise read as a plain provider crash.
            const failure = (yield* agentScope.oomKilled(input.threadId))
              ? { ...input.event.failure, message: "Killed: out of memory.", code: "oom_kill" }
              : input.event.failure;
            return [
              ...dismissed,
              yield* makeDomainEvent(input, {
                type: "turn-item.updated",
                payload: makeProviderFailureTurnItem({
                  driver: input.event.driver,
                  threadId: input.threadId,
                  runId: input.runId ?? null,
                  nodeId: input.nodeId ?? null,
                  providerThreadId: input.event.providerThreadId,
                  providerTurnId: input.event.providerTurnId,
                  itemOrdinal: input.event.failureItemOrdinal,
                  failure,
                  ...(input.event.retry === undefined ? {} : { retry: input.event.retry }),
                  ...(input.event.retryStartedAt === undefined
                    ? {}
                    : { retryStartedAt: input.event.retryStartedAt }),
                  occurredAt,
                }),
              }),
            ];
          }
        }
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderEventNormalizeError({
              providerSessionId: input.providerSessionId,
              threadId: input.threadId,
              providerEvent: input.event,
              cause,
            }),
        ),
      );

    const nativeAdmissionEvents = Effect.fnUntraced(function* (
      input: Omit<ProviderEventIngestInput, "event"> & {
        readonly event: Pick<ProviderAdapter.ProviderAdapterV2Event, "driver">;
      },
    ) {
      const admission = input.nativeWorkerAdmission;
      if (admission === undefined) return [];
      if (
        admission.summary.gaps.some((gap) => gap === "provenance_mismatch" || gap === "stale_owner")
      ) {
        return [];
      }
      const launch = admission.summary.launch;
      const existing = (yield* projections.getThreadRecords(input.threadId, ["subagents"]))
        .subagents;
      const now = yield* DateTime.now;
      const parent = yield* projections.getThread(input.threadId);
      const identity = Hex.encode(
        yield* NodeCrypto.make.digest(
          "SHA-256",
          new TextEncoder().encode(
            encodeNativeWorkerIdentity([
              launch.providerInstanceId,
              launch.coordinatorSessionId,
              launch.instanceId,
            ]),
          ),
        ),
      );
      const id = NodeId.make(`pi-worker:${identity}`);
      const prior = existing.find((member) => member.id === id);
      if (prior) {
        if (
          launch.source !== "architecture_roster" ||
          !prior.childThreadId ||
          !prior.providerThreadId
        )
          return [];
        const child = (yield* projections.getThreadRecords(prior.childThreadId, [
          "providerThreads",
        ])).providerThreads.find((thread) => thread.id === prior.providerThreadId);
        const previous = child?.nativeMetadata?.nativeWorkerCheckpoint;
        if (!child || !previous) return [];
        if (
          NodeUtil.isDeepStrictEqual(previous.owner, admission.checkpoint.owner) &&
          previous.ownerGeneration > admission.checkpoint.ownerGeneration
        )
          return [];
        const sameOwner =
          NodeUtil.isDeepStrictEqual(previous.owner, admission.checkpoint.owner) &&
          previous.ownerGeneration === admission.checkpoint.ownerGeneration;
        const nativeWorker = sameOwner
          ? {
              ...(prior.nativeWorker ?? admission.summary),
              launch,
              roleId: admission.summary.roleId,
              execution: admission.summary.execution,
              ...(admission.summary.mandate ? { mandate: admission.summary.mandate } : {}),
              ...(admission.summary.brief !== undefined &&
              (admission.summary.briefComplete ||
                admission.summary.briefSource === "original WorkBrief" ||
                !prior.nativeWorker?.briefComplete)
                ? {
                    brief: admission.summary.brief,
                    briefComplete: admission.summary.briefComplete === true,
                    ...(admission.summary.briefSource
                      ? { briefSource: admission.summary.briefSource }
                      : {}),
                  }
                : {}),
            }
          : { ...prior.nativeWorker, ...admission.summary };
        if (sameOwner && NodeUtil.isDeepStrictEqual(prior.nativeWorker, nativeWorker)) return [];
        const checkpoint = {
          ...(sameOwner ? previous : admission.checkpoint),
          launch,
        };
        return [
          yield* makeDomainEvent(input, {
            type: "subagent.updated",
            nodeId: id,
            payload: {
              ...prior,
              nativeWorker,
              runId: null,
              parentNodeId: launch.parentNodeId,
              prompt: nativeWorker.mandate?.text ?? nativeWorker.brief ?? prior.prompt,
              updatedAt: now,
            },
          }),
          yield* makeDomainEvent(input, {
            type: "provider-thread.updated",
            threadId: prior.childThreadId,
            payload: {
              ...child,
              providerSessionId: input.providerSessionId,
              nativeMetadata: { ...child.nativeMetadata, nativeWorkerCheckpoint: checkpoint },
            },
          }),
        ];
      }
      const childThreadId = ThreadId.make(`pi-worker:${identity}`);
      const providerThreadId = ProviderThreadId.make(`pi-worker:${identity}`);
      const selection: ModelSelection =
        admission.summary.activated != null
          ? {
              instanceId: input.providerInstanceId,
              model: admission.summary.activated.model,
              options: [{ id: "thinking", value: admission.summary.activated.reasoning }],
            }
          : admission.summary.selected !== undefined
            ? {
                instanceId: input.providerInstanceId,
                model: admission.summary.selected.model,
                ...(admission.summary.selected.reasoning === undefined
                  ? {}
                  : {
                      options: [{ id: "thinking", value: admission.summary.selected.reasoning }],
                    }),
              }
            : parent.modelSelection;
      const subagent: OrchestrationV2Subagent = {
        id,
        threadId: input.threadId,
        runId: launch.source === "architecture_roster" ? null : launch.runId,
        parentNodeId:
          launch.source === "architecture_roster" ? launch.parentNodeId : launch.toolNodeId,
        origin: "provider_native",
        createdBy: "agent",
        driver: input.event.driver,
        providerInstanceId: input.providerInstanceId,
        providerThreadId,
        childThreadId,
        nativeTaskRef: null,
        prompt: admission.summary.mandate?.text ?? admission.summary.brief ?? "",
        title: admission.summary.roleId ?? launch.instanceId,
        model: selection.model,
        modelSelection: selection,
        status:
          admission.summary.execution === "accepted"
            ? "pending"
            : (admission.summary.execution ?? "pending"),
        nativeWorker: admission.summary,
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      };
      return [
        yield* makeDomainEvent(input, {
          type: "thread.created",
          threadId: childThreadId,
          payload: makeSubagentChildThread({
            parentThread: parent,
            childThreadId,
            parentNodeId: id,
            activeProviderThreadId: providerThreadId,
            providerInstanceId: input.providerInstanceId,
            modelSelection: selection,
            title: subagent.title ?? launch.instanceId,
            now,
            createdBy: "agent",
            creationSource: "provider",
          }),
        }),
        yield* makeDomainEvent(input, {
          type: "provider-thread.updated",
          threadId: childThreadId,
          payload: {
            id: providerThreadId,
            driver: input.event.driver,
            providerInstanceId: input.providerInstanceId,
            providerSessionId: input.providerSessionId,
            appThreadId: childThreadId,
            ownerNodeId: id,
            nativeThreadRef: null,
            nativeConversationHeadRef: null,
            status: "not_loaded",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            nativeMetadata: { nativeWorkerCheckpoint: admission.checkpoint },
            createdAt: now,
            updatedAt: now,
          },
        }),
        yield* makeDomainEvent(input, { type: "subagent.updated", nodeId: id, payload: subagent }),
      ];
    });

    return ProviderEventIngestorV2.of({
      ingestArchitecture: (input) => {
        const coordinatorSessionId = input.batch.checkpoint.owner.coordinatorSessionId;
        const tableId = encodeNativeWorkerIdentity([
          input.providerInstanceId,
          coordinatorSessionId,
          input.batch.roomId,
        ]);
        return eventSink.writeArchitecture({
          threadId: input.threadId,
          tableId,
          isConnected: input.isConnected,
          build: (previous) =>
            Effect.gen(function* () {
              const room = input.batch.records.find((record) => record.kind === "room");
              const sameOwner =
                previous &&
                NodeUtil.isDeepStrictEqual(previous.checkpoint.owner, input.batch.checkpoint.owner);
              const captureLoss =
                !room &&
                input.batch.records.length === 0 &&
                input.batch.capture === "incomplete" &&
                input.batch.gaps.length > 0 &&
                sameOwner;
              if (
                (!room && !captureLoss) ||
                (room &&
                  (room.id !== input.batch.roomId ||
                    room.origin.coordinatorSessionId !== coordinatorSessionId))
              )
                return undefined;
              if (
                sameOwner &&
                (previous.retired ||
                  previous.checkpoint.ownerGeneration > input.batch.checkpoint.ownerGeneration)
              )
                return undefined;
              const confirmedRoom =
                room ?? previous?.records.find((record) => record.kind === "room");
              if (!confirmedRoom) return undefined;
              // A failed first page observes loss, not a new source cut.
              const checkpoint =
                captureLoss && previous
                  ? {
                      ...previous.checkpoint,
                      ownerGeneration: input.batch.checkpoint.ownerGeneration,
                    }
                  : input.batch.checkpoint;
              const gaps =
                captureLoss && previous
                  ? [...new Set([...previous.summary.gaps, ...input.batch.gaps])]
                  : input.batch.gaps;
              if (
                previous &&
                NodeUtil.isDeepStrictEqual(previous.checkpoint.owner, checkpoint.owner) &&
                previous.checkpoint.cut > checkpoint.cut
              )
                return undefined;
              const records = new Map(
                previous?.records.map((record) => [`${record.kind}:${record.id}`, record]),
              );
              for (const record of input.batch.records)
                records.set(`${record.kind}:${record.id}`, record);
              const members = [...records.values()].filter((record) => record.kind === "member");
              const knownOriginal = previous?.original;
              const receivedOriginal = input.batch.original;
              const original =
                knownOriginal &&
                (!receivedOriginal ||
                  receivedOriginal.source === "unavailable" ||
                  (knownOriginal.source === "original WorkBrief" &&
                    receivedOriginal.source !== "original WorkBrief") ||
                  (knownOriginal.complete && knownOriginal.source === receivedOriginal.source))
                  ? knownOriginal
                  : receivedOriginal;
              if (
                previous &&
                NodeUtil.isDeepStrictEqual(previous.checkpoint, checkpoint) &&
                previous.summary.capture === input.batch.capture &&
                NodeUtil.isDeepStrictEqual(previous.summary.gaps, gaps) &&
                NodeUtil.isDeepStrictEqual(previous.original, original)
              )
                return undefined;
              const summary: PiArchitectureSummary = {
                tableId,
                threadId: input.threadId,
                providerInstanceId: input.providerInstanceId,
                coordinatorSessionId,
                roomId: input.batch.roomId,
                mode: confirmedRoom.mode,
                participants: members.length,
                lifecycle: confirmedRoom.lifecycle,
                capture: input.batch.capture,
                observation: input.batch.capture === "incomplete" ? "partial" : "stale",
                gaps,
              };
              const current = yield* projections.getThread(input.threadId);
              const events: OrchestrationV2DomainEvent[] = [];
              const context = {
                threadId: input.threadId,
                providerInstanceId: input.providerInstanceId,
                providerSessionId: input.providerSessionId,
                event: { driver: ProviderDriverKind.make("pi") },
              };
              for (const member of room ? members : []) {
                if (
                  !member.instanceId.trim() ||
                  (member.workerRef && member.workerRef.instanceId !== member.instanceId) ||
                  member.conversationRefs.some((ref) => ref.instanceId !== member.instanceId)
                )
                  continue;
                const launch = {
                  source: "architecture_roster" as const,
                  providerInstanceId: input.providerInstanceId,
                  providerSessionId: input.providerSessionId,
                  parentThreadId: input.threadId,
                  parentProviderThreadId: input.parentProviderThreadId,
                  parentNodeId: input.parentNodeId,
                  coordinatorSessionId,
                  roomId: input.batch.roomId,
                  memberId: member.id,
                  assignmentId: member.assignmentId,
                  instanceId: member.instanceId,
                  roleId: member.roleId,
                };
                const admitted = yield* nativeAdmissionEvents({
                  ...context,
                  nativeWorkerAdmission: {
                    summary: {
                      launch,
                      roleId: member.roleId,
                      acceptedAt: member.admittedAt,
                      ...(member.mandate.source === "TeamAssignment.task"
                        ? {
                            mandate: {
                              source: member.mandate.source,
                              text: architecturePreview(member.mandate.text),
                              complete: member.mandate.text.length <= 512,
                            },
                          }
                        : original
                          ? {
                              brief: architecturePreview(original.text),
                              briefComplete: original.complete && original.text.length <= 512,
                              ...(original.source === "unavailable"
                                ? {}
                                : { briefSource: original.source }),
                            }
                          : {}),
                      execution: member.execution,
                      result: member.execution === "completed" ? "available" : "pending",
                      capture: "open",
                      availability: "ready",
                      gaps: [],
                    },
                    checkpoint: {
                      launch,
                      ownerGeneration: input.batch.checkpoint.ownerGeneration,
                      owner: input.batch.checkpoint.owner,
                      bindings: [],
                      captureEpoch: null,
                      bindingGeneration: null,
                      bindingCursors: [],
                      cursor: null,
                      cut: 0,
                      recovery: false,
                      capture: "opening",
                      finalSequence: null,
                      gaps: [],
                    },
                  },
                });
                events.push(...admitted);
              }
              const nativeTables = [
                ...(current.nativeTables ?? []).filter((table) => table.tableId !== tableId),
                summary,
              ].sort((a, b) => a.tableId.localeCompare(b.tableId));
              if (!NodeUtil.isDeepStrictEqual(current.nativeTables, nativeTables))
                events.push(
                  yield* makeDomainEvent(context, {
                    type: "thread.metadata-updated",
                    payload: { ...current, nativeTables },
                  }),
                );
              return {
                archive: {
                  summary,
                  providerSessionId: input.providerSessionId,
                  parentProviderThreadId: input.parentProviderThreadId,
                  checkpoint,
                  records: [...records.values()],
                  ...(original ? { original } : {}),
                },
                events,
              };
            }),
        });
      },
      ingestNativeWorker: (batch, isConnected) =>
        eventSink.writeNativeWorker({
          ...(isConnected === undefined ? {} : { isConnected }),
          launch: batch.launch,
          ownerGeneration: batch.checkpoint.ownerGeneration,
          expectedCheckpoint: batch.expectedCheckpoint,
          checkpoint: batch.checkpoint,
          build: (subagent, providerThread) =>
            Effect.gen(function* () {
              if (
                !NodeUtil.isDeepStrictEqual(batch.summary.launch, batch.launch) ||
                !NodeUtil.isDeepStrictEqual(batch.checkpoint.bindings, batch.bindings)
              ) {
                return yield* Effect.die(new Error("Native batch provenance mismatch."));
              }
              const summary = {
                ...subagent.nativeWorker,
                ...batch.summary,
                ...(subagent.nativeWorker?.briefComplete && !batch.summary.briefComplete
                  ? {
                      brief: subagent.nativeWorker.brief,
                      briefComplete: true,
                      briefSource: subagent.nativeWorker.briefSource,
                    }
                  : {}),
              };
              const childThreadId = subagent.childThreadId!;
              const previous = yield* projections.getThreadRecords(childThreadId, ["turnItems"]);
              const now = yield* DateTime.now;
              const input: ProviderEventIngestInput = {
                threadId: childThreadId,
                providerInstanceId: batch.launch.providerInstanceId,
                providerSessionId: batch.launch.providerSessionId,
                event: { type: "subagent.updated", driver: subagent.driver, subagent },
              };
              const events: Array<OrchestrationV2DomainEvent> = [];
              const items = new Map(previous.turnItems.map((item) => [item.id, item]));
              let sourceProgress = false;
              for (const record of batch.records) {
                const p = record.provenance;
                if (
                  p.instanceId !== batch.launch.instanceId ||
                  !NodeUtil.isDeepStrictEqual(p.owner, batch.checkpoint.owner) ||
                  p.captureEpoch !== batch.checkpoint.captureEpoch ||
                  !batch.bindings.some((binding) => binding.generation === p.binding)
                ) {
                  return yield* Effect.die(new Error("Native record provenance mismatch."));
                }
                if (record.kind === "branch_changed") continue;
                const key = record.kind === "tool" ? (p.toolCallId ?? p.id) : p.id;
                const id = TurnItemId.make(
                  `pi-worker:${subagent.id}:${p.binding}:${record.kind}:${key}`,
                );
                const stored = items.get(id);
                const ledger =
                  stored?.nativeWorkerRecords ??
                  (stored?.nativeWorkerRecord === undefined ? [] : [stored.nativeWorkerRecord]);
                const sameRecord = (entry: typeof p) =>
                  entry.id === p.id &&
                  entry.binding === p.binding &&
                  entry.captureEpoch === p.captureEpoch &&
                  entry.instanceId === p.instanceId &&
                  NodeUtil.isDeepStrictEqual(entry.owner, p.owner);
                if ((ledger.find(sameRecord)?.revision ?? 0) >= p.revision) continue;
                const nativeWorkerRecords = [...ledger.filter((entry) => !sameRecord(entry)), p];
                const rendered = stored?.nativeWorkerRecord;
                const updatesRenderedRecord =
                  rendered === undefined || p.order >= rendered.order || sameRecord(rendered);
                sourceProgress = true;
                const base = {
                  id,
                  threadId: childThreadId,
                  runId: null,
                  nodeId: subagent.id,
                  providerThreadId: providerThread.id,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId:
                    record.kind === "tool" && record.parentToolCallId !== undefined
                      ? TurnItemId.make(
                          `pi-worker:${subagent.id}:${p.binding}:tool:${record.parentToolCallId}`,
                        )
                      : null,
                  ordinal: stored?.ordinal ?? p.order + 1,
                  title: null,
                  startedAt: stored?.startedAt ?? now,
                  completedAt: null,
                  updatedAt: now,
                  nativeWorkerRecord: updatesRenderedRecord ? p : rendered,
                  nativeWorkerRecords,
                };
                let item: OrchestrationV2TurnItem;
                if (record.kind === "message") {
                  let text = record.payload.blocks
                    .filter((block) => block.type === "text")
                    .map((block) => block.text)
                    .join("\n");
                  if (record.payload.role === "assistant") {
                    const notices: string[] = [];
                    const diagnostic = record.payload.errorMessage?.trim();
                    if (diagnostic) {
                      notices.push(`[T3 notice] ${diagnostic}`);
                    } else if (record.payload.stopReason === "error") {
                      notices.push("[T3 notice] The response failed.");
                    }
                    if (record.payload.stopReason === "aborted") {
                      notices.push("[T3 notice] The response was aborted.");
                    } else if (record.payload.stopReason === "length") {
                      notices.push("[T3 notice] The response reached its length limit.");
                    }
                    if (notices.length > 0) {
                      text = [...(text.length > 0 ? [text] : []), ...notices].join("\n\n");
                    }
                  }
                  const artifacts = makeSubagentConversationArtifacts({
                    messageId: MessageId.make(`${id}:message`),
                    turnItemId: id,
                    threadId: childThreadId,
                    rootNodeId: subagent.id,
                    providerThreadId: providerThread.id,
                    providerTurnId: null,
                    nativeItemRef: null,
                    role: record.payload.role,
                    text,
                    ordinal: base.ordinal,
                    now,
                  });
                  events.push(
                    yield* makeDomainEvent(input, {
                      type: "message.updated",
                      payload: artifacts.message,
                    }),
                  );
                  item = { ...artifacts.turnItem, nativeWorkerRecord: p, nativeWorkerRecords };
                } else {
                  const oldTool = stored?.type === "dynamic_tool" ? stored : undefined;
                  item = {
                    ...base,
                    type: "dynamic_tool",
                    toolName: updatesRenderedRecord
                      ? record.payload.name
                      : (oldTool?.toolName ?? record.payload.name),
                    status:
                      !updatesRenderedRecord && oldTool !== undefined
                        ? oldTool.status
                        : record.payload.status === "started" || record.payload.status === "updated"
                          ? "running"
                          : record.payload.status === "error"
                            ? "failed"
                            : "completed",
                    input:
                      record.payload.argsState === "available"
                        ? record.payload.args
                        : oldTool?.input,
                    output: updatesRenderedRecord
                      ? (record.payload.todoSnapshot ?? record.payload.result ?? oldTool?.output)
                      : oldTool?.output,
                  };
                }
                items.set(id, item);
                events.push(
                  yield* makeDomainEvent(input, { type: "turn-item.updated", payload: item }),
                );
              }
              const old = batch.expectedCheckpoint;
              const summaryChanged = !NodeUtil.isDeepStrictEqual(subagent.nativeWorker, summary);
              sourceProgress ||=
                !NodeUtil.isDeepStrictEqual(old?.bindings, batch.bindings) ||
                old?.capture !== batch.checkpoint.capture ||
                old?.finalSequence !== batch.checkpoint.finalSequence ||
                subagent.nativeWorker?.execution !== summary.execution ||
                subagent.nativeWorker?.capture !== summary.capture ||
                subagent.nativeWorker?.result !== summary.result ||
                (batch.result?.complete === true && subagent.result !== batch.result.text);
              const selection: ModelSelection | undefined =
                summary.activated != null
                  ? {
                      instanceId: batch.launch.providerInstanceId,
                      model: summary.activated.model,
                      options: [{ id: "thinking", value: summary.activated.reasoning }],
                    }
                  : summary.selected !== undefined
                    ? {
                        instanceId: batch.launch.providerInstanceId,
                        model: summary.selected.model,
                        ...(summary.selected.reasoning === undefined
                          ? {}
                          : {
                              options: [{ id: "thinking", value: summary.selected.reasoning }],
                            }),
                      }
                    : undefined;
              if (selection !== undefined) {
                const child = yield* projections.getThread(childThreadId);
                if (!modelSelectionsEqual(child.modelSelection, selection)) {
                  events.push(
                    yield* makeDomainEvent(input, {
                      type: "thread.model-selection-updated",
                      payload: { ...child, modelSelection: selection, updatedAt: now },
                    }),
                  );
                }
              }
              let checklistCapture:
                | { providerThreadId: ProviderThreadId; capture: PiChecklistCapture }
                | undefined;
              const active = batch.bindings.find((binding) => binding.state === "active");
              const head = active?.todoHead;
              const selected =
                head?.state === "ready" && head.sourceEntryId !== null
                  ? [...items.values()].find(
                      (item) =>
                        item.type === "dynamic_tool" &&
                        (
                          item.nativeWorkerRecords ??
                          (item.nativeWorkerRecord === undefined ? [] : [item.nativeWorkerRecord])
                        ).some(
                          (entry) =>
                            entry.binding === active?.generation &&
                            entry.nativeEntryId === head.sourceEntryId,
                        ),
                    )
                  : undefined;
              if (active !== undefined) {
                const snapshot =
                  selected?.type === "dynamic_tool"
                    ? decodeNativeTodoSnapshot(selected.output)
                    : undefined;
                const pointer = providerThread.nativeMetadata?.checklistSelection;
                const source = normalizePiTodoSelection({
                  head,
                  snapshot: snapshot?._tag === "Some" ? snapshot.value : undefined,
                });
                const oldSelected = previous.turnItems.find((item) => item.id === selected?.id);
                const changed =
                  pointer?.state !== source.state ||
                  pointer.list.nativeSessionId !== active.sessionId ||
                  pointer.sessionGeneration !== active.generation ||
                  pointer.sourceEntryId !== source.sourceEntryId ||
                  (selected?.type === "dynamic_tool" &&
                    oldSelected?.type === "dynamic_tool" &&
                    !NodeUtil.isDeepStrictEqual(selected.output, oldSelected.output));
                if (changed) {
                  checklistCapture = {
                    providerThreadId: providerThread.id,
                    capture: {
                      list: {
                        threadId: childThreadId,
                        providerInstanceId: batch.launch.providerInstanceId,
                        nativeSessionId: active.sessionId,
                      },
                      ownerGeneration: batch.checkpoint.ownerGeneration,
                      sessionGeneration: active.generation,
                      captureVersion: (pointer?.captureVersion ?? 0) + 1,
                      expectedPointer:
                        pointer === undefined ? { kind: "absent" } : { kind: "current", pointer },
                      authority:
                        pointer !== undefined && pointer.list.nativeSessionId !== active.sessionId
                          ? "session_reset"
                          : "capture",
                      source,
                    },
                  };
                }
              }
              events.push(
                yield* makeDomainEvent(input, {
                  type: "provider-thread.updated",
                  payload: {
                    ...providerThread,
                    nativeThreadRef:
                      active === undefined
                        ? providerThread.nativeThreadRef
                        : {
                            driver: subagent.driver,
                            strength: "strong",
                            nativeId: active.sessionId,
                          },
                    nativeMetadata: {
                      ...providerThread.nativeMetadata,
                      nativeWorkerCheckpoint: batch.checkpoint,
                    },
                    updatedAt: now,
                  },
                }),
                yield* makeDomainEvent(input, {
                  type: "subagent.updated",
                  threadId: subagent.threadId,
                  runId: subagent.runId,
                  payload: {
                    ...subagent,
                    nativeWorker: summary,
                    status:
                      summary.execution === "accepted"
                        ? "pending"
                        : (summary.execution ?? subagent.status),
                    ...(selection === undefined
                      ? {}
                      : { model: selection.model, modelSelection: selection }),
                    result: batch.result?.complete === true ? batch.result.text : subagent.result,
                    updatedAt: now,
                  },
                }),
              );
              return {
                events:
                  !sourceProgress &&
                  checklistCapture === undefined &&
                  !summaryChanged &&
                  NodeUtil.isDeepStrictEqual(old, batch.checkpoint)
                    ? []
                    : events,
                ...(checklistCapture === undefined ? {} : { checklistCapture }),
                sourceProgress,
              };
            }),
        }),
      normalize,
      ingestNormalized: (input) =>
        Effect.gen(function* () {
          const events = [
            ...(yield* normalize(input)),
            ...(yield* (
              input.event.type === "turn_item.updated"
                ? nativeAdmissionEvents(input)
                : Effect.succeed([])
            ).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderEventPublishError({
                    providerSessionId: input.providerSessionId,
                    eventCount: 3,
                    cause,
                  }),
              ),
            )),
          ];
          if (events.length === 0) {
            return [];
          }
          const mapWriteError = (cause: unknown) =>
            new ProviderEventPublishError({
              providerSessionId: input.providerSessionId,
              eventCount: events.length,
              cause,
            });
          const checklistCapture =
            input.event.type === "turn_item.updated" &&
            input.event.turnItem.type === "dynamic_tool" &&
            (input.event.turnItem.status === "completed" ||
              input.event.turnItem.status === "failed") &&
            input.event.turnItem.providerThreadId !== null &&
            input.event.checklistCapture !== undefined
              ? {
                  providerThreadId: input.event.turnItem.providerThreadId,
                  capture: input.event.checklistCapture,
                }
              : undefined;
          if (input.writeIfProviderThreadOwner !== undefined) {
            const ownerResult = yield* eventSink
              .writeIfProviderThreadOwner({
                guardPendingUserInputCancellations: true,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                ...input.writeIfProviderThreadOwner,
                events,
              })
              .pipe(Effect.mapError(mapWriteError));
            return ownerResult.storedEvents;
          }
          if (input.writeIfRunCurrent === undefined) {
            return yield* eventSink
              .write({
                ...(checklistCapture === undefined ? {} : { checklistCapture }),
                guardPendingUserInputCancellations: true,
                ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                events,
              })
              .pipe(Effect.mapError(mapWriteError));
          }
          const result = yield* eventSink
            .writeIfRunCurrent({
              ...(input.nativeWorkerAdmission?.isConnected === undefined
                ? {}
                : {
                    isConnected: input.nativeWorkerAdmission.isConnected,
                  }),
              ...(checklistCapture === undefined ? {} : { checklistCapture }),
              guardPendingUserInputCancellations: true,
              ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
              threadId: input.threadId,
              ...input.writeIfRunCurrent,
              events,
            })
            .pipe(Effect.mapError(mapWriteError));
          return result.storedEvents;
        }).pipe(
          Effect.flatMap((storedEvents) =>
            storedEvents.length === 0 || input.event.type !== "subagent.updated"
              ? Effect.succeed(storedEvents)
              : syncSubagentThreadModel(input, input.event.subagent).pipe(
                  Effect.map((synced) => [...storedEvents, ...synced]),
                  Effect.mapError(
                    (cause) =>
                      new ProviderEventPublishError({
                        providerSessionId: input.providerSessionId,
                        eventCount: 1,
                        cause,
                      }),
                  ),
                ),
          ),
          Effect.tap((storedEvents) =>
            Effect.gen(function* () {
              if (storedEvents.length === 0 || input.event.type !== "provider_turn.updated") return;
              const providerTurn = input.event.providerTurn;
              if (
                providerTurn.status !== "completed" &&
                providerTurn.status !== "failed" &&
                providerTurn.status !== "interrupted" &&
                providerTurn.status !== "cancelled"
              )
                return;
              const key = `${input.providerInstanceId}:${providerTurn.id}`;
              if (completedTurnAnalytics.has(key)) return;
              completedTurnAnalytics.add(key);
              if (completedTurnAnalytics.size > 4096) {
                const oldest = completedTurnAnalytics.values().next().value;
                if (oldest !== undefined) completedTurnAnalytics.delete(oldest);
              }
              yield* analytics.record(
                providerTurnAnalyticsProperties({
                  driver: input.event.driver,
                  providerTurn,
                  ...(input.analyticsContext === undefined
                    ? {}
                    : { context: input.analyticsContext }),
                }),
              );
            }),
          ),
          (effect) =>
            input.event.type === "app_thread.created"
              ? threadCommands.withLock(input.event.appThread.id, effect)
              : effect,
        ),
    });
  }),
);
