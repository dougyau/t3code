import * as HostProcess from "@t3tools/shared/HostProcess";
import { AgentScope } from "@t3tools/shared/AgentScope";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { readToolOutputImage } from "@t3tools/shared/toolOutput";
import * as Predicate from "effect/Predicate";
import {
  ProviderDriverKind,
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderRetry,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type OrchestrationV2UserInputQuestion,
  type ProviderApprovalDecision,
  type ProviderInstanceId,
  type OrchestrationV2ProviderTurnTokenUsage,
  type OrchestrationV2Subagent,
  type OrchestrationV2AppThread,
  type NodeId,
  type RunId,
} from "@t3tools/contracts";
import { PiSettings } from "../settings.ts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Option from "effect/Option";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { providerMessageTextWithAttachmentPaths } from "@t3tools/provider-core/server/attachmentPrompt";
import { mcpToolPresentation } from "@t3tools/provider-core/server/mcpToolPresentation";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { parsePiCompactCommand, type PiCompactCommand } from "./commands.ts";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "@t3tools/provider-core/server/adapterDriver";
import {
  makeProviderFailure,
  makeProviderRetryTurnItem,
} from "@t3tools/provider-core/server/failure";
import { turnScopedSelectionTransition } from "@t3tools/provider-core/server/selectionTransition";
import {
  makePiRpcConnection,
  parsePiModelSlug,
  piRecordField as recordField,
  piRecordNumber as recordNumber,
  piRecordString as recordString,
  type PiRpcConnection,
  type PiRpcRecord,
} from "./rpc.ts";
import {
  buildPiRpcLaunch,
  materializePiT3McpExtension,
  resolvePiLaunchArgs,
} from "./mcpInjection.ts";
import { PI_FILE_CHANGE_TOOLS } from "./mcpExtensionSource.ts";
import {
  piSubagentResults,
  piSubagentSnapshot,
  piSubagentRunStates,
  piSubagentWorkflowRunIds,
  piSubagentObservationId,
  piSubagentLifecycle,
  piSubagentLifecycleHints,
  piSubagentMetadataString,
  PI_SUBAGENT_LIFECYCLE_MAX_BYTES,
  type PiSubagentLifecycle,
  type PiSubagentObservation,
} from "./subagents.ts";
import {
  PiSubagentInspectReply,
  PiSubagentMessage,
  piSubagentToolSummaries,
} from "./inspection.ts";
import {
  makeSubagentChildThread,
  makeSubagentConversationArtifacts,
  subagentThreadTitle,
} from "@t3tools/provider-core/server/subagentProjection";
import { piSubagentMessages } from "./inspection.ts";

export const PI_PROVIDER = ProviderDriverKind.make("pi");
const PI_DRIVER_KIND = PI_PROVIDER;
const DEFAULT_PI_SETTINGS = Schema.decodeSync(PiSettings)({});

// Inherit Pi's configured defaultProvider/defaultModel without calling set_model.
const PI_INHERIT_MODEL_SLUG = "default";

const STREAM_FLUSH_MS = 50;
const PI_REQUEST_TIMEOUT_MS = 15_000;
const PI_SESSION_TIMEOUT_MS = 60_000;
const PI_UNSOLICITED_ACTIVITY_ERROR =
  "Pi started agent work outside an active T3 turn. The session was stopped to prevent invisible tool execution.";
const PI_SUBAGENT_UNKNOWN_TOOL_OUTCOME =
  "Earlier tool activity has no live-call identity; its outcome is unknown because the native result cannot be correlated.";
const NATIVE_OBSERVATION_WINDOW_MS = 30 * 60 * 1000;
const NATIVE_RECENT_OWNER_LIMIT = 32;
const NATIVE_REPLAY_KEY_LIMIT = 64;
const SETTLE_PROBE_MAX_ATTEMPTS = 3;
const SETTLE_PROBE_RETRY_DELAY = Duration.millis(100);

const PiProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "client-boundary" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    // Restart on mode changes so the permission hook retains one immutable lifetime policy.
    supportsRuntimeModeSwitchInSession: false,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: true,
    canRollbackThread: true,
    canForkThread: true,
    canForkFromTurn: true,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: true,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: true,
    // CommandPolicy requires a snapshot for rollback; rollbackThread returns the updated provider thread.
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: true,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface PiAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: PiSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly continuationRequests?: {
    readonly offer: (
      request: ProviderContinuationRequests.ProviderContinuationRequest,
    ) => Effect.Effect<void>;
  };
}

function contentText(content: unknown): string {
  if (!Array.isArray(content)) {
    return typeof content === "string" ? content : "";
  }
  return content
    .map((block) => {
      if (recordField(block, "type") === "text") return recordString(block, "text") ?? "";
      return "";
    })
    .join("");
}

const MAX_STRUCTURED_TOOL_OUTPUT_BYTES = 64 * 1024;

function isBoundedStructuredToolValue(value: unknown): boolean {
  let bytes = MAX_STRUCTURED_TOOL_OUTPUT_BYTES;
  let nodes = 4_096;
  const visit = (entry: unknown, depth: number): boolean => {
    if (depth > 32 || --nodes < 0 || bytes < 0) return false;
    if (Array.isArray(entry)) {
      bytes -= 2 + entry.length;
      return entry.every((item) => visit(item, depth + 1));
    }
    if (Predicate.isObject(entry)) {
      bytes -= 2;
      for (const key in entry) {
        if (!Object.hasOwn(entry, key)) continue;
        if (key.length > bytes) return false;
        bytes -= Buffer.byteLength(JSON.stringify(key), "utf8") + 2;
        if (!visit(entry[key], depth + 1)) return false;
      }
      return bytes >= 0;
    }
    if (typeof entry === "string" && entry.length > bytes) return false;
    bytes -= Buffer.byteLength(JSON.stringify(entry) ?? "null", "utf8");
    return bytes >= 0;
  };
  return visit(value, 0);
}

function piToolOutput(result: unknown, toolName: string): unknown {
  const rawContent = recordField(result, "content");
  const content = Array.isArray(rawContent)
    ? rawContent
    : typeof rawContent === "string"
      ? [{ type: "text", text: rawContent }]
      : [];
  let structured = recordField(result, "structuredContent");
  const details = recordField(result, "details");
  const isMcpResult =
    toolName.startsWith("mcp__") &&
    recordString(details, "server") !== undefined &&
    recordString(details, "tool") !== undefined &&
    Array.isArray(recordField(structured, "content"));
  if (isMcpResult) {
    structured = recordField(structured, "structuredContent");
  }
  if (typeof structured === "string" && structured === contentText(content)) {
    structured = undefined;
  }
  // Native read mirrors image/note in structured output; drop only that mirror, retaining extension metadata.
  if (toolName === "read" && Predicate.isObject(structured)) {
    const image = readToolOutputImage(structured);
    const note =
      recordString(
        content.find((block) => recordField(block, "type") === "text"),
        "text",
      ) ?? "";
    if (
      image !== null &&
      Object.keys(structured).every((key) => ["type", "data", "mimeType", "note"].includes(key)) &&
      recordString(structured, "note") === note &&
      content.some((block) => {
        const match = readToolOutputImage(block);
        return match !== null && match.mimeType === image.mimeType && match.data === image.data;
      })
    ) {
      structured = undefined;
    }
  }
  if (structured !== undefined && !isBoundedStructuredToolValue(structured)) {
    return {
      content: [
        ...content,
        {
          type: "text",
          text: "Structured output omitted because it exceeds the stored result limit.",
        },
      ],
    };
  }
  if (
    !isMcpResult &&
    structured === undefined &&
    content.every((block) => recordField(block, "type") === "text")
  ) {
    const text = contentText(content);
    return text.length > 0 ? text : undefined;
  }
  return {
    content,
    ...(structured === undefined ? {} : { structuredContent: structured }),
  };
}

function providerRef(
  nativeId: string,
  strength: "strong" | "weak" = "strong",
): OrchestrationV2ProviderRef {
  return { driver: PI_PROVIDER, nativeId, strength };
}

const PI_THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);

interface PiStreamItemState {
  readonly nativeItemId: string;
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  completed: boolean;
  flushScheduled: boolean;
  readonly startedAt: DateTime.Utc;
}

type PiCompactionStatus = "running" | "completed" | "failed" | "cancelled";

interface PiCompactionState {
  readonly nativeItemId: string;
  readonly startedAt: DateTime.Utc;
}

interface PiProviderRetryState {
  readonly retry: OrchestrationV2ProviderRetry;
  readonly failure: OrchestrationV2ProviderFailure;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinal: number;
}

function compactionTitle(status: PiCompactionStatus): string {
  switch (status) {
    case "running":
      return "Compacting context...";
    case "completed":
      return "Context compacted";
    case "failed":
      return "Context compaction failed";
    case "cancelled":
      return "Context compaction stopped";
  }
}

interface ActivePiTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  messageOrdinal: number;
  readonly streamItems: Map<string, PiStreamItemState>;
  readonly toolArgs: Map<string, unknown>;
  readonly toolStartedAt: Map<string, DateTime.Utc>;
  interrupted: boolean;
  nativeAborted: boolean;
  sawAgentActivity: boolean;
  adoptedWake: boolean;
  readonly promptMayBeCommandOnly: boolean;
  latestCompactionAfterTokens: number | null;
  lastLiveUsedTokens: number | null;
  settleProbeGeneration: number;
  /** An extension may start compaction immediately after Pi emits agent_settled. */
  settleWhenIdle: boolean;
  sawCompaction: boolean;
  manualCompactInFlight: boolean;
  activeCompaction: PiCompactionState | null;
  activeProviderRetry: PiProviderRetryState | null;
  failure: ReturnType<typeof makeProviderFailure> | null;
  rejectedPromptFailure: ReturnType<typeof makeProviderFailure> | null;
  stopTreeRefs?: PiTurnTreeRefs | null;
  nativeParentSettled?: boolean;
  nativeHistoryEligibleAt?: number;
}

interface PiTurnTreeRefs {
  /** Omitted keeps the synthetic ref; null means the turn left nothing on the active branch. */
  readonly nativeTurnRef: OrchestrationV2ProviderRef | null | undefined;
  readonly leafId: string | null;
  readonly retainedNativeTurnIds: ReadonlyArray<string> | undefined;
}

interface PendingPiPrompt {
  readonly nativeRequestId: string;
  readonly method: "select" | "confirm" | "input" | "editor";
  readonly questionId: string;
  readonly approvalKey: string;
  readonly expiresAt: number | null;
  expirationFiber: Fiber.Fiber<void> | null;
  runtimeRequest: OrchestrationV2RuntimeRequest;
  readonly node: OrchestrationV2ExecutionNode;
  readonly turnItem: OrchestrationV2TurnItem;
}

function piApprovalRequestKind(title: string): "command" | "file-change" {
  const toolName = /^Allow (\S+)\?$/.exec(title)?.[1];
  return toolName !== undefined &&
    (PI_FILE_CHANGE_TOOLS as ReadonlyArray<string>).includes(toolName)
    ? "file-change"
    : "command";
}

interface PiThreadState {
  providerThread: OrchestrationV2ProviderThread;
  activeTurn: ActivePiTurn | null;
  lastTurn?: ActivePiTurn;
}

interface PiWake {
  readonly state: PiThreadState;
  readonly events: Array<PiRpcRecord>;
}

export const makePiAdapterV2 = Effect.fn("makePiAdapterV2")(function* (
  options: PiAdapterV2Options,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const agentScope = yield* AgentScope;
  const fileSystem = yield* FileSystem.FileSystem;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const host = yield* ProviderHost.ProviderHost;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  const { continuationRequests } = options;

  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: PI_PROVIDER,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: PI_PROVIDER,
    getCapabilities: Effect.fn("PiAdapter.getCapabilities")(function () {
      return Effect.succeed(PiProviderCapabilitiesV2);
    }),
    planSelectionTransition: Effect.fn("PiAdapter.planSelectionTransition")(function () {
      return Effect.succeed(turnScopedSelectionTransition());
    }),
    openSession: Effect.fn("PiAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? host.paths.cwd;
      const mcpSession = yield* mcpSessions.read(input.threadId);
      const provideCacheFs = <A, E>(effect: Effect.Effect<A, E, FileSystem.FileSystem>) =>
        effect.pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: PI_PROVIDER,
                providerSessionId: input.providerSessionId,
                cause,
              }),
          ),
        );
      // Always inject the permission hook even without MCP credentials; otherwise Supervised becomes unrestricted.
      const extensionPath = yield* provideCacheFs(
        materializePiT3McpExtension(host.paths.providerStatusCacheDir),
      );
      const resolvedLaunchArgs = resolvePiLaunchArgs(options.settings.launchArgs);
      if (!resolvedLaunchArgs.ok) {
        return yield* protocolError(resolvedLaunchArgs.message);
      }
      const launch = buildPiRpcLaunch({
        launchArgs: resolvedLaunchArgs.args,
        environment: options.environment,
        mcpSession,
        extensionPath,
        runtimeMode: input.runtimePolicy.runtimeMode,
      });
      const scopedLaunch = yield* agentScope.wrap({
        command: options.settings.binaryPath || "pi",
        args: launch.args,
        name: "pi",
        threadId: input.threadId,
        env: launch.env,
      });
      // Finalizers run in reverse order: reap detached work after terminating Pi.
      const scopeUnit = scopedLaunch.unit;
      const stopScope =
        scopeUnit === undefined ? Effect.void : yield* Effect.cached(agentScope.stop(scopeUnit));
      yield* Effect.addFinalizer(() => stopScope);
      const connection: PiRpcConnection = yield* makePiRpcConnection({
        command: scopedLaunch.command,
        args: scopedLaunch.args,
        cwd,
        env: launch.env,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.mapError(
          (cause) =>
            new ProviderAdapter.ProviderAdapterOpenSessionError({
              driver: PI_PROVIDER,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );
      // Detached children can keep stdout open after Pi exits.
      if (scopeUnit !== undefined) {
        yield* connection.exited.pipe(
          Effect.matchEffect({ onFailure: () => stopScope, onSuccess: () => stopScope }),
          Effect.forkIn(scope),
        );
      }
      const now = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: PI_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: PiProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      const pendingPrompts = new Map<string, PendingPiPrompt>();
      const sessionApprovals = new Set<string>();
      // Pi can settle immediately after dialog response; serialize so terminal cannot overtake request resolution.
      const sessionEventPermit = yield* Semaphore.make(1);
      let threadState: PiThreadState | null = null;
      let registrationAttempted = false;
      let lastNativeThreadId: string | null = null;
      let stopRequested = false;
      let unsolicitedActivityDetected = false;
      let pendingWake: PiWake | null = null;
      let rollbackBarrier: Deferred.Deferred<
        void,
        ProviderAdapter.ProviderAdapterProtocolError
      > | null = null;
      let closed = false;
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          closed = true;
          pendingWake = null;
          if (rollbackBarrier !== null)
            yield* Deferred.fail(
              rollbackBarrier,
              protocolError("Pi session closed during rollback"),
            );
        }),
      );
      let appliedModel: string | null = null;
      let appliedThinking: string | null = null;
      let appliedSessionName: string | null = null;
      const outOfTurnExtensionErrors: Array<PiRpcRecord> = [];
      let lastKnownLeaf: string | null = null;
      // Failed get_entries leaves an untrustworthy turn boundary; re-sync the cursor on next capture.
      let leafCursorStale = false;
      // Pi has no unset commands; capture initial defaults so Pi default can replay them.
      let baselineModel: { provider: string; modelId: string } | null = null;
      let baselineThinking: string | null = null;
      let contextWindow: number | null = null;
      const modelContextWindows = new Map<string, number>();
      let modelsDiscovered = false;
      // Prompt ACKs lack IDs; retain session send order and ownership so late ACKs cannot affect new turns.
      const pendingPromptResponses: Array<{
        readonly providerTurnId: OrchestrationV2ProviderTurn["id"];
        readonly kind: "turn_start" | "steer";
      }> = [];
      const pendingCompactResponses: Array<{
        readonly providerTurnId: OrchestrationV2ProviderTurn["id"];
        readonly kind: "turn_start" | "steer";
      }> = [];

      const compactRpcRecord = (command: PiCompactCommand): PiRpcRecord =>
        command.customInstructions === undefined
          ? { type: "compact" }
          : { type: "compact", customInstructions: command.customInstructions };

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const subagentRuns = new Map<
        string,
        {
          turn: ActivePiTurn;
          toolCallId?: string | undefined;
          awaitingFirstChild?: boolean;
          firstChildDeadline?: number;
          observationExpired?: boolean;
          confirmedLaunch?: boolean;
          executionMode?: "attached" | "async";
          foregroundActive?: boolean;
          asyncDir?: string | undefined;
          lifecycle?: PiSubagentLifecycle;
          workflow?: boolean;
          nativeStatus?: string;
          finalLaunchEvent?: Extract<
            ProviderAdapter.ProviderAdapterV2Event,
            { type: "turn_item.updated" }
          >;
          childHints?: Map<string, { asyncDir: string; workflowKey: string }>;
          children?: Map<string, { workflowKey: string; selfChildId: string; canonicalId: string }>;
        }
      >();
      type NativeSubagent = {
        observation: PiSubagentObservation;
        turn: ActivePiTurn;
        subagent: OrchestrationV2Subagent;
        childThread: OrchestrationV2AppThread;
        providerThread: OrchestrationV2ProviderThread;
        rootNodeId: NodeId;
        items: Map<string, { ordinal: number; fingerprint: string }>;
        inspectionPhase: "requesting" | "delivering" | null;
        refreshNeeded: boolean;
        observationClosed?: boolean;
      };
      const nativeSubagents = new Map<string, NativeSubagent>();
      const retiredNativeKeys = new Set<string>();
      const nativeMaintenanceChanged = yield* Queue.sliding<void>(1);
      let drainedStreamFailure: ProviderAdapter.ProviderAdapterEventStreamError | null = null;
      const pendingDeliveries = new Map<
        ProviderAdapter.ProviderAdapterV2Event,
        {
          runId: RunId;
          acknowledged?: Deferred.Deferred<void>;
        }
      >();
      const subagentRootEvent = (child: NativeSubagent, now: DateTime.Utc) => ({
        type: "node.updated" as const,
        driver: PI_PROVIDER,
        node: {
          id: child.rootNodeId,
          threadId: child.childThread.id,
          runId: null,
          parentNodeId: null,
          rootNodeId: child.rootNodeId,
          kind: "root_turn" as const,
          status: child.subagent.status,
          countsForRun: false,
          providerThreadId: child.providerThread.id,
          providerTurnId: null,
          nativeItemRef: child.subagent.nativeTaskRef,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: child.subagent.startedAt,
          completedAt: child.subagent.completedAt === null ? null : now,
        },
      });
      type BackgroundWorkStatus = "active" | "inactive" | "unavailable";
      let livenessReady = false;
      let livenessSequence = 0;
      const livenessRequests = new Map<string, Deferred.Deferred<BackgroundWorkStatus>>();
      const inspectionRequests = new Map<
        string,
        {
          readonly runId: string;
          readonly childId: string;
          readonly reply: Deferred.Deferred<typeof PiSubagentInspectReply.Type>;
        }
      >();
      const inspectSubagent = Effect.fn("Pi.inspectSubagent")(function* ({
        providerThread,
        subagent,
        inspectionTarget,
      }: {
        providerThread: OrchestrationV2ProviderThread;
        subagent: OrchestrationV2Subagent;
        inspectionTarget?: PiSubagentObservation["inspectionTarget"];
      }) {
        const unavailable = (reason: string) => ({
          availability: "unavailable" as const,
          reason,
          messages: [],
          truncated: false,
        });
        const prefix = `subagent:${providerThread.id}:`;
        const nativeId = subagent.nativeTaskRef?.nativeId;
        if (closed || !nativeId?.startsWith(prefix))
          return unavailable("Inspection is not available for this agent.");
        const target = Schema.decodeUnknownOption(
          Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String])),
        )(nativeId.slice(prefix.length));
        if (Option.isNone(target)) return unavailable("The native agent reference is unavailable.");
        const [runId, childId] =
          inspectionTarget === undefined
            ? target.value
            : [inspectionTarget.runId, inspectionTarget.childId];
        if (
          [runId, childId].some(
            (id) => !id || id.length > 256 || /\s/.test(id) || id.startsWith("--"),
          )
        )
          return unavailable("The native agent reference cannot be inspected.");
        const requestId = `inspect-${++livenessSequence}`;
        const reply = yield* Deferred.make<typeof PiSubagentInspectReply.Type>();
        inspectionRequests.set(requestId, { runId, childId, reply });
        return yield* request({
          type: "prompt",
          message: `/subagents-inspect-rpc ${requestId} ${runId} ${childId} --lines 100`,
        }).pipe(
          Effect.andThen(Deferred.await(reply)),
          Effect.map((response) =>
            response.error
              ? unavailable(response.error.message)
              : {
                  availability: "available" as const,
                  ...(response.task === undefined ? {} : { task: response.task }),
                  ...(response.finalOutput === undefined ? {} : { output: response.finalOutput }),
                  messages: response.messages ?? [],
                  truncated:
                    response.truncated !== undefined &&
                    (response.truncated.task ||
                      response.truncated.messages > 0 ||
                      response.truncated.finalOutput),
                },
          ),
          Effect.timeoutOrElse({
            duration: Duration.seconds(5),
            orElse: () =>
              Effect.succeed(
                unavailable(
                  "The native inspector did not respond. The extension may not support inspection.",
                ),
              ),
          }),
          Effect.catchCause(() =>
            Effect.succeed(unavailable("The provider session is no longer available.")),
          ),
          Effect.ensuring(Effect.sync(() => inspectionRequests.delete(requestId))),
        );
      });
      const observeBackgroundWork = Effect.fn("Pi.observeBackgroundWork")(function* () {
        if (!livenessReady || closed) return "unavailable" as const;
        const requestId = `observe-${++livenessSequence}`;
        const reply = yield* Deferred.make<BackgroundWorkStatus>();
        livenessRequests.set(requestId, reply);
        return yield* request({
          type: "prompt",
          message: `/t3-subagent-liveness ${requestId}`,
        }).pipe(
          Effect.andThen(Deferred.await(reply)),
          Effect.timeoutOrElse({
            duration: Duration.seconds(5),
            orElse: () => Effect.succeed("unavailable" as const),
          }),
          Effect.catchCause(() => Effect.succeed("unavailable" as const)),
          Effect.ensuring(Effect.sync(() => livenessRequests.delete(requestId))),
        );
      });
      const hasObservedChildren = () =>
        Array.from(nativeSubagents.values()).some(
          ({ subagent, inspectionPhase, refreshNeeded }) =>
            inspectionPhase === "requesting" ||
            refreshNeeded ||
            subagent.status === "running" ||
            subagent.status === "pending" ||
            subagent.status === "waiting",
        );

      const updateProviderSession =
        /* Provider events include streaming deltas; avoid a span for each update. */
        Effect.fnUntraced(function* (
          status: OrchestrationV2ProviderSession["status"],
          lastError: string | null = sessionEntity.lastError,
        ) {
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, status, lastError, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: PI_PROVIDER,
            providerSession: sessionEntity,
          });
        });

      const updateProviderThread =
        /* Provider events include streaming deltas; avoid a span for each update. */
        Effect.fnUntraced(function* (
          state: PiThreadState,
          patch: Partial<OrchestrationV2ProviderThread>,
          retainedNativeTurnIds?: ReadonlyArray<string>,
        ) {
          const updatedAt = yield* DateTime.now;
          state.providerThread = { ...state.providerThread, ...patch, updatedAt };
          yield* emit({
            type: "provider_thread.updated",
            driver: PI_PROVIDER,
            providerThread: state.providerThread,
            ...(retainedNativeTurnIds === undefined ? {} : { retainedNativeTurnIds }),
          });
        });

      const itemOrdinal = (turn: ActivePiTurn, nativeItemId: string): number => {
        const existing = turn.itemOrdinals.get(nativeItemId);
        if (existing !== undefined) return existing;
        const ordinal = turn.nextItemOrdinal++;
        turn.itemOrdinals.set(nativeItemId, ordinal);
        return ordinal;
      };

      const request = (record: PiRpcRecord, timeoutMs = PI_REQUEST_TIMEOUT_MS) =>
        connection.request(record, timeoutMs);

      const nonNegativeInteger = (input: unknown, key: string): number | undefined => {
        const value = recordNumber(input, key);
        return value === undefined ? undefined : Math.max(0, Math.trunc(value));
      };

      const rememberModelContextWindow = (model: unknown): number | null => {
        const provider = recordString(model, "provider");
        const id = recordString(model, "id");
        const capacity = nonNegativeInteger(model, "contextWindow");
        if (provider !== undefined && id !== undefined && capacity !== undefined && capacity > 0) {
          modelContextWindows.set(`${provider}/${id}`, capacity);
          return capacity;
        }
        return null;
      };

      const lifecycleRequest = (record: PiRpcRecord) =>
        request(record, PI_SESSION_TIMEOUT_MS).pipe(
          // Timeout does not cancel Pi's lifecycle hook; retire the process before fallback can race it.
          Effect.tapError((error) =>
            Effect.logWarning("Pi session lifecycle request failed", {
              providerSessionId: input.providerSessionId,
              operation: record["type"],
              errorTag: error._tag,
            }),
          ),
          Effect.catchTags({
            PiRpcTimeoutError: (error) =>
              connection.terminate.pipe(Effect.andThen(Effect.fail(error))),
          }),
          Effect.onInterrupt(() => connection.terminate),
        );

      const tokenUsageFromStats = (
        stats: unknown,
        fallbackUsedTokens: number | null,
        updatedAt: DateTime.Utc,
      ): OrchestrationV2ProviderTurnTokenUsage | undefined => {
        const contextUsage = recordField(stats, "contextUsage");
        const maxTokens = nonNegativeInteger(contextUsage, "contextWindow");
        const usedTokens =
          nonNegativeInteger(contextUsage, "tokens") ?? fallbackUsedTokens ?? undefined;
        if (usedTokens === undefined || maxTokens === undefined || maxTokens === 0)
          return undefined;

        const totals = recordField(stats, "tokens");
        const inputTokens = nonNegativeInteger(totals, "input");
        const cachedInputTokens = nonNegativeInteger(totals, "cacheRead");
        const outputTokens = nonNegativeInteger(totals, "output");
        return {
          usedTokens,
          maxTokens,
          ...(inputTokens === undefined ? {} : { inputTokens }),
          ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
          ...(outputTokens === undefined ? {} : { outputTokens }),
          updatedAt: DateTime.formatIso(updatedAt),
        };
      };

      const readTokenUsage = (fallbackUsedTokens: number | null, updatedAt: DateTime.Utc) =>
        request({ type: "get_session_stats" }, 2_000).pipe(
          Effect.map((stats) => tokenUsageFromStats(stats, fallbackUsedTokens, updatedAt)),
          Effect.orElseSucceed(() => undefined),
        );

      /**
       * Pi attaches the current message's cumulative usage to every streaming update (0.84.2+).
       */
      const reportLiveUsage =
        /* Provider events include streaming deltas; avoid a span for each update. */
        Effect.fnUntraced(function* (turn: ActivePiTurn, usage: unknown) {
          const usedTokens = nonNegativeInteger(usage, "totalTokens");
          if (
            usedTokens === undefined ||
            usedTokens === 0 ||
            usedTokens === turn.lastLiveUsedTokens ||
            contextWindow === null ||
            contextWindow === 0
          ) {
            return;
          }
          turn.lastLiveUsedTokens = usedTokens;
          const inputTokens = nonNegativeInteger(usage, "input");
          const cachedInputTokens = nonNegativeInteger(usage, "cacheRead");
          const outputTokens = nonNegativeInteger(usage, "output");
          const updatedAt = yield* DateTime.now;
          yield* emit({
            type: "provider_turn.updated",
            driver: PI_PROVIDER,
            threadId: turn.turnInput.threadId,
            providerTurn: {
              ...turn.providerTurn,
              tokenUsage: {
                usedTokens,
                maxTokens: contextWindow,
                ...(inputTokens === undefined ? {} : { inputTokens }),
                ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
                ...(outputTokens === undefined ? {} : { outputTokens }),
                updatedAt: DateTime.formatIso(updatedAt),
              },
            },
          });
        });

      const baseItemFields = (
        turn: ActivePiTurn,
        nativeItemId: string,
        startedAt: DateTime.Utc,
        updatedAt: DateTime.Utc,
      ) => ({
        id: idAllocator.derive.turnItemFromProviderItem({
          driver: PI_PROVIDER,
          nativeItemId,
        }),
        threadId: turn.turnInput.threadId,
        runId: turn.turnInput.runId,
        nodeId: idAllocator.derive.nodeFromProviderItem({
          driver: PI_PROVIDER,
          nativeItemId,
        }),
        providerThreadId: turn.turnInput.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: providerRef(nativeItemId),
        parentItemId: null,
        ordinal: itemOrdinal(turn, nativeItemId),
        startedAt,
        updatedAt,
      });

      const emitItemNode = (
        turn: ActivePiTurn,
        nativeItemId: string,
        kind: OrchestrationV2ExecutionNode["kind"],
        status: OrchestrationV2ExecutionNode["status"],
        startedAt: DateTime.Utc,
        completedAt: DateTime.Utc | null,
      ) =>
        emit({
          type: "node.updated",
          driver: PI_PROVIDER,
          node: {
            id: idAllocator.derive.nodeFromProviderItem({
              driver: PI_PROVIDER,
              nativeItemId,
            }),
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId: turn.turnInput.rootNodeId,
            rootNodeId: turn.turnInput.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: providerRef(nativeItemId),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt,
            completedAt,
          },
        });

      const emitProviderRetry = Effect.fnUntraced(function* (
        turn: ActivePiTurn,
        providerRetry: PiProviderRetryState,
        status: "running" | "completed" | "failed" | "interrupted" | "cancelled",
        updatedAt: DateTime.Utc,
      ) {
        yield* emit({
          type: "turn_item.updated",
          driver: PI_PROVIDER,
          turnItem: makeProviderRetryTurnItem({
            driver: PI_PROVIDER,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            nodeId: turn.turnInput.rootNodeId,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            itemOrdinal: providerRetry.itemOrdinal,
            failure: providerRetry.failure,
            retry: providerRetry.retry,
            status,
            startedAt: providerRetry.startedAt,
            updatedAt,
          }),
        });
      });

      const compactionNativeItemId = (turn: ActivePiTurn): string =>
        `compaction:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;

      const emitCompaction = Effect.fnUntraced(function* (
        turn: ActivePiTurn,
        compaction: PiCompactionState,
        status: PiCompactionStatus,
        details: {
          readonly summary?: string;
          readonly beforeTokenCount?: number;
          readonly afterTokenCount?: number;
        } = {},
      ) {
        const emittedAt = yield* DateTime.now;
        const completedAt = status === "running" ? null : emittedAt;
        yield* emitItemNode(
          turn,
          compaction.nativeItemId,
          "system",
          status,
          compaction.startedAt,
          completedAt,
        );
        yield* emit({
          type: "turn_item.updated",
          driver: PI_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, compaction.nativeItemId, compaction.startedAt, emittedAt),
            status,
            title: compactionTitle(status),
            completedAt,
            type: "compaction",
            driver: PI_PROVIDER,
            ...details,
          },
        });
      });

      const emitStreamItem =
        /* Provider events include streaming deltas; avoid a span for each update. */
        Effect.fnUntraced(function* (
          turn: ActivePiTurn,
          item: PiStreamItemState,
          streaming: boolean,
        ) {
          const emittedAt = yield* DateTime.now;
          const base = baseItemFields(turn, item.nativeItemId, item.startedAt, emittedAt);
          yield* emitItemNode(
            turn,
            item.nativeItemId,
            item.kind,
            streaming ? "running" : "completed",
            item.startedAt,
            streaming ? null : emittedAt,
          );
          if (item.kind === "assistant_message") {
            const messageId = idAllocator.derive.messageFromProviderItem({
              driver: PI_PROVIDER,
              nativeItemId: item.nativeItemId,
            });
            yield* emit({
              type: "turn_item.updated",
              driver: PI_PROVIDER,
              turnItem: {
                ...base,
                status: streaming ? "running" : "completed",
                title: null,
                completedAt: streaming ? null : emittedAt,
                type: "assistant_message",
                messageId,
                text: item.text,
                streaming,
              },
            });
            yield* emit({
              type: "message.updated",
              driver: PI_PROVIDER,
              message: {
                id: messageId,
                threadId: turn.turnInput.threadId,
                runId: turn.turnInput.runId,
                nodeId: idAllocator.derive.nodeFromProviderItem({
                  driver: PI_PROVIDER,
                  nativeItemId: item.nativeItemId,
                }),
                role: "assistant",
                text: item.text,
                attachments: [],
                streaming,
                createdBy: "agent",
                creationSource: "provider",
                createdAt: item.startedAt,
                updatedAt: emittedAt,
              },
            });
            return;
          }
          yield* emit({
            type: "turn_item.updated",
            driver: PI_PROVIDER,
            turnItem: {
              ...base,
              status: streaming ? "running" : "completed",
              title: null,
              completedAt: streaming ? null : emittedAt,
              type: "reasoning",
              text: item.text,
              streaming,
            },
          });
        });

      const scheduleStreamFlush =
        /* Provider events include streaming deltas; avoid a span for each update. */
        Effect.fnUntraced(function* (turn: ActivePiTurn, item: PiStreamItemState) {
          if (item.flushScheduled || item.completed) return;
          item.flushScheduled = true;
          yield* Effect.sleep(Duration.millis(STREAM_FLUSH_MS)).pipe(
            Effect.andThen(
              Effect.suspend(() => {
                item.flushScheduled = false;
                return item.completed ? Effect.void : emitStreamItem(turn, item, true);
              }),
            ),
            Effect.forkIn(scope),
          );
        });

      const streamItemFor = Effect.fnUntraced(function* (
        turn: ActivePiTurn,
        kind: PiStreamItemState["kind"],
        contentIndex: number,
      ) {
        const nativeItemId = `${turn.providerTurn.id}:m${turn.messageOrdinal}:c${contentIndex}`;
        const existing = turn.streamItems.get(nativeItemId);
        if (existing !== undefined) return existing;
        const startedAt = yield* DateTime.now;
        const item: PiStreamItemState = {
          nativeItemId,
          kind,
          text: "",
          completed: false,
          flushScheduled: false,
          startedAt,
        };
        turn.streamItems.set(nativeItemId, item);
        itemOrdinal(turn, nativeItemId);
        return item;
      });

      const completeStreamItem = (turn: ActivePiTurn, item: PiStreamItemState, text?: string) =>
        Effect.suspend(() => {
          if (item.completed) return Effect.void;
          item.completed = true;
          if (text !== undefined && text.length > 0) item.text = text;
          return item.text.length === 0 ? Effect.void : emitStreamItem(turn, item, false);
        });

      const completeOpenStreamItems = (turn: ActivePiTurn) =>
        Effect.forEach(
          Array.from(turn.streamItems.values()).filter((item) => !item.completed),
          (item) => completeStreamItem(turn, item),
          { discard: true },
        );

      const emitToolItem = Effect.fnUntraced(function* (
        turn: ActivePiTurn,
        event: PiRpcRecord,
        phase: "start" | "update" | "end",
      ) {
        const toolCallId = recordString(event, "toolCallId");
        const toolName = recordString(event, "toolName") ?? "tool";
        if (toolCallId === undefined) return;
        if (toolName === "subagent" && phase !== "start") {
          const details = recordField(
            phase === "end" ? event["result"] : event["partialResult"],
            "details",
          );
          const runId = recordString(details, "runId") ?? recordString(details, "asyncId");
          const binding = runId === undefined ? undefined : subagentRuns.get(runId);
          // After replay-key eviction, only an actual current tool start can admit an unknown callback.
          if (
            binding?.observationExpired ||
            (runId !== undefined && retiredNativeKeys.has(runId)) ||
            (!turn.toolArgs.has(toolCallId) && binding?.turn !== turn)
          )
            return;
        }
        if (phase === "start") {
          turn.toolArgs.set(toolCallId, event["args"]);
        }
        const args = event["args"] ?? turn.toolArgs.get(toolCallId);
        const emittedAt = yield* DateTime.now;
        const startedAt = turn.toolStartedAt.get(toolCallId) ?? emittedAt;
        turn.toolStartedAt.set(toolCallId, startedAt);
        const completed = phase === "end";
        const isError = event["isError"] === true;
        const resultRecord = completed ? event["result"] : event["partialResult"];
        const outputText = contentText(recordField(resultRecord, "content"));
        const status = completed
          ? isError
            ? turn.interrupted
              ? "interrupted"
              : "failed"
            : "completed"
          : "running";
        const base = baseItemFields(turn, toolCallId, startedAt, emittedAt);
        yield* emitItemNode(
          turn,
          toolCallId,
          "tool_call",
          status,
          startedAt,
          completed ? emittedAt : null,
        );
        const shared = {
          ...base,
          status,
          completedAt: completed ? emittedAt : null,
        } as const;
        if (toolName === "bash" || toolName === "powershell") {
          const exitCode =
            recordNumber(recordField(resultRecord, "structuredContent"), "exit_code") ??
            recordNumber(recordField(resultRecord, "details"), "exitCode");
          yield* emit({
            type: "turn_item.updated",
            driver: PI_PROVIDER,
            turnItem: {
              ...shared,
              title: toolName,
              type: "command_execution",
              input: recordString(args, "command") ?? "",
              ...(outputText.length > 0 ? { output: outputText } : {}),
              ...(exitCode === undefined ? {} : { exitCode }),
            },
          });
          return;
        }
        if (toolName === "edit" || toolName === "write") {
          const fileName = recordString(args, "path") ?? recordString(args, "file_path");
          if (fileName !== undefined) {
            const diffStr =
              recordString(recordField(resultRecord, "details"), "patch") ??
              (isError && outputText.trim().length > 0 ? outputText : undefined);
            const newStr = toolName === "write" ? recordString(args, "content") : undefined;
            yield* emit({
              type: "turn_item.updated",
              driver: PI_PROVIDER,
              turnItem: {
                ...shared,
                title: toolName,
                type: "file_change",
                fileName,
                ...(diffStr === undefined ? {} : { diffStr }),
                ...(newStr === undefined ? {} : { newStr }),
              },
            });
            return;
          }
        }
        const output = piToolOutput(resultRecord, toolName);
        const toolEvent = {
          type: "turn_item.updated",
          driver: PI_PROVIDER,
          turnItem: {
            ...shared,
            title: toolName,
            type: "dynamic_tool",
            ...mcpToolPresentation({ toolName }),
            toolName,
            input: args ?? {},
            ...(output === undefined ? {} : { output }),
          },
        } satisfies ProviderAdapter.ProviderAdapterV2Event;
        yield* emit(toolEvent);
        if (toolName === "subagent") {
          yield* emitSubagentTasks(turn, toolCallId, resultRecord, completed, !isError);
          if (completed) {
            for (const binding of subagentRuns.values())
              if (
                binding.confirmedLaunch &&
                binding.turn === turn &&
                binding.toolCallId === toolCallId
              )
                binding.finalLaunchEvent = toolEvent;
          }
        }
      });

      const emitSubagentTasks = Effect.fnUntraced(function* (
        turn: ActivePiTurn,
        toolCallId: string,
        resultRecord: unknown,
        completed: boolean,
        successful = true,
      ) {
        const details = recordField(resultRecord, "details");
        const args = turn.toolArgs.get(toolCallId);
        const action = recordString(args, "action");
        const asyncId = piSubagentMetadataString(details, "asyncId");
        const runId = recordString(details, "runId") ?? recordString(details, "asyncId");
        const binding = runId === undefined ? undefined : subagentRuns.get(runId);
        if (binding?.observationExpired || (runId !== undefined && retiredNativeKeys.has(runId)))
          return;
        // Resume allocates a new async run; its target is not the execution being launched.
        const resumes =
          action === "resume" &&
          completed &&
          successful &&
          recordField(resultRecord, "isError") !== true &&
          turn.toolArgs.has(toolCallId) &&
          recordString(details, "mode") === "single" &&
          asyncId !== undefined &&
          runId === asyncId &&
          asyncId !== recordString(args, "id") &&
          asyncId !== recordString(args, "runId") &&
          (binding === undefined ||
            (binding.turn === turn &&
              (binding.toolCallId === toolCallId ||
                (binding.toolCallId === undefined && binding.confirmedLaunch !== true)))) &&
          ![...subagentRuns].some(
            ([id, run]) =>
              id !== asyncId &&
              run.confirmedLaunch &&
              run.turn === turn &&
              run.toolCallId === toolCallId,
          );
        if (action === "resume" && !resumes) return;
        const launches = action === undefined || resumes;
        if (!launches && binding === undefined) return;
        if (
          runId !== undefined &&
          launches &&
          (binding === undefined || (binding.turn === turn && binding.toolCallId === undefined))
        ) {
          subagentRuns.set(runId, {
            ...binding,
            turn,
            toolCallId,
            confirmedLaunch: true,
            asyncDir: piSubagentMetadataString(details, "asyncDir", 4096) ?? binding?.asyncDir,
          });
        }
        const launchBinding = runId === undefined ? undefined : subagentRuns.get(runId);
        if (launches && launchBinding?.turn === turn && launchBinding.toolCallId === toolCallId) {
          // The foreground callback adds runId even when defaults selected its mode; async receipts use asyncId.
          if (recordString(details, "asyncId") === runId) launchBinding.executionMode = "async";
          else if (
            launchBinding.executionMode === undefined &&
            recordString(details, "asyncId") === undefined &&
            (recordField(turn.toolArgs.get(toolCallId), "async") === false ||
              (!completed &&
                turn.toolArgs.has(toolCallId) &&
                recordString(details, "runId") === runId)) &&
            ["single", "parallel", "chain", "workflow"].includes(
              recordString(details, "mode") ?? "",
            )
          )
            launchBinding.executionMode = "attached";
          // A late child callback cannot reopen its already-settled foreground tool.
          if (
            launchBinding.executionMode === "attached" &&
            (completed || launchBinding.foregroundActive === undefined)
          )
            launchBinding.foregroundActive = !completed;
        }
        // Only a confirmed async receipt creates this pre-child obligation, once per native run.
        if (
          launches &&
          recordString(details, "asyncId") !== undefined &&
          launchBinding !== undefined &&
          launchBinding.awaitingFirstChild === undefined
        ) {
          launchBinding.awaitingFirstChild = ![...nativeSubagents.values()].some(
            (child) => child.observation.runId === runId,
          );
          if (launchBinding.awaitingFirstChild)
            launchBinding.firstChildDeadline =
              DateTime.toEpochMillis(yield* DateTime.now) + NATIVE_OBSERVATION_WINDOW_MS;
        }
        const observations = piSubagentResults(resultRecord, toolCallId, completed).map(
          canonicalizeSubagentObservation,
        );
        const fences =
          launchBinding?.executionMode === "async"
            ? undefined
            : new Map<NativeSubagent, ProviderAdapter.ProviderAdapterV2Event>();
        yield* emitSubagentObservations(
          observations,
          binding?.turn ?? (launches ? turn : null),
          binding?.toolCallId ?? (launches ? toolCallId : undefined),
          false,
          fences,
        );
        const results = recordField(details, "results");
        if (Array.isArray(results)) {
          for (const observation of observations) {
            const child = nativeSubagents.get(observation.id);
            const row =
              observation.resultPosition === undefined
                ? undefined
                : results[observation.resultPosition];
            if (child !== undefined && (fences === undefined || fences.has(child)))
              yield* emitSubagentConversation(child, [
                ...piSubagentMessages(recordField(row, "messages")),
                ...piSubagentToolSummaries(recordField(row, "toolCalls")),
              ]);
          }
        }
        // Compact tool summaries follow the observation's final text in the same delivery batch.
        for (const fence of fences?.values() ?? []) yield* emit(fence);
      });

      // Both native windows and foreground results feed the existing child-chat projection.
      const emitSubagentConversation: (
        child: NativeSubagent,
        messages: ReadonlyArray<PiSubagentMessage>,
        task?: string,
        output?: string,
      ) => Effect.Effect<void> = Effect.fnUntraced(function* (
        child: NativeSubagent,
        messages: ReadonlyArray<PiSubagentMessage>,
        task?: string,
        output?: string,
      ) {
        const now = yield* DateTime.now;
        const occurrences = new Map<string, number>();
        const rows: ReadonlyArray<PiSubagentMessage> = [
          ...(task || child.subagent.prompt
            ? [{ role: "user", kind: "text" as const, text: task || child.subagent.prompt }]
            : []),
          ...messages.filter(
            (message) =>
              message.role !== "user" || message.text !== (task || child.subagent.prompt),
          ),
          ...(output && !messages.some((message) => message.text === output)
            ? [{ role: "assistant", kind: "text" as const, text: output }]
            : []),
        ];
        let hasUnknownToolOutcome = false;
        for (const message of rows) {
          const hash = idAllocator.derive.turnItemFromProviderItem({
            driver: PI_PROVIDER,
            nativeItemId: yield* Schema.encodeEffect(Schema.fromJsonString(PiSubagentMessage))(
              message,
            ).pipe(Effect.orDie),
          });
          const occurrence = occurrences.get(hash) ?? 0;
          occurrences.set(hash, occurrence + 1);
          const nativeItemId = `${child.subagent.id}:${hash}:${occurrence}`;
          const prior = child.items.get(nativeItemId);
          // Bounded history has no live call identity, even while the child is running.
          const fingerprint = hash;
          if (prior?.fingerprint === fingerprint) continue;
          const ordinal = prior?.ordinal ?? (child.items.size + 1) * 100;
          child.items.set(nativeItemId, {
            ordinal,
            fingerprint,
          });
          const turnItemId = idAllocator.derive.turnItemFromProviderItem({
            driver: PI_PROVIDER,
            nativeItemId,
          });
          if (message.role === "system") {
            yield* emit({
              type: "turn_item.updated",
              driver: PI_PROVIDER,
              turnItem: {
                id: turnItemId,
                threadId: child.childThread.id,
                runId: null,
                nodeId: child.rootNodeId,
                providerThreadId: child.providerThread.id,
                providerTurnId: null,
                nativeItemRef: providerRef(nativeItemId),
                parentItemId: null,
                ordinal,
                status: "completed",
                title: null,
                type: "system_notice",
                message: message.text,
                startedAt: now,
                completedAt: now,
                updatedAt: now,
              },
            });
          } else if (
            message.kind === "text" &&
            (message.role === "user" || message.role === "assistant")
          ) {
            const artifacts = makeSubagentConversationArtifacts({
              messageId: idAllocator.derive.messageFromProviderItem({
                driver: PI_PROVIDER,
                nativeItemId,
              }),
              turnItemId,
              threadId: child.childThread.id,
              rootNodeId: child.rootNodeId,
              providerThreadId: child.providerThread.id,
              providerTurnId: null,
              nativeItemRef: providerRef(nativeItemId),
              ...(child.childThread.lineage.parentThreadId === null
                ? {}
                : { senderThreadId: child.childThread.lineage.parentThreadId }),
              role: message.role,
              text: message.text,
              ordinal,
              now,
            });
            yield* emit({
              type: "message.updated",
              driver: PI_PROVIDER,
              message: artifacts.message,
            });
            yield* emit({
              type: "turn_item.updated",
              driver: PI_PROVIDER,
              turnItem: artifacts.turnItem,
            });
          } else {
            const turnItem = {
              id: turnItemId,
              threadId: child.childThread.id,
              runId: null,
              nodeId: child.rootNodeId,
              providerThreadId: child.providerThread.id,
              providerTurnId: null,
              nativeItemRef: providerRef(nativeItemId),
              parentItemId: null,
              ordinal,
              status: message.isError
                ? "failed"
                : message.kind === "toolCall"
                  ? "idle"
                  : "completed",
              title: message.name ?? "Tool result",
              type: "dynamic_tool",
              toolName: message.name ?? null,
              input: message.kind === "toolCall" ? message.text : null,
              ...(message.kind === "toolCall" ? {} : { output: message.text }),
              startedAt: now,
              completedAt: now,
              updatedAt: now,
            } satisfies Extract<OrchestrationV2TurnItem, { type: "dynamic_tool" }>;
            if (turnItem.status === "idle") hasUnknownToolOutcome = true;
            yield* emit({ type: "turn_item.updated", driver: PI_PROVIDER, turnItem });
          }
        }
        if (hasUnknownToolOutcome)
          yield* emitSubagentConversation(child, [
            { role: "system", kind: "text", text: PI_SUBAGENT_UNKNOWN_TOOL_OUTCOME },
          ]);
      });

      const refreshSubagentConversation = Effect.fn("Pi.refreshSubagentConversation")(function* (
        child: NativeSubagent,
      ) {
        if (child.inspectionPhase !== null) {
          child.refreshNeeded = true;
          return;
        }
        child.inspectionPhase = "requesting";
        yield* Effect.gen(function* () {
          do {
            child.refreshNeeded = false;
            child.inspectionPhase = "requesting";
            const result = yield* inspectSubagent({
              providerThread: child.turn.turnInput.providerThread,
              subagent: child.subagent,
              inspectionTarget: child.observation.inspectionTarget,
            });
            yield* sessionEventPermit.withPermit(
              Effect.gen(function* () {
                if (child.observationClosed) return;
                yield* result.availability === "available"
                  ? emitSubagentConversation(child, result.messages, result.task, result.output)
                  : emitSubagentConversation(child, [
                      {
                        role: "system",
                        kind: "text",
                        text: result.reason ?? "Native worker activity is unavailable.",
                      },
                    ]);
                const now = yield* DateTime.now;
                const fence = subagentRootEvent(child, now);
                pendingDeliveries.set(fence, { runId: child.turn.turnInput.runId });
                child.inspectionPhase = "delivering";
                // FIFO ingestion, not the queue offer, releases this capture's ownership.
                yield* emit(fence);
              }),
            );
          } while (child.refreshNeeded && !closed);
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              child.inspectionPhase = null;
            }).pipe(Effect.andThen(Queue.offer(nativeMaintenanceChanged, undefined))),
          ),
        );
      });

      const readSubagentLifecycle = Effect.fn("Pi.readSubagentLifecycle")(function* (
        asyncDir: string,
        runId: string,
      ) {
        return yield* Effect.gen(function* () {
          const chunks = yield* fileSystem
            .stream(`${asyncDir}/status.json`, { bytesToRead: PI_SUBAGENT_LIFECYCLE_MAX_BYTES + 1 })
            .pipe(Stream.runCollect);
          const size = chunks.reduce((total, chunk) => total + chunk.length, 0);
          if (size > PI_SUBAGENT_LIFECYCLE_MAX_BYTES) return undefined;
          const bytes = new Uint8Array(size);
          let offset = 0;
          for (const chunk of chunks) {
            bytes.set(chunk, offset);
            offset += chunk.length;
          }
          return piSubagentLifecycle(new TextDecoder().decode(bytes), runId);
        }).pipe(Effect.catchCause(() => Effect.succeed(undefined)));
      });

      const canonicalizeSubagentObservation = (
        rawObservation: PiSubagentObservation,
      ): PiSubagentObservation => {
        let parentId = rawObservation.parentId;
        // Only reciprocal lifecycle proofs install these grouped/promoted aliases.
        for (const [parentRunId, binding] of subagentRuns) {
          for (const [childRunId, child] of binding.children ?? []) {
            if (
              parentId === piSubagentObservationId(childRunId, child.selfChildId) ||
              parentId === piSubagentObservationId(parentRunId, child.workflowKey)
            )
              parentId = child.canonicalId;
          }
        }
        const observation =
          parentId === rawObservation.parentId ? rawObservation : { ...rawObservation, parentId };
        for (const [parentRunId, binding] of subagentRuns) {
          for (const [childRunId, child] of binding.children ?? []) {
            const grouped =
              observation.runId === parentRunId && observation.childId === child.workflowKey;
            const promoted =
              observation.runId === childRunId && observation.childId === child.selfChildId;
            if (!grouped && !promoted) continue;
            const existing = nativeSubagents.get(child.canonicalId);
            return {
              ...observation,
              id: child.canonicalId,
              runId: parentRunId,
              childId: child.workflowKey,
              parentId:
                existing === undefined ? observation.parentId : existing.observation.parentId,
              inspectionTarget: { runId: childRunId, childId: child.selfChildId },
            };
          }
        }
        return observation;
      };

      const reconcileSubagentSnapshot = Effect.fn("Pi.reconcileSubagentSnapshot")(function* (
        lines: unknown,
      ) {
        const observations = piSubagentSnapshot(lines);
        for (const runId of new Set([
          ...piSubagentRunStates(lines).keys(),
          ...observations.map((observation) => observation.runId),
        ])) {
          const binding = subagentRuns.get(runId);
          if (binding !== undefined) binding.executionMode = "async";
        }
        const workflows = piSubagentWorkflowRunIds(lines);
        for (const runId of workflows) {
          const binding = subagentRuns.get(runId);
          if (binding !== undefined) {
            binding.workflow = true;
          }
        }
        const hints = piSubagentLifecycleHints(lines);
        for (const hint of hints) {
          const binding = subagentRuns.get(hint.runId);
          if (binding?.confirmedLaunch !== true) continue;
          // child-status.asyncDir belongs to runId (the workflow), not childRunId.
          if (binding.asyncDir === undefined) binding.asyncDir = hint.asyncDir;
        }
        // Only explicit launch/hint directories are read. Run ids never become paths.
        const observedRuns = new Set([
          ...piSubagentRunStates(lines).keys(),
          ...observations.map((observation) => observation.runId),
          ...hints.map((hint) => hint.runId),
        ]);
        let remainingReads = 64;
        for (const runId of [...observedRuns].slice(0, 64)) {
          const binding = subagentRuns.get(runId);
          if (binding?.confirmedLaunch !== true || binding.asyncDir === undefined) continue;
          if (remainingReads-- <= 0) break;
          const lifecycle = yield* readSubagentLifecycle(binding.asyncDir, runId);
          if (lifecycle !== undefined) binding.lifecycle = lifecycle;
          if (binding.lifecycle?.mode !== "workflow") continue;
          binding.workflow = true;
          for (const hint of hints) {
            if (hint.childId !== undefined || hint.childRunId !== undefined) continue;
            const steps = binding.lifecycle.steps.filter(
              (step) => step.runId === hint.runId && step.workflowKey !== undefined,
            );
            if (steps.length !== 1) continue;
            binding.childHints ??= new Map();
            if (binding.childHints.size < 64 || binding.childHints.has(hint.runId))
              binding.childHints.set(hint.runId, {
                asyncDir: hint.asyncDir,
                workflowKey: steps[0]!.workflowKey!,
              });
          }
          for (const [childRunId, hint] of binding.childHints ?? []) {
            if (binding.children?.has(childRunId)) continue;
            const steps = binding.lifecycle.steps.filter(
              (step) => step.runId === childRunId && step.workflowKey === hint.workflowKey,
            );
            if (steps.length !== 1) continue;
            const childBinding = subagentRuns.get(childRunId);
            // A later hint cannot steal a child already launched by another owner/tool.
            if (
              childBinding !== undefined &&
              (childBinding.turn !== binding.turn ||
                (childBinding.confirmedLaunch === true &&
                  childBinding.toolCallId !== binding.toolCallId))
            )
              continue;
            if (remainingReads-- <= 0) break;
            const childLifecycle = yield* readSubagentLifecycle(hint.asyncDir, childRunId);
            if (
              childLifecycle?.mode !== "single" ||
              childLifecycle.runId !== childRunId ||
              childLifecycle.parentWorkflowRunId !== runId ||
              childLifecycle.workflowKey !== hint.workflowKey ||
              childLifecycle.steps.length !== 1
            )
              continue;
            const groupedId = piSubagentObservationId(runId, hint.workflowKey);
            const selfChildId = childLifecycle.steps[0]!.childId;
            const rootId = piSubagentObservationId(childRunId, selfChildId);
            const root = nativeSubagents.get(rootId);
            if (root !== undefined && root.turn !== binding.turn) continue;
            binding.children ??= new Map();
            if (!binding.children.has(childRunId))
              binding.children.set(childRunId, {
                workflowKey: hint.workflowKey,
                selfChildId,
                canonicalId:
                  nativeSubagents.has(groupedId) || root === undefined ? groupedId : rootId,
              });
          }
        }
        return observations.flatMap((rawObservation): PiSubagentObservation[] => {
          const observation = canonicalizeSubagentObservation(rawObservation);
          if (observation.inspectionTarget !== undefined) return [observation];
          const binding = subagentRuns.get(observation.runId);
          // Unknown promoted roots may be the same worker as a retained workflow lane.
          // Without both lifecycle proofs, keep the original projection rather than guess.
          if (
            observation.parentId === null &&
            !nativeSubagents.has(observation.id) &&
            binding?.confirmedLaunch !== true &&
            !workflows.has(observation.runId) &&
            (workflows.size > 0 || [...subagentRuns.values()].some((run) => run.workflow))
          )
            return [];
          return [observation];
        });
      });

      // These upserts run in the serialized event pump, alongside streaming updates.
      const emitSubagentObservations = Effect.fnUntraced(function* (
        observations: ReadonlyArray<PiSubagentObservation>,
        fallbackTurn: ActivePiTurn | null,
        toolCallId?: string,
        fromSnapshot = false,
        foregroundFences?: Map<NativeSubagent, ProviderAdapter.ProviderAdapterV2Event>,
      ) {
        const emittedAt = yield* DateTime.now;
        const reserveForegroundDelivery = (child: NativeSubagent) => {
          if (foregroundFences === undefined || foregroundFences.has(child)) return;
          const fence = subagentRootEvent(child, emittedAt);
          pendingDeliveries.set(fence, { runId: child.turn.turnInput.runId });
          foregroundFences.set(child, fence);
        };
        for (const observation of observations) {
          const existing = nativeSubagents.get(observation.id);
          const binding = subagentRuns.get(observation.runId);
          if (
            stopRequested ||
            binding?.observationExpired ||
            retiredNativeKeys.has(observation.runId) ||
            existing?.observationClosed
          )
            continue;
          const parentAgent =
            observation.parentId === null ? undefined : nativeSubagents.get(observation.parentId);
          if (existing === undefined && parentAgent?.observationClosed) continue;
          // A snapshot includes retained history, not just work launched by this turn.
          if (
            fromSnapshot &&
            existing === undefined &&
            binding === undefined &&
            parentAgent === undefined
          )
            continue;
          const turn = existing?.turn ?? binding?.turn ?? parentAgent?.turn ?? fallbackTurn;
          if (turn === null) continue;
          const observationTime = observation.observedAt ?? observation.updatedAt;
          const previousTime = existing?.observation.observedAt ?? existing?.observation.updatedAt;
          if (
            observationTime !== undefined &&
            previousTime !== undefined &&
            observationTime < previousTime
          )
            continue;
          const changed =
            existing === undefined ||
            observation.status !== existing.subagent.status ||
            observation.paused !== existing.observation.paused ||
            observation.partial !== existing.observation.partial ||
            observation.title !== existing.subagent.title ||
            observation.parentId !== existing.observation.parentId ||
            observation.inspectionTarget?.runId !== existing.observation.inspectionTarget?.runId ||
            observation.inspectionTarget?.childId !==
              existing.observation.inspectionTarget?.childId ||
            // Snapshot generation advances on reads; only actual child activity refreshes its tail.
            (observation.updatedAt !== undefined &&
              observation.updatedAt !== existing.observation.updatedAt) ||
            (observation.prompt !== undefined && observation.prompt !== existing.subagent.prompt) ||
            (observation.model !== undefined && observation.model !== existing.subagent.model) ||
            (observation.progress !== undefined &&
              observation.progress !== existing.subagent.progress) ||
            (observation.result !== undefined && observation.result !== existing.subagent.result);
          if (!changed && existing !== undefined) {
            reserveForegroundDelivery(existing);
            existing.observation = observation;
            continue;
          }
          const wasTerminal =
            existing !== undefined &&
            !["running", "pending", "waiting", "idle"].includes(existing.subagent.status);
          if (wasTerminal && ["running", "pending", "waiting"].includes(observation.status))
            continue;
          subagentRuns.set(observation.runId, binding ?? { turn, toolCallId });
          const nativeTaskId = `subagent:${turn.turnInput.providerThread.id}:${observation.id}`;
          const subagentId = idAllocator.derive.nodeFromProviderItem({
            driver: PI_PROVIDER,
            nativeItemId: nativeTaskId,
          });
          const causalTool = binding?.toolCallId ?? toolCallId;
          const parentNodeId =
            (observation.inspectionTarget === undefined
              ? undefined
              : existing?.subagent.parentNodeId) ??
            parentAgent?.subagent.id ??
            (causalTool === undefined
              ? turn.turnInput.rootNodeId
              : idAllocator.derive.nodeFromProviderItem({
                  driver: PI_PROVIDER,
                  nativeItemId: causalTool,
                }));
          const startedAt = existing?.subagent.startedAt ?? emittedAt;
          const status = observation.status;
          const finished = !["pending", "running", "waiting", "idle"].includes(status);
          const progress =
            !finished && observation.progress !== undefined
              ? { progress: observation.progress }
              : {};
          const resultText = observation.result ?? existing?.subagent.result ?? null;
          const childThreadId =
            existing?.childThread.id ??
            idAllocator.derive.threadFromProviderThread({
              driver: PI_PROVIDER,
              nativeThreadId: nativeTaskId,
            });
          const childProviderThread = existing?.providerThread ?? {
            ...turn.turnInput.providerThread,
            id: idAllocator.derive.providerThread({
              driver: PI_PROVIDER,
              nativeThreadId: nativeTaskId,
            }),
            appThreadId: childThreadId,
            ownerNodeId: null,
            nativeThreadRef: providerRef(nativeTaskId),
            nativeConversationHeadRef: null,
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: {
              providerThreadId: turn.turnInput.providerThread.id,
              providerTurnId: turn.providerTurn.id,
            },
            createdAt: startedAt,
          };
          const childThread =
            existing?.childThread ??
            makeSubagentChildThread({
              parentThread: parentAgent?.childThread ?? turn.turnInput.appThread,
              childThreadId,
              parentNodeId: subagentId,
              activeProviderThreadId: childProviderThread.id,
              providerInstanceId: options.instanceId,
              modelSelection: {
                ...turn.turnInput.modelSelection,
                model: observation.model ?? turn.turnInput.modelSelection.model,
              },
              title: subagentThreadTitle({
                parentTitle: turn.turnInput.appThread.title,
                title: observation.title,
                prompt: observation.prompt ?? "",
                ordinal: nativeSubagents.size + 1,
              }),
              now: startedAt,
              createdBy: "agent",
              creationSource: "provider",
            });
          const childRootNodeId =
            existing?.rootNodeId ??
            idAllocator.derive.nodeFromProviderItem({
              driver: PI_PROVIDER,
              nativeItemId: `${nativeTaskId}:root`,
            });
          const subagent = {
            ...existing?.subagent,
            id: subagentId,
            threadId:
              (observation.inspectionTarget === undefined
                ? undefined
                : existing?.subagent.threadId) ??
              parentAgent?.childThread.id ??
              turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId,
            origin: "provider_native" as const,
            createdBy: "agent" as const,
            driver: PI_PROVIDER,
            providerInstanceId: options.instanceId,
            providerThreadId: childProviderThread.id,
            childThreadId,
            nativeTaskRef: providerRef(nativeTaskId),
            prompt: observation.prompt ?? existing?.subagent.prompt ?? "",
            title: observation.title,
            model: observation.model ?? existing?.subagent.model ?? null,
            status,
            ...progress,
            result: resultText,
            startedAt,
            completedAt: finished ? (existing?.subagent.completedAt ?? emittedAt) : null,
            updatedAt: emittedAt,
          };
          const child: NativeSubagent = existing ?? {
            observation,
            turn,
            subagent,
            childThread,
            providerThread: childProviderThread,
            rootNodeId: childRootNodeId,
            items: new Map(),
            inspectionPhase: null,
            refreshNeeded: false,
          };
          child.observation = observation;
          child.subagent = subagent;
          // Final transcript capture still belongs to the run after the child stops.
          if (fromSnapshot) child.refreshNeeded = true;
          reserveForegroundDelivery(child);
          nativeSubagents.set(observation.id, child);
          if (binding !== undefined) binding.awaitingFirstChild = false;
          yield* emit({
            type: "subagent.updated",
            driver: PI_PROVIDER,
            subagent,
          });
          // The owning run enrolls the child before its delayed native snapshot creates the chat.
          if (existing === undefined) {
            yield* emit({
              type: "app_thread.created",
              driver: PI_PROVIDER,
              appThread: childThread,
              createOnly: true,
            });
          }
          yield* emit({
            type: "provider_thread.updated",
            driver: PI_PROVIDER,
            providerThread: {
              ...childProviderThread,
              status: finished || status === "idle" ? "idle" : "active",
              updatedAt: emittedAt,
            },
          });
          yield* emit(subagentRootEvent(child, emittedAt));
          yield* emit({
            type: "turn_item.updated",
            driver: PI_PROVIDER,
            turnItem: {
              ...baseItemFields(turn, nativeTaskId, startedAt, emittedAt),
              threadId: subagent.threadId,
              status,
              title: observation.title,
              completedAt: subagent.completedAt,
              type: "subagent",
              subagentId,
              origin: "provider_native",
              driver: PI_PROVIDER,
              providerInstanceId: options.instanceId,
              childThreadId,
              prompt: subagent.prompt,
              ...progress,
              result: resultText,
            },
          });
          yield* emitSubagentConversation(
            child,
            [
              ...(existing === undefined
                ? [
                    {
                      role: "system",
                      kind: "text" as const,
                      text: "Recent native worker activity is shown here. Earlier activity may be unavailable.",
                    },
                  ]
                : []),
              ...(observation.partial
                ? [
                    {
                      role: "system",
                      kind: "text" as const,
                      text: "Native worker returned a partial result and is no longer active. The final outcome is unknown; physical worker exit is not confirmed.",
                    },
                  ]
                : []),
            ],
            observation.prompt,
            resultText ?? undefined,
          );
          if (fromSnapshot) yield* refreshSubagentConversation(child).pipe(Effect.forkIn(scope));
        }
      });

      // Enroll every original-run fence before any inactive event can close its consumer.
      const closeNativeObservations = Effect.fn("Pi.closeNativeObservations")(function* (
        children: ReadonlyArray<NativeSubagent>,
        launches: ReadonlyArray<string>,
        notice: string,
      ) {
        const now = yield* DateTime.now;
        const fences: ProviderAdapter.ProviderAdapterV2Event[] = [];
        const acknowledgements: Deferred.Deferred<void>[] = [];
        for (const child of children) {
          child.observationClosed = true;
          child.refreshNeeded = false;
          const status = ["pending", "running", "waiting", "idle"].includes(child.subagent.status)
            ? "interrupted"
            : child.subagent.status;
          child.observation = { ...child.observation, status };
          child.subagent = {
            ...child.subagent,
            status,
            completedAt: child.subagent.completedAt ?? now,
            updatedAt: now,
          };
          const fence = subagentRootEvent(child, now);
          const acknowledged = yield* Deferred.make<void>();
          pendingDeliveries.set(fence, { runId: child.turn.turnInput.runId, acknowledged });
          acknowledgements.push(acknowledged);
          fences.push(fence);
        }
        for (const runId of launches) {
          const binding = subagentRuns.get(runId)!;
          const turn = binding.turn;
          const fence = {
            type: "turn_item.updated" as const,
            driver: PI_PROVIDER,
            turnItem: {
              ...baseItemFields(turn, `native-stop:${runId}`, now, now),
              status: "completed" as const,
              completedAt: now,
              title: null,
              type: "system_notice" as const,
              message: notice,
            },
          };
          const acknowledged = yield* Deferred.make<void>();
          pendingDeliveries.set(fence, { runId: turn.turnInput.runId, acknowledged });
          acknowledgements.push(acknowledged);
          fences.push(fence);
          binding.awaitingFirstChild = false;
        }
        for (const child of children) {
          yield* emit({ type: "subagent.updated", driver: PI_PROVIDER, subagent: child.subagent });
          yield* emit({
            type: "provider_thread.updated",
            driver: PI_PROVIDER,
            providerThread: { ...child.providerThread, status: "idle", updatedAt: now },
          });
          yield* emit({
            type: "turn_item.updated",
            driver: PI_PROVIDER,
            turnItem: {
              ...baseItemFields(
                child.turn,
                child.subagent.nativeTaskRef!.nativeId!,
                child.subagent.startedAt ?? now,
                now,
              ),
              threadId: child.subagent.threadId,
              status: child.subagent.status,
              title: child.subagent.title,
              completedAt: child.subagent.completedAt,
              type: "subagent",
              subagentId: child.subagent.id,
              origin: "provider_native",
              driver: PI_PROVIDER,
              providerInstanceId: options.instanceId,
              childThreadId: child.childThread.id,
              prompt: child.subagent.prompt,
              result: child.subagent.result,
            },
          });
          yield* emitSubagentConversation(child, [{ role: "system", kind: "text", text: notice }]);
        }
        for (const fence of fences) yield* emit(fence);
        return acknowledgements;
      });

      // Per-event maintenance shares the pump's trace/permit; expiry reserves its fence before release.
      const maintainNativeHistory = Effect.fnUntraced(function* () {
        const now = DateTime.toEpochMillis(yield* DateTime.now);
        const expired = [...subagentRuns].filter(
          ([, binding]) =>
            binding.awaitingFirstChild &&
            binding.firstChildDeadline !== undefined &&
            binding.firstChildDeadline <= now,
        );
        if (expired.length > 0) {
          yield* closeNativeObservations(
            [],
            expired.map(([runId]) => runId),
            "Native worker observation window expired before its first child was available. Worker outcome is unknown; the worker may still be running. Observation has ended.",
          );
          for (const [, binding] of expired) binding.observationExpired = true;
        }
        const owners = new Set<ActivePiTurn>();
        const blockedRuns = new Set(
          [...pendingDeliveries.values()].map((delivery) => delivery.runId),
        );
        for (const binding of subagentRuns.values()) {
          owners.add(binding.turn);
          if (
            binding.awaitingFirstChild ||
            binding.foregroundActive ||
            (binding.executionMode === "async" &&
              !binding.observationExpired &&
              // Only native paused or ended partial states map to idle; descendants block independently.
              ["running", "pending", "waiting"].includes(binding.nativeStatus ?? ""))
          )
            blockedRuns.add(binding.turn.turnInput.runId);
        }
        // An empty owner needs its actual finalized launch item to close delivery canonically.
        // A missing tool end is not evidence that an original consumer can be released.
        const fenceOwners = new Set([...nativeSubagents.values()].map((child) => child.turn));
        for (const binding of subagentRuns.values())
          if (binding.finalLaunchEvent !== undefined) fenceOwners.add(binding.turn);
        for (const turn of owners)
          if (!fenceOwners.has(turn)) blockedRuns.add(turn.turnInput.runId);
        for (const child of nativeSubagents.values()) {
          owners.add(child.turn);
          if (
            child.inspectionPhase !== null ||
            child.refreshNeeded ||
            (!child.observationClosed &&
              (["running", "pending", "waiting"].includes(child.subagent.status) ||
                (child.subagent.status === "idle" &&
                  !child.observation.paused &&
                  !child.observation.partial)))
          )
            blockedRuns.add(child.turn.turnInput.runId);
        }
        const eligible = [...owners].filter(
          (turn) =>
            turn.nativeParentSettled &&
            pendingWake === null &&
            !blockedRuns.has(turn.turnInput.runId),
        );
        for (const turn of eligible) turn.nativeHistoryEligibleAt ??= now;
        eligible.sort((a, b) => a.nativeHistoryEligibleAt! - b.nativeHistoryEligibleAt!);
        const retiring = new Set(
          eligible.filter(
            (turn, index) =>
              index < eligible.length - NATIVE_RECENT_OWNER_LIMIT ||
              turn.nativeHistoryEligibleAt! + NATIVE_OBSERVATION_WINDOW_MS <= now,
          ),
        );
        const remember = (key: string) => {
          retiredNativeKeys.add(key);
          if (retiredNativeKeys.size > NATIVE_REPLAY_KEY_LIMIT)
            retiredNativeKeys.delete(retiredNativeKeys.values().next().value!);
        };
        const retirementFences = new Map<ActivePiTurn, ProviderAdapter.ProviderAdapterV2Event>();
        for (const child of nativeSubagents.values()) {
          if (!retiring.has(child.turn) || retirementFences.has(child.turn)) continue;
          const fence = subagentRootEvent(child, DateTime.makeUnsafe(now));
          pendingDeliveries.set(fence, { runId: child.turn.turnInput.runId });
          retirementFences.set(child.turn, fence);
        }
        for (const binding of subagentRuns.values()) {
          if (
            !retiring.has(binding.turn) ||
            retirementFences.has(binding.turn) ||
            binding.finalLaunchEvent === undefined
          )
            continue;
          // Same final item, fresh delivery identity: ingestion may be a no-op, but its accepted ACK is real.
          const fence = { ...binding.finalLaunchEvent };
          pendingDeliveries.set(fence, { runId: binding.turn.turnInput.runId });
          retirementFences.set(binding.turn, fence);
        }
        // Close admission under the pump permit only after every original final fence is reserved.
        for (const [runId, binding] of subagentRuns) {
          if (!retiring.has(binding.turn)) continue;
          remember(runId);
          for (const childRunId of binding.children?.keys() ?? []) remember(childRunId);
          subagentRuns.delete(runId);
        }
        for (const [id, child] of nativeSubagents)
          if (retiring.has(child.turn)) nativeSubagents.delete(id);
        for (const fence of retirementFences.values()) yield* emit(fence);
        let deadline = Infinity;
        for (const binding of subagentRuns.values())
          if (binding.awaitingFirstChild && binding.firstChildDeadline !== undefined)
            deadline = Math.min(deadline, binding.firstChildDeadline);
        for (const turn of eligible)
          if (!retiring.has(turn))
            deadline = Math.min(
              deadline,
              turn.nativeHistoryEligibleAt! + NATIVE_OBSERVATION_WINDOW_MS,
            );
        return deadline === Infinity ? undefined : Math.max(0, deadline - now);
      });

      yield* Effect.gen(function* () {
        while (true) {
          const delay = yield* sessionEventPermit.withPermit(maintainNativeHistory());
          const changed = Queue.take(nativeMaintenanceChanged);
          yield* delay === undefined
            ? changed
            : Effect.raceFirst(changed, Effect.sleep(Duration.millis(delay)));
        }
      }).pipe(Effect.forkIn(scope));

      const cancelPrompt = Effect.fn("PiAdapter.cancelPrompt")(function* (
        pending: PendingPiPrompt,
        resolvedAt: DateTime.Utc,
      ) {
        if (pending.expirationFiber !== null) yield* Fiber.interrupt(pending.expirationFiber);
        yield* connection
          .send({
            type: "extension_ui_response",
            id: pending.nativeRequestId,
            cancelled: true,
          })
          .pipe(Effect.ignore);
        pending.runtimeRequest = {
          ...pending.runtimeRequest,
          status: "cancelled",
          resolvedAt,
        };
        yield* emit({
          type: "runtime_request.updated",
          driver: PI_PROVIDER,
          threadId: pending.node.threadId,
          runtimeRequest: pending.runtimeRequest,
        });
        yield* emit({
          type: "node.updated",
          driver: PI_PROVIDER,
          node: { ...pending.node, status: "cancelled", completedAt: resolvedAt },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: PI_PROVIDER,
          turnItem: {
            ...pending.turnItem,
            status: "cancelled",
            completedAt: resolvedAt,
            updatedAt: resolvedAt,
          },
        });
      });

      const cancelPendingPrompts = Effect.fn("PiAdapter.cancelPendingPrompts")(function* (
        resolvedAt: DateTime.Utc,
      ) {
        const pending = Array.from(pendingPrompts.values());
        pendingPrompts.clear();
        yield* Effect.forEach(pending, (prompt) => cancelPrompt(prompt, resolvedAt), {
          discard: true,
        });
      });

      const handleExtensionUiRequest = Effect.fnUntraced(function* (event: PiRpcRecord) {
        const method = recordString(event, "method");
        const nativeRequestId = recordString(event, "id");
        if (method === undefined) return;
        if (method === "setWidget" && recordString(event, "widgetKey") === "subagent-inspect") {
          const lines = event["widgetLines"];
          const line = Array.isArray(lines) ? lines[0] : undefined;
          const prefix = "PI_SUBAGENT_INSPECT_JSON:";
          if (typeof line !== "string" || !line.startsWith(prefix)) return;
          const payload = line.slice(prefix.length);
          if (
            payload.length > 64 * 1024 ||
            new TextEncoder().encode(payload).byteLength > 64 * 1024
          )
            return;
          const response = Schema.decodeUnknownOption(
            Schema.fromJsonString(PiSubagentInspectReply),
          )(payload);
          if (Option.isNone(response)) return;
          const pending = inspectionRequests.get(response.value.requestId);
          if (
            pending !== undefined &&
            (response.value.asyncId === undefined || response.value.asyncId === pending.runId) &&
            (response.value.childId === undefined || response.value.childId === pending.childId)
          )
            yield* Deferred.succeed(pending.reply, response.value);
          return;
        }
        if (method === "setWidget" && recordString(event, "widgetKey") === "t3-subagent-liveness") {
          const lines = event["widgetLines"];
          if (!Array.isArray(lines) || typeof lines[0] !== "string") return;
          const data = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(
            lines[0],
          ).pipe(Effect.catchCause(() => Effect.succeed(undefined)));
          if (recordField(data, "version") !== 1) return;
          if (recordField(data, "ready") === true) livenessReady = true;
          const requestId = recordString(data, "requestId");
          const status = recordString(data, "status");
          const reply = requestId === undefined ? undefined : livenessRequests.get(requestId);
          if (
            reply !== undefined &&
            (status === "active" || status === "inactive" || status === "unavailable")
          ) {
            yield* Deferred.succeed(reply, status);
          }
          return;
        }
        if (
          method === "setWidget" &&
          (recordString(event, "widgetKey") === "subagent-async" ||
            recordString(event, "widgetKey") === "t3-subagents")
        ) {
          const state = threadState;
          yield* emitSubagentObservations(
            yield* reconcileSubagentSnapshot(event["widgetLines"]),
            state?.activeTurn ?? state?.lastTurn ?? null,
            undefined,
            true,
          );
          for (const runId of piSubagentWorkflowRunIds(event["widgetLines"])) {
            const binding = subagentRuns.get(runId);
            if (binding !== undefined) binding.workflow = true;
          }
          for (const [runId, status] of piSubagentRunStates(event["widgetLines"])) {
            const binding = subagentRuns.get(runId);
            if (binding !== undefined) {
              binding.executionMode = "async";
              if (
                !["completed", "failed", "interrupted"].includes(binding.nativeStatus ?? "") ||
                ["completed", "failed", "interrupted"].includes(status)
              )
                binding.nativeStatus = status;
              if (["completed", "failed", "interrupted", "idle"].includes(status))
                binding.awaitingFirstChild = false;
            }
          }
          return;
        }
        if (method === "notify") {
          const state = threadState;
          const turn = state?.activeTurn ?? null;
          const message = recordString(event, "message") ?? "";
          if (turn === null || message.length === 0) return;
          const emittedAt = yield* DateTime.now;
          const nativeItemId = `notify:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;
          yield* emitItemNode(turn, nativeItemId, "system", "completed", emittedAt, emittedAt);
          yield* emit({
            type: "turn_item.updated",
            driver: PI_PROVIDER,
            turnItem: {
              ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
              status: "completed",
              completedAt: emittedAt,
              title: message,
              type: "system_notice",
              message,
            },
          });
          return;
        }
        if (
          method !== "select" &&
          method !== "confirm" &&
          method !== "input" &&
          method !== "editor"
        ) {
          yield* Effect.logDebug("Ignoring pi extension UI update.", { method });
          return;
        }
        if (nativeRequestId === undefined) return;
        const approvalTitle = recordString(event, "title") ?? "";
        const approvalKey = `${approvalTitle.length}:${approvalTitle}${recordString(event, "message") ?? ""}`;
        if (method === "confirm" && sessionApprovals.has(approvalKey)) {
          yield* connection.send({
            type: "extension_ui_response",
            id: nativeRequestId,
            confirmed: true,
          });
          return;
        }
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        const createdAt = yield* DateTime.now;
        const timeout = recordNumber(event, "timeout");
        // Pi treats zero as no timeout and negative durations as immediate expiry.
        const expiresAt =
          timeout === undefined || timeout === 0
            ? null
            : DateTime.toEpochMillis(createdAt) + Math.max(0, timeout);
        const requestId = yield* idAllocator.allocate.runtimeRequest({
          driver: PI_PROVIDER,
          ...(turn === null ? {} : { providerTurnId: turn.providerTurn.id }),
          nativeRequestId,
        });
        const nodeId = idAllocator.derive.approvalNode({ requestId });
        const title = recordString(event, "title") ?? method;
        const threadId =
          turn?.turnInput.threadId ?? state?.providerThread.appThreadId ?? input.threadId;
        const providerThreadId = state?.providerThread.id ?? null;
        const providerTurnId = turn?.providerTurn.id ?? null;
        const runtimeRequest: OrchestrationV2RuntimeRequest = {
          id: requestId,
          nodeId,
          providerTurnId,
          nativeRequestRef: providerRef(nativeRequestId),
          kind: method === "confirm" ? "command" : "user_input",
          status: "pending",
          responseCapability: { type: "live", providerSessionId: input.providerSessionId },
          createdAt,
          resolvedAt: null,
        };
        const node: OrchestrationV2ExecutionNode = {
          id: nodeId,
          threadId,
          runId: turn?.turnInput.runId ?? null,
          parentNodeId: turn?.turnInput.rootNodeId ?? null,
          rootNodeId: turn?.turnInput.rootNodeId ?? nodeId,
          kind: method === "confirm" ? "approval_request" : "user_input_request",
          status: "waiting",
          countsForRun: false,
          providerThreadId,
          providerTurnId,
          nativeItemRef: providerRef(nativeRequestId),
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: createdAt,
          completedAt: null,
        };
        const itemBase = {
          id: idAllocator.derive.approvalTurnItem({ requestId }),
          threadId,
          runId: turn?.turnInput.runId ?? null,
          nodeId,
          providerThreadId,
          providerTurnId,
          nativeItemRef: providerRef(nativeRequestId),
          parentItemId: null,
          ordinal: turn === null ? 0 : itemOrdinal(turn, nativeRequestId),
          status: "waiting" as const,
          title,
          startedAt: createdAt,
          completedAt: null,
          updatedAt: createdAt,
        };
        const turnItem: OrchestrationV2TurnItem =
          method === "confirm"
            ? {
                ...itemBase,
                type: "approval_request",
                requestId,
                requestKind: piApprovalRequestKind(title),
                prompt: recordString(event, "message") ?? title,
              }
            : {
                ...itemBase,
                type: "user_input_request",
                requestId,
                questions: [piQuestion(nativeRequestId, method, title, event)],
              };
        const pending: PendingPiPrompt = {
          nativeRequestId,
          method,
          questionId: nativeRequestId,
          approvalKey,
          expiresAt,
          expirationFiber: null,
          runtimeRequest,
          node,
          turnItem,
        };
        pendingPrompts.set(String(requestId), pending);
        yield* emit({
          type: "runtime_request.updated",
          driver: PI_PROVIDER,
          threadId,
          runtimeRequest,
        });
        yield* emit({ type: "node.updated", driver: PI_PROVIDER, node });
        yield* emit({ type: "turn_item.updated", driver: PI_PROVIDER, turnItem });
        if (expiresAt !== null) {
          const now = yield* DateTime.now;
          pending.expirationFiber = yield* Effect.sleep(
            Duration.millis(Math.max(0, expiresAt - DateTime.toEpochMillis(now))),
          ).pipe(
            Effect.andThen(
              Queue.offer(connection.events, {
                type: "t3.ui_request_expired",
                requestId: String(requestId),
                pending,
              }),
            ),
            Effect.asVoid,
            Effect.forkIn(scope),
          );
        }
      });

      const emitExtensionError = Effect.fnUntraced(function* (event: PiRpcRecord) {
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        if (turn === null) {
          outOfTurnExtensionErrors.push(event);
          return;
        }
        const emittedAt = yield* DateTime.now;
        const nativeItemId = `extension-error:${turn.providerTurn.id}:${turn.nextItemOrdinal}`;
        const extensionName = piExtensionDisplayName(recordString(event, "extensionPath"));
        const extensionEvent = recordString(event, "event");
        const detail = recordString(event, "error")?.trim();
        const message = [
          `${extensionName} failed${extensionEvent === undefined ? "" : ` during ${extensionEvent}`}.`,
          detail === undefined || detail.length === 0 ? undefined : detail.slice(0, 2_000),
        ]
          .filter((part): part is string => part !== undefined)
          .join("\n\n");
        const failure = makeProviderFailure({
          message,
          class: "provider_error",
          retryable: false,
        });
        yield* emitItemNode(turn, nativeItemId, "system", "failed", emittedAt, emittedAt);
        yield* emit({
          type: "turn_item.updated",
          driver: PI_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
            status: "failed",
            title: extensionName,
            completedAt: emittedAt,
            type: "error",
            failure,
          },
        });
      });

      const listSessionEntries = (since: string | null, timeoutMs: number) =>
        request({ type: "get_entries", ...(since === null ? {} : { since }) }, timeoutMs).pipe(
          Effect.map(piSessionListing),
          Effect.orElseSucceed(() => null),
        );

      // get_entries includes abandoned branches in append order; follow leaf parentId ancestry instead.
      const captureTurnTreeRefs = Effect.fnUntraced(function* (
        timeoutMs = PI_REQUEST_TIMEOUT_MS,
      ): Effect.fn.Return<PiTurnTreeRefs | null> {
        const cursorWasStale = leafCursorStale;
        const previousLeaf = lastKnownLeaf;
        const fullListing = cursorWasStale || previousLeaf === null;
        const listing = yield* listSessionEntries(fullListing ? null : previousLeaf, timeoutMs);
        const turnWalk =
          listing === null ? null : walkPiBranch(listing.entries, listing.leafId, previousLeaf);
        const rewound = turnWalk !== null && previousLeaf !== null && !turnWalk.reachedStop;
        // since ends at the previous leaf; rewound ancestry requires the whole tree.
        const tree = rewound && !fullListing ? yield* listSessionEntries(null, timeoutMs) : listing;
        if (listing === null || turnWalk === null || tree === null) {
          leafCursorStale = true;
          return null;
        }
        lastKnownLeaf = listing.leafId;
        leafCursorStale = false;
        const turnStartEntryId = turnWalk.userEntryIds.at(-1);
        // Only a complete acyclic root walk proves all survivors.
        // The old leaf must remain in this tree to prove rewind rather than session replacement.
        const branch =
          rewound && previousLeaf !== null && tree.entries.has(previousLeaf)
            ? walkPiBranch(tree.entries, listing.leafId, null)
            : undefined;
        return {
          // Stale cursors span earlier turns; keep a synthetic ref to avoid rolling back too far.
          nativeTurnRef: cursorWasStale
            ? undefined
            : turnStartEntryId !== undefined
              ? providerRef(turnStartEntryId)
              : turnWalk.entryCount === 0
                ? null
                : undefined,
          leafId: listing.leafId,
          retainedNativeTurnIds: branch?.reachedStop === true ? branch.userEntryIds : undefined,
        };
      });

      const finalizeTurn = Effect.fnUntraced(function* (
        state: PiThreadState,
        readUsage = true,
        transportAlive = true,
      ) {
        const turn = state.activeTurn;
        if (turn === null) return;
        state.lastTurn = turn;
        state.activeTurn = null;
        const interrupted = turn.interrupted || turn.nativeAborted;
        const completedAt = yield* DateTime.now;
        yield* completeOpenStreamItems(turn);
        if (turn.activeCompaction !== null) {
          const status = interrupted ? "cancelled" : turn.failure === null ? "completed" : "failed";
          yield* emitCompaction(turn, turn.activeCompaction, status);
          turn.activeCompaction = null;
        }
        if (turn.activeProviderRetry !== null) {
          if (interrupted) {
            yield* emitProviderRetry(turn, turn.activeProviderRetry, "interrupted", completedAt);
            turn.activeProviderRetry = null;
          } else if (turn.failure === null) {
            yield* emitProviderRetry(turn, turn.activeProviderRetry, "completed", completedAt);
            turn.activeProviderRetry = null;
          }
        }
        yield* cancelPendingPrompts(completedAt);
        const treeRefs =
          turn.stopTreeRefs !== undefined ? turn.stopTreeRefs : yield* captureTurnTreeRefs();
        const tokenUsage = readUsage
          ? yield* readTokenUsage(turn.latestCompactionAfterTokens, completedAt)
          : undefined;
        const failure = interrupted ? null : (turn.rejectedPromptFailure ?? turn.failure);
        yield* emit({
          type: "provider_turn.updated",
          driver: PI_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn: {
            ...turn.providerTurn,
            ...(treeRefs === null || treeRefs.nativeTurnRef === undefined
              ? {}
              : { nativeTurnRef: treeRefs.nativeTurnRef }),
            status: interrupted ? "interrupted" : failure !== null ? "failed" : "completed",
            completedAt,
            ...(tokenUsage === undefined ? {} : { tokenUsage }),
          },
        });
        yield* updateProviderThread(
          state,
          {
            status: "idle",
            ...(treeRefs === null
              ? {}
              : {
                  nativeConversationHeadRef:
                    treeRefs.leafId === null ? null : providerRef(treeRefs.leafId),
                }),
          },
          treeRefs?.retainedNativeTurnIds,
        );
        yield* updateProviderSession(
          failure !== null ? "error" : "ready",
          failure?.message ?? null,
        );
        if (failure !== null) {
          const failureItemId = `terminal-failure:${turn.providerTurn.id}`;
          if (turn.activeProviderRetry !== null) {
            yield* emitProviderRetry(
              turn,
              { ...turn.activeProviderRetry, failure },
              "failed",
              completedAt,
            );
          } else {
            yield* emit({
              type: "turn_item.updated",
              driver: PI_PROVIDER,
              turnItem: {
                ...baseItemFields(turn, failureItemId, completedAt, completedAt),
                status: "failed",
                title: null,
                completedAt,
                type: "error",
                failure,
              },
            });
          }
          yield* emit({
            type: "turn.terminal",
            driver: PI_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            failureItemOrdinal: itemOrdinal(turn, failureItemId),
            status: "failed",
            failure,
            ...(transportAlive && !stopRequested && !interrupted
              ? { childTurnsContinue: true as const }
              : {}),
            ...(turn.activeProviderRetry === null
              ? {}
              : {
                  retry: turn.activeProviderRetry.retry,
                  retryStartedAt: turn.activeProviderRetry.startedAt,
                }),
            threadDisposition: "reusable",
          });
        } else {
          yield* emit({
            type: "turn.terminal",
            driver: PI_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            status: interrupted ? "interrupted" : "completed",
            failure: null,
            threadDisposition: "reusable",
          });
        }
        turn.nativeParentSettled = true;
        yield* Queue.offer(nativeMaintenanceChanged, undefined);
      });

      const scheduleSettleProbe = (
        turn: ActivePiTurn,
        settleAfterAgentActivity = false,
        attempt = 1,
      ) => {
        const providerTurnId = turn.providerTurn.id;
        const settleProbeGeneration = turn.settleProbeGeneration;
        return request({ type: "get_state" }, 2_000).pipe(
          Effect.matchEffect({
            onSuccess: (data) =>
              Queue.offer(connection.events, {
                type: "t3.settle_probe",
                providerTurnId,
                settleAfterAgentActivity,
                settleProbeGeneration,
                attempt,
                data,
              }),
            // Failed probes must reach the pump; command-only turns emit no agent events to settle them.
            onFailure: () =>
              Queue.offer(connection.events, {
                type: "t3.settle_probe",
                providerTurnId,
                settleAfterAgentActivity,
                settleProbeGeneration,
                attempt,
                probeFailed: true,
              }),
          }),
          Effect.ignore,
          Effect.forkIn(scope),
        );
      };

      const stopPendingWake = Effect.fnUntraced(function* (wake: PiWake) {
        if (pendingWake !== wake || threadState !== wake.state || closed) return;
        pendingWake = null;
        stopRequested = true;
        yield* cancelPendingPrompts(yield* DateTime.now);
        yield* connection.terminate;
      });

      const handleSessionEvent = Effect.fnUntraced(function* (event: PiRpcRecord) {
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        if (
          event["type"] === "tool_execution_update" &&
          recordString(event, "toolName") === "subagent"
        ) {
          const toolCallId = recordString(event, "toolCallId");
          const result = event["partialResult"];
          const runId = recordString(recordField(result, "details"), "runId");
          const retained = runId === undefined ? undefined : subagentRuns.get(runId);
          if (
            toolCallId !== undefined &&
            retained?.turn !== turn &&
            retained?.confirmedLaunch &&
            retained.executionMode === "attached"
          ) {
            if (retained.toolCallId === toolCallId)
              yield* emitSubagentTasks(retained.turn, toolCallId, result, false);
            return;
          }
        }
        // Model-selection hooks can await a dialog before startTurn can claim
        // a wake. Keep interactive requests session-scoped until a turn owns
        // them; buffer native output and notifications for that turn. Private observer
        // replies must still complete session-scoped requests while wake admission waits.
        const interactiveUiRequest =
          event["type"] === "extension_ui_request" &&
          ["select", "confirm", "input", "editor"].includes(String(event["method"]));
        if (
          threadState?.activeTurn === null &&
          pendingWake !== null &&
          !interactiveUiRequest &&
          !String(event["type"]).startsWith("t3.") &&
          event["type"] !== "response" &&
          !(
            event["type"] === "extension_ui_request" &&
            ["t3-subagent-liveness", "subagent-inspect", "t3-subagents", "subagent-async"].includes(
              recordString(event, "widgetKey") ?? "",
            )
          )
        ) {
          pendingWake.events.push(event);
          if (event["type"] === "agent_start") yield* updateProviderSession("running", null);
          if (event["type"] === "agent_settled") yield* updateProviderSession("ready", null);
          return;
        }
        switch (event["type"]) {
          case "agent_start": {
            if (
              turn === null &&
              state?.providerThread.appThreadId != null &&
              continuationRequests !== undefined &&
              !closed &&
              !stopRequested
            ) {
              const wake: PiWake = { state, events: [event] };
              pendingWake = wake;
              if (rollbackBarrier !== null) {
                yield* stopPendingWake(wake);
                return;
              }
              yield* updateProviderSession("running", null);
              yield* continuationRequests
                .offer({
                  threadId: state.providerThread.appThreadId,
                  providerThreadId: state.providerThread.id,
                  driver: PI_PROVIDER,
                  detail: null,
                  delivery: "adapter_buffered",
                  dispatchIfCurrent: (dispatch) =>
                    sessionEventPermit.withPermits(1)(
                      Effect.suspend(() =>
                        pendingWake === wake && threadState === state && !closed
                          ? dispatch.pipe(
                              Effect.asSome,
                              Effect.tapError(() => stopPendingWake(wake)),
                            )
                          : Effect.succeedNone,
                      ),
                    ),
                  clearIfCurrent: () => sessionEventPermit.withPermits(1)(stopPendingWake(wake)),
                })
                .pipe(Effect.tapError(() => stopPendingWake(wake)));
              return;
            }
            if (turn === null) {
              unsolicitedActivityDetected = true;
              yield* updateProviderSession("error", PI_UNSOLICITED_ACTIVITY_ERROR);
              yield* connection.terminate;
              return;
            }
            turn.sawAgentActivity = true;
            turn.nativeAborted = false;
            turn.settleProbeGeneration += 1;
            return;
          }
          case "message_start": {
            if (turn !== null && recordString(event["message"], "role") === "assistant") {
              turn.sawAgentActivity = true;
              turn.messageOrdinal += 1;
            }
            return;
          }
          case "message_update": {
            if (turn === null) return;
            turn.sawAgentActivity = true;
            yield* reportLiveUsage(turn, event["usage"]);
            const delta = event["assistantMessageEvent"];
            const deltaType = recordString(delta, "type");
            const contentIndex = recordNumber(delta, "contentIndex") ?? 0;
            if (deltaType === "text_delta" || deltaType === "thinking_delta") {
              const item = yield* streamItemFor(
                turn,
                deltaType === "text_delta" ? "assistant_message" : "reasoning",
                contentIndex,
              );
              item.text += recordString(delta, "delta") ?? "";
              yield* scheduleStreamFlush(turn, item);
              return;
            }
            if (deltaType === "text_end" || deltaType === "thinking_end") {
              const item = yield* streamItemFor(
                turn,
                deltaType === "text_end" ? "assistant_message" : "reasoning",
                contentIndex,
              );
              yield* completeStreamItem(
                turn,
                item,
                recordString(delta, "content") ?? recordString(delta, "thinking"),
              );
              return;
            }
            return;
          }
          case "message_end": {
            if (turn === null) return;
            const message = event["message"];
            if (recordString(message, "role") !== "assistant") return;
            yield* completeOpenStreamItems(turn);
            if (recordString(message, "stopReason") === "error" && turn.failure === null) {
              turn.failure = makeProviderFailure({
                message: recordString(message, "errorMessage") ?? "Pi reported a model error.",
                class: "provider_error",
              });
            }
            return;
          }
          case "tool_execution_start":
            if (turn !== null) {
              turn.sawAgentActivity = true;
              yield* emitToolItem(turn, event, "start");
            }
            return;
          case "tool_execution_update": {
            const toolCallId = recordString(event, "toolCallId");
            if (
              recordString(event, "toolName") === "subagent" &&
              toolCallId !== undefined &&
              !turn?.toolArgs.has(toolCallId) &&
              [...subagentRuns.values()].some(
                (binding) =>
                  binding.executionMode === "attached" && binding.toolCallId === toolCallId,
              )
            )
              return;
            if (turn !== null) yield* emitToolItem(turn, event, "update");
            return;
          }
          case "tool_execution_end":
            if (turn !== null) yield* emitToolItem(turn, event, "end");
            return;
          case "compaction_start": {
            if (turn === null) return;
            turn.settleProbeGeneration += 1;
            turn.sawCompaction = true;
            if (turn.activeCompaction !== null) {
              yield* emitCompaction(turn, turn.activeCompaction, "cancelled");
            }
            const startedAt = yield* DateTime.now;
            const compaction = {
              nativeItemId: compactionNativeItemId(turn),
              startedAt,
            } satisfies PiCompactionState;
            turn.activeCompaction = compaction;
            yield* emitCompaction(turn, compaction, "running");
            return;
          }
          case "compaction_end": {
            if (turn === null) return;
            const observedAt = yield* DateTime.now;
            const compaction = turn.activeCompaction ?? {
              nativeItemId: compactionNativeItemId(turn),
              startedAt: observedAt,
            };
            turn.activeCompaction = null;
            const result = event["result"];
            if (result === null || result === undefined) {
              if (event["aborted"] === true) {
                yield* emitCompaction(turn, compaction, "cancelled");
                if (turn.settleWhenIdle || !turn.sawAgentActivity) {
                  yield* scheduleSettleProbe(turn, turn.settleWhenIdle);
                }
                return;
              }
              const errorMessage =
                recordString(event, "errorMessage") ?? "Pi context compaction failed.";
              yield* emitCompaction(turn, compaction, "failed", {
                summary: errorMessage.slice(0, 1_000),
              });
              if (turn.settleWhenIdle || !turn.sawAgentActivity) {
                yield* scheduleSettleProbe(turn, turn.settleWhenIdle);
              }
              return;
            }
            // Clear overflow failures only when compaction will retry; ordinary compaction must retain exhausted retries.
            if (event["willRetry"] === true) turn.failure = null;
            turn.latestCompactionAfterTokens =
              nonNegativeInteger(result, "estimatedTokensAfter") ?? null;
            const summary = recordString(result, "summary");
            const beforeTokenCount = nonNegativeInteger(result, "tokensBefore");
            const afterTokenCount = nonNegativeInteger(result, "estimatedTokensAfter");
            yield* emitCompaction(turn, compaction, "completed", {
              ...(summary === undefined ? {} : { summary }),
              ...(beforeTokenCount === undefined ? {} : { beforeTokenCount }),
              ...(afterTokenCount === undefined ? {} : { afterTokenCount }),
            });
            if (turn.settleWhenIdle || !turn.sawAgentActivity) {
              yield* scheduleSettleProbe(turn, turn.settleWhenIdle);
            }
            return;
          }
          case "auto_retry_start": {
            if (turn === null) return;
            const emittedAt = yield* DateTime.now;
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            const maxAttempts = Math.max(
              attempt,
              Math.trunc(recordNumber(event, "maxAttempts") ?? attempt),
            );
            const retryDelayMs = Math.max(0, Math.trunc(recordNumber(event, "delayMs") ?? 0));
            const failure = makeProviderFailure({
              message: recordString(event, "errorMessage") ?? "Pi provider request failed.",
              class: "provider_error",
              retryable: true,
            });
            const current = turn.activeProviderRetry;
            const providerRetry = {
              retry: { attempt, maxAttempts, retryDelayMs },
              failure,
              startedAt: current?.startedAt ?? emittedAt,
              itemOrdinal:
                current?.itemOrdinal ??
                itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
            } satisfies PiProviderRetryState;
            turn.activeProviderRetry = providerRetry;
            yield* emitProviderRetry(turn, providerRetry, "running", emittedAt);
            return;
          }
          case "auto_retry_end": {
            if (turn === null) return;
            const emittedAt = yield* DateTime.now;
            if (event["success"] === true) {
              // Successful retries supersede the failure agent_settled would reuse.
              if (turn.activeProviderRetry !== null) {
                const attempt = Math.max(
                  1,
                  Math.trunc(
                    recordNumber(event, "attempt") ?? turn.activeProviderRetry.retry.attempt,
                  ),
                );
                const recoveredRetry = {
                  ...turn.activeProviderRetry,
                  retry: { ...turn.activeProviderRetry.retry, attempt },
                };
                yield* emitProviderRetry(turn, recoveredRetry, "completed", emittedAt);
                turn.activeProviderRetry = null;
              }
              turn.failure = null;
              return;
            }
            const failure = makeProviderFailure({
              message: recordString(event, "finalError") ?? "Pi auto-retry failed.",
              class: "provider_error",
              retryable: false,
            });
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            const current = turn.activeProviderRetry;
            const providerRetry = {
              retry: {
                attempt,
                maxAttempts: current?.retry.maxAttempts ?? attempt,
                retryDelayMs: current?.retry.retryDelayMs ?? null,
              },
              failure,
              startedAt: current?.startedAt ?? emittedAt,
              itemOrdinal:
                current?.itemOrdinal ??
                itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
            } satisfies PiProviderRetryState;
            turn.activeProviderRetry = providerRetry;
            turn.failure = failure;
            yield* emitProviderRetry(turn, providerRetry, "failed", emittedAt);
            return;
          }
          case "extension_ui_request":
            yield* handleExtensionUiRequest(event);
            return;
          case "extension_error": {
            yield* emitExtensionError(event);
            return;
          }
          case "agent_settled": {
            if (turn?.interrupted === true) {
              if (state !== null) yield* finalizeTurn(state);
              return;
            }
            if (turn !== null) {
              turn.nativeAborted = event["aborted"] === true;
              turn.settleWhenIdle = true;
              turn.settleProbeGeneration += 1;
              if (
                turn.adoptedWake &&
                pendingPromptResponses.some(
                  (pending) =>
                    pending.providerTurnId === turn.providerTurn.id &&
                    pending.kind === "turn_start",
                )
              )
                return;
              yield* scheduleSettleProbe(turn, true);
            }
            return;
          }
          case "response": {
            const command = recordString(event, "command");
            if (command === "compact") {
              const pendingCompact = pendingCompactResponses.shift();
              const compactTurn =
                pendingCompact?.providerTurnId === turn?.providerTurn.id ? turn : null;
              if (compactTurn !== null) compactTurn.manualCompactInFlight = false;
              if (event["success"] === true) {
                if (
                  pendingCompact?.kind === "turn_start" &&
                  compactTurn !== null &&
                  compactTurn.promptMayBeCommandOnly &&
                  !compactTurn.sawAgentActivity
                ) {
                  yield* scheduleSettleProbe(compactTurn);
                }
                return;
              }
              if (event["success"] !== false) return;
              if (compactTurn === null) return;
              if (compactTurn.activeCompaction !== null) return;
              if (pendingCompact?.kind === "steer") {
                yield* Effect.logWarning("Pi rejected a compact steer.", {
                  errorLength: recordString(event, "error")?.length,
                });
                return;
              }
              if (!compactTurn.sawCompaction) {
                compactTurn.failure = makeProviderFailure({
                  message: recordString(event, "error") ?? "Pi compact failed.",
                  class: "provider_error",
                });
                if (state !== null) yield* finalizeTurn(state);
                return;
              }
              if (!compactTurn.sawAgentActivity) {
                yield* scheduleSettleProbe(compactTurn);
              }
              return;
            }
            const pendingPrompt = command === "prompt" ? pendingPromptResponses.shift() : undefined;
            const responseTurn =
              pendingPrompt?.providerTurnId === turn?.providerTurn.id ? turn : null;
            if (event["success"] === true) {
              // Command-only prompts emit no agent_settled; probe idle and re-queue behind preceding native events.
              if (
                pendingPrompt?.kind === "turn_start" &&
                responseTurn !== null &&
                ((responseTurn.promptMayBeCommandOnly && !responseTurn.sawAgentActivity) ||
                  (responseTurn.adoptedWake &&
                    (responseTurn.settleWhenIdle || responseTurn.promptMayBeCommandOnly)))
              ) {
                yield* scheduleSettleProbe(responseTurn, responseTurn.adoptedWake);
              }
              return;
            }
            if (event["success"] !== false) return;
            if (command === "steer" || pendingPrompt?.kind === "steer") {
              // A rejected steer refuses only that message; its target turn keeps running.
              yield* Effect.logWarning("Pi rejected a steer message.", {
                errorLength: recordString(event, "error")?.length,
              });
              return;
            }
            const failedTurn =
              command === "prompt" && pendingPrompt?.kind === "turn_start"
                ? responseTurn
                : command === "parse"
                  ? turn
                  : null;
            if (failedTurn !== null) {
              const failure = makeProviderFailure({
                message: recordString(event, "error") ?? "Pi rejected the prompt.",
                class: "provider_error",
              });
              if (failedTurn.adoptedWake && command === "prompt") {
                failedTurn.rejectedPromptFailure = failure;
                failedTurn.settleWhenIdle = true;
                yield* scheduleSettleProbe(failedTurn, true);
              } else {
                failedTurn.failure = failure;
                if (state !== null) yield* finalizeTurn(state);
              }
            }
            return;
          }
          case "t3.ui_request_expired": {
            const requestId = recordString(event, "requestId");
            if (requestId === undefined) return;
            const pending = pendingPrompts.get(requestId);
            if (pending === undefined || pending !== event["pending"]) return;
            pendingPrompts.delete(requestId);
            yield* cancelPrompt(pending, yield* DateTime.now);
            return;
          }
          case "t3.flush_extension_errors": {
            for (const extensionError of outOfTurnExtensionErrors.splice(0)) {
              yield* emitExtensionError(extensionError);
            }
            return;
          }
          case "t3.rollback_barrier": {
            if (rollbackBarrier !== null && event["barrier"] === rollbackBarrier)
              yield* Deferred.succeed(rollbackBarrier, undefined);
            return;
          }
          case "t3.settle_probe": {
            const data = event["data"];
            const probeFailed = event["probeFailed"] === true;
            const settleAfterAgentActivity = event["settleAfterAgentActivity"] === true;
            const attempt = Math.max(1, Math.trunc(recordNumber(event, "attempt") ?? 1));
            if (
              turn === null ||
              turn.providerTurn.id !== event["providerTurnId"] ||
              turn.settleProbeGeneration !== event["settleProbeGeneration"] ||
              (!settleAfterAgentActivity && turn.sawAgentActivity) ||
              turn.activeCompaction !== null
            ) {
              return;
            }
            if (probeFailed) {
              if (!settleAfterAgentActivity) {
                if (state !== null) yield* finalizeTurn(state);
                return;
              }
              if (attempt < SETTLE_PROBE_MAX_ATTEMPTS) {
                yield* Effect.sleep(SETTLE_PROBE_RETRY_DELAY).pipe(
                  Effect.andThen(scheduleSettleProbe(turn, true, attempt + 1)),
                  Effect.forkIn(scope),
                );
                return;
              }
              stopRequested = true;
              yield* connection.terminate;
              return;
            }
            if (
              recordField(data, "isStreaming") !== true &&
              recordField(data, "isCompacting") !== true &&
              (recordNumber(data, "pendingMessageCount") ?? 0) === 0
            ) {
              turn.settleWhenIdle = false;
              if (state !== null) yield* finalizeTurn(state);
            }
            return;
          }
          default:
            return;
        }
      });

      yield* Effect.gen(function* () {
        while (true) {
          const event = yield* Queue.take(connection.events);
          yield* sessionEventPermit.withPermits(1)(
            Effect.gen(function* () {
              yield* maintainNativeHistory();
              yield* handleSessionEvent(event);
              yield* maintainNativeHistory();
              yield* Queue.offer(nativeMaintenanceChanged, undefined);
            }),
          );
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            const acknowledgements = yield* sessionEventPermit.withPermit(
              Effect.gen(function* () {
                livenessReady = false;
                const state = threadState;
                if (rollbackBarrier !== null)
                  yield* Deferred.fail(
                    rollbackBarrier,
                    protocolError("Pi transport closed during rollback"),
                  );
                pendingWake = null;
                const interrupted = state?.activeTurn?.interrupted === true;
                const retainedAcknowledgements: Deferred.Deferred<void>[] = [];
                for (const delivery of pendingDeliveries.values()) {
                  delivery.acknowledged ??= yield* Deferred.make<void>();
                  retainedAcknowledgements.push(delivery.acknowledged);
                }
                const acknowledgements = yield* closeNativeObservations(
                  [...nativeSubagents.values()].filter(
                    (child) =>
                      !child.observationClosed &&
                      (["running", "pending", "waiting", "idle"].includes(child.subagent.status) ||
                        child.inspectionPhase === "requesting" ||
                        child.refreshNeeded),
                  ),
                  [...subagentRuns]
                    .filter(([, binding]) => binding.awaitingFirstChild)
                    .map(([runId]) => runId),
                  "Native worker observer was lost. Final activity is unavailable. Workers may still be running; shutdown is unconfirmed.",
                );
                if (state?.activeTurn != null) {
                  state.activeTurn.failure = interrupted
                    ? null
                    : makeProviderFailure({
                        cause,
                        message: "Pi process exited unexpectedly.",
                        class: "transport_error",
                      });
                  yield* finalizeTurn(state, false, false);
                } else {
                  yield* cancelPendingPrompts(yield* DateTime.now);
                }
                return [...retainedAcknowledgements, ...acknowledgements];
              }),
            );
            // Ingestion needs the session permit; persistence ACKs must be awaited outside it.
            const persisted = yield* Effect.forEach(acknowledgements, Deferred.await, {
              discard: true,
            }).pipe(
              Effect.as(true),
              Effect.timeoutOrElse({
                duration: Duration.seconds(5),
                orElse: () => Effect.succeed(false),
              }),
            );
            if (!persisted)
              yield* Effect.logWarning(
                "Pi native observation cleanup persistence is unconfirmed after transport loss.",
              );
            const interrupted = threadState?.lastTurn?.interrupted === true;
            if (unsolicitedActivityDetected) {
              yield* updateProviderSession("error", PI_UNSOLICITED_ACTIVITY_ERROR);
              yield* Queue.end(events);
            } else if (stopRequested) {
              yield* updateProviderSession("stopped", null);
              yield* Queue.end(events);
            } else {
              yield* updateProviderSession(
                "error",
                interrupted ? "Pi process was stopped." : "Pi process exited unexpectedly.",
              );
              const failure = new ProviderAdapter.ProviderAdapterEventStreamError({
                driver: PI_PROVIDER,
                providerSessionId: input.providerSessionId,
                cause,
              });
              if (persisted) yield* Queue.fail(events, failure);
              else {
                drainedStreamFailure = failure;
                yield* Queue.end(events);
              }
            }
          }),
        ),
        Effect.forkIn(scope),
      );

      const registerThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
        publish = true,
      ) {
        if (rollbackBarrier !== null)
          return yield* protocolError("Cannot register a Pi thread during rollback");
        if (threadState !== null && threadState.activeTurn !== null) {
          return yield* protocolError("Cannot register a Pi thread while a turn is active");
        }
        if (pendingWake !== null) {
          const state = threadState;
          if (
            state !== null &&
            state === pendingWake.state &&
            state.providerThread.appThreadId === threadInput.threadId &&
            (threadInput.existingProviderThread === undefined ||
              threadInput.existingProviderThread.nativeThreadRef?.nativeId ===
                state.providerThread.nativeThreadRef?.nativeId)
          )
            return state.providerThread;
          return yield* protocolError(
            "Cannot switch Pi threads while native work is awaiting a turn",
          );
        }
        const existing = threadInput.existingProviderThread;
        const resumeId = existing?.nativeThreadRef?.nativeId;
        const needsNewSession = resumeId == null && registrationAttempted;
        registrationAttempted = true;
        if (resumeId != null || needsNewSession) {
          lastNativeThreadId = resumeId ?? lastNativeThreadId;
          // Failed lifecycle operations can still change native sessions; invalidate bindings and defaults.
          threadState = null;
          appliedModel = null;
          appliedThinking = null;
          appliedSessionName = null;
          baselineModel = null;
          baselineThinking = null;
          contextWindow = null;
          const result = yield* lifecycleRequest(
            resumeId != null
              ? { type: "switch_session", sessionPath: resumeId }
              : { type: "new_session" },
          );
          if (recordField(result, "cancelled") === true) {
            return yield* protocolError("A Pi extension cancelled the session switch");
          }
        }
        const stateData = yield* request({ type: "get_state" });
        if (!modelsDiscovered) {
          const modelsData = yield* request({ type: "get_available_models" }).pipe(
            Effect.orElseSucceed(() => undefined),
          );
          const models = recordField(modelsData, "models");
          if (Array.isArray(models)) {
            for (const model of models) rememberModelContextWindow(model);
            modelsDiscovered = true;
          }
        }
        contextWindow = rememberModelContextWindow(recordField(stateData, "model"));
        // Capture each baseline before applying selections so our own choices cannot become Pi defaults.
        if (baselineModel === null && appliedModel === null) {
          const stateModel = recordField(stateData, "model");
          const provider = recordString(stateModel, "provider");
          const modelId = recordString(stateModel, "id");
          if (provider !== undefined && modelId !== undefined) {
            baselineModel = { provider, modelId };
          }
        }
        if (baselineThinking === null && appliedThinking === null) {
          baselineThinking = recordString(stateData, "thinkingLevel") ?? null;
        }
        // switch_session accepts a file path, not Pi's display session UUID.
        const nativeId = recordString(stateData, "sessionFile");
        if (nativeId === undefined) {
          return yield* protocolError("get_state returned no persisted sessionFile", stateData);
        }
        if (needsNewSession && nativeId === lastNativeThreadId) {
          return yield* protocolError("Pi did not create a distinct session file");
        }
        lastNativeThreadId = nativeId;
        const createdAt = yield* DateTime.now;
        const providerThread: OrchestrationV2ProviderThread =
          existing !== undefined
            ? {
                ...existing,
                providerSessionId: input.providerSessionId,
                nativeThreadRef: providerRef(nativeId),
                ...(needsNewSession ? { nativeConversationHeadRef: null, contextUsage: null } : {}),
                status: "idle",
                updatedAt: createdAt,
              }
            : {
                id: idAllocator.derive.providerThread({
                  driver: PI_PROVIDER,
                  nativeThreadId: nativeId,
                }),
                driver: PI_PROVIDER,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: providerRef(nativeId),
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                pendingBackgroundTasks: [],
                createdAt,
                updatedAt: createdAt,
              };
        threadState = { providerThread, activeTurn: null };
        const baselineEntries = yield* request({ type: "get_entries" }).pipe(
          Effect.orElseSucceed(() => undefined),
        );
        lastKnownLeaf = recordString(baselineEntries, "leafId") ?? null;
        leafCursorStale = baselineEntries === undefined;
        if (publish)
          yield* emit({
            type: "provider_thread.updated",
            driver: PI_PROVIDER,
            providerThread,
          });
        return providerThread;
      });

      const applySelection = Effect.fnUntraced(function* (modelSelection: ModelSelection) {
        const thinking = getModelSelectionStringOptionValue(modelSelection, "thinking");
        if (modelSelection.model === PI_INHERIT_MODEL_SLUG) {
          if (appliedModel !== null && baselineModel !== null) {
            const restoredModel = yield* request({ type: "set_model", ...baselineModel });
            contextWindow = rememberModelContextWindow(restoredModel);
            appliedModel = null;
            const updatedAt = yield* DateTime.now;
            sessionEntity = { ...sessionEntity, model: PI_INHERIT_MODEL_SLUG, updatedAt };
            yield* emit({
              type: "provider_session.updated",
              driver: PI_PROVIDER,
              providerSession: sessionEntity,
            });
          }
          if (
            thinking === undefined &&
            appliedThinking !== null &&
            baselineThinking !== null &&
            appliedThinking !== baselineThinking
          ) {
            yield* request({ type: "set_thinking_level", level: baselineThinking });
            appliedThinking = null;
          }
        } else if (modelSelection.model !== appliedModel) {
          const parsed = parsePiModelSlug(modelSelection.model);
          if (parsed === null) {
            return yield* protocolError(
              `Pi model '${modelSelection.model}' must use provider/model format`,
            );
          }
          const selectedModel = yield* request({
            type: "set_model",
            provider: parsed.provider,
            modelId: parsed.modelId,
          });
          contextWindow = rememberModelContextWindow(selectedModel);
          appliedModel = modelSelection.model;
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, model: modelSelection.model, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: PI_PROVIDER,
            providerSession: sessionEntity,
          });
        }
        if (
          thinking !== undefined &&
          thinking !== appliedThinking &&
          PI_THINKING_LEVELS.has(thinking)
        ) {
          yield* request({ type: "set_thinking_level", level: thinking });
          appliedThinking = thinking;
        }
      });

      const resolvePromptPayload = Effect.fnUntraced(function* (
        text: string,
        attachments: ReadonlyArray<ChatAttachment>,
      ) {
        const images: Array<{ type: "image"; data: string; mimeType: string }> = [];
        const message = yield* providerMessageTextWithAttachmentPaths({
          text,
          attachments,
          resolveAttachmentPath: host.resolveAttachmentPath,
          resolveExtractedTextPath: host.resolveExtractedTextPath,
        });
        for (const attachment of attachments) {
          const path = host.resolveAttachmentPath(attachment);
          if (path === null) continue;
          if (attachment.mimeType.startsWith("image/")) {
            const bytes = yield* fileSystem.readFile(path);
            images.push({
              type: "image",
              data: Buffer.from(bytes).toString("base64"),
              mimeType: attachment.mimeType,
            });
          }
        }
        return { message, images };
      });

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: PI_PROVIDER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events).pipe(
          Stream.concat(
            Stream.suspend(() =>
              drainedStreamFailure === null ? Stream.empty : Stream.fail(drainedStreamFailure),
            ),
          ),
        ),
        backgroundWorkStatus: observeBackgroundWork(),
        hasPendingBackgroundWork: Effect.sync(
          () =>
            pendingWake !== null ||
            hasObservedChildren() ||
            pendingDeliveries.size > 0 ||
            [...subagentRuns.values()].some((binding) => binding.awaitingFirstChild === true),
        ),
        hasOpenEventSourceForRun: (runId) =>
          Effect.sync(
            () =>
              !closed &&
              [...subagentRuns.values()].some(
                (binding) => binding.turn.turnInput.runId === runId && !binding.observationExpired,
              ),
          ),
        hasPendingEventDeliveryForRun: (runId) =>
          Effect.sync(
            () =>
              [...pendingDeliveries.values()].some((delivery) => delivery.runId === runId) ||
              [...subagentRuns.values()].some(
                (binding) =>
                  binding.turn.turnInput.runId === runId && binding.awaitingFirstChild === true,
              ) ||
              [...nativeSubagents.values()].some(
                (child) =>
                  child.turn.turnInput.runId === runId &&
                  (child.inspectionPhase === "requesting" || child.refreshNeeded),
              ),
          ),
        // This runs for every delivered event, not just native cleanup fences.
        acknowledgeEvent: Effect.fnUntraced(function* (event, runId) {
          yield* sessionEventPermit.withPermit(
            Effect.gen(function* () {
              const delivery = pendingDeliveries.get(event);
              if (delivery?.runId !== runId) return;
              pendingDeliveries.delete(event);
              if (delivery.acknowledged !== undefined)
                yield* Deferred.succeed(delivery.acknowledged, undefined);
              yield* maintainNativeHistory();
              yield* Queue.offer(nativeMaintenanceChanged, undefined);
            }),
          );
        }),
        hasPendingBackgroundWorkForThread: Effect.fn("Pi.hasPendingBackgroundWorkForThread")(
          function* (providerThread: OrchestrationV2ProviderThread) {
            if (threadState?.providerThread.id !== providerThread.id) return false;
            // A detached run can be live before its first child snapshot arrives.
            return (
              pendingWake !== null ||
              hasObservedChildren() ||
              (yield* observeBackgroundWork()) === "active"
            );
          },
        ),
        getModelContextWindow: (selection) => {
          if (selection.instanceId !== options.instanceId) return undefined;
          const slug =
            selection.model === PI_INHERIT_MODEL_SLUG
              ? baselineModel === null
                ? undefined
                : `${baselineModel.provider}/${baselineModel.modelId}`
              : selection.model;
          return slug === undefined ? undefined : modelContextWindows.get(slug);
        },
        ensureThread: (threadInput) =>
          registerThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: PI_PROVIDER,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          registerThread({
            threadId:
              threadInput.threadId ?? threadInput.providerThread.appThreadId ?? input.threadId,
            modelSelection: threadInput.modelSelection ?? input.modelSelection,
            runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: threadInput.providerThread,
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: PI_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        compactThread: (turnInput) =>
          runtime.startTurn({
            ...turnInput,
            message: { ...turnInput.message, text: "/compact" },
          }),
        startTurn: Effect.fn("PiAdapter.startTurn")(
          function* (turnInput) {
            if (rollbackBarrier !== null)
              return yield* protocolError("Cannot start a Pi turn during rollback");
            const state = threadState;
            if (state === null) {
              return yield* protocolError("Pi session has no registered thread");
            }
            if (state.activeTurn !== null) {
              return yield* new ProviderAdapter.ProviderAdapterProtocolError({
                driver: PI_PROVIDER,
                detail: `Pi provider thread ${turnInput.providerThread.id} already has an active turn`,
                reason: "previous_turn_active",
              });
            }
            if (
              state.providerThread.nativeThreadRef?.nativeId !==
              turnInput.providerThread.nativeThreadRef?.nativeId
            ) {
              return yield* protocolError("Pi turn requested for a different native session");
            }
            // Fork adoption uses the orchestrator-allocated row; preserve that identity in later updates.
            const continuation =
              turnInput.message.createdBy === "agent" &&
              turnInput.message.creationSource === "provider";
            if (!continuation) yield* applySelection(turnInput.modelSelection);
            if (turnInput.appThread.title !== appliedSessionName) {
              yield* request({
                type: "set_session_name",
                name: turnInput.appThread.title,
              }).pipe(
                Effect.tap(() =>
                  Effect.sync(() => (appliedSessionName = turnInput.appThread.title)),
                ),
                Effect.ignore,
              );
            }
            // Resolve attachments before turn installation so failures cannot strand activeTurn.
            // Inject instructions through before_agent_start; wrapping user text breaks slash-command expansion.
            const compactCommand = continuation
              ? null
              : parsePiCompactCommand(turnInput.message.text);
            const payload =
              !continuation && compactCommand === null
                ? yield* resolvePromptPayload(turnInput.message.text, turnInput.message.attachments)
                : null;
            const startedAt = yield* DateTime.now;
            const syntheticNativeTurnId = `${state.providerThread.id}:attempt:${turnInput.attemptId}`;
            const providerTurn: OrchestrationV2ProviderTurn = {
              id: idAllocator.derive.providerTurn({
                driver: PI_PROVIDER,
                nativeTurnId: syntheticNativeTurnId,
              }),
              providerThreadId: turnInput.providerThread.id,
              nodeId: turnInput.rootNodeId,
              runAttemptId: turnInput.attemptId,
              nativeTurnRef: providerRef(syntheticNativeTurnId, "weak"),
              ordinal: turnInput.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            };
            const activeTurn: ActivePiTurn = {
              turnInput,
              providerTurn,
              startedAt,
              itemOrdinals: new Map(),
              nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              messageOrdinal: 0,
              streamItems: new Map(),
              toolArgs: new Map(),
              toolStartedAt: new Map(),
              interrupted: false,
              nativeAborted: false,
              sawAgentActivity: false,
              adoptedWake: false,
              promptMayBeCommandOnly:
                compactCommand !== null || (payload?.message.trimStart().startsWith("/") ?? false),
              latestCompactionAfterTokens: null,
              lastLiveUsedTokens: null,
              settleProbeGeneration: 0,
              settleWhenIdle: false,
              sawCompaction: false,
              manualCompactInFlight: compactCommand !== null,
              activeCompaction: null,
              activeProviderRetry: null,
              failure: null,
              rejectedPromptFailure: null,
            };
            // Lock only install/send/start events; correlated setup must leave the pump free for dialogs.
            yield* Effect.gen(function* () {
              if (
                threadState !== state ||
                state.activeTurn !== null ||
                closed ||
                stopRequested ||
                rollbackBarrier !== null ||
                state.providerThread.nativeThreadRef?.nativeId !==
                  turnInput.providerThread.nativeThreadRef?.nativeId
              ) {
                return yield* protocolError("Pi turn ownership changed before it could start");
              }
              const wake = pendingWake?.state === state ? pendingWake : null;
              if (wake !== null && wake.state.providerThread.appThreadId !== turnInput.threadId) {
                return yield* protocolError("Pi native wake belongs to a different app thread");
              }
              pendingWake = null;
              state.providerThread = turnInput.providerThread;
              state.activeTurn = activeTurn;
              activeTurn.adoptedWake = wake !== null && !continuation;
              if (compactCommand !== null) {
                yield* connection.send(compactRpcRecord(compactCommand));
                pendingCompactResponses.push({
                  providerTurnId: providerTurn.id,
                  kind: "turn_start",
                });
              } else if (payload !== null) {
                yield* connection.send({
                  type: "prompt",
                  message: payload.message,
                  streamingBehavior: "steer",
                  ...(payload.images.length === 0 ? {} : { images: payload.images }),
                });
                pendingPromptResponses.push({
                  providerTurnId: providerTurn.id,
                  kind: "turn_start",
                });
              }
              yield* emit({
                type: "provider_turn.updated",
                driver: PI_PROVIDER,
                threadId: turnInput.threadId,
                providerTurn,
              });
              yield* updateProviderThread(state, {
                status: "active",
                firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
                lastRunOrdinal: turnInput.runOrdinal,
              });
              yield* updateProviderSession("running", null);
              if (wake !== null) {
                for (const event of wake.events) {
                  if (!continuation && event["type"] === "agent_settled") {
                    activeTurn.nativeAborted = event["aborted"] === true;
                    activeTurn.settleWhenIdle = true;
                    continue;
                  }
                  yield* handleSessionEvent(event);
                }
              } else if (continuation) {
                yield* finalizeTurn(state);
              }
              if (outOfTurnExtensionErrors.length > 0) {
                yield* Queue.offer(connection.events, { type: "t3.flush_extension_errors" });
              }
            }).pipe(
              sessionEventPermit.withPermits(1),
              Effect.tapError(() =>
                Effect.sync(() => {
                  if (state.activeTurn === activeTurn) state.activeTurn = null;
                }),
              ),
            );
            // Pi acks `prompt` only after slash-command expansion completes,
            // and extension commands may block on user dialogs indefinitely.
            // Rejections therefore return later as id-less response records
            // handled by the event pump.
          },
          (effect, turnInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterTurnStartError({
                    driver: PI_PROVIDER,
                    threadId: turnInput.threadId,
                    providerThreadId: turnInput.providerThread.id,
                    runId: turnInput.runId,
                    cause,
                  }),
              ),
            ),
        ),
        steerTurn: Effect.fn("PiAdapter.steerTurn")(
          function* (steerInput: ProviderAdapter.ProviderAdapterV2SteerInput) {
            const turn = threadState?.activeTurn ?? null;
            if (turn === null || turn.providerTurn.id !== steerInput.providerTurnId) {
              return yield* protocolError(`Pi turn ${steerInput.providerTurnId} is not active`);
            }
            const compactCommand = parsePiCompactCommand(steerInput.message.text);
            const payload =
              compactCommand === null
                ? yield* resolvePromptPayload(
                    steerInput.message.text,
                    steerInput.message.attachments,
                  )
                : null;
            // prompt with steer atomically queues or starts; fire-and-forget under the permit avoids dialog deadlocks.
            // /compact uses its RPC, which aborts the agent first.
            yield* sessionEventPermit.withPermits(1)(
              Effect.gen(function* () {
                if (threadState?.activeTurn !== turn) {
                  return yield* protocolError(`Pi turn ${steerInput.providerTurnId} is not active`);
                }
                if (compactCommand !== null) {
                  turn.manualCompactInFlight = true;
                  yield* connection.send(compactRpcRecord(compactCommand));
                  pendingCompactResponses.push({
                    providerTurnId: turn.providerTurn.id,
                    kind: "steer",
                  });
                } else if (payload !== null) {
                  yield* connection.send({
                    type: "prompt",
                    message: payload.message,
                    streamingBehavior: "steer",
                    ...(payload.images.length === 0 ? {} : { images: payload.images }),
                  });
                  pendingPromptResponses.push({
                    providerTurnId: turn.providerTurn.id,
                    kind: "steer",
                  });
                }
                turn.settleProbeGeneration += 1;
              }),
            );
          },
          (effect, steerInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterSteerRunError({
                    driver: PI_PROVIDER,
                    providerThreadId: steerInput.providerThread.id,
                    providerTurnId: steerInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
        ),
        interruptTurn: Effect.fn("PiAdapter.interruptTurn")(
          function* (interruptInput) {
            const turn = threadState?.activeTurn ?? null;
            if (turn === null && interruptInput.requestRuntimeRestart === true) {
              if (pendingWake !== null)
                yield* sessionEventPermit.withPermits(1)(stopPendingWake(pendingWake));
              return;
            }
            if (turn === null || turn.providerTurn.id !== interruptInput.providerTurnId) {
              return yield* protocolError(`Pi turn ${interruptInput.providerTurnId} is not active`);
            }
            turn.interrupted = true;
            if (
              interruptInput.requestRuntimeRestart === true ||
              turn.settleWhenIdle ||
              turn.activeCompaction !== null ||
              turn.manualCompactInFlight
            ) {
              // Generic abort misses manual compaction; terminate so Stop covers /compact and recovery.
              stopRequested = true;
              if (interruptInput.requestRuntimeRestart === true && !turn.settleWhenIdle) {
                yield* request({ type: "abort" }, 2_000).pipe(Effect.ignore);
              }
              // Read rollback tree refs before termination breaks requests; the permit lets in-flight capture finish.
              yield* sessionEventPermit.withPermits(1)(
                Effect.gen(function* () {
                  if (threadState?.activeTurn === turn && turn.stopTreeRefs === undefined) {
                    turn.stopTreeRefs = yield* captureTurnTreeRefs(2_000);
                  }
                  yield* connection.terminate;
                }),
              );
              return;
            }
            yield* request({ type: "abort" }).pipe(
              Effect.tapError(() => Effect.sync(() => (turn.interrupted = false))),
            );
          },
          (effect, interruptInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: PI_PROVIDER,
                    providerThreadId: interruptInput.providerThread.id,
                    providerTurnId: interruptInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
        ),
        respondToRuntimeRequest: Effect.fn("PiAdapter.respondToRuntimeRequest")(
          function* (requestInput) {
            const pending = pendingPrompts.get(String(requestInput.requestId));
            if (pending === undefined) {
              return yield* protocolError(
                `No pending Pi extension request ${requestInput.requestId}`,
              );
            }
            const answeredAt = yield* DateTime.now;
            if (
              pending.expiresAt !== null &&
              DateTime.toEpochMillis(answeredAt) >= pending.expiresAt
            ) {
              pendingPrompts.delete(String(requestInput.requestId));
              yield* cancelPrompt(pending, answeredAt);
              return yield* protocolError(
                `Pi extension request ${requestInput.requestId} has expired`,
              );
            }
            const response = piUiResponse(pending, requestInput.decision, requestInput.answers);
            yield* connection.send({
              type: "extension_ui_response",
              id: pending.nativeRequestId,
              ...response,
            });
            pendingPrompts.delete(String(requestInput.requestId));
            if (pending.expirationFiber !== null) yield* Fiber.interrupt(pending.expirationFiber);
            if (pending.method === "confirm" && requestInput.decision === "acceptForSession") {
              sessionApprovals.add(pending.approvalKey);
            }
            const resolvedAt = yield* DateTime.now;
            pending.runtimeRequest = {
              ...pending.runtimeRequest,
              status: "resolved",
              resolvedAt,
            };
            yield* emit({
              type: "runtime_request.updated",
              driver: PI_PROVIDER,
              threadId: pending.node.threadId,
              runtimeRequest: pending.runtimeRequest,
            });
            yield* emit({
              type: "node.updated",
              driver: PI_PROVIDER,
              node: { ...pending.node, status: "completed", completedAt: resolvedAt },
            });
            yield* emit({
              type: "turn_item.updated",
              driver: PI_PROVIDER,
              turnItem: {
                ...pending.turnItem,
                status: "completed",
                completedAt: resolvedAt,
                updatedAt: resolvedAt,
              },
            });
          },
          (effect, requestInput) =>
            effect.pipe(
              sessionEventPermit.withPermits(1),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
                    driver: PI_PROVIDER,
                    requestId: requestInput.requestId,
                    cause,
                  }),
              ),
            ),
        ),
        readThreadSnapshot: Effect.fn("PiAdapter.readThreadSnapshot")(
          function* (snapshotInput) {
            const state = threadState;
            const boundNativeId = state?.providerThread.nativeThreadRef?.nativeId;
            const wantedNativeId = snapshotInput.providerThread.nativeThreadRef?.nativeId;
            if (state === null || wantedNativeId == null || boundNativeId !== wantedNativeId) {
              return yield* protocolError(
                "Pi snapshot requested for a thread this session does not host",
              );
            }
            // get_messages contains only the active branch; get_entries would leak abandoned branches into handoffs.
            const messagesData = yield* request({ type: "get_messages" });
            const activeMessages = recordField(messagesData, "messages");
            const threadId = state.providerThread.appThreadId ?? input.threadId;
            const messages = (Array.isArray(activeMessages) ? activeMessages : []).flatMap(
              (message, index) => {
                const role = recordString(message, "role");
                if (role !== "user" && role !== "assistant") return [];
                const text = contentText(recordField(message, "content"));
                if (text.length === 0) return [];
                const timestamp = recordNumber(message, "timestamp");
                const at = Option.getOrElse(
                  DateTime.make(timestamp ?? Number.NaN),
                  () => state.providerThread.createdAt,
                );
                return [
                  {
                    id: idAllocator.derive.messageFromProviderItem({
                      driver: PI_PROVIDER,
                      nativeItemId: `${wantedNativeId}:snapshot-message:${index}`,
                    }),
                    threadId,
                    runId: null,
                    nodeId: null,
                    role: role as "user" | "assistant",
                    text,
                    attachments: [],
                    streaming: false,
                    createdBy: role === "user" ? ("user" as const) : ("agent" as const),
                    creationSource: "provider" as const,
                    createdAt: at,
                    updatedAt: at,
                  },
                ];
              },
            );
            return {
              providerThread: state.providerThread,
              providerTurns: [],
              messages,
              runtimeRequests: [],
            };
          },
          (effect, snapshotInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterReadThreadSnapshotError({
                    driver: PI_PROVIDER,
                    providerThreadId: snapshotInput.providerThread.id,
                    cause,
                  }),
              ),
            ),
        ),
        rollbackThread: Effect.fn("PiAdapter.rollbackThread")(
          function* (rollbackInput) {
            const state = threadState;
            if (state === null) {
              return yield* protocolError("Pi session has no registered thread");
            }
            if (state.providerThread.id !== rollbackInput.providerThread.id) {
              return yield* protocolError(
                "Pi rollback requested for a thread this session does not host",
              );
            }
            if (state.activeTurn !== null) {
              return yield* protocolError("Cannot roll back while a Pi turn is active");
            }
            if (pendingWake !== null) {
              return yield* protocolError("Cannot roll back while native Pi work awaits a turn");
            }
            const boundary = piForkBoundary(rollbackInput);
            if (boundary.type === "nothing") {
              return piThreadSnapshot(state.providerThread);
            }
            const forkEntryId = yield* piForkEntry(
              boundary,
              rollbackInput.providerThread.createdAt,
              request({ type: "get_entries" }),
            );
            return yield* Effect.acquireUseRelease(
              sessionEventPermit.withPermits(1)(
                Effect.gen(function* () {
                  if (
                    rollbackBarrier !== null ||
                    threadState !== state ||
                    state.activeTurn !== null ||
                    pendingWake !== null ||
                    closed ||
                    stopRequested
                  )
                    return yield* protocolError(
                      "Pi rollback ownership changed before its fork could start",
                    );
                  const barrier = yield* Deferred.make<
                    void,
                    ProviderAdapter.ProviderAdapterProtocolError
                  >();
                  rollbackBarrier = barrier;
                  return barrier;
                }),
              ),
              (barrier) =>
                Effect.gen(function* () {
                  const forkData = yield* lifecycleRequest({ type: "fork", entryId: forkEntryId });
                  if (recordField(forkData, "cancelled") === true)
                    return yield* protocolError("A Pi extension cancelled the session fork");
                  const forkState = yield* request({ type: "get_state" }).pipe(
                    Effect.onError(() =>
                      Effect.sync(() => {
                        threadState = null;
                      }),
                    ),
                  );
                  const forkSessionFile = recordString(forkState, "sessionFile");
                  if (forkSessionFile === undefined) {
                    threadState = null;
                    return yield* protocolError("Pi fork did not return a persisted session file");
                  }
                  const entriesData = yield* request({ type: "get_entries" }).pipe(
                    Effect.orElseSucceed(() => undefined),
                  );
                  const leafId = recordString(entriesData, "leafId") ?? null;
                  // RPC responses bypass the pump; drain prior native events before replacing their binding.
                  const queued = yield* Queue.offer(connection.events, {
                    type: "t3.rollback_barrier",
                    barrier,
                  });
                  if (!queued) return yield* protocolError("Pi transport closed during rollback");
                  yield* Deferred.await(barrier);
                  return yield* sessionEventPermit.withPermits(1)(
                    Effect.gen(function* () {
                      if (pendingWake !== null) {
                        yield* stopPendingWake(pendingWake);
                        return yield* protocolError(
                          "Native Pi work started while rollback was changing the session",
                        );
                      }
                      if (
                        threadState !== state ||
                        state.activeTurn !== null ||
                        closed ||
                        stopRequested
                      )
                        return yield* protocolError(
                          "Pi rollback ownership changed during its fork",
                        );
                      lastNativeThreadId = forkSessionFile;
                      appliedModel = null;
                      appliedThinking = null;
                      appliedSessionName = null;
                      lastKnownLeaf = leafId;
                      leafCursorStale = entriesData === undefined;
                      yield* updateProviderThread(state, {
                        nativeThreadRef: providerRef(forkSessionFile),
                        nativeConversationHeadRef: leafId === null ? null : providerRef(leafId),
                      });
                      rollbackBarrier = null;
                      return piThreadSnapshot(state.providerThread);
                    }),
                  );
                }).pipe(Effect.onInterrupt(() => connection.terminate)),
              (barrier) =>
                Effect.sync(() => {
                  if (rollbackBarrier === barrier) rollbackBarrier = null;
                }),
            );
          },
          (effect, rollbackInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterRollbackThreadError({
                    driver: PI_PROVIDER,
                    providerThreadId: rollbackInput.providerThread.id,
                    checkpointId: rollbackInput.target.checkpointId,
                    cause,
                  }),
              ),
            ),
        ),
        forkThread: Effect.fn("PiAdapter.forkThread")(
          function* (forkInput) {
            if (threadState?.activeTurn != null) {
              return yield* protocolError("Cannot fork while a Pi turn is active");
            }
            const source = forkInput.sourceProviderThread;
            const sourceFile = source.nativeThreadRef?.nativeId;
            if (sourceFile == null)
              return yield* protocolError("Pi fork source has no session file");
            const sourceTurns = (forkInput.sourceProviderTurns ?? []).filter(
              (turn) => turn.providerThreadId === source.id,
            );
            const target = sourceTurns.find((turn) => turn.id === forkInput.providerTurnId);
            if (forkInput.providerTurnId !== undefined && target === undefined) {
              return yield* protocolError("Pi fork target turn is missing");
            }
            const boundary: PiForkBoundary =
              target === undefined
                ? { type: "nothing" }
                : piForkBoundary({
                    target: { type: "provider_turn", providerTurn: target },
                    providerThreadTurns: sourceTurns,
                  });
            // CLI --fork uses Pi's own session format and sets the destination cwd. RPC
            // switch_session/clone alone would retain the source cwd.
            const forkLaunch = buildPiRpcLaunch({
              launchArgs: resolvedLaunchArgs.args,
              environment: options.environment,
              mcpSession: undefined,
              extensionPath: undefined,
              disableExtensions: true,
              disableTools: true,
            });
            const nativeId = yield* Effect.scoped(
              Effect.gen(function* () {
                const forkConnection = yield* makePiRpcConnection({
                  command: options.settings.binaryPath || "pi",
                  args: [...forkLaunch.args, "--fork", sourceFile],
                  cwd,
                  env: forkLaunch.env,
                });
                yield* Stream.fromQueue(forkConnection.events).pipe(
                  Stream.runDrain,
                  Effect.ignore,
                  Effect.forkScoped,
                );
                const beforeEntry =
                  boundary.type === "nothing"
                    ? null
                    : yield* piForkEntry(
                        boundary,
                        source.createdAt,
                        forkConnection.request({ type: "get_entries" }),
                      );
                if (beforeEntry !== null) {
                  const result = yield* forkConnection.request({
                    type: "fork",
                    entryId: beforeEntry,
                  });
                  if (recordField(result, "cancelled") === true) {
                    return yield* protocolError("A Pi extension cancelled the session fork");
                  }
                }
                const state = yield* forkConnection.request({ type: "get_state" });
                const file = recordString(state, "sessionFile");
                if (file === undefined || file === sourceFile) {
                  return yield* protocolError("Pi fork did not create a distinct session file");
                }
                return file;
              }),
            ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
            const now = yield* DateTime.now;
            return yield* registerThread(
              {
                threadId: forkInput.targetThreadId,
                modelSelection: forkInput.modelSelection ?? input.modelSelection,
                runtimePolicy: forkInput.runtimePolicy ?? input.runtimePolicy,
                existingProviderThread: {
                  ...source,
                  id: idAllocator.derive.providerThread({
                    driver: PI_PROVIDER,
                    nativeThreadId: nativeId,
                  }),
                  appThreadId: forkInput.targetThreadId,
                  providerSessionId: input.providerSessionId,
                  providerInstanceId: options.instanceId,
                  ownerNodeId: forkInput.ownerNodeId ?? null,
                  nativeThreadRef: providerRef(nativeId),
                  nativeConversationHeadRef: null,
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  pendingBackgroundTasks: [],
                  forkedFrom: {
                    providerThreadId: source.id,
                    ...(forkInput.providerTurnId === undefined
                      ? {}
                      : { providerTurnId: forkInput.providerTurnId }),
                  },
                  createdAt: now,
                  updatedAt: now,
                },
              },
              false,
            );
          },
          (effect, forkInput) =>
            effect.pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterForkThreadError({
                    driver: PI_PROVIDER,
                    providerThreadId: forkInput.sourceProviderThread.id,
                    cause,
                  }),
              ),
            ),
        ),
      };
      return runtime;
    }),
  });
});

interface PiSessionEntry {
  readonly parentId: string | null;
  readonly isUserMessage: boolean;
  /** Pi's `timestamp` as epoch milliseconds; null when missing or unparsable. */
  readonly timestamp: number | null;
}

interface PiSessionListing {
  readonly entries: ReadonlyMap<string, PiSessionEntry>;
  readonly leafId: string | null;
}

function piSessionListing(data: unknown): PiSessionListing | null {
  const leafId = recordField(data, "leafId");
  const entries = piSessionEntries(recordField(data, "entries"));
  return entries !== null && (typeof leafId === "string" || leafId === null)
    ? { entries, leafId }
    : null;
}

function piSessionEntries(entries: unknown): ReadonlyMap<string, PiSessionEntry> | null {
  const byId = new Map<string, PiSessionEntry>();
  if (!Array.isArray(entries)) return null;
  for (const entry of entries) {
    const id = recordString(entry, "id");
    const parentId = recordField(entry, "parentId");
    if (id === undefined || byId.has(id) || (parentId !== null && typeof parentId !== "string"))
      return null;
    const timestamp = recordString(entry, "timestamp");
    byId.set(id, {
      parentId,
      isUserMessage:
        recordField(entry, "type") === "message" &&
        recordString(recordField(entry, "message"), "role") === "user",
      timestamp:
        timestamp === undefined
          ? null
          : Option.getOrNull(Option.map(DateTime.make(timestamp), DateTime.toEpochMillis)),
    });
  }
  return byId;
}

function walkPiBranch(
  entries: ReadonlyMap<string, PiSessionEntry>,
  leafId: string | null,
  stopAt: string | null,
): {
  readonly entryCount: number;
  readonly userEntryIds: ReadonlyArray<string>;
  readonly reachedStop: boolean;
} | null {
  const visited = new Set<string>();
  const userEntryIds: Array<string> = [];
  let current = leafId;
  while (current !== null && current !== stopAt) {
    const entry = entries.get(current);
    if (entry === undefined) break;
    if (visited.has(current)) return null;
    visited.add(current);
    if (entry.isUserMessage) userEntryIds.push(current);
    current = entry.parentId;
  }
  return { entryCount: visited.size, userEntryIds, reachedStop: current === stopAt };
}

type PiForkBoundary =
  | { readonly type: "entry"; readonly entryId: string }
  | { readonly type: "nothing" }
  | {
      readonly type: "uncaptured";
      readonly turn: OrchestrationV2ProviderTurn;
      readonly target: OrchestrationV2ProviderTurn | null;
    };

/**
 * Turns without a ref left nothing on the active branch, so the boundary is the first discarded
 * turn that has one. `uncaptured` is a boundary turn still carrying its synthetic ref (only
 * `captureTurnTreeRefs` records strong ones); `piForkEntry` locates it in the session tree.
 */
function piForkBoundary(input: {
  readonly target:
    | { readonly type: "thread_start" }
    | { readonly type: "provider_turn"; readonly providerTurn: OrchestrationV2ProviderTurn };
  readonly providerThreadTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
}): PiForkBoundary {
  const target = input.target.type === "thread_start" ? null : input.target.providerTurn;
  const boundary = input.providerThreadTurns
    .filter((turn) => turn.ordinal > (target?.ordinal ?? 0) && turn.nativeTurnRef !== null)
    .sort((a, b) => a.ordinal - b.ordinal)[0];
  const ref = boundary?.nativeTurnRef;
  if (boundary === undefined || ref == null) return { type: "nothing" };
  return ref.strength === "strong" && ref.nativeId !== null
    ? { type: "entry", entryId: ref.nativeId }
    : { type: "uncaptured", turn: boundary, target };
}

/** Reads the session tree only for an uncaptured boundary. */
function piForkEntry<E>(
  boundary: Exclude<PiForkBoundary, { type: "nothing" }>,
  providerThreadCreatedAt: DateTime.Utc,
  readEntries: Effect.Effect<unknown, E>,
): Effect.Effect<string, E | ProviderAdapter.ProviderAdapterProtocolError> {
  if (boundary.type === "entry") return Effect.succeed(boundary.entryId);
  return readEntries.pipe(
    Effect.flatMap((data) => {
      const listing = piSessionListing(data);
      const entryId =
        listing === null
          ? undefined
          : piUncapturedForkEntry(listing, boundary, providerThreadCreatedAt);
      return entryId === undefined
        ? Effect.fail(
            new ProviderAdapter.ProviderAdapterProtocolError({
              driver: PI_PROVIDER,
              detail: `Pi turn ${boundary.turn.ordinal} has no captured session-tree entry and none was found in the session tree`,
            }),
          )
        : Effect.succeed(entryId);
    }),
  );
}

/**
 * Pi shares the server clock. An adopted wake can precede its T3 turn, so search from the target
 * turn's end through the boundary turn's end. A boundary turn still running has no end yet, so
 * the window stays open through the tree read. At thread start, use the earlier of thread
 * creation and turn start to include adopted wakes while preserving inherited session history.
 */
function piUncapturedForkEntry(
  listing: PiSessionListing,
  boundary: Extract<PiForkBoundary, { type: "uncaptured" }>,
  providerThreadCreatedAt: DateTime.Utc,
): string | undefined {
  const { startedAt, completedAt } = boundary.turn;
  if (startedAt === null) return undefined;
  const from =
    boundary.target === null
      ? Math.min(DateTime.toEpochMillis(providerThreadCreatedAt), DateTime.toEpochMillis(startedAt))
      : DateTime.toEpochMillis(boundary.target.completedAt ?? startedAt);
  const to = completedAt === null ? Infinity : DateTime.toEpochMillis(completedAt);
  const targetRef = boundary.target?.nativeTurnRef;
  const stopAt = targetRef?.strength === "strong" ? targetRef.nativeId : null;
  if (stopAt !== null && !listing.entries.has(stopAt)) return undefined;
  const walk = walkPiBranch(listing.entries, listing.leafId, stopAt);
  if (walk === null || !walk.reachedStop) return undefined;
  let earliest: string | undefined;
  for (const id of walk.userEntryIds) {
    const timestamp = listing.entries.get(id)?.timestamp;
    if (timestamp == null) return undefined;
    if (timestamp >= from && timestamp <= to) earliest = id;
  }
  return earliest;
}

function piExtensionDisplayName(extensionPath: string | undefined): string {
  if (extensionPath === undefined) return "Pi extension";
  const normalized = extensionPath.replace(/\\/g, "/").replace(/\/+$/, "");
  const name = normalized.slice(normalized.lastIndexOf("/") + 1).replace(/\.[^.]+$/, "");
  return name.length === 0 ? "Pi extension" : name;
}

function piThreadSnapshot(
  providerThread: OrchestrationV2ProviderThread,
): ProviderAdapter.ProviderAdapterV2ThreadSnapshot {
  return { providerThread, providerTurns: [], messages: [], runtimeRequests: [] };
}

function piQuestion(
  questionId: string,
  method: "select" | "input" | "editor",
  title: string,
  event: PiRpcRecord,
): OrchestrationV2UserInputQuestion {
  const options =
    method === "select" && Array.isArray(event["options"])
      ? event["options"]
          .filter((option): option is string => typeof option === "string")
          .map((option) => ({
            label: option || "Empty value",
            description: option || "Empty value",
            value: option,
          }))
      : method === "editor"
        ? []
        : [
            {
              label: "Submit empty value",
              description: "Send an empty string to the extension.",
              value: "",
            },
          ];
  // Extensions may send a blank placeholder, which is not a valid question.
  const question =
    [recordString(event, "message"), recordString(event, "placeholder")].find(
      (text) => text !== undefined && text.trim().length > 0,
    ) ?? title;
  return {
    id: questionId,
    header: title,
    question,
    ...(method === "editor" ? { initialAnswer: recordString(event, "prefill") ?? "" } : {}),
    options,
  };
}

function piUiResponse(
  pending: PendingPiPrompt,
  decision: ProviderApprovalDecision | undefined,
  answers: Record<string, unknown> | undefined,
): PiRpcRecord {
  if (pending.method === "confirm") {
    if (decision === "accept" || decision === "acceptForSession") return { confirmed: true };
    if (decision === "decline") return { confirmed: false };
    return { cancelled: true };
  }
  const answer = answers?.[pending.questionId];
  // RPC distinguishes empty dialog values from cancellation (undefined).
  if (typeof answer === "string") return { value: answer };
  return { cancelled: true };
}

export type PiAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | McpProviderSessions.McpProviderSessions
  | ProviderHost.ProviderHost;

export const PiAdapterV2Driver: ProviderAdapterDriver<PiSettings, PiAdapterV2DriverEnv> = {
  driverKind: PI_DRIVER_KIND,
  configSchema: PiSettings,
  defaultConfig: (): PiSettings => DEFAULT_PI_SETTINGS,
  create: Effect.fn("PiAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<PiSettings>) {
      const hostEnvironment = yield* HostProcess.Environment;
      const continuationRequests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
      return yield* makePiAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: yield* mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        continuationRequests,
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: PI_DRIVER_KIND,
              instanceId: input.instanceId,
              detail: "Failed to create Pi adapter.",
              cause,
            }),
        ),
      ),
  ),
};
