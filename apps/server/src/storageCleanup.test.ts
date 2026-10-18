import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Path from "effect/Path";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as RpcTest from "effect/rpc/RpcTest";
import {
  DEFAULT_SERVER_SETTINGS,
  StorageCleanupReport,
  WS_METHODS,
  WsRpcGroup,
  AuthOrchestrationReadScope,
  AuthSourceControlWriteScope,
  type AuthEnvironmentScope,
  type WorktreeKeepWhen,
} from "@t3tools/contracts";
import * as ServerConfig from "./config.ts";
import * as StorageCleanup from "./storageCleanup.ts";
import * as RpcAuthorization from "./auth/RpcAuthorization.ts";
import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";
import * as GitManager from "./git/GitManager.ts";
import * as Settings from "./serverSettings.ts";
import * as ProjectionStore from "./orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import * as Orchestrator from "./orchestration-v2/Orchestrator.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import * as SqlitePersistence from "./persistence/Sqlite.ts";
import * as EffectOutbox from "./orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "./orchestration-v2/EffectWorker.ts";
import { CodexProviderCapabilitiesV2 } from "./orchestration-v2/Adapters/CodexAdapterV2.ts";
import { ServerActivation } from "./serverActivation.ts";
import { describe, expect, it } from "@effect/vitest";
import {
  ProjectId,
  ProviderInstanceId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  EventId,
  NodeId,
  ProviderThreadId,
  ProviderSessionId,
  ProviderTurnId,
  RunAttemptId,
  MessageId,
  CommandId,
  TurnItemId,
  ProviderDriverKind,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Subagent,
  type OrchestrationV2Run,
  type PiNativeWorkerCheckpoint,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import {
  storageCleanupActivityAt,
  storageCleanupPullRequestMerged,
  storageCleanupThreadIdle,
} from "./storageCleanup.ts";

const NOW_MS = Date.parse("2026-06-10T12:00:00.000Z");
const DAY_MS = 24 * 60 * 60 * 1_000;
const decodeCleanupReport = Schema.decodeSync(StorageCleanupReport);

function at(offsetMs: number): DateTime.Utc {
  return DateTime.makeUnsafe(NOW_MS + offsetMs);
}

function shell(overrides: Partial<OrchestrationV2ThreadShell> = {}): OrchestrationV2ThreadShell {
  return {
    id: ThreadId.make("thread-1"),
    projectId: ProjectId.make("project-1"),
    title: "Thread",
    providerInstanceId: ProviderInstanceId.make("codex"),
    modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: {
      rootThreadId: ThreadId.make("thread-1"),
      parentThreadId: null,
      relationshipToParent: null,
    },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    activeRunId: null,
    latestVisibleMessage: null,
    hasActionableProposedPlan: false,
    itemCount: 0,
    visibleItemCount: 0,
    lastVisitedAt: null,
    deletedAt: null,
    branch: null,
    linkedPullRequest: null,
    status: "idle",
    activityRunStatus: null,
    pendingRuntimeRequest: null,
    pendingBackgroundTasks: [],
    latestRunId: null,
    latestRunRequestedAt: null,
    latestRunStartedAt: null,
    latestRunCompletedAt: null,
    latestUserMessageAt: null,
    createdAt: at(-30 * DAY_MS),
    updatedAt: at(-10 * DAY_MS),
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    snoozedUntil: null,
    snoozedAt: null,
    pinnedAt: null,
    ...overrides,
  };
}

describe("V2 storage cleanup eligibility", () => {
  const candidate = () => shell({ branch: "feature", worktreePath: "/worktrees/feature" });

  it("allows an idle worktree and rejects the project checkout", () => {
    expect(storageCleanupThreadIdle(candidate(), NOW_MS)).toBe(true);
    expect(storageCleanupThreadIdle(shell(), NOW_MS)).toBe(false);
  });

  it.each(["running", "starting", "preparing", "waiting", "queued"] as const)(
    "retains a worktree while its thread is %s",
    (status) => {
      expect(storageCleanupThreadIdle(candidateWithStatus(status), NOW_MS)).toBe(false);
    },
  );

  it.each(["idle", "completed", "interrupted", "failed", "cancelled", "rolled_back"] as const)(
    "allows cleanup once its thread is %s",
    (status) => {
      expect(storageCleanupThreadIdle(candidateWithStatus(status), NOW_MS)).toBe(true);
    },
  );

  it.each(["completed", "interrupted", "cancelled", "rolled_back"] as const)(
    "retains %s while background work is pending",
    (status) => {
      expect(
        storageCleanupThreadIdle(
          {
            ...candidateWithStatus(status),
            pendingBackgroundTasks: [{ taskId: "task-1", kind: "command" }],
          },
          NOW_MS,
        ),
      ).toBe(false);
    },
  );

  it.each(["completed", "interrupted", "cancelled", "rolled_back"] as const)(
    "retains %s while a runtime request is pending",
    (status) => {
      expect(
        storageCleanupThreadIdle(
          {
            ...candidateWithStatus(status),
            pendingRuntimeRequest: {
              id: RuntimeRequestId.make("request-1"),
              kind: "command",
              createdAt: at(0),
            },
          },
          NOW_MS,
        ),
      ).toBe(false);
    },
  );

  it("retains an active run even if the shell status is idle", () => {
    expect(
      storageCleanupThreadIdle({ ...candidate(), activeRunId: RunId.make("run") }, NOW_MS),
    ).toBe(false);
  });

  it("retains a queued prompt before the new run has been projected", () => {
    expect(
      storageCleanupThreadIdle({ ...candidate(), latestUserMessageAt: at(-1_000) }, NOW_MS),
    ).toBe(false);
  });

  it("uses V2 run activity instead of metadata refreshes for retention", () => {
    const thread = candidate();
    const runTime = at(-3 * DAY_MS);
    expect(
      storageCleanupActivityAt({ ...thread, latestRunCompletedAt: runTime, updatedAt: at(0) }),
    ).toBe(DateTime.toEpochMillis(runTime));
  });

  function candidateWithStatus(status: OrchestrationV2ThreadShell["status"]) {
    return { ...candidate(), status };
  }
});

describe("merged pull request cleanup", () => {
  const HEAD_SHA = "a".repeat(40);
  const integrated = {
    branch: "feature",
    defaultBranch: "main",
    headSha: HEAD_SHA,
    integrated: true,
  };
  const squashed = { ...integrated, integrated: false };
  const pullRequest = (
    overrides: Partial<NonNullable<Parameters<typeof storageCleanupPullRequestMerged>[0]>> = {},
  ) => ({
    state: "merged" as const,
    headRef: "feature",
    baseRef: "main",
    headSha: HEAD_SHA,
    ...overrides,
  });

  it("removes a worktree whose head reached the default branch through a merged pull request", () => {
    expect(storageCleanupPullRequestMerged(pullRequest({ headSha: null }), integrated)).toBe(true);
  });

  it("removes a squash-merged worktree when the pull request names its exact head", () => {
    expect(storageCleanupPullRequestMerged(pullRequest(), squashed)).toBe(true);
  });

  it.each([
    ["has a later commit than the merged head", { headSha: "c".repeat(40) }],
    ["was merged into a release branch", { baseRef: "release" }],
    ["was merged into its stack parent", { baseRef: "stack-parent" }],
    ["was merged without a reported head commit", { headSha: null }],
    ["belongs to a different branch", { headRef: "other" }],
    ["is still open", { state: "open" }],
    ["was closed without merging", { state: "closed" }],
  ] as const)("keeps a squash worktree whose pull request %s", (_name, overrides) => {
    expect(storageCleanupPullRequestMerged(pullRequest(overrides), squashed)).toBe(false);
  });

  it("keeps a worktree with no pull request, or one that is not merged", () => {
    expect(storageCleanupPullRequestMerged(null, squashed)).toBe(false);
    expect(storageCleanupPullRequestMerged(null, integrated)).toBe(false);
    expect(storageCleanupPullRequestMerged(pullRequest({ state: "open" }), integrated)).toBe(false);
  });
});

const cleanupFixture = Effect.gen(function* () {
  const persistedSql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const fs = yield* FileSystem.FileSystem;
  const config = yield* ServerConfig.ServerConfig;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const repo = yield* fs.makeTempDirectoryScoped({ prefix: "cleanup-repo-" });
  const command = (cwd: string, args: string[]) =>
    git.execute({ operation: "cleanup-test", cwd, args });
  yield* command(repo, ["init", "-b", "main"]);
  yield* command(repo, ["config", "user.email", "cleanup@example.test"]);
  yield* command(repo, ["config", "user.name", "Cleanup test"]);
  yield* fs.writeFileString(`${repo}/tracked.txt`, "original\n");
  yield* fs.writeFileString(`${repo}/.gitignore`, ".env\ndist/\nnode_modules/\n");
  yield* command(repo, ["add", "."]);
  yield* command(repo, ["commit", "-m", "initial"]);
  yield* fs.makeDirectory(config.worktreesDir, { recursive: true });
  const worktree = `${config.worktreesDir}/feature`;
  yield* command(repo, ["worktree", "add", "-b", "feature", worktree]);
  let settings: import("@t3tools/contracts").ServerSettings = {
    ...DEFAULT_SERVER_SETTINGS,
    storageCleanup: { ...DEFAULT_SERVER_SETTINGS.storageCleanup, worktreeAfterDays: 8 },
  };
  let threads = [shell({ branch: "feature", worktreePath: worktree })];
  let editBeforeRemoval = false;
  let openSessionCwd: string | null = null;
  let fullStatusCalls = 0;
  let drainEffects: ReturnType<EffectWorker.OrchestrationEffectWorkerV2Shape["drain"]> =
    Effect.succeed(0);
  const context = yield* Layer.build(StorageCleanup.layer).pipe(
    Effect.provideService(EffectWorker.OrchestrationEffectWorkerV2, {
      awaitWork: Effect.void,
      runOnce: Effect.succeed(false),
      runRecoveryOnce: Effect.succeed(false),
      nextClaimableAt: Effect.succeed(Option.none()),
      drain: () => Effect.suspend(() => drainEffects),
    }),
    Effect.provideService(GitVcsDriver.GitVcsDriver, {
      ...git,
      execute: (input) =>
        Effect.gen(function* () {
          if (editBeforeRemoval && input.args.includes("remove"))
            yield* fs.writeFileString(`${worktree}/tracked.txt`, "late edit\n");
          return yield* git.execute(input);
        }),
      statusDetailsLocal: (...args) => {
        fullStatusCalls++;
        return git.statusDetailsLocal(...args);
      },
    }),
    Effect.provideService(Settings.ServerSettingsService, {
      getSettings: Effect.sync(() => settings),
      subscribeChanges: Effect.succeed(Stream.empty),
    } as unknown as Settings.ServerSettingsService["Service"]),
    Effect.provideService(ProjectStore.ProjectStoreV2, {
      listShells: () => Effect.succeed([{ id: ProjectId.make("project-1"), workspaceRoot: repo }]),
    } as unknown as ProjectStore.ProjectStoreV2["Service"]),
    Effect.provideService(ProjectionStore.ProjectionStoreV2, {
      getShellSnapshot: (input?: { location?: string }) =>
        Effect.sync(() => ({
          threads: threads.filter((thread) =>
            input?.location === "archive" ? thread.archivedAt !== null : thread.archivedAt === null,
          ),
        })),
    } as unknown as ProjectionStore.ProjectionStoreV2["Service"]),
    Effect.provideService(Orchestrator.OrchestratorV2, {
      streamDomainEvents: Stream.empty,
    } as unknown as Orchestrator.OrchestratorV2["Service"]),
    Effect.provideService(
      SqlClient.SqlClient,
      Option.getOrElse(
        persistedSql,
        () =>
          ((strings: TemplateStringsArray) =>
            Effect.sync(() =>
              openSessionCwd !== null && strings.join("").includes("projection_provider_sessions")
                ? [
                    {
                      payload_json: JSON.stringify({
                        id: "session-1",
                        driver: "codex",
                        providerInstanceId: "codex",
                        status: "ready",
                        cwd: openSessionCwd,
                        model: null,
                        capabilities: CodexProviderCapabilitiesV2,
                        createdAt: "2026-06-01T12:00:00.000Z",
                        updatedAt: "2026-06-01T12:00:00.000Z",
                        lastError: null,
                      }),
                    },
                  ]
                : [],
            )) as unknown as SqlClient.SqlClient,
      ),
    ),
    Effect.provideService(GitManager.GitManager, {
      invalidateStatus: () => Effect.void,
    } as unknown as GitManager.GitManager["Service"]),
    Effect.provideService(TerminalManager.TerminalManager, {
      subscribeMetadata: () => Effect.succeed(() => {}),
    } as unknown as TerminalManager.TerminalManager["Service"]),
    Effect.provideService(ServerActivation, Effect.never),
  );
  return {
    service: Context.get(context, StorageCleanup.StorageCleanup),
    editBeforeRemoval: () => {
      editBeforeRemoval = true;
    },
    fs,
    worktree,
    repo,
    config,
    command,
    enableFiles: () => {
      settings = {
        ...settings,
        storageCleanup: {
          ...settings.storageCleanup,
          browserArtifactsAfterDays: 8,
          logsAfterDays: 8,
        },
      };
    },
    setPolicy: (worktreeKeepWhen: WorktreeKeepWhen) => {
      settings = { ...settings, storageCleanup: { ...settings.storageCleanup, worktreeKeepWhen } };
    },
    disable: () => {
      settings = {
        ...settings,
        storageCleanup: { ...settings.storageCleanup, worktreeAfterDays: null },
      };
    },
    drainWith: (effect: typeof drainEffects) => {
      drainEffects = effect;
    },
    onDelete: (enabled: boolean) => {
      settings = {
        ...settings,
        storageCleanup: { ...settings.storageCleanup, worktreeOnDelete: enabled },
      };
    },
    disableProjectCleanup: (projectId: ProjectId) => {
      settings = {
        ...settings,
        projectSettingsOverrides: {
          ...settings.projectSettingsOverrides,
          [projectId]: { worktreeCleanup: { mode: "off" } },
        },
      };
    },
    setThreads: (next: typeof threads) => {
      threads = next;
    },
    setOpenSessionCwd: (cwd: string | null) => {
      openSessionCwd = cwd;
    },
    fullStatusCalls: () => fullStatusCalls,
  };
});
const cleanupTestLayer = GitVcsDriver.layer.pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "storage-cleanup-" })),
  Layer.provideMerge(NodeServices.layer),
);
const runCleanupTest = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    | FileSystem.FileSystem
    | ServerConfig.ServerConfig
    | GitVcsDriver.GitVcsDriver
    | Path.Path
    | Scope.Scope
  >,
) => effect.pipe(Effect.provide(cleanupTestLayer), Effect.scoped);

const nativePersistenceLayer = Layer.mergeAll(ProjectionStore.layer, EffectOutbox.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

const nativeCleanupFixture = Effect.gen(function* () {
  const fixture = yield* cleanupFixture;
  const sql = yield* SqlClient.SqlClient;
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  let sequence = 0;
  const stamp = () => ({ id: EventId.make(`cleanup:${++sequence}`), occurredAt: at(0) });
  const parent = shell({
    providerInstanceId: ProviderInstanceId.make("pi"),
    modelSelection: { instanceId: ProviderInstanceId.make("pi"), model: "default" },
    activeProviderThreadId: ProviderThreadId.make("parent-provider-thread"),
    branch: "feature",
    worktreePath: fixture.worktree,
  });
  yield* sql`
    INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
    VALUES (${parent.projectId}, 'Cleanup', ${fixture.repo}, '[]', '2026-06-01T00:00:00Z', '2026-06-01T00:00:00Z')
  `;
  yield* store.apply({
    ...stamp(),
    type: "thread.created",
    threadId: parent.id,
    payload: { ...parent, lastVisitedAt: parent.lastVisitedAt ?? null },
  });
  const providerThread = (thread: OrchestrationV2ThreadShell): OrchestrationV2ProviderThread => ({
    id: thread.activeProviderThreadId!,
    driver: ProviderDriverKind.make("pi"),
    providerInstanceId: parent.providerInstanceId,
    providerSessionId: ProviderSessionId.make("pi-session"),
    appThreadId: thread.id,
    ownerNodeId: null,
    nativeThreadRef: null,
    nativeConversationHeadRef: null,
    status: "not_loaded",
    firstRunOrdinal: null,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    createdAt: at(0),
    updatedAt: at(0),
  });
  const persistProvider = (provider: OrchestrationV2ProviderThread) =>
    store.apply({
      ...stamp(),
      type: "provider-thread.updated",
      threadId: provider.appThreadId!,
      payload: provider,
    });
  yield* persistProvider(providerThread(parent));
  const observers: OrchestrationV2ThreadShell[] = [];
  const addObserver = Effect.fnUntraced(function* (owner = parent, archived = false) {
    const index = observers.length + 1;
    const id = NodeId.make(`observer-${index}`);
    const runId = RunId.make(`native-run-${index}`);
    const child = shell({
      ...owner,
      id: ThreadId.make(`child-${index}`),
      title: `Observation ${index}`,
      // The shell deliberately inherits running: persisted capture, not shell
      // idleness, determines whether it is an independent checkout owner.
      status: "running",
      archivedAt: archived ? at(0) : null,
      activeProviderThreadId: ProviderThreadId.make(`child-provider-${index}`),
      createdBy: "agent",
      creationSource: "provider",
      lineage: {
        rootThreadId: parent.id,
        parentThreadId: owner.id,
        relationshipToParent: "subagent",
      },
      forkedFrom: { type: "node", nodeId: id },
    });
    const run: OrchestrationV2Run = {
      id: runId,
      threadId: owner.id,
      ordinal: index,
      providerInstanceId: parent.providerInstanceId,
      modelSelection: parent.modelSelection,
      providerThreadId: owner.activeProviderThreadId,
      userMessageId: MessageId.make(`message-${index}`),
      rootNodeId: NodeId.make(`root-${index}`),
      activeAttemptId: RunAttemptId.make(`attempt-${index}`),
      status: "completed",
      requestedAt: at(0),
      startedAt: at(0),
      completedAt: at(0),
      checkpointId: null,
      contextHandoffId: null,
    };
    yield* store.apply({
      ...stamp(),
      type: "run.updated",
      threadId: owner.id,
      runId,
      payload: run,
    });
    const launch: PiNativeWorkerCheckpoint["launch"] = {
      source: "agent_spawn",
      instanceId: `instance-${index}`,
      providerInstanceId: parent.providerInstanceId,
      providerSessionId: ProviderSessionId.make("pi-session"),
      parentThreadId: owner.id,
      parentProviderThreadId: owner.activeProviderThreadId!,
      parentProviderTurnId: ProviderTurnId.make(`turn-${index}`),
      runId,
      runAttemptId: run.activeAttemptId!,
      toolNodeId: NodeId.make(`tool-${index}`),
      coordinatorSessionId: "coordinator",
      toolCallId: `call-${index}`,
    };
    const checkpoint: PiNativeWorkerCheckpoint = {
      launch,
      owner: { processId: "process", coordinatorSessionId: "coordinator", epoch: "epoch" },
      ownerGeneration: 2,
      captureEpoch: "capture",
      capture: "closed",
      finalSequence: 4,
      bindings: [
        {
          generation: 1,
          historyId: "history",
          sessionId: `native-session-${index}`,
          state: "historical",
          headEntryId: "entry",
          capture: "closed",
          gaps: [],
          attemptIds: [],
          attempts: [],
        },
      ],
      bindingGeneration: 1,
      bindingCursors: [{ generation: 1, cursor: "end", cut: 4, recovery: false, more: false }],
      cursor: "end",
      cut: 4,
      recovery: false,
      gaps: [],
    };
    const member: OrchestrationV2Subagent = {
      id,
      threadId: owner.id,
      runId,
      parentNodeId: launch.toolNodeId,
      origin: "provider_native",
      createdBy: "agent",
      driver: ProviderDriverKind.make("pi"),
      providerInstanceId: parent.providerInstanceId,
      providerThreadId: child.activeProviderThreadId,
      childThreadId: child.id,
      nativeTaskRef: null,
      prompt: "Observe",
      title: child.title,
      model: null,
      status: "completed",
      nativeWorker: {
        launch,
        execution: "completed",
        capture: "closed",
        availability: "ready",
        gaps: [],
      },
      result: "Finished",
      startedAt: at(0),
      completedAt: at(0),
      updatedAt: at(0),
    };
    const provider: OrchestrationV2ProviderThread = {
      ...providerThread(child),
      ownerNodeId: id,
      nativeMetadata: { nativeWorkerCheckpoint: checkpoint },
    };
    const persistMember = (value: OrchestrationV2Subagent) =>
      store.apply({
        ...stamp(),
        type: "subagent.updated",
        threadId: owner.id,
        nodeId: id,
        payload: value,
      });
    yield* store.apply({
      ...stamp(),
      type: "thread.created",
      threadId: child.id,
      payload: { ...child, lastVisitedAt: child.lastVisitedAt ?? null },
    });
    yield* persistProvider(provider);
    yield* persistMember(member);
    observers.push(child);
    return {
      child,
      checkpoint,
      member,
      provider,
      persistMember,
      persistCheckpoint: (next: PiNativeWorkerCheckpoint) =>
        persistProvider({ ...provider, nativeMetadata: { nativeWorkerCheckpoint: next } }),
    };
  });
  const deleteParent = Effect.gen(function* () {
    yield* store.apply({
      ...stamp(),
      type: "thread.deleted",
      threadId: parent.id,
      payload: { ...parent, deletedAt: at(0), lastVisitedAt: parent.lastVisitedAt ?? null },
    });
    fixture.setThreads([...observers]);
  });
  const history = sql<{ payload_json: string }>`
    SELECT payload_json FROM orchestration_v2_projection_threads ORDER BY thread_id
  `;
  fixture.onDelete(true);
  return {
    ...fixture,
    parent,
    addObserver,
    deleteParent,
    sql,
    store,
    outbox,
    history,
    remove: fixture.service.removeDeletedThreadWorktree({
      threadId: parent.id,
      expectedWorktreePath: fixture.worktree,
    }),
  };
});

const runNativeCleanupTest = <A, E>(
  effect: Effect.Effect<A, E, Effect.Services<typeof nativeCleanupFixture>>,
) => runCleanupTest(effect.pipe(Effect.provide(nativePersistenceLayer)));

describe("persisted native observations and protected deletion", () => {
  it.live.each([false, true])(
    "coordinates teardown without bypassing another worker (already running=%s)",
    (alreadyRunning) =>
      runNativeCleanupTest(
        Effect.gen(function* () {
          const f = yield* nativeCleanupFixture;
          yield* f.addObserver();
          yield* f.deleteParent;
          yield* f.outbox.enqueue([
            {
              id: "directed-teardown",
              commandId: CommandId.make("delete"),
              threadId: f.parent.id,
              request: { type: "terminal.cleanup" },
            },
          ]);
          const started = yield* Deferred.make<void>();
          const released = yield* Deferred.make<void>();
          const workerContext = yield* Layer.build(EffectWorker.layer).pipe(
            Effect.provideService(EffectWorker.OrchestrationEffectExecutorV2, {
              execute: () =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(started, undefined);
                  yield* Deferred.await(released);
                }),
            }),
          );
          const worker = Context.get(workerContext, EffectWorker.OrchestrationEffectWorkerV2);
          f.drainWith(worker.drain());
          if (alreadyRunning) {
            const teardown = yield* worker.runOnce.pipe(Effect.forkChild);
            yield* Deferred.await(started);
            expect(yield* f.remove).toMatchObject({
              outcome: "kept",
              reason: "Thread deletion or cleanup is still pending",
            });
            expect(yield* f.fs.exists(f.worktree)).toBe(true);
            yield* Deferred.succeed(released, undefined);
            yield* Fiber.join(teardown);
            expect(yield* f.remove).toMatchObject({ outcome: "removed" });
          } else {
            const removal = yield* f.remove.pipe(Effect.forkChild);
            yield* Deferred.await(started);
            expect(yield* f.fs.exists(f.worktree)).toBe(true);
            yield* Deferred.succeed(released, undefined);
            expect(yield* Fiber.join(removal)).toMatchObject({ outcome: "removed" });
          }
          expect(yield* f.fs.exists(f.worktree)).toBe(false);
          expect(yield* f.history).toHaveLength(2);
        }),
      ),
  );

  it.live("serves advisory and protected deletion through the scoped RPC service boundary", () =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        const observer = yield* f.addObserver();
        f.setThreads([observer.child, f.parent]);
        f.onDelete(false);
        const tested = [
          WS_METHODS.serverGetThreadWorktreeCleanupEligibility,
          WS_METHODS.serverRemoveDeletedThreadWorktree,
        ] as const;
        const group = WsRpcGroup.omit(
          ...[...WsRpcGroup.requests.keys()].filter(
            (
              tag,
            ): tag is Exclude<
              keyof typeof RpcAuthorization.RPC_REQUIRED_SCOPES,
              (typeof tested)[number]
            > => !(tested as ReadonlyArray<string>).includes(tag),
          ),
        );
        const clientFor = (scopes: readonly AuthEnvironmentScope[]) =>
          RpcTest.makeClient(group).pipe(
            Effect.provide(
              Layer.mergeAll(
                group.toLayerHandler(
                  WS_METHODS.serverGetThreadWorktreeCleanupEligibility,
                  f.service.getThreadWorktreeCleanupEligibility,
                ),
                group.toLayerHandler(
                  WS_METHODS.serverRemoveDeletedThreadWorktree,
                  f.service.removeDeletedThreadWorktree,
                ),
                RpcAuthorization.layer(scopes),
              ),
            ),
          );
        const reader = yield* clientFor([AuthOrchestrationReadScope]);
        expect(
          yield* reader[WS_METHODS.serverGetThreadWorktreeCleanupEligibility]({
            threadId: f.parent.id,
          }),
        ).toMatchObject({ eligible: true });
        yield* f.deleteParent;
        const input = { threadId: f.parent.id, expectedWorktreePath: f.worktree };
        expect(
          yield* reader[WS_METHODS.serverRemoveDeletedThreadWorktree](input).pipe(Effect.flip),
        ).toMatchObject({
          _tag: "EnvironmentAuthorizationError",
          requiredPermission: AuthSourceControlWriteScope,
        });
        expect(yield* f.fs.exists(f.worktree)).toBe(true);
        const writer = yield* clientFor([AuthSourceControlWriteScope]);
        expect(yield* writer[WS_METHODS.serverRemoveDeletedThreadWorktree](input)).toMatchObject({
          outcome: "removed",
          path: f.worktree,
        });
        expect(yield* f.fs.exists(f.worktree)).toBe(false);
        expect(yield* f.history).toHaveLength(2);
      }),
    ),
  );

  it.live.each([false, true])(
    "advises and removes a last-owner checkout with settled observations (archived=%s)",
    (archived) =>
      runNativeCleanupTest(
        Effect.gen(function* () {
          const f = yield* nativeCleanupFixture;
          const observer = yield* f.addObserver(f.parent, archived);
          f.setThreads([observer.child, f.parent]);
          f.onDelete(false);
          expect(
            yield* f.service.getThreadWorktreeCleanupEligibility({ threadId: f.parent.id }),
          ).toEqual({ eligible: true, worktreePath: f.worktree });
          expect(yield* f.fs.exists(f.worktree)).toBe(true);
          expect(
            yield* f.service.getThreadWorktreeCleanupEligibility({ threadId: observer.child.id }),
          ).toMatchObject({ eligible: false });
          f.onDelete(true);
          expect(
            yield* f.service.getThreadWorktreeCleanupEligibility({ threadId: f.parent.id }),
          ).toEqual({ eligible: false, reason: "Automatic worktree cleanup is enabled" });
          expect(yield* f.fs.exists(f.worktree)).toBe(true);
          f.onDelete(false);
          yield* f.deleteParent;
          const history = yield* f.history;
          expect(yield* f.remove).toMatchObject({ outcome: "removed" });
          expect(yield* f.fs.exists(f.worktree)).toBe(false);
          expect(yield* f.history).toEqual(history);
        }),
      ),
  );

  it.live("rereads real owners created after an affirmative advisory", () =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        f.onDelete(false);
        f.setThreads([f.parent]);
        expect(
          yield* f.service.getThreadWorktreeCleanupEligibility({ threadId: f.parent.id }),
        ).toMatchObject({ eligible: true });
        yield* f.deleteParent;
        const owner = shell({
          ...f.parent,
          id: ThreadId.make("late-owner"),
          activeProviderThreadId: null,
        });
        yield* f.store.apply({
          id: EventId.make("late-owner"),
          occurredAt: at(0),
          type: "thread.created",
          threadId: owner.id,
          payload: { ...owner, lastVisitedAt: null },
        });
        f.setThreads([owner]);
        expect(yield* f.remove).toMatchObject({ outcome: "kept" });
        expect(yield* f.fs.exists(f.worktree)).toBe(true);
        expect(yield* f.history).toHaveLength(2);
      }),
    ),
  );

  it.live("includes archived owners absent from the active shell in advisory decisions", () =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        f.onDelete(false);
        const owner = shell({
          ...f.parent,
          id: ThreadId.make("archived-owner"),
          archivedAt: at(0),
          activeProviderThreadId: null,
        });
        f.setThreads([f.parent, owner]);
        expect(
          yield* f.service.getThreadWorktreeCleanupEligibility({ threadId: f.parent.id }),
        ).toEqual({ eligible: false, reason: "Checkout has another owning thread" });
        expect(yield* f.fs.exists(f.worktree)).toBe(true);
      }),
    ),
  );

  it.live("offers cleanup for the last persisted owner in a sequential bulk deletion", () =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        f.onDelete(false);
        const owner = shell({
          ...f.parent,
          id: ThreadId.make("bulk-second"),
          activeProviderThreadId: null,
        });
        yield* f.store.apply({
          id: EventId.make("bulk-created"),
          occurredAt: at(0),
          type: "thread.created",
          threadId: owner.id,
          payload: { ...owner, lastVisitedAt: null },
        });
        f.setThreads([f.parent, owner]);
        expect(
          yield* f.service.getThreadWorktreeCleanupEligibility({ threadId: f.parent.id }),
        ).toMatchObject({ eligible: false });
        yield* f.deleteParent;
        f.setThreads([owner]);
        expect(
          yield* f.service.getThreadWorktreeCleanupEligibility({ threadId: owner.id }),
        ).toEqual({ eligible: true, worktreePath: f.worktree });
        yield* f.store.apply({
          id: EventId.make("bulk-deleted"),
          occurredAt: at(0),
          type: "thread.deleted",
          threadId: owner.id,
          payload: { ...owner, lastVisitedAt: null, deletedAt: at(0) },
        });
        f.setThreads([]);
        expect(
          yield* f.service.removeDeletedThreadWorktree({
            threadId: owner.id,
            expectedWorktreePath: f.worktree,
          }),
        ).toMatchObject({ outcome: "removed" });
        expect(yield* f.fs.exists(f.worktree)).toBe(false);
        expect(yield* f.history).toHaveLength(2);
      }),
    ),
  );

  it.live("chooses a deleted parent with an applicable policy", () =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        yield* f.addObserver();
        yield* f.deleteParent;
        const disabledParent = {
          ...f.parent,
          id: ThreadId.make("aaa-disabled-parent"),
          projectId: ProjectId.make("off"),
          activeProviderThreadId: null,
          deletedAt: at(0),
          lastVisitedAt: null,
        };
        f.disableProjectCleanup(disabledParent.projectId);
        yield* f.sql`
        INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
        VALUES (${disabledParent.projectId}, 'Other', ${f.repo}, '[]', '2026-06-01T00:00:00Z', '2026-06-01T00:00:00Z')
      `;
        yield* f.store.apply({
          id: EventId.make("disabled-deleted-parent"),
          occurredAt: at(0),
          type: "thread.deleted",
          threadId: disabledParent.id,
          payload: disabledParent,
        });
        const report = yield* f.service.runNow;
        expect(report.entries).toMatchObject([{ outcome: "removed", threadId: f.parent.id }]);
        expect(yield* f.fs.exists(f.worktree)).toBe(false);
      }),
    ),
  );

  it.live.each([false, true])(
    "selects the real live owner, not the first reference (reverse=%s)",
    (reverse) =>
      runNativeCleanupTest(
        Effect.gen(function* () {
          const f = yield* nativeCleanupFixture;
          const o = yield* f.addObserver();
          f.setThreads(reverse ? [o.child, f.parent] : [f.parent, o.child]);
          const report = yield* f.service.runNow;
          expect(report.entries).toMatchObject([{ outcome: "removed", threadId: f.parent.id }]);
          expect(yield* f.fs.exists(f.worktree)).toBe(false);
        }),
      ),
  );

  it.live("does not promote an observation when its owner is missing", () =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        const o = yield* f.addObserver();
        f.setThreads([o.child]);
        yield* f.sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id = ${f.parent.id}`;
        const report = yield* f.service.runNow;
        expect(report.entries).toMatchObject([
          {
            outcome: "kept",
            threadId: null,
            reason: "No matching owning thread or deleted parent",
          },
        ]);
        expect(yield* f.fs.exists(f.worktree)).toBe(true);
      }),
    ),
  );

  it.live.each([
    { reverse: false, archived: false },
    { reverse: true, archived: false },
    { reverse: false, archived: true },
    { reverse: true, archived: true },
  ])(
    "deletes a removed parent's checkout with settled observers (reverse=$reverse, archived=$archived)",
    ({ reverse, archived }) =>
      runNativeCleanupTest(
        Effect.gen(function* () {
          const f = yield* nativeCleanupFixture;
          const first = yield* f.addObserver(f.parent, archived);
          const second = yield* f.addObserver(f.parent, archived);
          yield* f.deleteParent;
          f.setThreads(reverse ? [second.child, first.child] : [first.child, second.child]);
          yield* f.store.apply({
            id: EventId.make("native-conversation"),
            occurredAt: at(0),
            type: "turn-item.updated",
            threadId: first.child.id,
            payload: {
              id: TurnItemId.make("native-item"),
              threadId: first.child.id,
              runId: null,
              nodeId: null,
              providerThreadId: first.provider.id,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 0,
              status: "completed",
              title: null,
              startedAt: at(0),
              completedAt: at(0),
              updatedAt: at(0),
              type: "assistant_message",
              messageId: MessageId.make("native-message"),
              text: "Preserve this conversation",
              streaming: false,
              nativeWorkerRecord: {
                instanceId: first.checkpoint.launch.instanceId,
                owner: first.checkpoint.owner,
                captureEpoch: "capture",
                id: "native-record",
                binding: 1,
                revision: 1,
                sequence: 4,
                order: 0,
                nativeOrder: 0,
                occurredAt: NOW_MS,
                observedAt: NOW_MS,
                nativeTime: null,
                nativeEntryId: "entry",
                parentEntryId: null,
                toolCallId: null,
              },
            },
          });
          const history = yield* f.history;
          const conversation =
            yield* f.sql`SELECT payload_json FROM orchestration_v2_projection_turn_items ORDER BY turn_item_id`;
          const runs =
            yield* f.sql`SELECT payload_json FROM orchestration_v2_projection_runs ORDER BY run_id`;
          const nativeHistory =
            yield* f.sql`SELECT payload_json FROM orchestration_v2_projection_subagents ORDER BY subagent_id`;
          const checkpoints =
            yield* f.sql`SELECT payload_json FROM orchestration_v2_projection_provider_threads ORDER BY provider_thread_id`;
          const report = yield* f.service.runNow;
          expect(report.entries).toMatchObject([{ outcome: "removed", threadId: f.parent.id }]);
          expect(yield* f.fs.exists(f.worktree)).toBe(false);
          expect(yield* f.history).toEqual(history);
          expect(
            yield* f.sql`SELECT payload_json FROM orchestration_v2_projection_turn_items ORDER BY turn_item_id`,
          ).toEqual(conversation);
          expect(
            yield* f.sql`SELECT payload_json FROM orchestration_v2_projection_runs ORDER BY run_id`,
          ).toEqual(runs);
          expect(
            yield* f.sql`SELECT payload_json FROM orchestration_v2_projection_subagents ORDER BY subagent_id`,
          ).toEqual(nativeHistory);
          expect(
            yield* f.sql`SELECT payload_json FROM orchestration_v2_projection_provider_threads ORDER BY provider_thread_id`,
          ).toEqual(checkpoints);
        }),
      ),
  );

  it.live.each([
    ...[
      "execution",
      "capture",
      "availability",
      "summary gap",
      "checkpoint gap",
      "active binding",
      "binding capture",
      "binding gap",
      "missing cursor",
      "page more",
      "fragment",
      "cursor recovery",
      "owner identity",
      "launch identity",
      "missing checkpoint",
      "missing member",
      "run attempt",
      "contradictory parent",
      "unsupported execution",
      "unknown gap",
      "cursor fragment",
      "cursor omission",
      "checkpoint capture",
      "binding duplicate",
    ].map((kind) => ({ kind, archived: false })),
    { kind: "execution", archived: true },
    { kind: "capture", archived: true },
    { kind: "launch identity", archived: true },
  ])("preserves checkout for $kind (archived=$archived)", ({ kind, archived }) =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        const o = yield* f.addObserver(f.parent, archived);
        yield* f.deleteParent;
        const cp = o.checkpoint;
        const summary = o.member.nativeWorker!;
        switch (kind) {
          case "execution":
            yield* o.persistMember({
              ...o.member,
              nativeWorker: { ...summary, execution: "running" },
            });
            break;
          case "capture":
            yield* o.persistMember({
              ...o.member,
              nativeWorker: { ...summary, capture: "incomplete" },
            });
            break;
          case "availability":
            yield* o.persistMember({
              ...o.member,
              nativeWorker: { ...summary, availability: "unavailable" },
            });
            break;
          case "summary gap":
            yield* o.persistMember({
              ...o.member,
              nativeWorker: { ...summary, gaps: ["pending"] },
            });
            break;
          case "checkpoint gap":
            yield* o.persistCheckpoint({ ...cp, gaps: ["stale_owner"] });
            break;
          case "active binding":
            yield* o.persistCheckpoint({
              ...cp,
              bindings: [{ ...cp.bindings[0]!, state: "active" }],
            });
            break;
          case "binding capture":
            yield* o.persistCheckpoint({
              ...cp,
              bindings: [{ ...cp.bindings[0]!, capture: "incomplete" }],
            });
            break;
          case "binding gap":
            yield* o.persistCheckpoint({
              ...cp,
              bindings: [{ ...cp.bindings[0]!, gaps: ["not_captured"] }],
            });
            break;
          case "missing cursor":
            yield* o.persistCheckpoint({ ...cp, bindingCursors: [] });
            break;
          case "page more":
            yield* o.persistCheckpoint({
              ...cp,
              bindingCursors: [{ ...cp.bindingCursors[0]!, more: true }],
            });
            break;
          case "fragment":
            yield* o.persistCheckpoint({
              ...cp,
              fragment: {
                id: "fragment",
                binding: 1,
                revision: 1,
                epoch: "capture",
                sequence: 4,
                totalBytes: 10,
                cursor: "start",
              },
            });
            break;
          case "cursor recovery":
            yield* o.persistCheckpoint({ ...cp, recovery: true });
            break;
          case "owner identity":
            yield* o.persistCheckpoint({
              ...cp,
              owner: { ...cp.owner, coordinatorSessionId: "other" },
            });
            break;
          case "launch identity":
            yield* o.persistCheckpoint({ ...cp, launch: { ...cp.launch, instanceId: "other" } });
            break;
          case "missing checkpoint":
            yield* f.sql`DELETE FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = ${o.provider.id}`;
            break;
          case "missing member":
            yield* f.sql`DELETE FROM orchestration_v2_projection_subagents WHERE subagent_id = ${o.member.id}`;
            break;
          case "run attempt":
            yield* f.sql`UPDATE orchestration_v2_projection_runs SET payload_json = json_set(payload_json, '$.activeAttemptId', 'other')`;
            break;
          case "contradictory parent":
            yield* o.persistMember({ ...o.member, threadId: o.child.id });
            break;
          case "unsupported execution":
            yield* f.sql`UPDATE orchestration_v2_projection_subagents SET payload_json = json_set(payload_json, '$.nativeWorker.execution', 'future-state')`;
            break;
          case "unknown gap":
            yield* f.sql`UPDATE orchestration_v2_projection_subagents SET payload_json = json_set(payload_json, '$.nativeWorker.gaps', json('["future-gap"]'))`;
            break;
          case "cursor fragment":
            yield* o.persistCheckpoint({
              ...cp,
              bindingCursors: [
                {
                  ...cp.bindingCursors[0]!,
                  fragment: {
                    id: "fragment",
                    binding: 1,
                    revision: 1,
                    epoch: "capture",
                    sequence: 4,
                    totalBytes: 10,
                    cursor: "start",
                  },
                },
              ],
            });
            break;
          case "cursor omission":
            yield* o.persistCheckpoint({
              ...cp,
              bindingCursors: [
                {
                  ...cp.bindingCursors[0]!,
                  omission: "invalid_record",
                },
              ],
            });
            break;
          case "checkpoint capture":
            yield* o.persistCheckpoint({ ...cp, capture: "incomplete" });
            break;
          case "binding duplicate":
            yield* o.persistCheckpoint({ ...cp, bindings: [cp.bindings[0]!, cp.bindings[0]!] });
            break;
        }
        const report = yield* f.service.runNow;
        expect(report.entries[0]?.outcome).toBe(
          kind === "unsupported execution" || kind === "unknown gap" ? "failed" : "kept",
        );
        if (kind !== "unsupported execution" && kind !== "unknown gap")
          expect(report.entries[0]?.reason).toMatch(/Native observation/);
        expect(yield* f.fs.exists(f.worktree)).toBe(true);
      }),
    ),
  );

  it.live.each(
    ["independent", "app-owned", "other provider"].flatMap((kind) =>
      [false, true].map((archived) => ({ kind, archived })),
    ),
  )(
    "preserves a real $kind owner (archived=$archived)",
    ({ kind, archived }) =>
      runNativeCleanupTest(
        Effect.gen(function* () {
          const f = yield* nativeCleanupFixture;
          const o = yield* f.addObserver(f.parent, archived);
          yield* f.deleteParent;
          if (kind === "independent") {
            const owner = shell({
              id: ThreadId.make("independent"),
              branch: "feature",
              worktreePath: f.worktree,
              archivedAt: archived ? at(0) : null,
            });
            yield* f.store.apply({
              id: EventId.make("independent-owner"),
              occurredAt: at(0),
              type: "thread.created",
              threadId: owner.id,
              payload: { ...owner, lastVisitedAt: null },
            });
            f.setThreads([o.child, owner]);
          } else {
            // A superficially similar subagent is not a Pitako exemption.
            yield* o.persistMember({
              ...o.member,
              nativeWorker: undefined,
              origin: kind === "app-owned" ? "app_owned" : "provider_native",
              driver: ProviderDriverKind.make(kind === "app-owned" ? "pi" : "codex"),
            });
            yield* f.sql`DELETE FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = ${o.provider.id}`;
          }
          const result = yield* f.remove;
          expect(result).toMatchObject({ outcome: "kept", reason: "Shared by 1 threads" });
          expect(yield* f.fs.exists(f.worktree)).toBe(true);
        }),
      ),
  );

  it.live.each([
    { settled: false, archived: false },
    { settled: true, archived: false },
    { settled: false, archived: true },
    { settled: true, archived: true },
  ])("requires descendants to settle (settled=$settled, archived=$archived)", ({ settled, archived }) =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        const child = yield* f.addObserver();
        const descendant = yield* f.addObserver(child.child, archived);
        if (!settled)
          yield* descendant.persistMember({
            ...descendant.member,
            nativeWorker: { ...descendant.member.nativeWorker!, execution: "running" },
          });
        yield* f.deleteParent;
        const history = yield* f.history;
        const result = yield* f.remove;
        expect(result.outcome).toBe(settled ? "removed" : "kept");
        if (!settled) expect(result.reason).toMatch(/Native observation/);
        expect(yield* f.fs.exists(f.worktree)).toBe(!settled);
        expect(yield* f.history).toEqual(history);
      }),
    ),
  );

  it.live("blocks a fully linked descendant cycle", () =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        const a = yield* f.addObserver();
        const b = yield* f.addObserver(a.child);
        const launch = {
          ...a.checkpoint.launch,
          parentThreadId: b.child.id,
          parentProviderThreadId: b.provider.id,
        };
        yield* a.persistMember({
          ...a.member,
          threadId: b.child.id,
          nativeWorker: { ...a.member.nativeWorker!, launch },
        });
        yield* a.persistCheckpoint({ ...a.checkpoint, launch });
        yield* f.sql`
          UPDATE orchestration_v2_projection_runs
          SET thread_id = ${b.child.id},
              payload_json = json_set(payload_json, '$.threadId', ${b.child.id}, '$.providerThreadId', ${b.provider.id})
          WHERE run_id = ${launch.runId}
        `;
        yield* f.deleteParent;
        f.setThreads([
          { ...a.child, lineage: { ...a.child.lineage, parentThreadId: b.child.id } },
          b.child,
        ]);
        expect(yield* f.remove).toMatchObject({
          outcome: "kept",
          reason: "Native observation has cyclic descendants",
        });
        expect(yield* f.fs.exists(f.worktree)).toBe(true);
      }),
    ),
  );

  it.live("blocks a descendant with missing native records even outside the path group", () =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        const child = yield* f.addObserver();
        const descendant = yield* f.addObserver(child.child);
        yield* f.deleteParent;
        f.setThreads([child.child, { ...descendant.child, worktreePath: null }]);
        yield* f.sql`DELETE FROM orchestration_v2_projection_subagents WHERE subagent_id = ${descendant.member.id}`;
        yield* f.sql`DELETE FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = ${descendant.provider.id}`;
        expect(yield* f.remove).toMatchObject({
          outcome: "kept",
          reason: "Native observation descendant is an independent owner",
        });
        expect(yield* f.fs.exists(f.worktree)).toBe(true);
      }),
    ),
  );

  it.live("also waits for a settled observer's queued cleanup", () =>
    runNativeCleanupTest(
      Effect.gen(function* () {
        const f = yield* nativeCleanupFixture;
        const o = yield* f.addObserver();
        yield* f.deleteParent;
        yield* f.outbox.enqueue([
          {
            id: "observer-cleanup",
            commandId: CommandId.make("observer-delete"),
            threadId: o.child.id,
            request: { type: "terminal.cleanup" },
          },
        ]);
        expect(yield* f.remove).toMatchObject({
          outcome: "kept",
          reason: "Thread deletion or cleanup is still pending",
        });
        expect(yield* f.fs.exists(f.worktree)).toBe(true);
        yield* f.outbox.cancelUnsettled({
          threadId: o.child.id,
          effectTypes: ["terminal.cleanup"],
          reason: "explicit cancellation",
        });
        expect((yield* f.remove).outcome).toBe("removed");
        expect(yield* f.fs.exists(f.worktree)).toBe(false);
      }),
    ),
  );

  it.live.each(["succeeded", "cancelled", "failed"] as const)(
    "waits for deletion effects (%s)",
    (status) =>
      runNativeCleanupTest(
        Effect.gen(function* () {
          const f = yield* nativeCleanupFixture;
          yield* f.addObserver();
          yield* f.deleteParent;
          yield* f.outbox.enqueue([
            {
              id: "delete-cleanup",
              commandId: CommandId.make("delete"),
              threadId: f.parent.id,
              request: { type: "terminal.cleanup" },
            },
          ]);
          expect(yield* f.remove).toMatchObject({
            outcome: "kept",
            reason: "Thread deletion or cleanup is still pending",
          });
          expect(yield* f.fs.exists(f.worktree)).toBe(true);
          if (status === "cancelled") {
            yield* f.outbox.cancelUnsettled({
              threadId: f.parent.id,
              effectTypes: ["terminal.cleanup"],
              reason: "explicit test cancellation",
            });
          } else {
            const claimed = yield* f.outbox.claimNext({
              workerId: "test-worker",
              leaseDurationMs: 60_000,
            });
            expect(Option.isSome(claimed)).toBe(true);
            if (status === "succeeded")
              yield* f.outbox.succeed({ effectId: "delete-cleanup", workerId: "test-worker" });
            else
              yield* f.outbox.fail({
                effectId: "delete-cleanup",
                workerId: "test-worker",
                error: "cleanup failed",
              });
          }
          expect((yield* f.remove).outcome).toBe(status === "failed" ? "kept" : "removed");
          expect(yield* f.fs.exists(f.worktree)).toBe(status === "failed");
        }),
      ),
  );

  it.live(
    "targets only the deleted parent's expected checkout and is repeatable with automatic deletion off",
    () =>
      runNativeCleanupTest(
        Effect.gen(function* () {
          const f = yield* nativeCleanupFixture;
          yield* f.addObserver();
          yield* f.deleteParent;
          f.onDelete(false);
          f.disable();
          const other = `${f.config.worktreesDir}/other`;
          yield* f.command(f.repo, ["worktree", "add", "-b", "other", other]);
          const otherThread = shell({
            id: ThreadId.make("other-thread"),
            branch: "other",
            worktreePath: other,
          });
          yield* f.store.apply({
            id: EventId.make("other-thread"),
            type: "thread.created",
            threadId: otherThread.id,
            occurredAt: at(0),
            payload: {
              ...otherThread,
              deletedAt: at(0),
              lastVisitedAt: otherThread.lastVisitedAt ?? null,
            },
          });
          expect(
            yield* f.service.removeDeletedThreadWorktree({
              threadId: f.parent.id,
              expectedWorktreePath: other,
            }),
          ).toMatchObject({
            outcome: "kept",
            reason: "No matching owning thread or deleted parent",
          });
          expect(yield* f.fs.exists(f.worktree)).toBe(true);
          expect(yield* f.fs.exists(other)).toBe(true);
          const history = yield* f.history;
          expect(yield* f.remove).toMatchObject({ outcome: "removed", path: f.worktree });
          expect(yield* f.remove).toMatchObject({
            outcome: "kept",
            reason: "Worktree is already absent",
          });
          expect(yield* f.fs.exists(other)).toBe(true);
          expect(yield* f.history).toEqual(history);
        }),
      ),
  );

  it.live.each(["generation", "binding", "descendant", "activity"])(
    "cancels deletion when %s changes during measurement",
    (kind) =>
      runNativeCleanupTest(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const measuring = yield* Deferred.make<void>();
          const resume = yield* Deferred.make<void>();
          let measuredPath: string | undefined;
          const f = yield* nativeCleanupFixture.pipe(
            Effect.provideService(FileSystem.FileSystem, {
              ...fs,
              readDirectory: (directory) =>
                Effect.gen(function* () {
                  if (directory === measuredPath) {
                    yield* Deferred.succeed(measuring, undefined);
                    yield* Deferred.await(resume);
                  }
                  return yield* fs.readDirectory(directory);
                }),
            }),
          );
          const o = yield* f.addObserver();
          yield* f.deleteParent;
          measuredPath = yield* fs.realPath(f.worktree);
          const cleanup = yield* f.remove.pipe(Effect.forkChild);
          yield* Deferred.await(measuring);
          switch (kind) {
            case "generation":
              yield* o.persistCheckpoint({ ...o.checkpoint, ownerGeneration: 3 });
              break;
            case "binding":
              yield* o.persistCheckpoint({
                ...o.checkpoint,
                bindingCursors: [{ ...o.checkpoint.bindingCursors[0]!, more: true }],
              });
              break;
            case "descendant": {
              const descendant = yield* f.addObserver(o.child);
              f.setThreads([o.child, descendant.child]);
              break;
            }
            case "activity":
              f.setThreads([{ ...o.child, latestUserMessageAt: at(1) }]);
              break;
          }
          yield* Deferred.succeed(resume, undefined);
          const result = yield* Fiber.join(cleanup);
          expect(result).toMatchObject({
            outcome: "kept",
            reason: "Native observation identity, generation or descendants changed since check",
          });
          expect(yield* fs.exists(f.worktree)).toBe(true);
        }),
      ),
  );
});

describe("storage cleanup reports and local file policies", () => {
  it.live("measures nested regular files without following symlinks", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const { service, fs, worktree, setPolicy } = yield* cleanupFixture;
        setPolicy("tracked-changes");
        const sizes = yield* Effect.forEach(yield* fs.readDirectory(worktree), (name) =>
          fs.stat(`${worktree}/${name}`).pipe(Effect.map((stat) => Number(stat.size))),
        );
        const outside = yield* fs.makeTempDirectoryScoped();
        yield* fs.writeFileString(`${outside}/external`, "must not count");
        yield* fs.makeDirectory(`${worktree}/node_modules/nested`, { recursive: true });
        yield* fs.writeFileString(`${worktree}/node_modules/nested/file`, "count me");
        yield* fs.symlink(outside, `${worktree}/node_modules/directory-link`);
        yield* fs.symlink(`${outside}/external`, `${worktree}/node_modules/file-link`);
        yield* fs.symlink(`${outside}/missing`, `${worktree}/node_modules/dangling-link`);
        const report = yield* service.runNow;
        const bytes = sizes.reduce((sum, size) => sum + size, 8);
        expect(report.entries[0]).toMatchObject({ outcome: "removed", bytes, files: null });
        expect(report.bytesFreed).toBe(bytes);
        expect(yield* fs.readFileString(`${outside}/external`)).toBe("must not count");
      }),
    ),
  );
  it.live("still removes a worktree when measuring its size fails", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const { service, worktree } = yield* cleanupFixture.pipe(
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            readDirectory: (directory) => fs.readDirectory(`${directory}/missing-directory`),
          }),
        );
        const report = yield* service.runNow;
        expect(report.entries[0]).toMatchObject({ outcome: "removed", bytes: null, files: null });
        expect(report.bytesFreed).toBe(0);
        expect(yield* fs.exists(worktree)).toBe(false);
      }),
    ),
  );
  it.live("keeps a worktree when its thread starts during size measurement", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const measuring = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();
        let measuredPath: string | undefined;
        const fixture = yield* cleanupFixture.pipe(
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            readDirectory: (directory) =>
              Effect.gen(function* () {
                if (directory === measuredPath) {
                  yield* Deferred.succeed(measuring, undefined);
                  yield* Deferred.await(resume);
                }
                return yield* fs.readDirectory(directory);
              }),
          }),
        );
        measuredPath = yield* fs.realPath(fixture.worktree);
        fixture.setPolicy("tracked-changes");
        yield* fs.writeFileString(`${fixture.worktree}/notes.txt`, "untracked\n");
        const cleanup = yield* fixture.service.runNow.pipe(Effect.forkChild);
        yield* Deferred.await(measuring);
        fixture.setThreads([
          shell({ branch: "feature", worktreePath: fixture.worktree, status: "running" }),
        ]);
        yield* Deferred.succeed(resume, undefined);
        const report = yield* Fiber.join(cleanup);
        expect(report.entries[0]).toMatchObject({
          outcome: "kept",
          reason: "Thread activity or shared worktree changed since check",
          bytes: null,
        });
        expect(report.bytesFreed).toBe(0);
        expect(yield* fs.exists(fixture.worktree)).toBe(true);
        expect(yield* fs.readFileString(`${fixture.worktree}/notes.txt`)).toBe("untracked\n");
      }),
    ),
  );
  it.live("does not measure a worktree kept for an open provider session", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const walked: Array<string> = [];
        const fixture = yield* cleanupFixture.pipe(
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            readDirectory: (directory, options) => {
              walked.push(directory);
              return fs.readDirectory(directory, options);
            },
          }),
        );
        const root = yield* fs.realPath(fixture.worktree);
        fixture.setOpenSessionCwd(fixture.worktree);
        const kept = yield* fixture.service.runNow;
        expect(kept.entries[0]).toMatchObject({
          outcome: "kept",
          reason: "Provider session is still open",
          bytes: null,
        });
        expect(walked.filter((directory) => directory.startsWith(root))).toEqual([]);
        fixture.setOpenSessionCwd(null);
        const removed = yield* fixture.service.runNow;
        expect(removed.entries[0]).toMatchObject({ outcome: "removed" });
        expect(removed.entries[0]?.bytes).toBeGreaterThan(0);
        expect(walked).toContain(root);
      }),
    ),
  );
  it.live("compares the checked-out branch without a full status", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const { service, fs, worktree, command, setThreads, fullStatusCalls } =
          yield* cleanupFixture;
        const changed = { outcome: "kept", reason: "Repository or branch changed" };
        yield* command(worktree, ["checkout", "--detach"]);
        expect((yield* service.runNow).entries[0]).toMatchObject(changed);
        // A branch that has no commit yet is still a different branch.
        yield* command(worktree, ["switch", "--orphan", "other"]);
        expect((yield* service.runNow).entries[0]).toMatchObject(changed);
        // Status output reserves a leading "(" for a detached HEAD.
        yield* command(worktree, ["switch", "-c", "(feature)", "feature"]);
        setThreads([shell({ branch: "(feature)", worktreePath: worktree })]);
        expect((yield* service.runNow).entries[0]).toMatchObject(changed);
        setThreads([shell({ branch: "feature", worktreePath: worktree })]);
        // Tags named after the branch or HEAD make Git's short names ambiguous.
        yield* command(worktree, ["tag", "feature", "feature"]);
        // Newer Git refuses to create a tag named HEAD, but still reads one.
        yield* command(worktree, ["update-ref", "refs/tags/HEAD", "refs/heads/feature"]);
        yield* command(worktree, ["switch", "feature"]);
        const index = (yield* command(worktree, ["rev-parse", "--git-path", "index"])).stdout;
        yield* fs.writeFileString(`${index.trim()}.lock`, "");
        const locked = (yield* service.runNow).entries[0];
        expect(locked).toMatchObject({ outcome: "failed" });
        expect(locked?.reason).toContain("Git index is locked");
        yield* fs.remove(`${index.trim()}.lock`);
        const gitFile = yield* fs.readFileString(`${worktree}/.git`);
        yield* fs.writeFileString(`${worktree}/.git`, "gitdir: /missing-git-directory\n");
        expect((yield* service.runNow).entries[0]).toMatchObject(changed);
        yield* fs.writeFileString(`${worktree}/.git`, gitFile);
        expect((yield* service.runNow).entries[0]).toMatchObject({ outcome: "removed" });
        expect(fullStatusCalls()).toBe(0);
      }),
    ),
  );
  it("decodes reports from servers without size fields", () => {
    const report = decodeCleanupReport({
      trigger: "manual",
      startedAt: "2026-06-10T12:00:00.000Z",
      finishedAt: "2026-06-10T12:00:01.000Z",
      entries: [
        {
          kind: "worktree",
          outcome: "removed",
          reason: "Removed",
          path: null,
          threadId: null,
          threadTitle: null,
        },
      ],
      counts: { removed: 1, kept: 0, failed: 0 },
      omittedCount: 0,
    });
    expect(report.bytesFreed).toBe(0);
    expect(report.entries[0]).toMatchObject({ bytes: null, files: null });
  });
  it.live.each([
    ["any-local-files", ".env", "kept"],
    ["uncommitted-changes", ".env", "removed"],
    ["tracked-changes", ".env", "removed"],
    ["any-local-files", "notes.txt", "kept"],
    ["uncommitted-changes", "notes.txt", "kept"],
    ["tracked-changes", "notes.txt", "removed"],
    ["any-local-files", "tracked.txt", "kept"],
    ["uncommitted-changes", "tracked.txt", "kept"],
    ["tracked-changes", "tracked.txt", "kept"],
  ] as const)("applies %j", ([policy, file, outcome]) =>
    runCleanupTest(
      Effect.gen(function* () {
        const { service, fs, worktree, setPolicy } = yield* cleanupFixture;
        setPolicy(policy);
        expect(yield* service.latestReport).toBeNull();
        yield* fs.writeFileString(`${worktree}/${file}`, "local data\n");
        const sizes = yield* Effect.forEach(yield* fs.readDirectory(worktree), (name) =>
          fs.stat(`${worktree}/${name}`).pipe(Effect.map((stat) => Number(stat.size))),
        );
        const bytes = sizes.reduce((sum, size) => sum + size, 0);
        const report = yield* service.runNow;
        yield* service.drain;
        expect(report.trigger).toBe("manual");
        expect(report.entries.filter((entry) => entry.kind === "worktree")).toMatchObject([
          {
            outcome,
            path: worktree,
            threadId: "thread-1",
            threadTitle: "Thread",
            bytes: outcome === "removed" ? bytes : null,
            files: null,
          },
        ]);
        expect(report.bytesFreed).toBe(outcome === "removed" ? bytes : 0);
        expect(yield* fs.exists(worktree)).toBe(outcome === "kept");
        expect(yield* service.latestReport).toEqual(report);
        if (outcome === "kept")
          expect(report.entries[0]?.reason).toContain(
            file === ".env" ? "ignored files (.env)" : "uncommitted changes (1 file)",
          );
      }),
    ),
  );
  it.live("preserves tracked edits written after the final status check", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const fixture = yield* cleanupFixture;
        fixture.setPolicy("tracked-changes");
        fixture.editBeforeRemoval();
        yield* fixture.fs.writeFileString(`${fixture.worktree}/notes.txt`, "untracked\n");
        const report = yield* fixture.service.runNow;
        expect(report.entries[0]).toMatchObject({ outcome: "failed", bytes: null, files: null });
        expect(report.bytesFreed).toBe(0);
        expect(report.entries[0]?.reason).toContain("contains modified or untracked files");
        expect(yield* fixture.fs.readFileString(`${fixture.worktree}/tracked.txt`)).toBe(
          "late edit\n",
        );
        expect(yield* fixture.fs.exists(`${fixture.worktree}/notes.txt`)).toBe(false);
      }),
    ),
  );
  it.live("streams the current report and subsequent runs", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const { service } = yield* cleanupFixture;
        const reports = yield* service.reports.pipe(
          Stream.take(2),
          Stream.tap((report) => (report === null ? service.runNow : Effect.void)),
          Stream.runCollect,
        );
        expect(reports[0]).toBeNull();
        expect(reports[1]).toEqual(yield* service.latestReport);
        expect(reports[1]?.counts).toEqual({ removed: 1, kept: 0, failed: 0 });
      }),
    ),
  );
  it.live("caps entries while preserving totals and prioritizing removals", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const { service, fs, worktree, setThreads } = yield* cleanupFixture;
        const threads = [];
        for (let i = 0; i < 205; i++) {
          const worktreePath = `${worktree}-${i}`;
          yield* fs.makeDirectory(worktreePath);
          threads.push(
            shell({
              id: ThreadId.make(`thread-${i + 2}`),
              branch: "feature",
              worktreePath,
              status: "running",
            }),
          );
        }
        threads.push(shell({ branch: "feature", worktreePath: worktree }));
        setThreads(threads);
        const report = yield* service.runNow;
        expect(report.entries).toHaveLength(200);
        expect(report.omittedCount).toBe(6);
        expect(report.counts).toEqual({ removed: 1, kept: 205, failed: 0 });
        expect(report.entries[0]?.outcome).toBe("removed");
        expect(report.entries[0]?.bytes).toBeGreaterThan(0);
        expect(report.bytesFreed).toBe(report.entries[0]?.bytes);
      }),
    ),
  );
  it.live("keeps staged edits in tracked-only mode", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const { service, fs, worktree, setPolicy, command } = yield* cleanupFixture;
        setPolicy("tracked-changes");
        yield* fs.writeFileString(`${worktree}/tracked.txt`, "staged\n");
        yield* command(worktree, ["add", "tracked.txt"]);
        expect((yield* service.runNow).entries[0]).toMatchObject({
          outcome: "kept",
          reason: "Has uncommitted changes (1 file)",
        });
        expect(yield* fs.exists(worktree)).toBe(true);
      }),
    ),
  );
  it.live(
    "reports shared and running worktrees but omits disabled rules and missing directories",
    () =>
      runCleanupTest(
        Effect.gen(function* () {
          const fixture = yield* cleanupFixture;
          const { service, fs, worktree, setThreads } = fixture;
          setThreads([shell({ branch: "feature", worktreePath: worktree, status: "running" })]);
          expect((yield* service.runNow).entries[0]?.reason).toContain("Thread is running");
          setThreads([
            shell({ branch: "feature", worktreePath: worktree }),
            shell({ id: ThreadId.make("thread-2"), branch: "feature", worktreePath: worktree }),
          ]);
          expect((yield* service.runNow).entries[0]?.reason).toBe("Shared by 2 threads");
          fixture.disable();
          expect((yield* service.runNow).entries).toEqual([]);
          yield* fs.remove(worktree, { recursive: true });
          expect(
            (yield* service.runNow).entries.filter((entry) => entry.kind === "worktree"),
          ).toEqual([]);
        }),
      ),
  );
  it.live("serializes concurrent requests and returns each run's report", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const { service } = yield* cleanupFixture;
        const reports = yield* Effect.all([service.runNow, service.runNow], {
          concurrency: "unbounded",
        });
        yield* service.drain;
        expect(
          reports.flatMap((report) => report.entries).filter((entry) => entry.kind === "worktree"),
        ).toMatchObject([{ outcome: "removed" }]);
        expect(yield* service.latestReport).toEqual(reports[1]);
      }),
    ),
  );
  it.live("aggregates expired artifacts and rotated logs while retaining current logs", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const { service, fs, config, enableFiles, disable } = yield* cleanupFixture;
        disable();
        enableFiles();
        yield* fs.makeDirectory(config.browserArtifactsDir, { recursive: true });
        yield* fs.makeDirectory(config.logsDir, { recursive: true });
        for (const name of ["one.png", "two.webm"]) {
          const file = config.browserArtifactsDir + "/" + name;
          yield* fs.writeFileString(file, "capture");
          yield* fs.utimes(file, 1, 1);
        }
        for (const name of ["server.log.1", "server.log.2", "server.log"]) {
          const file = config.logsDir + "/" + name;
          yield* fs.writeFileString(file, "log");
          yield* fs.utimes(file, 1, 1);
        }
        const report = yield* service.runNow;
        expect(report.entries.filter((entry) => entry.kind !== "worktree")).toMatchObject([
          {
            kind: "browser-artifacts",
            outcome: "removed",
            reason: "Removed 2 browser artifacts",
            bytes: 14,
            files: 2,
          },
          {
            kind: "logs",
            outcome: "removed",
            reason: "Removed 2 rotated logs",
            bytes: 6,
            files: 2,
          },
        ]);
        expect(report.bytesFreed).toBe(20);
        expect(yield* fs.readDirectory(config.browserArtifactsDir)).toEqual([]);
        expect(yield* fs.exists(config.logsDir + "/server.log")).toBe(true);
        expect(yield* fs.exists(config.logsDir + "/server.log.1")).toBe(false);
        expect(yield* fs.exists(config.logsDir + "/server.log.2")).toBe(false);
      }),
    ),
  );
  it.live("reports git failures without aborting the remaining categories", () =>
    runCleanupTest(
      Effect.gen(function* () {
        const { service, worktree, command, enableFiles } = yield* cleanupFixture;
        enableFiles();
        yield* command(worktree, ["worktree", "lock", worktree]);
        const report = yield* service.runNow;
        expect(report.entries[0]).toMatchObject({ outcome: "failed" });
        expect(report.entries[0]?.reason).toContain("git worktree remove: fatal:");
        expect(report.entries.map((entry) => entry.kind)).toEqual([
          "worktree",
          "browser-artifacts",
          "logs",
        ]);
      }),
    ),
  );
});
