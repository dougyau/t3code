// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Offline fixture creates private files and observes atomic publications from its captured child.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeChildProcess from "node:child_process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert } from "@effect/vitest";
import { DateTime } from "effect";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import {
  DEFAULT_SERVER_SETTINGS,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { AgentScope } from "@t3tools/shared/AgentScope";
import { PiSettings } from "../settings.ts";
import { makePiAdapterV2 } from "./adapter.ts";
import { PiWorkerBridge, WorkerInspectionError } from "./workerBridge.ts";
import { connect } from "./pitako/bridge/client.js";

const source = NodeProcess.env.T3_PITAKO_NATIVE_TEST_ROOT;
const packagedClient = NodeProcess.env.T3_PACKAGED_PITAKO_CLIENT;
const minimumNode = NodeProcess.env.T3_PITAKO_MIN_NODE;
const decodeSettings = Schema.decodeEffect(PiSettings);
const isWorkerInspectionError = Schema.is(WorkerInspectionError);

/** Atomic file publication, not timing/polling, drives every native milestone. */
function publication(
  root: string,
  signal: AbortSignal,
  file: string,
  accept: (value: unknown) => boolean,
  fresh = false,
) {
  return new Promise<unknown>((resolve, reject) => {
    const watcher = NodeFS.watch(root, { signal }, (_event, changed) => {
      if (signal.aborted) return;
      if (String(changed) === file) check();
    });
    const check = () => {
      try {
        const value: unknown = JSON.parse(NodeFS.readFileSync(NodePath.join(root, file), "utf8"));
        if (accept(value)) {
          watcher.close();
          resolve(value);
        }
      } catch {
        /* The producer has not published yet. */
      }
    };
    watcher.once("error", (error) => {
      watcher.close();
      reject(error);
    });
    if (!fresh) check();
  });
}

const Probe = Schema.Struct({
  coordinatorCalls: Schema.Number,
  childCalls: Schema.Number,
  frontierEntered: Schema.Number,
  sessionId: Schema.String,
  sessionFile: Schema.String,
  argv: Schema.Array(Schema.String),
  pid: Schema.Number,
  rows: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      outcome: Schema.Boolean,
      resultTaken: Schema.Boolean,
      live: Schema.Boolean,
    }),
  ),
  roles: Schema.Array(Schema.Array(Schema.String)),
});
const decodeProbe = Schema.decodeUnknownSync(Probe);

function extensionSource(root: string, pitako: string, spawnWorkers = true, extendedReads = false) {
  const literal = JSON.stringify;
  return `
import { writeFileSync, renameSync, readFileSync, watch } from "node:fs";
import { createAssistantMessageEventStream } from ${literal(NodePath.join(pitako, "node_modules/@earendil-works/pi-ai/dist/utils/event-stream.js"))};
import { listObservations, subscribeChanges } from ${literal(NodePath.join(pitako, "extensions/agent/observe.ts"))};
import { teamEvaluationForSession } from ${literal(NodePath.join(pitako, "extensions/team.ts"))};
const root = ${literal(root)};
let coordinatorCalls = 0, childCalls = 0, frontierEntered = 0, context, watcher, unsubscribe, release, releaseFrontier;
const released = new Promise(resolve => { release = resolve; });
const frontier = new Promise(resolve => { releaseFrontier = resolve; });
const consumed = [];
function publish() {
  if (!context) return;
  const sessionId = context.sessionManager.getSessionId();
  const evaluation = teamEvaluationForSession(sessionId, false);
  const observations = listObservations(evaluation?.token, sessionId);
  const bag = globalThis[Symbol.for("pitako.backgroundWorkers")];
  const rows = observations.map(item => {
    const row = bag?.rows.get(item.id);
    return { id: item.id, outcome: !!row?.outcome, live: !!row?.live, resultTaken: !!item.resultTaken };
  });
  const roles = [...(globalThis[Symbol.for("pitako.teamRegistry")]?.evaluations.get(sessionId)?.roles ?? [])];
  const state = { coordinatorCalls, childCalls, frontierEntered, sessionId, sessionFile: context.sessionManager.getSessionFile(),
    argv: process.argv, pid: process.pid, rows, roles };
  writeFileSync(root + "/state.json.tmp", JSON.stringify(state));
  renameSync(root + "/state.json.tmp", root + "/state.json");
}
globalThis[Symbol.for("t3.pitako.fixture")] = { released, frontier, publish,
  child: () => { childCalls++; publish(); },
  frontierEnter: () => { frontierEntered++; publish(); },
  consumed: (messages) => {
    consumed.push(messages);
    writeFileSync(root + "/consumed.json.tmp", JSON.stringify(consumed));
    renameSync(root + "/consumed.json.tmp", root + "/consumed.json");
  } };
export default function(pi) {
  const model = { id: "controlled", name: "Offline controlled", compat: { supportsOpenAIGrammarTools: true },
    reasoning: false, input: ["text"], contextWindow: 1000000, maxTokens: 128,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } };
  pi.registerProvider("t3-fixture", {
    api: "openai-responses", baseUrl: "http://127.0.0.1", apiKey: "fixture", models: [model],
    streamSimple(model) {
      coordinatorCalls++; publish();
      const stream = createAssistantMessageEventStream();
      const content = ${spawnWorkers} && coordinatorCalls === 1 ? [
        { type: "toolCall", id: "spawn-background", name: "agent_spawn", arguments: { role: "researcher", task: ${JSON.stringify(`Offline background fixture${extendedReads ? " brief 😀".repeat(200) : ""}`)} } },
        { type: "toolCall", id: "assign-team", name: "team_assign", arguments: { role: "researcher", task: "Offline Team fixture" } }
      ] : [{ type: "text", text: "Coordinator idle; native children continue." }];
      const message = { role: "assistant", content, api: model.api, provider: model.provider, model: model.id,
        stopReason: ${spawnWorkers} && coordinatorCalls === 1 ? "toolUse" : "stop", timestamp: Date.now(),
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      queueMicrotask(() => {
        stream.push({ type: "start", partial: { ...message, content: [] } });
        for (const [contentIndex, block] of content.entries()) {
          if (block.type === "toolCall") {
            stream.push({ type: "toolcall_start", contentIndex, partial: message });
            stream.push({ type: "toolcall_end", contentIndex, toolCall: block, partial: message });
          } else {
            stream.push({ type: "text_start", contentIndex, partial: message });
            stream.push({ type: "text_delta", contentIndex, delta: block.text, partial: message });
            stream.push({ type: "text_end", contentIndex, content: block.text, partial: message });
          }
        }
        stream.push({ type: "done", reason: message.stopReason, message }); stream.end(message);
      });
      return stream;
    }
  });
  pi.on("session_start", (_event, ctx) => {
    context = ctx;
    unsubscribe = subscribeChanges(publish);
    watcher = watch(root, (_event, file) => {
      if (String(file) === "release") release();
      if (String(file) === "frontier") releaseFrontier();
      if (String(file) === "snapshot") publish();
    });
    publish();
  });
  pi.on("agent_settled", publish);
  pi.on("session_shutdown", () => { release(); releaseFrontier(); watcher?.close(); unsubscribe?.(); });
}
`;
}

function workerSource(pitako: string, controls = false) {
  return `
import { createAssistantMessageEventStream } from ${JSON.stringify(NodePath.join(pitako, "node_modules/@earendil-works/pi-ai/dist/utils/event-stream.js"))};
export default function(pi) {
  let calls = 0;
  pi.registerTool({ name: "fixture_frontier", label: "Offline frontier", description: "Controlled offline tool boundary",
    parameters: { type: "object", properties: {} },
    async execute() {
      const fixture = globalThis[Symbol.for("t3.pitako.fixture")];
      fixture.frontierEnter();
      await fixture.frontier;
      return { content: [{ type: "text", text: "frontier released" }] };
    } });
  pi.registerProvider("t3-worker", {
    api: "openai-responses", baseUrl: "http://127.0.0.1", apiKey: "fixture",
    models: [{ id: "controlled", name: "Offline child", compat: { supportsOpenAIGrammarTools: true },
      reasoning: false, input: ["text"], contextWindow: 1000000, maxTokens: 128,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    streamSimple(model, context) {
      const fixture = globalThis[Symbol.for("t3.pitako.fixture")];
      if (!fixture) throw Error("Only fixture-owned workers may run");
      fixture.child();
      calls++;
      // Native steering defaults to one queued note per boundary. Keep later
      // tool frontiers available for all three deliberately admitted notes.
      const atFrontier = ${controls} && calls <= 3;
      if (${controls} && calls > 1) fixture.consumed(context.messages);
      const stream = createAssistantMessageEventStream();
      const message = { role: "assistant", content: atFrontier
          ? [{ type: "toolCall", id: "frontier", name: "fixture_frontier", arguments: {} }]
          : [{ type: "text", text: "native result 😀".repeat(200) }],
        api: model.api, provider: model.provider, model: model.id, stopReason: atFrontier ? "toolUse" : "stop", timestamp: Date.now(),
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      queueMicrotask(async () => {
        stream.push({ type: "start", partial: { ...message, content: [] } });
        if (!atFrontier) await fixture.released;
        if (atFrontier) {
          stream.push({ type: "toolcall_start", contentIndex: 0, partial: message });
          stream.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0], partial: message });
        } else {
          stream.push({ type: "text_start", contentIndex: 0, partial: message });
          stream.push({ type: "text_delta", contentIndex: 0, delta: message.content[0].text, partial: message });
          stream.push({ type: "text_end", contentIndex: 0, content: message.content[0].text, partial: message });
        }
        stream.push({ type: "done", reason: message.stopReason, message }); stream.end(message);
      });
      return stream;
    }
  });
}
`;
}

export const nativeIntegration = (checkMissingBridge: boolean, onReady?: (fixture: {
  workers: PiWorkerBridge;
  runtime: import("@t3tools/provider-core/server/ProviderAdapter").ProviderAdapterV2SessionRuntime;
  providerThread: import("@t3tools/contracts").OrchestrationV2ProviderThread;
  providerSessionId: ProviderSessionId;
  threadId: ThreadId;
  appThread: OrchestrationV2AppThread;
  root: string;
  releaseFrontier: () => void;
  releaseSettlement: () => void;
  consumed: (interactionIds: ReadonlyArray<string>) => Effect.Effect<void>;
}) => Effect.Effect<void, never>, control?: { runtimeMode: "full-access" | "approval-required" | "auto" }) =>
  Effect.gen(function* () {
    assert.isDefined(source);
    const pitako = NodeFS.realpathSync(source!);
    const root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-native-pw-"));
    const publications = new AbortController();
    const agentDir = NodePath.join(root, "agent");
    const cwd = NodePath.join(root, "workspace");
    for (const dir of [
      cwd,
      agentDir,
      NodePath.join(root, "temp"),
      NodePath.join(agentDir, "extensions"),
      NodePath.join(agentDir, "pitako"),
    ])
      NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
    NodeFS.writeFileSync(
      NodePath.join(agentDir, "settings.json"),
      JSON.stringify({
        enableInstallTelemetry: false,
        compaction: { enabled: false },
        cacheWarming: "off",
      }),
    );
    NodeFS.writeFileSync(
      NodePath.join(agentDir, "pitako/config.toml"),
      '[model_policies.researcher.primary]\nmodel = "t3-worker/controlled"\nreasoning = "off"\n' +
      (onReady ? '[model_policies.developer_mid.primary]\nmodel = "t3-worker/controlled"\nreasoning = "off"\n' : ""),
    );
    const fixture = NodePath.join(root, "fixture.mjs");
    const coordinator = extensionSource(root, pitako, true, onReady !== undefined);
    // T2 needs an actual captured native route. Run the real developer preflight with
    // no classifier available (no network or paid classifier call); it records that gap.
    NodeFS.writeFileSync(fixture, onReady ? coordinator
      .replaceAll('role: "researcher"', 'role: "developer"')
      .replace("context = ctx;", `context = ctx;
        const available = ctx.modelRegistry.getAvailableOfType.bind(ctx.modelRegistry);
        ctx.modelRegistry.getAvailableOfType = (type, ...args) => type === "classifier" ? Promise.resolve([]) : available(type, ...args);`)
      : coordinator);
    NodeFS.writeFileSync(NodePath.join(agentDir, "extensions/worker.js"), workerSource(pitako, control !== undefined));
    const binary = NodePath.join(root, "pi");
    const cli = NodeFS.realpathSync(
      NodePath.join(pitako, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
    );
    NodeFS.writeFileSync(
      binary,
      `#!/bin/sh\nexec ${JSON.stringify(process.execPath)} ${JSON.stringify(cli)} "$@"\n`,
      { mode: 0o700 },
    );
    const instanceId = ProviderInstanceId.make("native-pi");
    const threadId = ThreadId.make("native-thread");
    const providerSessionId = ProviderSessionId.make("native-session");
    const modelSelection = { instanceId, model: "default" };
    const runtimePolicy = {
      cwd,
      runtimeMode: control?.runtimeMode ?? "full-access",
      interactionMode: "default" as const,
    };
    let bridgeDirectory: string | undefined;
    let wrapped = 0;
    const wrappedArgs: Array<ReadonlyArray<string>> = [];
    let stage = "open";
    const program = Effect.gen(function* () {
      const adapter = yield* makePiAdapterV2({
        instanceId,
        settings: yield* decodeSettings({
          enabled: true,
          binaryPath: binary,
          workerBridge: true,
          workerHistory: onReady !== undefined,
          workerRoute: onReady !== undefined,
          workerControl: control !== undefined,
          launchArgs: `--offline -e ${JSON.stringify(NodePath.join(pitako, "extensions/index.ts"))} -e ${JSON.stringify(NodePath.join(pitako, "extensions/agent/index.ts"))} -e ${JSON.stringify(fixture)} --provider t3-fixture --model controlled`,
        }),
        // An allowlist: no inherited provider secrets, personal agent files or live state.
        environment: {
          PATH: "/usr/bin:/bin",
          HOME: root,
          TMPDIR: NodePath.join(root, "temp"),
          XDG_CONFIG_HOME: NodePath.join(root, "config"),
          PI_CODING_AGENT_DIR: agentDir,
          PI_OFFLINE: "1",
          PITAKO_PROFILE: "coding",
        },
      });
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      assert.instanceOf(runtime.workers, PiWorkerBridge);
      const workers = runtime.workers as PiWorkerBridge;
      bridgeDirectory = workers.directory;
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      stage = "thread ready";
      assert.equal(workers.observation.state, "last_observation");
      const first = decodeProbe(
        yield* Effect.promise(() =>
          publication(root, publications.signal, "state.json", () => true),
        ),
      );
      assert.equal(workers.runtimeMode, control?.runtimeMode === "auto" ? "approval-required" : runtimePolicy.runtimeMode);
      assert.equal(
        workers.observation.state === "last_observation" &&
          workers.observation.owner.coordinatorSessionId,
        first.sessionId,
      );
      yield* Effect.promise(async () => {
        try {
          await connect(NodePath.join(workers.directory!, "descriptor.json"), {
            expectedSessionId: "not-the-correlated-session",
          });
          assert.fail("A descriptor must not supply its own expected session identity");
        } catch (error) {
          assert.equal((error as Error).name, "BridgeError");
        }
      });
      yield* workers.bind(first.sessionId);
      assert.equal(wrapped, 1);
      yield* Effect.gen(function* () {
        for (let index = 0; index < 3; index++) yield* workers.openView();
        const blocked = yield* workers.openView().pipe(Effect.flip);
        if (!isWorkerInspectionError(blocked)) throw blocked;
        assert.equal(blocked.code, "busy");
        // Private-view admission never displaces the ordinary observation socket.
        yield* workers.read((client) => client.list());
      }).pipe(Effect.scoped);
      if (packagedClient !== undefined && minimumNode !== undefined) {
        yield* Effect.promise(
          () =>
            new Promise<void>((resolve, reject) => {
              const script = `import { connect } from ${JSON.stringify(packagedClient)};
              const client = await connect(${JSON.stringify(NodePath.join(workers.directory!, "descriptor.json"))},
                { expectedSessionId: ${JSON.stringify(first.sessionId)} });
              await client.list(); await client.disconnect();
              console.log(process.version);`;
              // execFile captures and joins this exact child; no model or extension import.
              NodeChildProcess.execFile(
                minimumNode,
                ["--input-type=module", "-e", script],
                { cwd: root, env: { PATH: "/usr/bin:/bin", HOME: root }, timeout: 20_000 },
                (error, stdout) => {
                  if (error) reject(error);
                  else {
                    assert.equal(stdout.trim(), "v22.16.0");
                    resolve();
                  }
                },
              );
            }),
        );
      }
      assert.notEqual(first.sessionId, providerThread.nativeThreadRef?.nativeId);
      assert.equal(providerThread.nativeThreadRef?.nativeId, first.sessionFile);
      assert.isFalse(first.argv.includes("--no-session"));
      assert.isFalse(first.argv.includes("--no-extensions"));
      assert.isTrue(first.argv.some((arg) => arg.endsWith("pi-t3-mcp-extension.ts")));
      assert.isTrue(first.argv.some((arg) => arg.startsWith("--pitako-agent-bridge=")));
      assert.equal(wrapped, 1);
      const now = yield* DateTime.now;
      const appThread = {
        id: threadId,
        projectId: "project:native" as OrchestrationV2AppThread["projectId"],
        title: "native fixture",
        createdBy: "user",
        creationSource: "web",
        providerInstanceId: instanceId,
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      } satisfies OrchestrationV2AppThread;
      const events =
        yield* Queue.unbounded<
          import("@t3tools/provider-core/server/ProviderAdapter").ProviderAdapterV2Event
        >();
      yield* runtime.events.pipe(
        Stream.runForEach((event) => Queue.offer(events, event)),
        Effect.forkScoped,
      );
      yield* runtime.startTurn({
        appThread,
        threadId,
        providerThread,
        runId: RunId.make("native-run"),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make("native-attempt"),
        rootNodeId: NodeId.make("native-root"),
        message: {
          messageId: "message:native" as never,
          text: "Start the offline workers",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
        modelSelection,
        runtimePolicy,
      });
      stage = "turn terminal";
      while (true) {
        const event = yield* Queue.take(events);
        if (event.type === "turn.terminal") break;
        if (control && event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending") {
          // This fixture deliberately approves only its offline setup. The launch remains supervised.
          yield* runtime.respondToRuntimeRequest({ requestId: event.runtimeRequest.id, decision: "accept" });
        }
      }
      stage = "live children";
      const live = decodeProbe(
        yield* Effect.promise(() =>
          publication(root, publications.signal, "state.json", (value) => {
            const probe = decodeProbe(value);
            return (
              probe.rows.length === 2 &&
              (probe.childCalls === 2 || probe.rows.every((row) => row.outcome))
            );
          }),
        ),
      );
      assert.equal(live.childCalls, 2);
      yield* workers.refresh;
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      assert.isFalse(yield* runtime.hasPendingBackgroundWorkForThread!(providerThread));
      assert.equal(live.roles.length, 1);
      const list = yield* workers.read((client) => client.list());
      assert.deepEqual(list.items.map((item) => item.source).sort(), [
        "agent_spawn",
        "team_assign",
      ]);
      const ready = {
        workers, runtime, providerThread, providerSessionId, threadId, appThread, root,
        releaseFrontier: () => NodeFS.writeFileSync(NodePath.join(root, "frontier"), ""),
        releaseSettlement: () => NodeFS.writeFileSync(NodePath.join(root, "release"), ""),
        consumed: (interactionIds: ReadonlyArray<string>) => Effect.promise(() =>
          publication(root, publications.signal, "consumed.json", (messages) =>
            interactionIds.every((id) => JSON.stringify(messages).includes(id)))).pipe(Effect.asVoid),
      };
      if (control && onReady) {
        yield* Effect.promise(() => publication(root, publications.signal, "state.json",
          (state) => decodeProbe(state).frontierEntered === 2));
        stage = "live controls";
        yield* onReady(ready);
        stage = "controls complete";
        return;
      }
      for (const item of list.items) {
        const detail = yield* workers.read((client) => client.detail(item.id));
        yield* workers.read((client) => client.result(detail.ref));
        yield* workers.read((client) => client.activity(detail.ref));
      }
      const liveSnapshot = publication(root, publications.signal, "state.json", () => true, true);
      NodeFS.writeFileSync(NodePath.join(root, "snapshot"), "");
      const liveAfter = decodeProbe(yield* Effect.promise(() => liveSnapshot));
      assert.deepEqual(liveAfter.roles, live.roles);
      assert.deepEqual(liveAfter.rows, live.rows);
      assert.equal(liveAfter.coordinatorCalls, live.coordinatorCalls);
      const observation = workers.observations.pipe(
        Stream.filter(
          (state) => state.state === "last_observation" && state.revision > list.revision,
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      const changed = yield* observation;
      const terminal = publication(root, publications.signal, "state.json", (value) => {
        const probe = decodeProbe(value);
        return probe.rows.length === 2 && probe.rows.every((row) => row.outcome && !row.live);
      });
      NodeFS.writeFileSync(NodePath.join(root, "release"), "");
      stage = "settlement";
      yield* Effect.promise(() => terminal);
      yield* Fiber.join(changed);
      yield* workers.refresh;
      assert.isFalse(yield* runtime.hasPendingBackgroundWork!);
      const before = decodeProbe(
        yield* Effect.promise(() =>
          publication(root, publications.signal, "state.json", () => true),
        ),
      );
      for (const item of list.items) {
        const detail = yield* workers.read((client) => client.detail(item.id));
        const result = yield* workers.read((client) => client.result(detail.ref));
        assert.equal(result.state, "ready");
        if (result.state !== "ready") throw new Error("Settled native result unavailable");
        assert.isAtMost(result.text.text.length, 1_024);
        assert.isNotNull(result.nextText);
        let page = result;
        let text = result.text.text;
        while (page.nextText !== null) {
          const cursor = page.nextText;
          const next = yield* workers.read((client) => client.result(detail.ref, cursor));
          if (next.state !== "ready") throw new Error("Native result continuation unavailable");
          assert.equal(next.text.offset, page.text.nextOffset);
          assert.isAtMost(next.text.text.length, 1_024);
          text += next.text.text;
          assert.isAtMost(text.length, "native result 😀".repeat(200).length);
          page = next;
        }
        assert.equal(text, "native result 😀".repeat(200));
        assert.deepEqual(yield* workers.read((client) => client.result(detail.ref)), result);
        yield* workers.read((client) => client.activity(detail.ref));
        yield* workers.read((client) => client.requests(item.id));
      }
      const afterPublished = publication(root, publications.signal, "state.json", () => true, true);
      NodeFS.writeFileSync(NodePath.join(root, "snapshot"), "");
      const after = decodeProbe(yield* Effect.promise(() => afterPublished));
      assert.equal(after.coordinatorCalls, before.coordinatorCalls);
      assert.equal(after.coordinatorCalls, 2);
      assert.deepEqual(after.rows, before.rows);
      assert.isTrue(after.rows.every((row) => !row.resultTaken));
      assert.deepEqual(after.roles, before.roles);
      assert.equal(after.pid, first.pid);
      assert.equal(wrapped, 1);
      assert.isTrue(NodeFS.existsSync(after.sessionFile));
      if (onReady) yield* onReady(ready);
      // Rebinding to a different correlated session rejects this descriptor and
      // joins the old client's work, then allows an explicit valid rebind.
      yield* workers.bind("not-the-correlated-session");
      assert.equal(workers.observation.state, "error");
      yield* workers.bind(first.sessionId);
      assert.equal(workers.observation.state, "last_observation");
      // No fake close callback: disconnect the actual ordinary native client.
      yield* workers.read((client) => client.disconnect());
      assert.equal(workers.observation.state, "last_observation");
      assert.isTrue((yield* Effect.exit(workers.refresh))._tag === "Failure");
      assert.isTrue(yield* runtime.hasPendingBackgroundWork!);
      assert.equal(workers.observation.state, "error");
      yield* workers.invalidate;
      assert.equal(workers.observation.state, "stale_owner");
      if (checkMissingBridge) {
        // No Pitako bridge extension at all: flags must not break ordinary Pi chat.
        for (const registersFlags of [false, true]) {
        const absent = NodePath.join(root, "absent");
        const home = NodePath.join(absent, registersFlags ? "no-descriptor" : "rejected");
        NodeFS.mkdirSync(home, { recursive: true, mode: 0o700 });
        const absentAgent = NodePath.join(home, "agent");
        NodeFS.mkdirSync(absentAgent, { recursive: true, mode: 0o700 });
        const absentFixture = NodePath.join(home, "fixture.mjs");
        // Fixture-only registrations model a responsive extension without a
        // descriptor. They are not added to T3's injected extension.
        NodeFS.writeFileSync(absentFixture, extensionSource(home, pitako, false).replace(
          "export default function(pi) {",
          `export default function(pi) { ${registersFlags ? `
            pi.registerFlag("pitako-agent-bridge", { type: "string", description: "Fixture only" });
            pi.registerFlag("pitako-agent-bridge-grants", { type: "string", description: "Fixture only" });
          ` : ""}`,
        ));
        const nativeSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const attempts: Array<{
          exited: boolean; stdoutDrained: boolean; stderrDrained: boolean; joined: boolean;
        }> = [];
        const observedSpawner = ChildProcessSpawner.make((command) => Effect.gen(function* () {
          if (attempts.length > 0) {
            assert.deepEqual(attempts[0], {
              exited: true, stdoutDrained: true, stderrDrained: true, joined: true,
            });
            assert.isFalse(NodeFS.existsSync(NodePath.join(home, "state.json")));
          }
          const attempt = { exited: false, stdoutDrained: false, stderrDrained: false, joined: false };
          attempts.push(attempt);
          // Registered before spawn, so this marker runs after the owned
          // native process and transport finalizers have joined.
          yield* Effect.addFinalizer(() => Effect.sync(() => { attempt.joined = true; }));
          const child = yield* nativeSpawner.spawn(command);
          return ChildProcessSpawner.makeHandle({
            ...child,
            exitCode: child.exitCode.pipe(Effect.tap(() => Effect.sync(() => { attempt.exited = true; }))),
            stdout: child.stdout.pipe(Stream.ensuring(Effect.sync(() => { attempt.stdoutDrained = true; }))),
            stderr: child.stderr.pipe(Stream.ensuring(Effect.sync(() => { attempt.stderrDrained = true; }))),
          });
        }));
        const beforeWrap = wrapped;
        const absentAdapter = yield* makePiAdapterV2({
          instanceId,
          settings: yield* decodeSettings({
            enabled: true,
            binaryPath: binary,
            workerBridge: true,
            workerHistory: true,
            workerRoute: true,
            workerControl: true,
            launchArgs: `--offline -e ${JSON.stringify(absentFixture)} --provider t3-fixture --model controlled`,
          }),
          environment: {
            PATH: "/usr/bin:/bin",
            HOME: home,
            XDG_CONFIG_HOME: NodePath.join(home, "config"),
            PI_CODING_AGENT_DIR: absentAgent,
            PI_OFFLINE: "1",
          },
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, observedSpawner));
        const absentRuntime = yield* absentAdapter.openSession({
          threadId,
          providerSessionId: ProviderSessionId.make("absent-bridge"),
          modelSelection,
          runtimePolicy: { ...runtimePolicy, runtimeMode: "auto" },
        });
        assert.equal(attempts.length, registersFlags ? 1 : 2);
        assert.equal(wrapped - beforeWrap, registersFlags ? 1 : 2);
        if (!registersFlags) {
          assert.deepEqual(wrappedArgs[beforeWrap + 1],
            wrappedArgs[beforeWrap]!.filter((arg) => !arg.startsWith("--pitako-agent-bridge")));
          const rejectedDirectory = wrappedArgs[beforeWrap]!
            .find((arg) => arg.startsWith("--pitako-agent-bridge="))!.split("=")[1]!;
          assert.isTrue(NodeFS.existsSync(rejectedDirectory));
        }
        const absentWorkers = absentRuntime.workers as PiWorkerBridge;
        const absentThread = yield* absentRuntime.resumeThread({
          providerThread,
        });
        assert.equal(absentThread.nativeThreadRef?.nativeId, first.sessionFile);
        assert.equal(absentRuntime.workers?.runtimeMode, "approval-required");
        if (registersFlags) {
          yield* absentWorkers.observations.pipe(
            Stream.filter((state) => state.state === "error"),
            Stream.runHead,
          );
          assert.equal(absentWorkers.observation.state === "error" && absentWorkers.observation.code, "bridge_not_ready");
        } else {
          assert.deepEqual(absentWorkers.observation, { state: "error", code: "bridge_launch_failed" });
          yield* absentWorkers.bind("late-retired-owner");
          const failure = yield* absentWorkers.refresh.pipe(Effect.flip);
          assert.equal(failure.code, "bridge_launch_failed");
          yield* absentWorkers.invalidate;
          assert.deepEqual(absentWorkers.observation, { state: "error", code: "bridge_launch_failed" });
        }
        assert.isTrue(yield* absentRuntime.hasPendingBackgroundWork!);
        const beforeChat = decodeProbe(yield* Effect.promise(() =>
          publication(home, publications.signal, "state.json", () => true),
        ));
        assert.equal(beforeChat.coordinatorCalls, 0);
        assert.equal(beforeChat.childCalls, 0);
        assert.isTrue(beforeChat.argv.some((arg) => arg.endsWith("pi-t3-mcp-extension.ts")));
        assert.equal(beforeChat.argv.some((arg) => arg.startsWith("--pitako-agent-bridge")), registersFlags);
        yield* absentRuntime.events.pipe(
          Stream.runForEach((event) => Queue.offer(events, event)),
          Effect.forkScoped,
        );
        yield* absentRuntime.startTurn({
          appThread,
          threadId,
          providerThread: absentThread,
          runId: RunId.make("absent-run"),
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make("absent-attempt"),
          rootNodeId: NodeId.make("absent-root"),
          message: {
            messageId: "message:absent" as never,
            text: "Chat still works",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection,
          runtimePolicy,
        });
        let terminalEvent;
        do {
          terminalEvent = yield* Queue.take(events);
        } while (terminalEvent.type !== "turn.terminal");
        assert.equal(terminalEvent.status, "completed");
        assert.equal(attempts.length, registersFlags ? 1 : 2);
        }
      }
      stage = "close";
    }).pipe(
      Effect.onError(() =>
        Effect.sync(() => {
          NodeProcess.stderr.write(`Native diagnostic stage ${stage}\n`);
          const path = NodePath.join(root, "state.json");
          if (NodeFS.existsSync(path))
            NodeProcess.stderr.write(NodeFS.readFileSync(path, "utf8") + "\n");
        }),
      ),
      Effect.ensuring(Effect.sync(() => {
        NodeFS.writeFileSync(NodePath.join(root, "frontier"), "");
        NodeFS.writeFileSync(NodePath.join(root, "release"), "");
      })),
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(
          NodeServices.layer,
          IdAllocator.layer,
          McpProviderSessions.layer,
          TestProviderHost.layer({ cwd, settings: DEFAULT_SERVER_SETTINGS }).pipe(
            Layer.provide(NodeServices.layer),
          ),
        ),
      ),
      Effect.provideService(AgentScope, {
        wrap: (input) =>
          Effect.sync(() => {
            wrapped++;
            wrappedArgs.push([...input.args]);
            return { command: input.command, args: input.args };
          }),
        clear: () => Effect.void,
        oomKilled: () => Effect.succeed(false),
      }),
    );
    yield* program.pipe(
      Effect.ensuring(
        Effect.sync(() => {
          NodeFS.writeFileSync(NodePath.join(root, "release"), "");
          // The adapter scope joins its captured process before this fixture deletes history.
          publications.abort();
          NodeFS.rmSync(root, { recursive: true, force: true });
        }),
      ),
    );
    assert.isDefined(bridgeDirectory);
    assert.isFalse(NodeFS.existsSync(bridgeDirectory!));
  });
