// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Offline fixture creates private files and observes atomic publications from its captured child.
import * as NodeFS from "node:fs";
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
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import type { ProviderNativeWorkerBatch } from "@t3tools/provider-core/server/ProviderAdapter";
import {
  DEFAULT_SERVER_SETTINGS,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type PiNativeWorkerLaunch,
  type PiNativeWorkerCheckpoint,
} from "@t3tools/contracts";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { AgentScope } from "@t3tools/shared/AgentScope";
import { PiSettings } from "../settings.ts";
import { makePiAdapterV2 } from "./adapter.ts";
import { PiWorkerBridge, type WorkerSourceError } from "./workerBridge.ts";
import { connect, type BridgeClient } from "./pitako/bridge/client.js";

import { prepareNativeFixture, publication, decodeProbe, extensionSource } from "./workerBridge.native.shared.ts";

const packagedClient = NodeProcess.env.T3_PACKAGED_PITAKO_CLIENT;
const minimumNode = NodeProcess.env.T3_PITAKO_MIN_NODE;
const decodeSettings = Schema.decodeEffect(PiSettings);

interface PublicFixture {
  client: BridgeClient;
  workers: PiWorkerBridge;
  launches: ReadonlyArray<PiNativeWorkerLaunch>;
  signal: (name: string) => void;
  wait: (name: string, accept?: (value: unknown) => boolean) => Promise<unknown>;
  snapshot: () => Promise<unknown>;
  command: (command: { id: string; action: "branch" | "burst"; head?: string }) => Promise<unknown>;
  root: string;
}

/** Default driver now reads the public conversation, not the deprecated inspector projection. */
function observePublicWorkers(fixture: PublicFixture) { return Effect.gen(function* () {
  const checkpoints = new Map<string, PiNativeWorkerCheckpoint>();
  const consume = (launch: PiNativeWorkerLaunch) => Effect.gen(function* () {
    let batch: ProviderNativeWorkerBatch;
    do {
      batch = (yield* fixture.workers.read(launch, checkpoints.get(launch.instanceId)));
      assert.notInclude(batch.checkpoint.gaps, "invalid_record");
      checkpoints.set(launch.instanceId, batch.checkpoint);
    } while (batch.more);
    return batch;
  });
  (yield* Effect.promise(() => fixture.wait("coordinator-waiting.json")));
  fixture.signal("launch");
  (yield* Effect.promise(() => fixture.wait("team-waiting.json")));
  for (let step = 1; step <= 7; step++) {
    (yield* Effect.promise(() => fixture.wait(`step-${step}.json`)));
    const before = (yield* Effect.promise(() => fixture.snapshot()));
    const list = (yield* Effect.promise(() => fixture.client.list()));
    for (const item of list.items) {
      const detail = (yield* Effect.promise(() => fixture.client.publicDetail(item.id)));
      if (!("bindings" in detail)) throw Error("Public native detail unavailable");
      (yield* Effect.promise(() => fixture.client.publicActivity(detail.bindings[0]!.ref)));
      (yield* Effect.promise(() => fixture.client.publicResult(detail.ref)));
    }
    for (const launch of fixture.launches) {
      assert.equal(((yield* fixture.workers.reconcile(launch))).availability, "ready");
      (yield* consume(launch));
    }
    assert.deepEqual((yield* Effect.promise(() => fixture.snapshot())), before);
    fixture.signal(`step-${step}`);
  }
  (yield* Effect.promise(() => fixture.wait("tool-entered.json")));
  fixture.signal("tool-end");
  (yield* Effect.promise(() => fixture.wait("step-8.json")));
  fixture.signal("step-8");
  (yield* Effect.promise(() => fixture.wait("evict-tool-entered.json")));
  fixture.signal("evict-tool-end");
  (yield* Effect.promise(() => fixture.wait("step-9.json")));
  fixture.signal("step-9");
  fixture.signal("team-end");
  (yield* Effect.promise(() => fixture.wait("disposal.json")));
  fixture.signal("close-capture");
  const settled = (yield* Effect.promise(() => fixture.wait("state.json", value => {
    const probe = decodeProbe(value);
    return probe.rows.length === 2 && probe.rows.every(row => row.outcome && !row.live);
  })));
  const before = decodeProbe(settled);
  assert.equal(before.coordinatorCalls, 2);
  for (const item of ((yield* Effect.promise(() => fixture.client.list()))).items) {
    const detail = (yield* Effect.promise(() => fixture.client.publicDetail(item.id)));
    if (!("bindings" in detail)) throw Error("Closed public native detail unavailable");
    const result = (yield* Effect.promise(() => fixture.client.publicResult(detail.ref)));
    assert.equal(result.state, "ready");
    if (result.state !== "ready") throw Error("Closed native result unavailable");
    assert.isAtMost(result.text.text.length, 1024);
    assert.isNotNull(result.nextText);
    let resultPage = result;
    let text = result.text.text;
    while (resultPage.nextText !== null) {
      const nextText = resultPage.nextText;
      const next = (yield* Effect.promise(() => fixture.client.publicResult(detail.ref, nextText)));
      if (next.state !== "ready") throw Error("Native result continuation unavailable");
      assert.equal(next.text.offset, resultPage.text.nextOffset);
      assert.isAtMost(next.text.text.length, 1024);
      text += next.text.text;
      assert.isAtMost(text.length, "native result 😀".repeat(200).length);
      resultPage = next;
    }
    assert.equal(text, "native result 😀".repeat(200));
    assert.deepEqual((yield* Effect.promise(() => fixture.client.publicResult(detail.ref))), result);
    (yield* Effect.promise(() => fixture.client.publicActivity(detail.bindings[0]!.ref)));
  }
  const after = decodeProbe((yield* Effect.promise(() => fixture.snapshot())));
  assert.deepEqual(after, before);
  assert.isTrue(after.rows.every(row => !row.resultTaken));
  for (const launch of fixture.launches) {
    const batch = (yield* consume(launch));
    assert.equal(batch.summary.execution, "completed");
    assert.equal(batch.summary.capture, "closed");
    assert.deepEqual(batch.result, { status: "completed", complete: true,
      text: "native result 😀".repeat(200) });
  }
  assert.deepEqual(decodeProbe((yield* Effect.promise(() => fixture.snapshot()))), before);
}); }

export const nativeIntegration = (checkMissingBridge: boolean,
  publicDriver: (fixture: PublicFixture) => Effect.Effect<void, WorkerSourceError, Scope.Scope> = observePublicWorkers) =>
  Effect.gen(function* () {
    const { pitako, root, cancellation, publications, agentDir, cwd, fixture, binary, cli } = prepareNativeFixture();
    const ownedPids = new Set<number>();
    const instanceId = ProviderInstanceId.make("native-pi");
    const threadId = ThreadId.make("native-thread");
    const providerSessionId = ProviderSessionId.make("native-session");
    const modelSelection = { instanceId, model: "default" };
    const runtimePolicy = {
      cwd,
      runtimeMode: "full-access" as const,
      interactionMode: "default" as const,
    };
    let bridgeDirectory: string | undefined;
    let wrapped = 0;
    const wrappedArgs: Array<ReadonlyArray<string>> = [];
    let stage = "open";
    let ownedWorkers: PiWorkerBridge | undefined;
    let publicClientClosed = false;
    const processReceipts: Array<{
      pid: number; exitCode?: number; stdoutDrained: boolean; stderrDrained: boolean; joined: boolean;
    }> = [];
    const program = Effect.gen(function* () {
      const nativeSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const ownedSpawner = ChildProcessSpawner.make((command) => Effect.gen(function* () {
        const receipt: (typeof processReceipts)[number] = {
          pid: 0, stdoutDrained: false, stderrDrained: false, joined: false,
        };
        processReceipts.push(receipt);
        yield* Effect.addFinalizer(() => Effect.sync(() => { receipt.joined = true; }));
        const child = yield* nativeSpawner.spawn(command);
        receipt.pid = Number(child.pid);
        ownedPids.add(receipt.pid);
        return ChildProcessSpawner.makeHandle({
          ...child,
          exitCode: child.exitCode.pipe(Effect.tap(code => Effect.sync(() => { receipt.exitCode = Number(code); }))),
          stdout: child.stdout.pipe(Stream.ensuring(Effect.sync(() => { receipt.stdoutDrained = true; }))),
          stderr: child.stderr.pipe(Stream.ensuring(Effect.sync(() => { receipt.stderrDrained = true; }))),
        });
      }));
      const adapter = yield* makePiAdapterV2({
        instanceId,
        settings: yield* decodeSettings({
          enabled: true,
          binaryPath: binary,
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
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, ownedSpawner));
      const runtime = yield* adapter.openSession({
        threadId,
        providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      assert.instanceOf(runtime.nativeWorkers, PiWorkerBridge);
      const workers = runtime.nativeWorkers as PiWorkerBridge;
      ownedWorkers = workers;
      bridgeDirectory = workers.directory;
      const providerThread = yield* runtime.ensureThread({
        threadId,
        modelSelection,
        runtimePolicy,
      });
      stage = "thread ready";
      const first = decodeProbe(
        yield* Effect.promise(() =>
          publication(root, publications, "state.json", () => true),
        ),
      );
      ownedPids.add(first.pid);
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
      assert.deepEqual(yield* workers.readBackgroundPending(), { state: "none" });
      assert.equal(workers.owner?.coordinatorSessionId, first.sessionId);
      assert.equal(wrapped, 1);
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
      if (publicDriver) {
        stage = "public conversation";
        const launches: PiNativeWorkerLaunch[] = [];
        while (launches.length < 2) {
          const event = yield* Queue.take(events);
          if (event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool" &&
            event.turnItem.status === "completed" && event.turnItem.nativeWorkerLaunch &&
            !launches.some(launch => launch.instanceId === event.turnItem.nativeWorkerLaunch?.instanceId)) {
            launches.push(event.turnItem.nativeWorkerLaunch);
          }
        }
        const client = yield* Effect.acquireRelease(
          Effect.promise(() => connect(NodePath.join(workers.directory!, "descriptor.json"), { expectedSessionId: first.sessionId })),
          (client) => Effect.promise(async () => { await client.disconnect(); publicClientClosed = true; }),
        );
        yield* publicDriver({
          client, workers, launches, root,
          signal: (name) => NodeFS.writeFileSync(NodePath.join(root, name), ""),
          wait: (name, accept = () => true) => publication(root, publications, name, accept),
          snapshot: async () => {
            const next = publication(root, publications, "state.json", () => true, true);
            NodeFS.writeFileSync(NodePath.join(root, "snapshot"), "");
            return next;
          },
          command: async (command) => {
            const next = publication(root, publications, command.id + ".json", () => true, true);
            NodeFS.writeFileSync(NodePath.join(root, "command.json.tmp"), JSON.stringify(command));
            NodeFS.renameSync(NodePath.join(root, "command.json.tmp"), NodePath.join(root, "command.json"));
            return next;
          },
        });
        stage = "public close";
      }
      yield* workers.bind("not-the-correlated-session");
      assert.equal(workers.owner, undefined);
      yield* workers.bind(first.sessionId);
      assert.equal(workers.owner?.coordinatorSessionId, first.sessionId);
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
          ` : ""}`,
        ));
        const sourceSpawner = ownedSpawner;
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
          const child = yield* sourceSpawner.spawn(command);
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
        const absentWorkers = absentRuntime.nativeWorkers as PiWorkerBridge;
        const absentThread = yield* absentRuntime.resumeThread({
          providerThread,
        });
        assert.equal(absentThread.nativeThreadRef?.nativeId, first.sessionFile);
        // Source failure must not hold ordinary chat or claim launch admission.
        yield* absentWorkers.bind(undefined);
        assert.isFalse(yield* absentRuntime.hasPendingBackgroundWork!);
        assert.isFalse(yield* absentWorkers.hasPendingWork([]));
        const beforeChat = decodeProbe(yield* Effect.promise(() =>
          publication(home, publications, "state.json", () => true),
        ));
        ownedPids.add(beforeChat.pid);
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
        let terminalEvent = yield* Queue.take(events);
        while (terminalEvent.type !== "turn.terminal") {
          terminalEvent = yield* Queue.take(events);
        }
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
        Effect.promise(async () => {
          NodeFS.writeFileSync(NodePath.join(root, "release"), "");
          // The adapter scope joins its captured process before this fixture deletes history.
          cancellation.abort();
          await Promise.all(publications.closed);
          if (NodeProcess.platform === "linux") {
            for (const pid of ownedPids) assert.isFalse(NodeFS.existsSync(`/proc/${pid}`));
          }
          assert.isTrue(processReceipts.every(receipt =>
            receipt.joined && receipt.stdoutDrained && receipt.stderrDrained));
          NodeFS.rmSync(root, { recursive: true, force: true });
          const evidence = NodeProcess.env.T3_PITAKO_NATIVE_EVIDENCE;
          if (evidence) NodeFS.writeFileSync(
            NodePath.join(evidence, `closure-${NodePath.basename(root)}.json`),
            JSON.stringify({ root, pids: [...ownedPids], processReceipts,
              runtime: { executable: NodeProcess.execPath, version: NodeProcess.version, cli },
              publicClientClosed, bridgeOwnerReleased: ownedWorkers?.owner === undefined,
              watchers: publications.closed.size,
              rootRemoved: !NodeFS.existsSync(root), bridgeDirectory,
              bridgeRemoved: bridgeDirectory !== undefined && !NodeFS.existsSync(bridgeDirectory) }, null, 2),
          );
        }),
      ),
    );
    assert.isDefined(bridgeDirectory);
    assert.isFalse(NodeFS.existsSync(bridgeDirectory!));
  });
