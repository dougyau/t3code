// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Isolated native integration evidence and private fixture files.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeProcess from "node:process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert } from "@effect/vitest";
import { CommandId, MessageId, NodeId, ProjectId, ProviderInstanceId, ProviderSessionId,
  RunAttemptId, RunId, ThreadId, DEFAULT_SERVER_SETTINGS, type OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { AgentScope } from "@t3tools/shared/AgentScope";
import { PiSettings } from "../../../../packages/provider-pi/src/settings.ts";
import { makePiAdapterV2 } from "../../../../packages/provider-pi/src/server/adapter.ts";
import { PiWorkerBridge } from "../../../../packages/provider-pi/src/server/workerBridge.ts";
import { prepareNativeFixture, publication, decodeProbe } from "../../../../packages/provider-pi/src/server/workerBridge.native.shared.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as EventSink from "./EventSink.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const idleMs = 30 * 60_000;
type Owner = ProviderAdapter.ProviderNativeWorkersPort["owner"];
type Pending = Effect.Success<ReturnType<ProviderAdapter.ProviderNativeWorkersPort["readBackgroundPending"]>>;
interface SleepReceipt {
  sequence: number; caller: number; delegate: number; ms: number; time: number; ended: boolean;
}
interface ReadReceipt {
  sequence: number; caller: number; owner: Owner; result: Pending; time: number;
}

/** Real clocks/processes at the boundary; only consumer Effect waits use TestClock. */
export const nativeResidency = (pending: boolean) => Effect.gen(function* () {
  const fixture = prepareNativeFixture(pending);
  const { root, publications, cancellation, binary, cwd, pitako, cli } = fixture;
  const providerSessionId = ProviderSessionId.make(pending ? "native-pending" : "native-none");
  const threadId = ThreadId.make(`${providerSessionId}-thread`);
  const instanceId = ProviderInstanceId.make("native-pi");
  const modelSelection = { instanceId, model: "default" };
  const runtimePolicy = { cwd, runtimeMode: "full-access" as const, interactionMode: "default" as const };
  const originalClock = yield* Clock.Clock;
  const changes = yield* Queue.unbounded<void>();
  const journal: Array<Record<string, unknown>> = [];
  const sleeps: SleepReceipt[] = [];
  const reads: ReadReceipt[] = [];
  const callers = new Map<number, Fiber.Fiber<unknown, unknown>>();
  const processes: Array<{ pid: number; exitCode?: number; exit?: string; stdoutDrained: boolean; stderrDrained: boolean; joined: boolean }> = [];
  let sequence = 0;
  const record = (kind: string, data: Record<string, unknown> = {}) => {
    const row = { ...data, sequence: ++sequence, time: originalClock.currentTimeMillisUnsafe(), providerSessionId, kind };
    journal.push(row);
    Queue.offerUnsafe(changes, undefined);
    return row.sequence;
  };
  // Wait only on observed events. No polling, wall sleeps, or timer flushing.
  const until = <A>(get: () => A | undefined) => Effect.gen(function* () {
    while (true) {
      const result = get();
      if (result !== undefined) return result;
      yield* Queue.take(changes);
    }
  });
  const clock: Clock.Clock = { ...originalClock, sleep: duration => Duration.toMillis(duration) <= 0 ? originalClock.sleep(duration) : Effect.uninterruptibleMask(restore => Effect.gen(function* () {
    const callerFiber = yield* Effect.withFiber(Effect.succeed);
    const caller = callerFiber.id;
    callers.set(caller, callerFiber);
    // TestClock pushes its wait before its first await. Immediate start reaches
    // that registration before returning; capture the idle caller, not this child.
    const delegate = yield* restore(originalClock.sleep(duration)).pipe(Effect.forkChild({ startImmediately: true }));
    const receipt: SleepReceipt = { sequence: 0, caller, delegate: delegate.id,
      ms: Duration.toMillis(duration), time: originalClock.currentTimeMillisUnsafe(), ended: false };
    receipt.sequence = record("sleep.registered", { ...receipt });
    sleeps.push(receipt);
    return yield* restore(Fiber.join(delegate)).pipe(Effect.ensuring(
      Fiber.interrupt(delegate).pipe(Effect.andThen(Effect.sync(() => {
        receipt.ended = true; record("sleep.joined", { registration: receipt.sequence, caller });
      }))),
    ));
  })) };
  const scope = yield* Scope.make();
  let raw: ProviderAdapter.ProviderAdapterV2SessionRuntime | undefined;
  let workers: PiWorkerBridge | undefined;
  let bridgeDirectory: string | undefined;
  let bridgeIdentity: { dev: number; ino: number } | undefined;
  let operationFinished = false;
  let scopeJoined = false;
  let processIsRunning = Effect.succeed(false);
  let wrappedArgs: ReadonlyArray<string> = [];
  const rounds: Array<Record<string, unknown>> = [];
  const environment = {
    PATH: "/usr/bin:/bin", HOME: root, TMPDIR: NodePath.join(root, "temp"),
    XDG_CONFIG_HOME: NodePath.join(root, "config"), PI_CODING_AGENT_DIR: fixture.agentDir,
    PI_OFFLINE: "1", PITAKO_PROFILE: "coding",
  };
  const program = Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const observedSpawner = ChildProcessSpawner.make(command => Effect.gen(function* () {
      const receipt: (typeof processes)[number] = { pid: 0, stdoutDrained: false, stderrDrained: false, joined: false };
      processes.push(receipt);
      let captureExit: Effect.Effect<void> = Effect.void;
      yield* Effect.addFinalizer(() => captureExit.pipe(Effect.andThen(Effect.sync(() => { receipt.joined = true; record("process.scope.joined", { ...receipt }); }))));
      const child = yield* spawner.spawn(command);
      receipt.pid = Number(child.pid);
      processIsRunning = child.isRunning.pipe(Effect.orDie);
      captureExit = Effect.exit(child.exitCode).pipe(Effect.tap(result => Effect.sync(() => {
        receipt.exit = String(result);
        if (Exit.isSuccess(result)) receipt.exitCode = Number(result.value);
      })), Effect.asVoid);
      record("process.spawned", { pid: receipt.pid });
      return ChildProcessSpawner.makeHandle({ ...child,
        exitCode: child.exitCode.pipe(Effect.tap(code => Effect.sync(() => {
          receipt.exitCode = Number(code); record("process.exited", { pid: receipt.pid, code: Number(code) });
        }))),
        stdout: child.stdout.pipe(Stream.ensuring(Effect.sync(() => { receipt.stdoutDrained = true; record("stdout.joined"); }))),
        stderr: child.stderr.pipe(Stream.ensuring(Effect.sync(() => { receipt.stderrDrained = true; record("stderr.joined"); }))),
      });
    }));
    const adapterLayer = Layer.effect(ProviderAdapterRegistry.ProviderAdapterRegistryV2, Effect.gen(function* () {
      const adapter = yield* makePiAdapterV2({ instanceId,
        settings: yield* Schema.decodeEffect(PiSettings)({ enabled: true, binaryPath: binary,
          launchArgs: `--offline -e ${JSON.stringify(NodePath.join(pitako, "extensions/index.ts"))} -e ${JSON.stringify(NodePath.join(pitako, "extensions/agent/index.ts"))} -e ${JSON.stringify(fixture.fixture)} --provider t3-fixture --model controlled` }),
        environment,
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, observedSpawner));
      return ProviderAdapterRegistry.ProviderAdapterRegistryV2.of({
        get: () => Effect.succeed({ ...adapter, openSession: input => Effect.gen(function* () {
          raw = yield* adapter.openSession(input);
          assert.instanceOf(raw.nativeWorkers, PiWorkerBridge);
          workers = raw.nativeWorkers as PiWorkerBridge;
          bridgeDirectory = workers.directory;
          if (bridgeDirectory !== undefined) {
            const stat = NodeFS.lstatSync(bridgeDirectory);
            bridgeIdentity = { dev: stat.dev, ino: stat.ino };
          }
          const port = raw.nativeWorkers!;
          // Preserve live ownership/generation, not a spread snapshot of getters.
          const observedPort: ProviderAdapter.ProviderNativeWorkersPort = {
            get owner() { return port.owner; }, get ownerGeneration() { return port.ownerGeneration; },
            hints: port.hints, read: port.read, reconcile: port.reconcile, hasPendingWork: port.hasPendingWork,
            readBackgroundPending: () => Effect.gen(function* () {
              const caller = yield* Effect.fiberId;
              const owner = port.owner;
              const result = yield* port.readBackgroundPending();
              const receipt = { sequence: record("manager.native-read.finished", { caller, owner, result }), caller, owner, result, time: originalClock.currentTimeMillisUnsafe() };
              reads.push(receipt);
              return result;
            }),
          };
          return { ...raw, nativeWorkers: observedPort };
        }) }),
        list: () => Effect.succeed([instanceId]),
      });
    })).pipe(Layer.provideMerge(Layer.mergeAll(IdAllocator.layer,
      TestProviderHost.layer({ cwd, settings: DEFAULT_SERVER_SETTINGS }).pipe(Layer.provide(NodeServices.layer)))), Layer.provide(NodeServices.layer));
    const layer = ProviderReplayHarness.layerWithRegistry({ name: "native-residency" }, adapterLayer, { runEffectWorker: false });
    const context = yield* Layer.build(layer);
    yield* Effect.gen(function* () {
      const manager = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sink = yield* EventSink.EventSinkV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const domainEvents: OrchestrationV2DomainEvent[] = [];
      const idleReleased = () => domainEvents.find(event => event.type === "provider-session.updated" && event.payload.id === providerSessionId && event.payload.status === "stopped");
      yield* orchestrator.streamDomainEvents.pipe(Stream.runForEach(event => Effect.sync(() => {
        domainEvents.push(event); record("domain-event", { type: event.type, payload: event.payload });
      })), Effect.forkIn(scope));
      yield* orchestrator.dispatch({ type: "thread.create", commandId: CommandId.make("native-create"), threadId,
        projectId: ProjectId.make("native-project"), title: "Native residency", modelSelection,
        runtimeMode: "full-access", interactionMode: "default", branch: null, worktreePath: cwd,
        createdBy: "user", creationSource: "web" });
      const runtime = yield* manager.open({ threadId, providerSessionId, modelSelection, runtimePolicy });
      const providerThread = yield* runtime.ensureThread({ threadId, modelSelection, runtimePolicy });
      const first = decodeProbe(yield* Effect.promise(() => publication(root, publications, "state.json", () => true)));
      assert.equal(processes[0]?.pid, first.pid);
      yield* workers!.hints.pipe(Stream.filter(() => workers!.owner?.coordinatorSessionId === first.sessionId), Stream.take(1), Stream.runDrain);
      assert.equal(workers!.owner?.coordinatorSessionId, first.sessionId);
      const descriptor: unknown = JSON.parse(NodeFS.readFileSync(NodePath.join(bridgeDirectory!, "descriptor.json"), "utf8"));
      const identity = yield* Schema.decodeUnknownEffect(Schema.Struct({ owner: Schema.Struct({
        processId: Schema.String, coordinatorSessionId: Schema.String, epoch: Schema.String,
      }) }))(descriptor);
      assert.deepEqual(identity.owner, workers!.owner);
      record("descriptor.correlated", { descriptor, owner: workers!.owner, probe: first });
      assert.equal(providerThread.nativeThreadRef?.nativeId, first.sessionFile);
      assert.isTrue(first.argv.some(arg => arg.startsWith("--pitako-agent-bridge=")));
      const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
      const subscription = yield* runtime.subscribeEvents!;
      let subscriptionExit: string | undefined;
      yield* subscription.events.pipe(Stream.runForEach(event => Effect.sync(() => {
        events.push(event); record("provider-event", { event });
      })), Effect.onExit(exit => Effect.sync(() => {
        subscriptionExit = String(exit); record("subscription.joined", { exit: subscriptionExit });
      })), Effect.exit, Effect.forkIn(scope));
      const runId = RunId.make(`${providerSessionId}-run`);
      yield* runtime.startTurn({ appThread: (yield* orchestrator.getThreadProjection(threadId)).thread, threadId, providerThread,
        runId, runOrdinal: 1, providerTurnOrdinal: 1, attemptId: RunAttemptId.make(`${runId}-attempt`), rootNodeId: NodeId.make(`${runId}-root`),
        message: { messageId: MessageId.make(`${runId}-message`), text: "Run the offline turn", attachments: [], createdBy: "user", creationSource: "web" },
        modelSelection, runtimePolicy });
      if (pending) {
        const launches = yield* until(() => {
          const distinct = new Map(events.flatMap(event => event.type === "turn_item.updated" && event.turnItem.type === "dynamic_tool" &&
            event.turnItem.status === "completed" && event.turnItem.nativeWorkerLaunch !== undefined
              ? [[event.turnItem.nativeWorkerLaunch.instanceId, event.turnItem] as const] : []));
          return distinct.size === 2 ? [...distinct.values()] : undefined;
        });
        assert.sameMembers(launches.map(item => item.toolName), ["agent_spawn", "team_assign"]);
        record("native.launches.observed", { launches });
        yield* Effect.promise(() => publication(root, publications, "coordinator-waiting.json", () => true));
        NodeFS.writeFileSync(NodePath.join(root, "coordinator"), "");
      }
      // Only actual short settling waits may move before the target terminal.
      const terminal = () => events.find(event => event.type === "turn.terminal" && event.providerThreadId === providerThread.id && event.runOrdinal === 1);
      while (terminal() === undefined) {
        const next = yield* until(() => terminal() ?? sleeps.find(sleep => !sleep.ended && sleep.ms < 1_000));
        if ("ms" in next) yield* TestClock.adjust(next.ms);
      }
      assert.equal(terminal()!.type, "turn.terminal");
      const ended = terminal()!;
      if (ended.type !== "turn.terminal") throw Error("Missing target terminal");
      assert.equal(ended.status, "completed");
      assert.isFalse(yield* raw!.hasPendingBackgroundWork!);
      assert.deepEqual(yield* workers!.readBackgroundPending(), { state: pending ? "pending" : "none" });
      assert.deepEqual(yield* sink.nativeWorkers(providerSessionId), []);
      const owner = workers!.owner;
      assert.isDefined(owner);
      record("target-root.terminal", { runId, providerThreadId: providerThread.id, owner });
      const stable = decodeProbe(yield* Effect.promise(() => publication(root, publications, "state.json", () => true)));
      assert.equal(stable.childCalls, 0);
      assert.equal(stable.coordinatorCalls, pending ? 2 : 1);
      record("native.gates.closed", { probe: stable, launch: false, closeCapture: false });
      let caller: number | undefined;
      let priorRead = reads.length;
      for (let round = 1; round <= (pending ? 10 : 1); round++) {
        // On the first round retain registered candidates, then bind the exact
        // previous wait to the caller identified by the manager's real read.
        yield* until(() => sleeps.find(sleep => !sleep.ended && sleep.ms === idleMs && (caller === undefined || sleep.caller === caller)));
        const before = record("idle.adjust", { round });
        yield* TestClock.adjust(idleMs);
        const read = yield* until(() => reads.slice(priorRead).find(read => caller === undefined || read.caller === caller));
        priorRead = reads.indexOf(read) + 1;
        caller = read.caller;
        const previous = sleeps.findLast(sleep => sleep.sequence < before && sleep.ms === idleMs && sleep.caller === caller);
        assert.isDefined(previous, "real idle read must have its own previous registered wait");
        assert.isBelow(previous!.sequence, before);
        assert.isTrue(previous!.ended);
        assert.equal(read.time - previous!.time, idleMs);
        assert.deepEqual(read.owner, owner);
        assert.deepEqual(read.result, { state: pending ? "pending" : "none" });
        if (pending) {
          const next = yield* until(() => sleeps.find(sleep => sleep.sequence > read.sequence && sleep.caller === caller && sleep.ms === idleMs && !sleep.ended));
          assert.isTrue(Option.isSome(yield* manager.getResident(providerSessionId)));
          assert.isTrue(yield* processIsRunning);
          if (NodeProcess.platform === "linux") assert.isTrue(NodeFS.existsSync(`/proc/${first.pid}`));
          assert.isFalse(yield* raw!.hasPendingBackgroundWork!);
          assert.deepEqual(yield* sink.nativeWorkers(providerSessionId), []);
          assert.isFalse(NodeFS.existsSync(NodePath.join(root, "launch")));
          assert.isFalse(NodeFS.existsSync(NodePath.join(root, "close-capture")));
          assert.deepEqual(decodeProbe(JSON.parse(NodeFS.readFileSync(NodePath.join(root, "state.json"), "utf8"))), stable);
          rounds.push({ round, previous: { ...previous }, read, next: { ...next }, decision: "resident", owner, ordinary: false, members: [] });
          record("idle.decision.resident", { round, caller, previous: previous!.sequence, read: read.sequence, next: next.sequence });
        } else {
          rounds.push({ round, previous: { ...previous }, read, decision: "idle-none", owner, ordinary: false, members: [] });
        }
      }
      if (pending) {
        // Dispatch the public command; the outbox worker, not a direct close,
        // supplies the authoritative manual shutdown while producer gates stay shut.
        const command = yield* orchestrator.dispatch({ type: "provider-session.detach", commandId: CommandId.make("native-detach"), threadId, providerSessionId }).pipe(Effect.forkIn(scope));
        yield* Fiber.join(command);
        record("detach.command.joined");
        const drain = yield* worker.drain().pipe(Effect.forkIn(scope));
        yield* closeOperation(drain);
        record("detach.drain.joined");
      } else {
        const release = yield* Effect.gen(function* () {
          yield* until(idleReleased);
          yield* until(() => processes.every(process => process.joined) ? true : undefined);
          assert.isTrue(Exit.isSuccess(yield* Fiber.await(callers.get(caller!)!)));
          record("idle.caller.joined", { caller });
        }).pipe(Effect.forkChild);
        yield* closeOperation(release);
      }
      assert.isTrue(Option.isNone(yield* manager.getResident(providerSessionId)));
      assert.isFalse(yield* processIsRunning);
      if (NodeProcess.platform === "linux") assert.isFalse(NodeFS.existsSync(`/proc/${first.pid}`));
      assert.deepEqual(yield* sink.nativeWorkers(providerSessionId), []);
      const projection = yield* orchestrator.getThreadProjection(threadId);
      record("persisted-session", { providerSessions: projection.providerSessions, subscriptionExit });
      if (!pending) {
        assert.include(subscriptionExit, "idle_timeout");
        assert.equal(projection.providerSessions.find(session => session.id === providerSessionId)?.status, "stopped");
      }
      assert.isFalse(NodeFS.existsSync(NodePath.join(root, "launch")));
      assert.isFalse(NodeFS.existsSync(NodePath.join(root, "close-capture")));
      assert.isTrue(pending ? domainEvents.some(event => event.type === "provider-session.detached") : idleReleased() !== undefined);
      operationFinished = true;
      record("operation.joined", { reason: pending ? "explicit-detach" : "idle-none" });
    }).pipe(Effect.provide(context));
  }).pipe(Effect.provideService(Scope.Scope, scope), Effect.provide(NodeServices.layer),
    Effect.provideService(AgentScope, { wrap: input => Effect.sync(() => {
      wrappedArgs = input.args; return { command: input.command, args: input.args };
    }), clear: () => Effect.void, oomKilled: () => Effect.succeed(false) }),
    Effect.provideService(Clock.Clock, clock));

  // Race completion with registered grace, never wait for a grace that did not
  // occur. The 30s manager scope timeout is deliberately NOT driven on success.
  const closeOperation = <A, E>(fiber: Fiber.Fiber<A, E>) => Effect.gen(function* () {
    let done = false;
    const joined = yield* Fiber.await(fiber).pipe(Effect.tap(() => Effect.sync(() => { done = true; record("close.operation.finished"); })), Effect.forkChild);
    const driven = new Set<number>();
    while (!done) {
      const grace = yield* until(() => done ? true : sleeps.find(sleep => !sleep.ended && sleep.ms === 1_000 && !driven.has(sleep.sequence)));
      if (grace === true) break;
      driven.add(grace.sequence);
      record("close.grace.adjust", { registration: grace.sequence, caller: grace.caller });
      yield* TestClock.adjust(1_000);
    }
    const result = yield* Fiber.join(joined);
    return yield* result;
  });
  const outcome = yield* program.pipe(Effect.timeout("60 seconds"), TestClock.withLive, Effect.exit);
  // The program uses the observed TestClock explicitly; do not change native
  // bridge Node deadlines. Keep observers alive while every finalizer joins.
  const closing = yield* Scope.close(scope, outcome).pipe(Effect.provideService(Clock.Clock, clock), Effect.forkChild);
  yield* closeOperation(closing);
  scopeJoined = true;
  cancellation.abort();
  yield* Effect.promise(() => Promise.all(publications.closed));
  const physicallyClosed = !(yield* processIsRunning) && processes.every(receipt => receipt.joined && receipt.stdoutDrained && receipt.stderrDrained &&
    (receipt.exitCode !== undefined || receipt.exit?.includes("Process interrupted due to receipt of signal:") === true) &&
    (NodeProcess.platform !== "linux" || !NodeFS.existsSync(`/proc/${receipt.pid}`)));
  const observersJoined = sleeps.every(sleep => sleep.ended);
  const bridgeRetainedAfterExit = bridgeDirectory !== undefined && NodeFS.existsSync(bridgeDirectory);
  if (physicallyClosed && scopeJoined && observersJoined) {
    // Production preserves nonempty directories after forced exit. This test
    // may remove its own unchanged private directory only after all joins.
    if (bridgeRetainedAfterExit) {
      const stat = NodeFS.lstatSync(bridgeDirectory!);
      assert.isFalse(stat.isSymbolicLink());
      assert.deepEqual({ dev: stat.dev, ino: stat.ino }, bridgeIdentity);
      NodeFS.rmSync(bridgeDirectory!, { recursive: true });
    }
    NodeFS.rmSync(root, { recursive: true, force: true });
  }
  const passed = Exit.isSuccess(outcome) && operationFinished && physicallyClosed && scopeJoined && observersJoined;
  const evidence = NodeProcess.env.T3_PITAKO_NATIVE_EVIDENCE;
  if (evidence) NodeFS.writeFileSync(NodePath.join(evidence, `manager-${providerSessionId}-${NodePath.basename(root)}.json`), JSON.stringify({
    passed, pending, root, environment, config: '[model_policies.researcher.primary]\nmodel = "t3-worker/controlled"\nreasoning = "off"\n',
    runtime: { executable: NodeProcess.execPath, version: NodeProcess.version, cli, pitako }, wrappedArgs,
    rounds, journal, processes, operationFinished, scopeJoined, physicallyClosed, observersJoined, bridgeRetainedAfterExit, bridgeDirectory, bridgeIdentity, watchersJoined: publications.closed.size === 0,
    rootRemoved: !NodeFS.existsSync(root), bridgeRemoved: bridgeDirectory !== undefined && !NodeFS.existsSync(bridgeDirectory),
    bridgeOwnerReleased: workers?.owner === undefined, outcome: String(outcome),
  }, null, 2));
  assert.isTrue(physicallyClosed);
  assert.isTrue(observersJoined);
  assert.isUndefined(workers?.owner);
  assert.isDefined(bridgeDirectory);
  assert.isFalse(NodeFS.existsSync(bridgeDirectory!));
  assert.isFalse(NodeFS.existsSync(root));
  yield* outcome;
  assert.isTrue(passed);
});
