// @effect-diagnostics nodeBuiltinImport:off -- Private native directory ownership is the boundary under test.
import * as NodeFS from "node:fs";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { PiSettings } from "../settings.ts";
import { PiWorkerBridge, WorkerInspectionError, classifyPiWorkerFailure } from "./workerBridge.ts";
import { BridgeError } from "./pitako/bridge/client.js";
import { errors } from "./pitako/bridge/protocol.js";
import { workerWireFixture } from "./workerBridge.wire.fixture.ts";
import { handoffWorkerRows, invalidHandoffWorkerRows, legacyWorkerRow } from "../../../contracts/src/providerWorkers.fixture.ts";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";

const decodeSettings = Schema.decodeUnknownSync(PiSettings);

describe("passive worker handoff wire boundary", () => {
  const owner = { processId: "process", coordinatorSessionId: "session", epoch: "epoch" };
  it.effect("conserves every public combination through real list and subscription replies", () =>
    Effect.gen(function* () {
      const f = yield* workerWireFixture(owner);
      for (const row of handoffWorkerRows) {
        const roster = { owner, revision: 1, cursor: null, items: [row] };
        f.policy.roster = roster;
        assert.deepEqual(yield* Effect.promise(() => f.client.list()), roster);
        const changed = Promise.withResolvers<unknown>();
        const subscription = yield* Effect.promise(() => f.client.subscribe(changed.resolve));
        assert.deepEqual(subscription.snapshot, roster);
        f.policy.roster = { ...roster, revision: 2 };
        f.notify(2);
        assert.deepEqual(yield* Effect.promise(() => changed.promise), {
          version: 1, method: "changed", owner, clientId: f.client.lease.clientId,
          subscriptionId: "subscription", revision: 2, refreshRequired: true, gap: false,
        });
        assert.deepEqual(yield* Effect.promise(() => subscription.refresh()), f.policy.roster);
        yield* Effect.promise(() => subscription.dispose());
      }
      assert.deepEqual(f.calls, ["connect", ...handoffWorkerRows.flatMap(() => [
        "workers.list", "subscribe", "workers.list", "unsubscribe",
      ])]);
      yield* Effect.promise(() => f.client.disconnect());
    }).pipe(Effect.scoped));

  it.effect("returns observable transport errors for invalid list and subscription payloads", () =>
    Effect.gen(function* () {
      const f = yield* workerWireFixture(owner);
      const badPages = [
        ...invalidHandoffWorkerRows.map((row) => ({ owner, revision: 1, cursor: null, items: [row] })),
        { owner, revision: 1, cursor: null, items: [{ ...legacyWorkerRow, futureField: true }] },
        { owner, revision: 1, cursor: null, items: [{ ...handoffWorkerRows[3], handoff: {
          collection: "collected", bookkeeping: "pending", delivery: "offered", continuation: "requested", futureField: true,
        } }] },
        { owner, revision: 1, cursor: null, items: Array.from({ length: 51 }, () => handoffWorkerRows[3]) },
        { owner: { ...owner, epoch: "foreign" }, revision: 1, cursor: null, items: [legacyWorkerRow] },
      ];
      for (const page of badPages) {
        for (const method of ["list", "subscribe"] as const) {
          const client = yield* Effect.promise(() => f.open());
          f.policy.roster = page;
          const error = yield* Effect.tryPromise(async () => method === "list"
            ? client.list() : client.subscribe(() => assert.fail("Invalid subscription must not notify"))).pipe(Effect.flip);
          assert.deepEqual(classifyPiWorkerFailure(error.cause), { kind: "transport", code: "invalid_reply" });
          yield* Effect.promise(() => client.disconnect());
        }
      }
      assert.isTrue(f.calls.every((call) => ["connect", "workers.list", "subscribe"].includes(call)));
    }).pipe(Effect.scoped));

  it.effect("keeps bounded pagination revisions and stale cursor errors on the passive client", () =>
    Effect.gen(function* () {
      const f = yield* workerWireFixture(owner);
      const first = { owner, revision: 7, cursor: "next-page", items: handoffWorkerRows.slice(0, 50) };
      f.policy.roster = first;
      assert.deepEqual(yield* Effect.promise(() => f.client.list()), first);
      const second = { ...first, cursor: null, items: handoffWorkerRows.slice(50) };
      f.policy.roster = second;
      assert.deepEqual(yield* Effect.promise(() => f.client.list(first.cursor, first.revision)), second);
      assert.deepEqual(f.requests.at(-1)?.params, { after: "next-page", revision: 7 });
      f.policy.pageError = "stale_cursor";
      const error = yield* Effect.tryPromise(() => f.client.list(first.cursor, first.revision)).pipe(Effect.flip);
      assert.deepEqual(classifyPiWorkerFailure(error.cause), { kind: "coded", code: "stale_cursor" });
    }).pipe(Effect.scoped));

  it.effect("publishes native revision changes without treating blocked handoff as physical pending", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge(decodeSettings({ workerBridge: true }), "full-access", "linux");
      try {
        if (bridge.directory === undefined) throw new Error("Expected private worker bridge directory");
        const f = yield* workerWireFixture(owner, bridge.directory);
        const row = { ...legacyWorkerRow, resultTaken: false, handoff: {
          collection: "collected", bookkeeping: "blocked", delivery: "offered", continuation: "blocked-unconfirmed",
        } };
        const ref = { kind: "read", ...owner, instanceId: "worker", source: "agent_spawn", attemptId: "attempt",
          native: { generation: 1, historyId: "history", sessionId: "child" } };
        const workbrief = { text: "passive inspection", offset: 0, nextOffset: null, source: "invocation task" };
        f.policy.roster = { owner, revision: 1, cursor: null, items: [row] };
        f.policy.detail = { state: "ready", ref, observedStatus: "completed", status: "completed",
          result: "ready", activity: "observed_binding", workbrief,
          metadata: { text: "{}", offset: 0, nextOffset: null }, nextMetadata: null, nextBrief: null,
          controls: { grant: "denied" } };
        f.policy.activity = { ref, state: "ready", page: {
          historyId: "history", sessionId: "child", active: false,
          native: { historyId: "history", sessionId: "child", leafId: null, persisted: true },
          workbrief, activity: [], interactions: [], gaps: [], cursor: 0, more: false,
        }, next: { ref, section: "activity", after: 0 } };
        yield* bridge.bind(owner.coordinatorSessionId);
        assert.equal(bridge.observation.state, "last_observation");
        assert.isFalse(bridge.pending);
        const listening = yield* Deferred.make<void>();
        const updated = yield* Deferred.make<void>();
        const listener = yield* bridge.observations.pipe(Stream.runForEach((observation) =>
          observation.state === "last_observation" && observation.revision === 2
            ? Deferred.succeed(updated, undefined) : Deferred.succeed(listening, undefined),
        ), Effect.forkScoped);
        yield* Deferred.await(listening);
        f.policy.roster = { owner, revision: 2, cursor: null, items: [{ ...row, resultTaken: true }] };
        f.notify(2);
        yield* Deferred.await(updated);
        assert.isFalse(bridge.pending);
        assert.deepEqual(yield* bridge.read((client) => client.list()), f.policy.roster);
        assert.deepEqual(f.calls, ["connect", "connect", "subscribe",
          "workers.list", "workers.detail", "workers.activity",
          "workers.list", "workers.detail", "workers.activity", "workers.list"]);
        yield* Fiber.interrupt(listener);
        yield* bridge.invalidate;
        assert.equal(bridge.observation.state, "stale_owner");
        assert.isTrue(bridge.pending);
        const error = yield* bridge.read((client) => client.list()).pipe(Effect.flip);
        assert.equal(error.code, "bridge_unavailable");
      } finally {
        yield* bridge.close;
        bridge.cleanupDirectory(true);
      }
    }).pipe(Effect.scoped));
});

describe("Pi worker failure provenance", () => {
  it("classifies the authoritative native vocabulary, not arbitrary code-shaped errors", () => {
    for (const code of Object.keys(errors)) {
      const request = classifyPiWorkerFailure(new BridgeError(code, "native"));
      assert.isTrue(request.kind === "coded" && request.code === code);
      const acquisition = classifyPiWorkerFailure(new WorkerInspectionError({ code }), "acquisition");
      assert.isTrue(acquisition.kind === "coded" && acquisition.code === code);
      assert.deepEqual(classifyPiWorkerFailure(new WorkerInspectionError({ code })), { kind: "unclassified" });
      assert.deepEqual(classifyPiWorkerFailure({ code }), { kind: "unclassified" });
      assert.deepEqual(classifyPiWorkerFailure({ _tag: "WorkerInspectionError", code }, "acquisition"), { kind: "unclassified" });
    }
    for (const code of ["invalid_reply", "disconnected"] as const)
      assert.deepEqual(classifyPiWorkerFailure(new BridgeError(code, "transport")), { kind: "transport", code });
    for (const cause of [new BridgeError("new_native_code", "unknown"), new Error("ordinary"),
      new WorkerInspectionError({ code: "inspection_failed" })])
      assert.deepEqual(classifyPiWorkerFailure(cause, "acquisition"), { kind: "unclassified" });
  });
  it.effect("observes real passive-client local rejection before write, and framing transport failure", () =>
    Effect.gen(function* () {
      const owner = { processId: "process", coordinatorSessionId: "session", epoch: "epoch" };
      const f = yield* workerWireFixture(owner);
      const selection = { ...owner, instanceId: "worker", generation: 1, historyId: "history", sessionId: "child",
        target: { kind: "background" as const, instanceId: "worker" } };
      for (const text of ["界".repeat(6000), "   "]) {
        const error = yield* Effect.tryPromise(() => f.client.input(selection, { intent: "query", text }, 8)).pipe(Effect.flip);
        assert.deepEqual(classifyPiWorkerFailure(error.cause), { kind: "coded", code: "invalid_request" });
      }
      assert.deepEqual(f.calls, ["connect"]);
      assert.deepEqual(yield* Effect.promise(() => f.client.requestStatus(8)), { state: "not_admitted", sequence: 8 });
      assert.equal(f.sockets.size, 1);
      f.policy.malformed = true;
      const error = yield* Effect.tryPromise(() => f.client.requestStatus(8)).pipe(Effect.flip);
      assert.deepEqual(classifyPiWorkerFailure(error.cause), { kind: "transport", code: "invalid_reply" });
      yield* Effect.promise(() => f.client.disconnect());
    }).pipe(Effect.scoped));
});

describe("Pi worker launch ownership", () => {
  it.effect("keeps a flag-free replacement unavailable across all inspection and retired-owner callbacks", () =>
    Effect.gen(function* () {
      const rejected = new PiWorkerBridge(
        decodeSettings({ workerBridge: true, workerControl: true }), "full-access", "linux",
      );
      const directory = rejected.directory!;
      const replacement = new PiWorkerBridge(
        decodeSettings({ workerBridge: true, workerControl: true }), "approval-required", "linux", true,
      );
      try {
        assert.deepEqual(replacement.launchArgs, []);
        assert.isUndefined(replacement.directory);
        yield* replacement.bind("requested-winner");
        for (const operation of [
          replacement.refresh,
          replacement.read(async () => assert.fail("No native client may be called")),
          Effect.scoped(replacement.openView()),
        ]) {
          const error = yield* operation.pipe(Effect.flip);
          assert.equal(error.code, "bridge_launch_failed");
        }
        // Late retirement of the first port has no authority over the winner.
        yield* rejected.invalidate;
        rejected.cleanupDirectory(false);
        yield* replacement.invalidate;
        yield* replacement.bind("late-callback");
        yield* replacement.close;
        replacement.cleanupDirectory(true);
        assert.deepEqual(replacement.observation, { state: "error", code: "bridge_launch_failed" });
        assert.isTrue(replacement.pending);
        assert.isTrue(NodeFS.existsSync(directory));
      } finally {
        rejected.cleanupDirectory(true);
      }
    }),
  );
  it.effect("keeps the disabled launch unchanged, including inspection and idle probe", () =>
    Effect.gen(function* () {
      const settings = decodeSettings({ enabled: true });
      assert.isFalse(settings.workerBridge);
      assert.isFalse(settings.workerHistory);
      assert.isFalse(settings.workerRoute);
      assert.isFalse(settings.workerControl);
      const bridge = new PiWorkerBridge(settings, "approval-required", "linux");
      assert.isUndefined(bridge.directory);
      assert.deepEqual(bridge.launchArgs, []);
      yield* bridge.bind("session");
      yield* bridge.refresh;
      yield* bridge.close;
      assert.equal(bridge.observation.state, "disabled");
      assert.isFalse(bridge.pending);
    }),
  );
  it.effect("captures independent launch ceilings in an empty canonical private directory", () =>
    Effect.gen(function* () {
      for (const grant of ["workerHistory", "workerRoute", "workerControl"] as const) {
        const settings = decodeSettings({ workerBridge: true, [grant]: true });
        const bridge = new PiWorkerBridge(settings, "full-access", "linux");
        const directory = bridge.directory!;
        try {
          assert.equal(NodeFS.realpathSync(directory), directory);
          assert.equal(NodeFS.statSync(directory).mode & 0o777, 0o700);
          assert.deepEqual(NodeFS.readdirSync(directory), []);
          assert.isAtMost(Buffer.byteLength(`${directory}/b-000000000000.sock`), 103);
          assert.deepEqual(bridge.launchArgs, [
            `--pitako-agent-bridge=${directory}`,
            `--pitako-agent-bridge-grants=${grant.slice(6).toLowerCase()}`,
          ]);
          yield* bridge.bind(undefined);
          assert.deepEqual(bridge.observation, { state: "error", code: "missing_session_id" });
          assert.isTrue(bridge.pending);
        } finally {
          bridge.cleanupDirectory(true);
        }
      }
    }),
  );
  it.effect("represents unsupported transport without passing bridge flags", () =>
    Effect.gen(function* () {
      const bridge = new PiWorkerBridge(
        decodeSettings({ workerBridge: true }),
        "full-access",
        "win32",
      );
      yield* bridge.refresh;
      assert.equal(bridge.observation.state, "unsupported");
      assert.deepEqual(bridge.launchArgs, []);
      assert.isTrue(bridge.pending);
    }),
  );
  it.effect("retains diagnostic directories on uncertain exit", () =>
    Effect.sync(() => {
      const bridge = new PiWorkerBridge(
        decodeSettings({ workerBridge: true }),
        "full-access",
        "linux",
      );
      const directory = bridge.directory!;
      bridge.cleanupDirectory(false);
      assert.isTrue(NodeFS.existsSync(directory));
      NodeFS.writeFileSync(`${directory}/diagnostic`, "uncertain native state");
      bridge.cleanupDirectory(true);
      assert.isTrue(NodeFS.existsSync(`${directory}/diagnostic`));
      NodeFS.unlinkSync(`${directory}/diagnostic`);
      bridge.cleanupDirectory(true);
      assert.isFalse(NodeFS.existsSync(directory));
    }),
  );
});
