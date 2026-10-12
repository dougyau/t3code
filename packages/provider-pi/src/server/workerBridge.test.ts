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

const decodeSettings = Schema.decodeUnknownSync(PiSettings);

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
