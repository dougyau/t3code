import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { nativeIntegration } from "./workerBridge.native.fixture.ts";
const source = process.env.T3_PITAKO_NATIVE_TEST_ROOT;
describe.skipIf(source === undefined)("native Pitako bridge through the real T3 adapter", () => {
  it.live("reads owner-global pending through the adapter without committed worker members", () =>
    nativeIntegration(false, (fixture) =>
      Effect.gen(function* () {
        yield* Effect.promise(() => fixture.wait("coordinator-waiting.json"));
        const before = yield* Effect.promise(() => fixture.snapshot());
        assert.deepEqual(yield* fixture.workers.readBackgroundPending(), { state: "pending" });
        assert.isFalse(yield* fixture.workers.hasPendingWork([]));
        assert.deepEqual(yield* Effect.promise(() => fixture.snapshot()), before);
        assert.isTrue(fixture.client.capabilities.includes("background.pending"));
      }),
    ),
  );
  it.live("observes idle background and Team work without model calls or consuming outcomes", () =>
    nativeIntegration(false),
  );
  it.live("keeps chat usable when the enabled bridge extension is missing", () =>
    nativeIntegration(true),
  );
});
