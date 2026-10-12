import { describe, it } from "@effect/vitest";
import { nativeIntegration } from "./workerBridge.native.fixture.ts";
const source = process.env.T3_PITAKO_NATIVE_TEST_ROOT;
describe.skipIf(source === undefined)("native Pitako bridge through the real T3 adapter", () => {
  it.live("observes idle background and Team work without model calls or consuming outcomes", () =>
    nativeIntegration(false),
  );
  it.live("keeps chat usable when the enabled bridge extension is missing", () =>
    nativeIntegration(true),
  );
});
