import { it } from "@effect/vitest";
import * as NodeProcess from "node:process";
import { nativeResidency } from "./ProviderSessionManager.native.fixture.ts";

const native = NodeProcess.env.T3_PITAKO_NATIVE_TEST_ROOT ? it.effect : it.effect.skip;
native("keeps real Pi background resident for ten completed idle decisions, then detaches", () => nativeResidency(true), 120_000);
native("releases a separate real Pi session on initial native none", () => nativeResidency(false), 120_000);
