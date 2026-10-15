import { createOrchestrationEnvironmentAtoms } from "@t3tools/client-runtime/state/orchestration";
import type { Atom } from "effect/reactivity";

import { connectionAtomRuntime } from "../connection/runtime";

type OrchestrationEnvironment = typeof connectionAtomRuntime extends Atom.AtomRuntime<infer R, infer E>
  ? ReturnType<typeof createOrchestrationEnvironmentAtoms<R, E>>
  : never;

export const orchestrationEnvironment: OrchestrationEnvironment =
  createOrchestrationEnvironmentAtoms(connectionAtomRuntime);
