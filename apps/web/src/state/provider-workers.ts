import { createProviderWorkersEnvironmentAtoms } from "@t3tools/client-runtime/state/provider-workers";
import { connectionAtomRuntime } from "../connection/runtime";
import { randomUUID } from "../lib/utils";

export const providerWorkers = createProviderWorkersEnvironmentAtoms(connectionAtomRuntime, randomUUID);
