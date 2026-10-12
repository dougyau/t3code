import { createProviderWorkersEnvironmentAtoms } from "@t3tools/client-runtime/state/provider-workers";
import * as Crypto from "expo-crypto";
import { connectionAtomRuntime } from "../connection/runtime";

export const providerWorkers = createProviderWorkersEnvironmentAtoms(connectionAtomRuntime, () =>
  Crypto.randomUUID(),
);
