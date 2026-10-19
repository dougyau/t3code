import {
  createPiArchitectureAtoms,
  type PiArchitectureAtoms,
} from "@t3tools/client-runtime/state/piArchitecture/queries";
import { connectionAtomRuntime } from "../../connection/runtime";
import { environmentThreadDetails } from "../../state/threads";

export const piArchitecture: PiArchitectureAtoms = createPiArchitectureAtoms(
  connectionAtomRuntime,
  (target) =>
    environmentThreadDetails.threadAtom({
      environmentId: target.environmentId,
      threadId: target.input.threadId,
    }),
);
