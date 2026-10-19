import {
  ORCHESTRATION_V2_WS_METHODS,
  type PiArchitectureDetailRecord,
  type PiArchitectureReadInput,
  type PiArchitectureReadResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Data from "effect/Data";
import { Atom, AtomRegistry } from "effect/reactivity";
import type { EnvironmentRegistry } from "../../connection/registry.ts";
import { createEnvironmentQueryAtomFamily, createEnvironmentRpcCommand } from "../runtime.ts";
import type { TableTarget } from "./model.ts";

export type ArchitectureQueryTarget = {
  readonly environmentId: TableTarget["environmentId"];
  readonly input: PiArchitectureReadInput;
  readonly captureKey?: string;
};
export type CapturedText = Extract<PiArchitectureReadResult, { kind: "text" }>;
class ArchitecturePageError extends Data.TaggedError("ArchitecturePageError")<{
  readonly message: string;
}> {}

export function createPiArchitectureAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
  refreshTrigger?: (target: ArchitectureQueryTarget) => Atom.Atom<unknown>,
) {
  const read = createEnvironmentRpcCommand(runtime, {
    label: "pi-architecture:read",
    tag: ORCHESTRATION_V2_WS_METHODS.getPiArchitecture,
  });
  const query = createEnvironmentQueryAtomFamily(runtime, {
    label: "pi-architecture:query",
    staleTimeMs: 30_000,
    refreshTrigger: ({ input }) =>
      input.input.kind !== "text" || input.input.textId.startsWith("control:")
        ? refreshTrigger?.(input)
        : undefined,
    execute: (target: ArchitectureQueryTarget) =>
      Effect.gen(function* () {
        const registry = yield* AtomRegistry.AtomRegistry;
        // The guarded command retains both origin and destination authorization.
        const fetch = (input: PiArchitectureReadInput) =>
          Effect.promise(() =>
            read.run(registry, { environmentId: target.environmentId, input }),
          ).pipe(
            Effect.flatMap((result) =>
              result._tag === "Success"
                ? Effect.succeed(result.value)
                : Effect.failCause(result.cause),
            ),
          );
        const initial = yield* fetch(target.input);
        if (initial.kind === "text") {
          let page: CapturedText = initial;
          const first = initial;
          if (target.input.kind !== "text" || first.offset !== (target.input.offset ?? 0)) {
            return yield* Effect.fail(
              new ArchitecturePageError({
                message: "Architecture text page gap or stale revision",
              }),
            );
          }
          let text = page.text;
          while (page.nextOffset !== null) {
            if (target.input.kind !== "text") return yield* Effect.die("Unexpected text page");
            const next = yield* fetch({
              ...target.input,
              offset: page.nextOffset,
              revision: first.revision,
            });
            if (
              next.kind !== "text" ||
              next.textId !== first.textId ||
              next.revision !== first.revision ||
              next.offset !== first.offset + text.length ||
              (next.nextOffset !== null && next.nextOffset <= next.offset)
            ) {
              return yield* Effect.fail(
                new ArchitecturePageError({
                  message: "Architecture text page gap or stale revision",
                }),
              );
            }
            text += next.text;
            page = next;
          }
          return { ...first, text, nextOffset: null };
        }
        if (initial.kind === "tables") {
          let page: Extract<PiArchitectureReadResult, { kind: "tables" }> = initial;
          const items = [...page.items];
          const seen = new Set<string>();
          while (page.next !== null) {
            if (seen.has(page.next))
              return yield* Effect.fail(
                new ArchitecturePageError({ message: "Architecture cursor repeated" }),
              );
            seen.add(page.next);
            const next = yield* fetch({
              kind: "tables",
              threadId: target.input.threadId,
              cursor: page.next,
            });
            if (next.kind !== "tables")
              return yield* Effect.fail(
                new ArchitecturePageError({ message: "Architecture page mismatch" }),
              );
            items.push(...next.items);
            page = next;
          }
          return { ...page, items };
        }
        let page: Extract<PiArchitectureReadResult, { kind: "records" }> = initial;
        const items: PiArchitectureDetailRecord[] = [...page.items];
        const seen = new Set<string>();
        while (page.next !== null) {
          if (target.input.kind !== "records") return yield* Effect.die("Unexpected records page");
          if (seen.has(page.next))
            return yield* Effect.fail(
              new ArchitecturePageError({ message: "Architecture cursor repeated" }),
            );
          seen.add(page.next);
          const next = yield* fetch({ ...target.input, cursor: page.next });
          if (next.kind !== "records")
            return yield* Effect.fail(
              new ArchitecturePageError({ message: "Architecture page mismatch" }),
            );
          items.push(...next.items);
          page = next;
        }
        return { ...page, items };
      }),
  });
  return {
    read,
    query: (target: ArchitectureQueryTarget) =>
      query({ environmentId: target.environmentId, input: target }),
  };
}
export type PiArchitectureAtoms = ReturnType<typeof createPiArchitectureAtoms>;
