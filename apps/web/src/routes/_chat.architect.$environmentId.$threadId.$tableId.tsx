import { useAtomValue } from "@effect/atom-react";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { ArchitectTable } from "./-piArchitecture/ArchitectTable";
import { piArchitecture } from "./-piArchitecture/state";
import { ThreadRouteView } from "../components/ThreadRouteView";
import { Button } from "../components/ui/button";
import { cn } from "../lib/utils";
import { COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS } from "../workspaceTitlebar";

export const Route = createFileRoute("/_chat/architect/$environmentId/$threadId/$tableId")({
  validateSearch: (search: Record<string, unknown>): { participant?: string } =>
    typeof search.participant === "string" ? { participant: search.participant } : {},
  component: ArchitectTableRoute,
});

function ArchitectTableRoute() {
  const params = Route.useParams();
  const { participant } = Route.useSearch();
  const target = {
    environmentId: EnvironmentId.make(params.environmentId),
    threadId: ThreadId.make(params.threadId),
    tableId: params.tableId,
  };
  const navigate = useNavigate();
  const result = useAtomValue(
    piArchitecture.query({
      environmentId: target.environmentId,
      input: { kind: "records", threadId: target.threadId, tableId: target.tableId },
    }),
  );
  const data = Option.getOrNull(AsyncResult.value(result));
  const member =
    data?.kind === "records"
      ? data.items.find((record) => record.kind === "member" && record.id === participant)
      : undefined;
  if (participant !== undefined)
    return (
      <section
        className="flex h-full min-h-0 flex-1 flex-col"
        aria-label="Participant conversation"
      >
        <div className={cn("shrink-0 border-b p-2", COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS)}>
          <Button
            variant="outline"
            size="sm"
            autoFocus
            onClick={() => {
              void navigate({ to: Route.fullPath, params, search: {} });
            }}
          >
            Back to Architect table
          </Button>
        </div>
        {member?.kind === "member" && member.workerThreadId && member.bindingCount > 0 ? (
          <ThreadRouteView
            target={{
              kind: "server",
              threadRef: scopeThreadRef(target.environmentId, member.workerThreadId),
            }}
          />
        ) : (
          <p role="status">Participant conversation unavailable.</p>
        )}
      </section>
    );
  return (
    <ArchitectTable
      target={target}
      onCoordinator={() => {
        void navigate({ to: "/$environmentId/$threadId", params: target });
      }}
      onParticipant={(memberId) => {
        void navigate({ to: Route.fullPath, params, search: { participant: memberId } });
      }}
    />
  );
}
