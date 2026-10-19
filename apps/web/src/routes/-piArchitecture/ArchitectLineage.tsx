import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, PiArchitectureSummary } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { useState } from "react";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { retainTimelinePosition } from "../../components/chat/timelineScrollAnchoring";
import { Button } from "../../components/ui/button";
import {
  participantExecution,
  participantLabel,
} from "@t3tools/client-runtime/state/piArchitecture/model";
import { piArchitecture } from "./state";
import { TableStatus } from "./TableStatus";

export function ArchitectLineage(props: {
  readonly environmentId: EnvironmentId;
  readonly tables: readonly PiArchitectureSummary[];
}) {
  return props.tables.map((table) => (
    <TableEntry key={table.tableId} environmentId={props.environmentId} table={table} />
  ));
}

function TableEntry({
  environmentId,
  table,
}: {
  readonly environmentId: EnvironmentId;
  readonly table: PiArchitectureSummary;
}) {
  const navigate = useNavigate();
  const [expanded, setExpanded] = useState(false);
  const canRead = useAtomValue(piArchitecture.read.permissionAtom(environmentId));
  return (
    <section className="min-w-0 rounded-lg border p-4 [overflow-wrap:anywhere]">
      <div className="flex min-w-0 flex-col gap-3">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <h3 className="text-sm font-semibold">Architect table</h3>
          <span className="text-xs text-muted-foreground">{table.participants} participants</span>
        </div>
        <TableStatus summary={table} />
        <div>
          <Button
            variant="default"
            size="sm"
            disabled={!canRead}
            onClick={() => {
              retainTimelinePosition(
                scopedThreadKey(scopeThreadRef(environmentId, table.threadId)),
              );
              void navigate({
                to: "/architect/$environmentId/$threadId/$tableId",
                params: { environmentId, threadId: table.threadId, tableId: table.tableId },
              });
            }}
          >
            Open Architect table
          </Button>
        </div>
        {!canRead ? <p className="text-xs">Requires orchestration read permission.</p> : null}
        <details onToggle={(event) => setExpanded(event.currentTarget.open)}>
          <summary className="cursor-pointer text-sm font-medium">Participants</summary>
          {expanded && canRead ? <TableRoster environmentId={environmentId} table={table} /> : null}
        </details>
      </div>
    </section>
  );
}

function TableRoster({
  environmentId,
  table,
}: {
  readonly environmentId: EnvironmentId;
  readonly table: PiArchitectureSummary;
}) {
  const result = useAtomValue(
    piArchitecture.query({
      environmentId,
      input: { kind: "records", threadId: table.threadId, tableId: table.tableId },
    }),
  );
  const data = Option.getOrNull(AsyncResult.value(result));
  return (
    <ul className="mt-3 flex min-w-0 flex-col gap-3 text-sm">
      {data?.kind === "records" ? (
        data.items
          .filter((record) => record.kind === "member")
          .map((member) => (
            <li key={member.id} className="min-w-0">
              <p className="font-medium">{participantLabel(member)}</p>
              <p className="text-xs text-muted-foreground">{participantExecution(member)}</p>
              <details className="mt-1 text-xs text-muted-foreground">
                <summary className="cursor-pointer">Technical identifiers</summary>
                <p>Member ID: {member.id}</p>
                <p>Assignment ID: {member.assignmentId}</p>
              </details>
            </li>
          ))
      ) : (
        <li>{result._tag === "Failure" ? "Roster unavailable" : "Loading roster…"}</li>
      )}
    </ul>
  );
}
