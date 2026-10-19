import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, PiArchitectureSummary } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { useState } from "react";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { retainTimelinePosition } from "../../components/chat/timelineScrollAnchoring";
import { Button } from "../../components/ui/button";
import { piArchitecture } from "./state";

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
    <details
      className="min-w-0 rounded-md border p-2"
      onToggle={(event) => setExpanded(event.currentTarget.open)}
    >
      <summary className="cursor-pointer text-sm font-medium">
        Architect table · {table.participants} participants · {table.lifecycle.protocolState}
      </summary>
      <div className="mt-2 flex min-w-0 flex-col gap-2">
        <p className="break-all text-xs text-muted-foreground">{table.tableId}</p>
        <p className="text-xs">
          Capture {table.capture} · {table.observation} · Native closure{" "}
          {table.lifecycle.closure.state}
        </p>
        <div>
          <Button
            variant="outline"
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
        {expanded && canRead ? <TableRoster environmentId={environmentId} table={table} /> : null}
      </div>
    </details>
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
    <ul className="text-sm">
      {data?.kind === "records" ? (
        data.items
          .filter((record) => record.kind === "member")
          .map((member) => (
            <li key={member.id}>
              {member.roleId} · {member.id} · {member.execution}
            </li>
          ))
      ) : (
        <li>{result._tag === "Failure" ? "Roster unavailable" : "Loading roster…"}</li>
      )}
    </ul>
  );
}
