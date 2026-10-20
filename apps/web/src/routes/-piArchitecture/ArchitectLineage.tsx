import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, PiArchitectureSummary } from "@t3tools/contracts";
import { useNavigate } from "@tanstack/react-router";
import { useId, useState } from "react";
import { ChevronDownIcon, PanelsTopLeftIcon } from "lucide-react";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { retainTimelinePosition } from "../../components/chat/timelineScrollAnchoring";
import { ThreadDetailsControl } from "../../components/chat/ThreadDetailsControl";
import {
  THREAD_DETAILS_PANEL_ICON_CLASS,
  THREAD_DETAILS_PANEL_SPLIT_GROUP_CLASS,
  THREAD_DETAILS_PANEL_SPLIT_SEPARATOR_CLASS,
} from "../../components/chat/threadDetailsPanelStyles";
import { cn } from "../../lib/utils";
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
  const rosterId = useId();
  const canRead = useAtomValue(piArchitecture.read.permissionAtom(environmentId));
  return (
    <section className="min-w-0 [overflow-wrap:anywhere]">
      <div className={THREAD_DETAILS_PANEL_SPLIT_GROUP_CLASS}>
        <ThreadDetailsControl
          part="primary"
          multiline
          aria-label="Open Architect table"
          disabled={!canRead}
          onClick={() => {
            retainTimelinePosition(scopedThreadKey(scopeThreadRef(environmentId, table.threadId)));
            void navigate({
              to: "/architect/$environmentId/$threadId/$tableId",
              params: { environmentId, threadId: table.threadId, tableId: table.tableId },
            });
          }}
        >
          <PanelsTopLeftIcon aria-hidden className={THREAD_DETAILS_PANEL_ICON_CLASS} />
          <span className="min-w-0 flex-1">
            <span className="block truncate text-sm font-medium">Architect table</span>
            <span className="block text-2xs font-normal text-muted-foreground">
              {table.participants} {table.participants === 1 ? "participant" : "participants"}
              {table.lifecycle.round === undefined ? null : ` · Round ${table.lifecycle.round}`}
            </span>
          </span>
        </ThreadDetailsControl>
        <span aria-hidden className={THREAD_DETAILS_PANEL_SPLIT_SEPARATOR_CLASS} />
        <ThreadDetailsControl
          part="secondary"
          aria-label={`${expanded ? "Collapse" : "Expand"} Architect participants`}
          aria-expanded={expanded}
          aria-controls={rosterId}
          disabled={!canRead}
          onClick={() => setExpanded((value) => !value)}
        >
          <ChevronDownIcon
            aria-hidden
            className={cn("size-3.5 text-muted-foreground", expanded && "rotate-180")}
          />
        </ThreadDetailsControl>
      </div>
      <div className="px-2.5 pb-1">
        <TableStatus summary={table} compact />
        {!canRead ? (
          <p className="mt-1 text-xs text-muted-foreground">
            Requires orchestration read permission.
          </p>
        ) : null}
      </div>
      <div id={rosterId}>
        {expanded && canRead ? <TableRoster environmentId={environmentId} table={table} /> : null}
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
    <ul className="flex min-w-0 list-none flex-col gap-2 py-1 ps-9 pe-2.5 text-sm">
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
