import { tableObservation, tableStatus } from "@t3tools/client-runtime/state/piArchitecture/model";
import type { PiArchitectureSummary } from "@t3tools/contracts";
import { useState } from "react";
import { Badge } from "../../components/ui/badge";

export function TableStatus({
  summary,
  compact = false,
}: {
  readonly summary: PiArchitectureSummary;
  readonly compact?: boolean;
}) {
  const [technical, setTechnical] = useState(false);
  const status = tableStatus(summary);
  return (
    <div
      className={`flex min-w-0 flex-col [overflow-wrap:anywhere] ${compact ? "gap-1 text-xs" : "gap-2 text-sm"}`}
    >
      <div className="flex flex-wrap items-center gap-1.5">
        <Badge
          size={compact ? "sm" : "default"}
          variant={
            status.state === "Finished"
              ? "success"
              : status.state === "Failed" ||
                  status.state === "Cancelled" ||
                  status.state === "Completion unconfirmed"
                ? "warning"
                : summary.observation === "current"
                  ? "info"
                  : "outline"
          }
        >
          {status.state}
        </Badge>
        <Badge
          size={compact ? "sm" : "default"}
          variant={summary.lifecycle.outcome === "available" ? "success" : "outline"}
        >
          {status.result}
        </Badge>
        {status.context ? (
          <Badge
            size={compact ? "sm" : "default"}
            variant={summary.observation === "partial" ? "warning" : "secondary"}
          >
            {status.context}
          </Badge>
        ) : null}
      </div>
      {status.warnings.map((warning) => (
        <p key={warning} role="status">
          {warning}
        </p>
      ))}
      {compact ? null : (
        <details
          open={technical}
          onToggle={(event) => setTechnical(event.currentTarget.open)}
          className="text-xs text-muted-foreground"
        >
          <summary className="cursor-pointer py-1">Technical details</summary>
          {technical ? (
            <>
              <p className="mt-1">Table ID: {summary.tableId}</p>
              <p className="mt-1">{tableObservation(summary)}</p>
            </>
          ) : null}
        </details>
      )}
    </div>
  );
}
