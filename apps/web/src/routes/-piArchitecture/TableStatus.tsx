import { tableObservation, tableStatus } from "@t3tools/client-runtime/state/piArchitecture/model";
import type { PiArchitectureSummary } from "@t3tools/contracts";
import { useState } from "react";

export function TableStatus({ summary }: { readonly summary: PiArchitectureSummary }) {
  const [technical, setTechnical] = useState(false);
  const status = tableStatus(summary);
  return (
    <div className="flex min-w-0 flex-col gap-2 text-sm [overflow-wrap:anywhere]">
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
        <span className="font-semibold">{status.state}</span>
        <span>{status.result}</span>
        {status.context ? <span className="text-muted-foreground">{status.context}</span> : null}
      </div>
      {status.warnings.map((warning) => (
        <p key={warning} role="status">
          {warning}
        </p>
      ))}
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
    </div>
  );
}
