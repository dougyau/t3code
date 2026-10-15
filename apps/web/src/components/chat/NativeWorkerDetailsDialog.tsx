import { nativeWorkerBriefPresentation } from "@t3tools/client-runtime/state/subagent-display";
import type { PiNativeWorkerSummary } from "@t3tools/contracts";
import { useState } from "react";

import { Button } from "../ui/button";
import {
  Dialog,
  DialogTrigger,
  DialogPopup,
  DialogHeader,
  DialogTitle,
  DialogPanel,
} from "../ui/dialog";
import { ToggleGroup, Toggle } from "../ui/toggle-group";

/** Reads the current summary, never a copy captured when the reader opened. */
export function NativeWorkerDetailsDialog({
  worker,
}: {
  readonly worker: PiNativeWorkerSummary | undefined;
}) {
  const [open, setOpen] = useState(false);
  const [section, setSection] = useState("brief");
  const brief = worker ? nativeWorkerBriefPresentation(worker) : null;
  const observations = worker
    ? [
        ["Execution", worker.execution ?? "unknown"],
        ["Capture", worker.capture],
        ["Availability", worker.availability],
        ["Result", worker.result ?? "Not reported"],
        ["Launch source", worker.launch.source],
        ["Instance ID", worker.launch.instanceId],
        ...(worker.launch.source === "team_assign"
          ? [["Assignment ID", worker.launch.assignmentId]]
          : []),
        ["Requested model", worker.selected?.requestedModel ?? "Not reported"],
        ["Selected model", worker.selected?.model ?? "Not reported"],
        ["Selected reasoning", worker.selected?.reasoning ?? "Not reported"],
        ["Activated model", worker.activated?.model ?? "Not reported"],
        ["Activated reasoning", worker.activated?.reasoning ?? "Not reported"],
      ]
    : [];

  return (
    <Dialog
      open={open}
      onOpenChange={(nextOpen) => {
        setOpen(nextOpen);
        if (nextOpen) setSection("brief");
      }}
    >
      {worker ? (
        <DialogTrigger render={<Button size="sm" variant="ghost" />}>Assignment</DialogTrigger>
      ) : null}
      <DialogPopup>
        <DialogHeader>
          <DialogTitle>Assignment</DialogTitle>
          <ToggleGroup
            aria-label="Assignment sections"
            value={[section]}
            onValueChange={(value) => {
              if (value[0]) setSection(value[0]);
            }}
          >
            <Toggle value="brief">Original brief</Toggle>
            <Toggle value="observation">Observation</Toggle>
          </ToggleGroup>
        </DialogHeader>
        <DialogPanel>
          <section className="min-w-0 max-w-prose space-y-4 text-sm text-foreground">
            <h2 className="font-semibold">
              {section === "brief" ? "Original brief" : "Observation"}
            </h2>
            {!worker ? (
              <p>Worker details unavailable</p>
            ) : section === "brief" ? (
              !brief || brief.state === "missing" ? (
                <p>Brief unavailable</p>
              ) : (
                <>
                  <p>
                    {brief.completeness === "partial"
                      ? "Partial brief — only the first page is available."
                      : brief.completeness === "unknown"
                        ? "Brief completeness not reported."
                        : "Complete brief"}
                  </p>
                  <div className="select-text whitespace-pre-wrap wrap-anywhere leading-relaxed">
                    {brief.text}
                  </div>
                </>
              )
            ) : (
              <>
                <dl className="grid min-w-0 gap-3">
                  {observations.map(([label, value]) => (
                    <div key={label}>
                      <dt className="font-medium">{label}</dt>
                      <dd className="select-text whitespace-pre-wrap wrap-anywhere">{value}</dd>
                    </div>
                  ))}
                </dl>
                <div className="space-y-2">
                  <h3 className="font-medium">Observation gaps</h3>
                  {worker.gaps.length > 0 ? (
                    <ul className="list-inside list-disc">
                      {worker.gaps.map((gap) => (
                        <li key={gap}>{gap}</li>
                      ))}
                    </ul>
                  ) : (
                    <p>No gaps reported.</p>
                  )}
                  <p>
                    No reported gaps does not establish complete capture. Gaps do not determine
                    execution status or identify the location, amount, or content of missing data.
                  </p>
                </div>
              </>
            )}
          </section>
        </DialogPanel>
      </DialogPopup>
    </Dialog>
  );
}
