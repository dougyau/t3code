import {
  formatProviderSubagentStatus,
  type ProviderSubagentStatus,
} from "@t3tools/client-runtime/state/thread-execution";
import { isOrchestrationV2WorkActive, type PiNativeWorkerSummary } from "@t3tools/contracts";
import { ArrowUpLeftIcon } from "lucide-react";
import { useLayoutEffect, useRef, useState } from "react";

import type { ProviderInstanceEntry } from "../../providerInstances";
import { Button } from "../ui/button";
import { ProviderInstanceIcon } from "./ProviderInstanceIcon";
import { ComposerBanner } from "./ComposerBanner";
import { NativeWorkerDetailsDialog } from "./NativeWorkerDetailsDialog";
import {
  ComposerTasksContent,
  type ComposerTasksProgress,
  type ComposerTaskStep,
} from "./ComposerTasksBadge";

/**
 * Stands in for the composer on a provider-native subagent thread. The
 * provider runs that conversation, so there is nothing to send; the bar says
 * which model is working, for how long, and leads back to the parent.
 */
export function ProviderSubagentBar(props: {
  /** The provider running the subagent; no icon while its catalog loads. */
  readonly provider: ProviderInstanceEntry | null;
  /** Several accounts on this provider: badge the glyph, as the composer does. */
  readonly showInstanceBadge: boolean;
  readonly modelLabel: string;
  /** Reasoning effort as the composer names it, when the subagent has one. */
  readonly effortLabel: string | null;
  /** Null until the subagent's root turn arrives. */
  readonly status: ProviderSubagentStatus | null;
  readonly onOpenParent: (() => void) | null;
  readonly nativeWorker?: PiNativeWorkerSummary | undefined;
  readonly tasksProgress?: ComposerTasksProgress | null | undefined;
  readonly taskSteps?: readonly ComposerTaskStep[] | null | undefined;
}) {
  const [tasksExpanded, setTasksExpanded] = useState(false);
  const native = props.nativeWorker;
  const identity =
    native?.roleId ??
    (native?.launch.source === "team_assign" ? native.launch.roleId : native?.launch.source);
  const execution = native ? `Execution ${native.execution ?? "unknown"}` : null;
  const statusRef = useRef<HTMLSpanElement>(null);
  const { status } = props;
  const live = !native && status !== null && isOrchestrationV2WorkActive(status.status);
  const modelLabel = native ? (native.activated?.model ?? "Not reported") : props.modelLabel;
  const effortLabel = native ? null : props.effortLabel;
  const modelDescription = effortLabel === null ? modelLabel : `${modelLabel}, ${effortLabel}`;
  // Announced once per transition; the ticking label below is not.
  const announcement =
    execution ??
    formatProviderSubagentStatus(status === null ? null : { ...status, startedAt: null }, 0);

  // The label is written from an effect, and live bars tick through DOM
  // writes, so a running timer never re-renders the chat view.
  useLayoutEffect(() => {
    const update = () => {
      if (statusRef.current) {
        statusRef.current.textContent =
          execution ?? formatProviderSubagentStatus(status, Date.now());
      }
    };
    update();
    if (!live) return;
    const id = setInterval(update, 1_000);
    return () => clearInterval(id);
  }, [live, status, execution]);

  return (
    <div>
      {props.tasksProgress && props.taskSteps ? (
        <ComposerBanner.Root density="comfortable" variant="default" placement="floating">
          <ComposerTasksContent
            progress={props.tasksProgress}
            steps={props.taskSteps}
            expanded={tasksExpanded}
            onToggle={() => setTasksExpanded((expanded) => !expanded)}
          />
        </ComposerBanner.Root>
      ) : null}
      <div className="flex min-h-12 items-center gap-3 rounded-3xl py-2 ps-5 pe-2 text-sm">
        <span className="flex min-w-0 items-center gap-2">
          {identity ? <span className="truncate font-medium">{identity}</span> : null}
          {props.provider ? (
            <ProviderInstanceIcon
              driverKind={props.provider.driverKind}
              displayName={props.provider.displayName}
              accentColor={props.provider.accentColor}
              acpRegistryAgentId={props.provider.acpRegistryAgentId}
              acpRegistryIconUrl={props.provider.acpRegistryIconUrl}
              showBadge={props.showInstanceBadge}
              className="size-4 shrink-0"
              iconClassName="size-4"
              badgeClassName="right-[-0.125rem] bottom-[-0.125rem] h-3 min-w-3 px-0.5 text-5xs"
            />
          ) : null}
          <span className="min-w-0 truncate font-medium text-foreground">{modelLabel}</span>
          {effortLabel === null ? null : (
            <span className="shrink-0 text-muted-foreground">{effortLabel}</span>
          )}
        </span>
        <span
          ref={statusRef}
          aria-hidden
          className="min-w-0 truncate text-muted-foreground tabular-nums"
        />
        <span role="status" className="sr-only">
          {`${identity ? `${identity}, ` : ""}${modelDescription} subagent: ${announcement}`}
        </span>
        <span className="ms-auto shrink-0 text-muted-foreground max-sm:hidden">
          Runs on its own
        </span>
        <NativeWorkerDetailsDialog worker={native} />
        {props.onOpenParent ? (
          <Button size="sm" variant="ghost" onClick={props.onOpenParent}>
            <ArrowUpLeftIcon />
            Open parent
          </Button>
        ) : null}
      </div>
    </div>
  );
}
