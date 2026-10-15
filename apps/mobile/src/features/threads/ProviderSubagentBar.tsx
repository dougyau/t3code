import {
  formatProviderSubagentStatus,
  type ProviderSubagentStatus,
} from "@t3tools/client-runtime/state/thread-execution";
import { isOrchestrationV2WorkActive } from "@t3tools/contracts";
import type { PiNativeWorkerSummary } from "@t3tools/contracts";
import { nativeWorkerPresentation } from "@t3tools/client-runtime/state/subagent-display";
import { View } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { ProviderIcon } from "../../components/ProviderIcon";
import { RequestActionButton } from "./RequestActionButton";
import { useVisibleSecondClock } from "./use-visible-second-clock";

/**
 * Replaces the composer on a provider-native subagent thread. The provider
 * runs that conversation, so there is nothing to send; the bar says which
 * model is working, for how long, and leads back to the parent.
 */
export function ProviderSubagentBar(props: {
  /** Driver and catalog icon of the provider running the subagent. */
  readonly provider: { readonly driver: string; readonly iconUrl?: string | undefined } | null;
  readonly modelLabel: string;
  /** Reasoning effort as the composer names it, when the subagent has one. */
  readonly effortLabel: string | null;
  /** Null until the subagent's root turn arrives. */
  readonly status: ProviderSubagentStatus | null;
  readonly onOpenParent: (() => void) | null;
  readonly nativeWorker?: PiNativeWorkerSummary | undefined;
}) {
  const native = props.nativeWorker ? nativeWorkerPresentation(props.nativeWorker) : null;
  const live = !native && props.status !== null && isOrchestrationV2WorkActive(props.status.status);
  const nowMs = useVisibleSecondClock(live);
  const statusLabel = native?.status ?? formatProviderSubagentStatus(props.status, nowMs);
  const modelLabel = native ? (native.model ?? "Not reported") : props.modelLabel;
  const effortLabel = native ? null : props.effortLabel;
  const modelDescription = effortLabel === null ? modelLabel : `${modelLabel}, ${effortLabel}`;

  return (
    <View>
      {native ? (
        <View className="gap-1 px-4 pb-2">
          <Text className="text-xs text-foreground-secondary">
            {native.identity ?? native.role}
          </Text>
          <Text className="text-xs text-foreground-secondary">{native.task}</Text>
          <Text className="text-xs text-foreground-secondary">{native.status}</Text>
          <Text className="text-xs text-foreground-secondary">
            Effective model: {native.model ?? "Not reported"}
          </Text>
        </View>
      ) : null}
      <View className="flex-row items-center gap-3 rounded-[20px] border border-border-subtle bg-card-alt py-2 pe-2 ps-4">
        {/* Only the text is one element, so "Open parent" stays reachable. */}
        <View
          accessible
          accessibilityLabel={`${modelDescription} subagent, ${statusLabel}. It runs on its own and cannot take messages.`}
          className="min-w-0 flex-1 gap-0.5"
        >
          <View className="min-w-0 flex-row items-center gap-1.5">
            {props.provider ? (
              <ProviderIcon
                iconUrl={props.provider.iconUrl}
                provider={props.provider.driver}
                size={16}
              />
            ) : null}
            <Text numberOfLines={1} className="min-w-0 shrink font-t3-bold text-sm text-foreground">
              {modelLabel}
            </Text>
            {effortLabel === null ? null : (
              <Text
                numberOfLines={1}
                className="shrink-0 font-sans text-sm text-foreground-secondary"
              >
                {effortLabel}
              </Text>
            )}
          </View>
          <Text
            numberOfLines={1}
            className="font-sans text-xs text-foreground-secondary"
            style={{ fontVariant: ["tabular-nums"] }}
          >
            {statusLabel} · Runs on its own
          </Text>
        </View>
        {props.onOpenParent ? (
          <RequestActionButton label="Open parent" tone="secondary" onPress={props.onOpenParent} />
        ) : null}
      </View>
    </View>
  );
}
