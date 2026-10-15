import { useAtomValue } from "@effect/atom-react";
import { useNavigation, type StaticScreenProps } from "@react-navigation/native";
import { nativeWorkerBriefPresentation } from "@t3tools/client-runtime/state/subagent-display";
import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { Platform, ScrollView, View } from "react-native";
import { Screen, ScreenStack, ScreenStackHeaderConfig } from "react-native-screens";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { withUniwind } from "uniwind";

import { AndroidSheetHeader } from "../../components/AndroidScreenHeader";
import { AppText as Text } from "../../components/AppText";
import { MaterialScreenContent } from "../../components/MaterialScreenContent";
import { NativeStackScreenOptions, nativeHeaderScrollEdgeEffects } from "../../native/StackHeader";
import { environmentThreadDetails } from "../../state/threads";
import { useAdaptiveWorkspaceLayout } from "../layout/AdaptiveWorkspaceLayout";
import { RequestActionButton } from "./RequestActionButton";

const NativeScreen = withUniwind(Screen);
const NativeHeader = withUniwind(ScreenStackHeaderConfig, {
  color: { fromClassName: "tintColorClassName", styleProperty: "accentColor" },
  titleColor: { fromClassName: "titleColorClassName", styleProperty: "accentColor" },
});
const HEADER_SCROLL_EDGE_EFFECTS = nativeHeaderScrollEdgeEffects(Platform.OS, Platform.Version);

type NativeWorkerDetailsTarget = {
  readonly environmentId: EnvironmentId;
  readonly parentThreadId: ThreadId;
  readonly childThreadId: ThreadId;
};

/** Reads the parent's current control plane, including assignments from earlier turns. */
export function ThreadNativeWorkerDetailsSheet({
  route,
}: StaticScreenProps<NativeWorkerDetailsTarget>) {
  const { environmentId, parentThreadId, childThreadId } = route.params;
  const parent = useAtomValue(
    environmentThreadDetails.threadAtom({ environmentId, threadId: parentThreadId }),
  );
  const worker = parent?.projection.subagents.find(
    (agent) => agent.childThreadId === childThreadId,
  )?.nativeWorker;
  const brief = worker ? nativeWorkerBriefPresentation(worker) : null;
  const navigation = useNavigation();
  const insets = useSafeAreaInsets();
  const { layout } = useAdaptiveWorkspaceLayout();
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

  const content = (
    <ScrollView
      nestedScrollEnabled
      className="flex-1"
      contentInsetAdjustmentBehavior={Platform.OS === "ios" ? "automatic" : "never"}
      contentContainerClassName="gap-5 px-5 pt-4"
      contentContainerStyle={{ paddingBottom: Math.max(insets.bottom, 16) + 8 }}
    >
      {Platform.OS !== "ios" ? (
        <View className="items-end">
          <RequestActionButton label="Close" tone="secondary" onPress={() => navigation.goBack()} />
        </View>
      ) : null}
      <View className="gap-3">
        <Text accessibilityRole="header" className="font-t3-bold text-lg text-foreground">
          Original brief
        </Text>
        {!worker ? (
          <Text>Worker details unavailable</Text>
        ) : !brief || brief.state === "missing" ? (
          <Text>Brief unavailable</Text>
        ) : (
          <>
            <Text className="text-sm text-foreground-secondary">
              {brief.completeness === "partial"
                ? "Partial brief — only the first page is available."
                : brief.completeness === "unknown"
                  ? "Brief completeness not reported."
                  : "Complete brief"}
            </Text>
            <Text selectable className="text-base text-foreground">
              {brief.text}
            </Text>
          </>
        )}
      </View>
      <View className="gap-3">
        <Text accessibilityRole="header" className="font-t3-bold text-lg text-foreground">
          Observation
        </Text>
        {!worker ? (
          <Text>Worker details unavailable</Text>
        ) : (
          <>
            {observations.map(([label, value]) => (
              <View key={label} className="gap-1">
                <Text className="font-t3-bold text-sm text-foreground">{label}</Text>
                <Text selectable className="text-base text-foreground">
                  {value}
                </Text>
              </View>
            ))}
            <Text className="font-t3-bold text-sm text-foreground">Observation gaps</Text>
            {worker.gaps.length > 0 ? (
              worker.gaps.map((gap) => (
                <Text key={gap} selectable>
                  {gap}
                </Text>
              ))
            ) : (
              <Text>No gaps reported.</Text>
            )}
            <Text className="text-sm text-foreground-secondary">
              No reported gaps does not establish complete capture. Gaps do not determine execution
              status or identify the location, amount, or content of missing data.
            </Text>
          </>
        )}
      </View>
    </ScrollView>
  );

  if (Platform.OS === "ios") {
    return (
      <View collapsable={false} className="flex-1 bg-sheet">
        <ScreenStack style={{ flex: 1 }}>
          <NativeScreen
            activityState={2}
            enabled
            isNativeStack
            screenId="thread-native-worker-details-sheet-native"
            scrollEdgeEffects={HEADER_SCROLL_EDGE_EFFECTS}
            className="flex-1 bg-sheet"
          >
            {content}
            <NativeHeader
              backgroundColor="rgba(0,0,0,0)"
              tintColorClassName="accent-foreground"
              hideBackButton
              hideShadow={false}
              title="Assignment"
              titleColorClassName="accent-foreground"
              titleFontSize={18}
              titleFontWeight="800"
              translucent
              headerRightBarButtonItems={[
                {
                  type: "button",
                  title: "Close",
                  variant: "done",
                  accessibilityLabel: "Close Assignment",
                  onPress: () => navigation.goBack(),
                },
              ]}
            />
          </NativeScreen>
        </ScreenStack>
      </View>
    );
  }

  return (
    <View collapsable={false} className="flex-1 bg-sheet">
      <NativeStackScreenOptions options={{ sheetCornerRadius: 28 }} />
      <AndroidSheetHeader title="Assignment" onBack={() => navigation.goBack()} />
      <MaterialScreenContent insetHorizontal={layout.usesSplitView}>
        {content}
      </MaterialScreenContent>
    </View>
  );
}
