import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { StackActions, useNavigation, type StaticScreenProps } from "@react-navigation/native";
import {
  debateGroups,
  participantModel,
  recordSections,
  tableObservation,
  tablePresentationAtom,
  tableTabs,
  type ReadingPoint,
  type TableTarget,
} from "@t3tools/client-runtime/state/piArchitecture/model";
import {
  createPiArchitectureAtoms,
  type PiArchitectureAtoms,
} from "@t3tools/client-runtime/state/piArchitecture/queries";
import type { PiArchitectureDetailRecord } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/reactivity";
import {
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type ComponentRef,
  type ReactNode,
} from "react";
import { Pressable, ScrollView, View } from "react-native";

import { AppText as Text } from "../../components/AppText";
import { ScreenHeader } from "../../components/ScreenHeader";
import { connectionAtomRuntime } from "../../connection/runtime";
import { SelectableMarkdownText } from "../../native/SelectableMarkdownText";
import { environmentThreadDetails } from "../../state/threads";
import { useMarkdownPreviewStyles } from "../files/FileMarkdownPreview";
import { tryOpenExternalUrl } from "../../lib/openExternalUrl";

export const mobilePiArchitecture = createPiArchitectureAtoms(connectionAtomRuntime, (target) =>
  environmentThreadDetails.threadAtom({
    environmentId: target.environmentId,
    threadId: target.input.threadId,
  }),
);

export function ThreadArchitectTableScreen({ route }: StaticScreenProps<TableTarget>) {
  const navigation = useNavigation();
  return (
    <NativeArchitectTable
      target={route.params}
      onCoordinator={() => navigation.goBack()}
      onParticipant={(threadId) =>
        navigation.dispatch(
          StackActions.push("Thread", {
            environmentId: route.params.environmentId,
            threadId,
            architectParticipant: true,
          }),
        )
      }
    />
  );
}

export function ArchitectureAction(props: {
  readonly children: ReactNode;
  readonly onPress: () => void;
  readonly expanded?: boolean;
  readonly selected?: boolean;
}) {
  return (
    <Pressable
      accessibilityRole={props.selected === undefined ? "button" : "tab"}
      accessibilityState={{
        ...(props.expanded === undefined ? {} : { expanded: props.expanded }),
        ...(props.selected === undefined ? {} : { selected: props.selected }),
      }}
      onPress={props.onPress}
      className="rounded-lg border border-border px-3 py-2 active:opacity-70"
    >
      <Text className="text-sm text-foreground">{props.children}</Text>
    </Pressable>
  );
}

/** The reading pane alone scrolls. A pushed participant leaves this screen mounted. */
export function NativeArchitectTable(props: {
  readonly target: TableTarget;
  readonly atoms?: PiArchitectureAtoms;
  readonly onCoordinator: () => void;
  readonly onParticipant: (threadId: import("@t3tools/contracts").ThreadId) => void;
}) {
  const { target } = props;
  const atoms = props.atoms ?? mobilePiArchitecture;
  const registry = useContext(RegistryContext);
  const stateAtom = tablePresentationAtom(target);
  const state = useAtomValue(stateAtom);
  const result = useAtomValue(
    atoms.query({
      environmentId: target.environmentId,
      input: { kind: "records", threadId: target.threadId, tableId: target.tableId },
    }),
  );
  const data = Option.getOrNull(AsyncResult.value(result));
  const records = data?.kind === "records" ? data.items : [];
  const summary = data?.kind === "records" ? data.summary : null;
  const members = records.filter((record) => record.kind === "member");
  const groups = useMemo(() => debateGroups(records), [records]);
  useEffect(() => {
    registry.update(stateAtom, (current) => {
      const missing = groups.filter((group) => current.expanded[group.id] === undefined);
      return missing.length
        ? {
            ...current,
            expanded: {
              ...current.expanded,
              ...Object.fromEntries(
                missing.map((group) => [group.id, group.id === groups.at(-1)?.id]),
              ),
            },
          }
        : current;
    });
  }, [groups, registry, stateAtom]);
  const update = (patch: Partial<typeof state>) =>
    registry.update(stateAtom, (current) => ({ ...current, ...patch }));
  const scroll = useRef<ComponentRef<typeof ScrollView>>(null);
  const positions = useRef(new Map<string, { y: number; height: number }>());
  const y = useRef(0);
  const readingKey = state.briefOpen ? "brief" : state.tab;
  const point = useRef<ReadingPoint | undefined>(state.reading[readingKey]);
  const [missingPoint, setMissingPoint] = useState(false);
  const save = () => {
    const visible = [...positions.current]
      .filter(([, box]) => box.y <= y.current)
      .sort((a, b) => b[1].y - a[1].y)[0];
    if (!visible) return;
    point.current = { id: visible[0], offset: y.current - visible[1].y, fallback: y.current };
    registry.update(stateAtom, (current) => ({
      ...current,
      reading: { ...current.reading, [readingKey]: point.current },
    }));
  };
  useLayoutEffect(() => {
    positions.current.clear();
    point.current = registry.get(stateAtom).reading[readingKey];
    y.current = point.current?.fallback ?? 0;
    scroll.current?.scrollTo({ y: y.current, animated: false });
  }, [readingKey, registry, stateAtom]);
  const block = (id: string, children: ReactNode) => {
    return (
      <View
        key={id}
        onLayout={(event) => {
          positions.current.set(id, event.nativeEvent.layout);
          const anchor = point.current;
          if (anchor?.id === id) {
            setMissingPoint(false);
            y.current = event.nativeEvent.layout.y + anchor.offset;
            scroll.current?.scrollTo({ y: y.current, animated: false });
          }
        }}
        className="min-w-0 gap-3 py-3"
      >
        {children}
      </View>
    );
  };
  const openReference = (memberId: string, messageId: string) => {
    const group = groups.find((group) =>
      group.publications.some(
        (publication) => publication.memberId === memberId && publication.messageId === messageId,
      ),
    );
    if (!group) return;
    const id = `publication:${messageId}`;
    const measured = readingKey === "Debate" ? positions.current.get(id) : undefined;
    point.current = { id, offset: 0, fallback: measured?.y ?? y.current };
    if (measured) {
      setMissingPoint(false);
      y.current = measured.y;
      scroll.current?.scrollTo({ y: y.current, animated: false });
    }
    registry.update(stateAtom, (current) => ({
      ...current,
      tab: tableTabs[1],
      briefOpen: false,
      expanded: { ...current.expanded, [group.id]: true },
      reading: { ...current.reading, Debate: point.current },
    }));
  };
  const content = (record: PiArchitectureDetailRecord) => (
    <NativeRecordContent
      key={`${record.kind}:${record.id}`}
      {...{ target, atoms, record, records }}
      onReference={openReference}
    />
  );
  return (
    <View className="flex-1 bg-screen">
      <ScreenHeader title="Architect table" onBack={props.onCoordinator} />
      <View className="gap-2 border-b border-border px-4 py-2">
        <Text className="text-xs text-foreground-muted">{target.tableId}</Text>
        {summary ? (
          <Text className="text-xs text-foreground-muted">{tableObservation(summary)}</Text>
        ) : null}
        {summary?.gaps.length ? (
          <Text accessibilityRole="alert">Capture gaps: {summary.gaps.join(", ")}</Text>
        ) : null}
        <View className="flex-row flex-wrap gap-2">
          {tableTabs.map((tab) => (
            <ArchitectureAction
              key={tab}
              selected={state.tab === tab}
              onPress={() => {
                save();
                update({ tab, briefOpen: false });
              }}
            >
              {tab}
            </ArchitectureAction>
          ))}
        </View>
      </View>
      <ScrollView
        ref={scroll}
        className="flex-1"
        contentContainerClassName="px-4 pb-8"
        maintainVisibleContentPosition={{ minIndexForVisible: 0 }}
        onContentSizeChange={() => {
          setMissingPoint(
            Boolean(
              point.current && positions.current.size && !positions.current.has(point.current.id),
            ),
          );
        }}
        scrollEventThrottle={32}
        onScroll={(event) => {
          y.current = event.nativeEvent.contentOffset.y;
          save();
        }}
      >
        {missingPoint ? (
          <Text accessibilityRole="alert">
            Saved reading location unavailable. Showing the nearest retained position.
          </Text>
        ) : null}
        {result._tag === "Failure" ? (
          <Text accessibilityRole="alert">
            Architecture read unavailable. Previously captured content is retained. Check connection
            and read permission.
          </Text>
        ) : null}
        {!summary ? (
          <Text>{result._tag === "Failure" ? "Table unavailable" : "Loading table…"}</Text>
        ) : state.briefOpen ? (
          block(
            "brief",
            <>
              <Text className="font-t3-bold text-foreground">Original brief</Text>
              <View className="flex-row flex-wrap gap-2">
                <ArchitectureAction
                  onPress={() => {
                    save();
                    update({ briefOpen: false });
                  }}
                >
                  Back to {state.tab}
                </ArchitectureAction>
                <ArchitectureAction
                  selected={!state.source}
                  onPress={() => update({ source: false })}
                >
                  Reading
                </ArchitectureAction>
                <ArchitectureAction
                  selected={state.source}
                  onPress={() => update({ source: true })}
                >
                  Source
                </ArchitectureAction>
              </View>
              <NativeCapturedContent
                {...{ target, atoms }}
                textId="original"
                captureKey={`${summary.capture}:${summary.observation}`}
              >
                {(text) =>
                  state.source ? (
                    <Text selectable className="font-mono text-sm text-foreground">
                      {text}
                    </Text>
                  ) : (
                    <NativeMarkdown text={text} />
                  )
                }
              </NativeCapturedContent>
            </>,
          )
        ) : state.tab === "Summary" ? (
          <>
            {block(
              "summary",
              <>
                <ArchitectureAction
                  onPress={() => {
                    save();
                    update({ briefOpen: true });
                  }}
                >
                  Read original brief
                </ArchitectureAction>
                <Text className="font-t3-bold text-foreground">Outcome and capture</Text>
                <Text>
                  Execution outcome, native closure and capture completeness are separate
                  observations.
                </Text>
                {!records.some((record) => record.kind === "outcome") ? (
                  <Text>No confirmed outcome captured.</Text>
                ) : null}
              </>,
            )}
            {records
              .filter((record) => record.kind === "outcome" || record.kind === "failure")
              .map((record) => block(`${record.kind}:${record.id}`, content(record)))}
            {block(
              "instructions",
              <Text className="font-t3-bold text-foreground">Later instructions</Text>,
            )}
            {records
              .filter((record) => record.kind === "control")
              .sort((a, b) => a.order - b.order)
              .map((record) => block(`control:${record.id}`, content(record)))}
            {block(
              "coordinator",
              <ArchitectureAction onPress={props.onCoordinator}>
                Return to coordinator to give instructions
              </ArchitectureAction>,
            )}
          </>
        ) : state.tab === "Debate" ? (
          <>
            {block(
              "preparation",
              <ArchitectureAction
                onPress={() => {
                  save();
                  update({ briefOpen: true });
                }}
              >
                Read original brief
              </ArchitectureAction>,
            )}
            {block(
              "latest",
              <ArchitectureAction
                onPress={() => {
                  const latest = groups.at(-1);
                  if (latest) update({ expanded: { ...state.expanded, [latest.id]: true } });
                  scroll.current?.scrollToEnd({ animated: false });
                }}
              >
                Go to latest contributions
              </ArchitectureAction>,
            )}
            {groups.length === 0 ? <Text>No settled contributions captured.</Text> : null}
            {groups.flatMap((group, index) => {
              const expanded = state.expanded[group.id] ?? index === groups.length - 1;
              return [
                block(
                  `group:${group.id}`,
                  <ArchitectureAction
                    expanded={expanded}
                    onPress={() => {
                      save();
                      if (expanded)
                        for (const record of group.publications) {
                          positions.current.delete(`publication:${record.messageId}`);
                        }
                      update({ expanded: { ...state.expanded, [group.id]: !expanded } });
                    }}
                  >
                    {group.id}
                  </ArchitectureAction>,
                ),
                ...(expanded
                  ? group.publications.map((publication) =>
                      block(
                        `publication:${publication.messageId}`,
                        <>
                          <Text className="font-t3-bold text-foreground">
                            {members.find((member) => member.id === publication.memberId)?.roleId ??
                              publication.memberId}{" "}
                            · {publication.memberId} · {publication.stage}
                          </Text>
                          <Text className="text-xs text-foreground-muted">
                            Snapshot {publication.snapshotId} · turn {publication.turnId} · attempt{" "}
                            {publication.attemptId}
                          </Text>
                          {content(publication)}
                        </>,
                      ),
                    )
                  : []),
              ];
            })}
            {records
              .filter((record) => record.kind === "failure")
              .map((record) => block(`failure:${record.id}`, content(record)))}
          </>
        ) : (
          members
            .sort((a, b) => a.ordinal - b.ordinal)
            .map((member) =>
              block(
                `member:${member.id}`,
                <>
                  <Text className="font-t3-bold text-foreground">
                    {member.roleId} · {member.id}
                  </Text>
                  <Text>
                    {member.function} · {member.execution}
                    {member.supersededByCompositionId ? " · replaced" : ""}
                  </Text>
                  <Text>{participantModel(member)}</Text>
                  {content(member)}
                  {member.mandateSource === "root-workbrief" ? (
                    <ArchitectureAction
                      onPress={() => {
                        save();
                        update({ briefOpen: true });
                      }}
                    >
                      Read original brief
                    </ArchitectureAction>
                  ) : null}
                  {member.workerThreadId && member.bindingCount > 0 ? (
                    <ArchitectureAction
                      onPress={() => {
                        save();
                        if (member.workerThreadId) props.onParticipant(member.workerThreadId);
                      }}
                    >
                      Read conversation: {member.roleId} · {member.id}
                    </ArchitectureAction>
                  ) : (
                    <Text>Conversation unavailable · {member.bindingCount} confirmed bindings</Text>
                  )}
                </>,
              ),
            )
        )}
      </ScrollView>
    </View>
  );
}

function NativeMarkdown({ text }: { readonly text: string }) {
  const { nativeTextStyle } = useMarkdownPreviewStyles();
  return (
    <SelectableMarkdownText
      markdown={text}
      textStyle={nativeTextStyle}
      onLinkPress={(href) => {
        void tryOpenExternalUrl(href, "markdown-link");
      }}
    />
  );
}

function NativeCapturedContent(props: {
  readonly target: TableTarget;
  readonly atoms: PiArchitectureAtoms;
  readonly textId: string;
  readonly captureKey?: string;
  readonly children: (text: string) => ReactNode;
}) {
  const result = useAtomValue(
    props.atoms.query({
      environmentId: props.target.environmentId,
      input: {
        kind: "text",
        threadId: props.target.threadId,
        tableId: props.target.tableId,
        textId: props.textId,
      },
      ...(props.captureKey === undefined ? {} : { captureKey: props.captureKey }),
    }),
  );
  const page = Option.getOrNull(AsyncResult.value(result));
  return (
    <>
      {result._tag === "Failure" ? (
        <Text accessibilityRole="alert">
          Captured text unavailable or stale. No pages have been combined across revisions.
        </Text>
      ) : null}
      {page?.kind !== "text" ? (
        result._tag !== "Failure" ? (
          <Text>Loading captured text pages…</Text>
        ) : null
      ) : (
        <>
          <Text className="text-xs text-foreground-muted">
            {page.source} · {page.complete ? "Complete capture" : "Partial native capture"}
          </Text>
          {page.source === "unavailable" ? (
            <Text>Original brief unavailable.</Text>
          ) : (
            props.children(page.text)
          )}
        </>
      )}
    </>
  );
}

function NativeRecordContent(props: {
  readonly target: TableTarget;
  readonly atoms: PiArchitectureAtoms;
  readonly record: PiArchitectureDetailRecord;
  readonly records: readonly PiArchitectureDetailRecord[];
  readonly onReference: (memberId: string, messageId: string) => void;
}) {
  if (!("textId" in props.record)) return null;
  return (
    <NativeCapturedContent target={props.target} atoms={props.atoms} textId={props.record.textId}>
      {(text) =>
        recordSections(props.record, text).map((section, index) => {
          const reference = section.reference;
          const available =
            reference &&
            props.records.some(
              (record) =>
                record.kind === "publication" &&
                record.memberId === reference.memberId &&
                record.messageId === reference.messageId,
            );
          return (
            <View key={index} className="gap-2 py-2">
              <Text className="font-t3-bold text-foreground">{section.title}</Text>
              {reference ? (
                available ? (
                  <ArchitectureAction
                    onPress={() => props.onReference(reference.memberId, reference.messageId)}
                  >
                    Read referenced contribution
                  </ArchitectureAction>
                ) : (
                  <Text>
                    Reference unavailable: {reference.memberId} · {reference.messageId}
                  </Text>
                )
              ) : null}
              <NativeMarkdown text={section.text} />
            </View>
          );
        })
      }
    </NativeCapturedContent>
  );
}
