import { RegistryContext, useAtomValue } from "@effect/atom-react";
import {
  debateGroups,
  participantLabel,
  participantModel,
  recordSections,
  tableKey,
  tablePresentationAtom,
  tableTabs,
  type ReadingPoint,
  type TableTarget,
  type TableTab,
} from "@t3tools/client-runtime/state/piArchitecture/model";
import type { PiArchitectureAtoms } from "@t3tools/client-runtime/state/piArchitecture/queries";
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
  type ReactNode,
} from "react";
import ChatMarkdown from "../../components/ChatMarkdown";
import { Button } from "../../components/ui/button";
import { observeResize } from "../../lib/observeResize";
import { cn } from "../../lib/utils";
import { piArchitecture } from "./state";
import { TableStatus } from "./TableStatus";

export interface ArchitectTableProps {
  readonly target: TableTarget;
  readonly atoms?: PiArchitectureAtoms;
  readonly onCoordinator: () => void;
  readonly onParticipant: (memberId: string) => void;
}

/** Main-content surface. Only the reading pane scrolls; header and tabs stay outside it. */
export function ArchitectTable({
  target,
  atoms = piArchitecture,
  onCoordinator,
  onParticipant,
}: ArchitectTableProps) {
  const registry = useContext(RegistryContext);
  const stateAtom = tablePresentationAtom(target);
  const presentation = useAtomValue(stateAtom);
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
  const pane = useRef<HTMLDivElement>(null);
  const content = useRef<HTMLDivElement>(null);
  const point = useRef<ReadingPoint | undefined>(undefined);
  const [missingPoint, setMissingPoint] = useState(false);
  const heading = useRef<HTMLHeadingElement>(null);
  const readingKey = presentation.briefOpen ? "brief" : presentation.tab;
  const key = tableKey(target);
  const update = (patch: Partial<typeof presentation>) =>
    registry.update(stateAtom, (current) => ({ ...current, ...patch }));
  useEffect(() => {
    heading.current?.focus();
  }, [key]);
  const saveReadingPoint = () => {
    const container = pane.current;
    if (!container) return;
    const top = container.getBoundingClientRect().top;
    const anchor = [...container.querySelectorAll<HTMLElement>("[data-reading-id]")]
      .filter((element) => element.getBoundingClientRect().top <= top + 1)
      .toSorted((a, b) => b.getBoundingClientRect().top - a.getBoundingClientRect().top)[0];
    if (!anchor?.dataset.readingId) return;
    point.current = {
      id: anchor.dataset.readingId,
      offset: top - anchor.getBoundingClientRect().top,
      fallback: container.scrollTop,
    };
    registry.update(stateAtom, (current) => ({
      ...current,
      reading: { ...current.reading, [readingKey]: point.current },
    }));
  };
  useLayoutEffect(() => {
    point.current = registry.get(stateAtom).reading[readingKey];
    const restore = () => {
      const container = pane.current;
      const saved = point.current;
      if (!container) return;
      const anchor = [...container.querySelectorAll<HTMLElement>("[data-reading-id]")].find(
        (element) => element.dataset.readingId === saved?.id,
      );
      setMissingPoint(Boolean(saved && !anchor));
      container.scrollTop =
        saved && anchor
          ? container.scrollTop +
            anchor.getBoundingClientRect().top -
            container.getBoundingClientRect().top +
            saved.offset
          : (saved?.fallback ?? 0);
    };
    restore();
    return content.current ? observeResize(content.current, restore) : undefined;
  }, [key, readingKey, registry, stateAtom]);
  const changeTab = (tab: TableTab) => update({ tab, briefOpen: false });
  const openReference = (memberId: string, messageId: string) => {
    const group = groups.find((group) =>
      group.publications.some(
        (record) => record.memberId === memberId && record.messageId === messageId,
      ),
    );
    if (!group) return;
    point.current = {
      id: `publication:${messageId}`,
      offset: 0,
      fallback: pane.current?.scrollTop ?? 0,
    };
    update({
      tab: "Debate",
      briefOpen: false,
      expanded: {
        ...presentation.expanded,
        [group.id]: true,
        [`contribution:${messageId}`]: true,
      },
      reading: { ...presentation.reading, Debate: point.current },
    });
    requestAnimationFrame(() => {
      const element = document.getElementById(`publication-${messageId}`);
      element?.scrollIntoView({ block: "start" });
      element?.focus();
    });
  };
  const textRecord = (record: PiArchitectureDetailRecord) => (
    <div
      key={`${record.kind}:${record.id}`}
      data-reading-id={record.kind === "publication" ? undefined : `${record.kind}:${record.id}`}
    >
      <RecordContent
        {...{ target, atoms, record }}
        records={records}
        onReference={openReference}
        expanded={
          record.kind === "publication"
            ? presentation.expanded[`contribution:${record.messageId}`] === true
            : false
        }
        onExpandedChange={(expanded) => {
          if (record.kind === "publication")
            update({
              expanded: {
                ...presentation.expanded,
                [`contribution:${record.messageId}`]: expanded,
              },
            });
        }}
      />
    </div>
  );
  return (
    <section
      className="flex h-full min-h-0 min-w-0 flex-1 flex-col overflow-hidden bg-background font-sans text-sm leading-relaxed text-foreground"
      aria-label="Architect table"
    >
      <header className="shrink-0 border-b py-4 ps-(--chat-lane-inset-start) pe-(--chat-lane-inset-end)">
        <div className="mx-auto box-content max-w-(--chat-content-max-width) px-3 sm:px-12">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h1 ref={heading} tabIndex={-1} className="text-lg font-semibold">
              Architect table
            </h1>
            <Button variant="outline" size="sm" onClick={onCoordinator}>
              Return to coordinator
            </Button>
          </div>
          {summary ? (
            <div className="mt-3">
              <TableStatus summary={summary} />
            </div>
          ) : null}
        </div>
      </header>
      <div
        role="tablist"
        aria-label="Architect table sections"
        className="shrink-0 border-b py-2 ps-(--chat-lane-inset-start) pe-(--chat-lane-inset-end)"
      >
        <div className="mx-auto box-content flex max-w-(--chat-content-max-width) flex-wrap gap-2 px-3 sm:px-12">
          {tableTabs.map((tab, index) => (
            <button
              key={tab}
              role="tab"
              id={`architect-tab-${tab}`}
              aria-controls="architect-reading-pane"
              aria-selected={presentation.tab === tab}
              tabIndex={presentation.tab === tab ? 0 : -1}
              type="button"
              className={cn(
                "rounded-md border-b-2 px-3 py-2 text-sm focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-ring",
                presentation.tab === tab
                  ? "border-primary bg-primary font-semibold text-primary-foreground focus-visible:outline-primary-foreground"
                  : "border-transparent font-normal text-muted-foreground hover:bg-muted hover:text-foreground",
              )}
              onClick={() => changeTab(tab)}
              onKeyDown={(event) => {
                const next =
                  event.key === "ArrowRight"
                    ? (index + 1) % tableTabs.length
                    : event.key === "ArrowLeft"
                      ? (index + tableTabs.length - 1) % tableTabs.length
                      : event.key === "Home"
                        ? 0
                        : event.key === "End"
                          ? tableTabs.length - 1
                          : null;
                if (next === null) return;
                event.preventDefault();
                const tab = tableTabs[next]!;
                changeTab(tab);
                document.getElementById(`architect-tab-${tab}`)?.focus();
              }}
            >
              {tab}
            </button>
          ))}
        </div>
      </div>
      <div
        ref={pane}
        id="architect-reading-pane"
        role="tabpanel"
        onScroll={saveReadingPoint}
        aria-labelledby={`architect-tab-${presentation.tab}`}
        tabIndex={0}
        className="scrollbar-gutter-both min-h-0 min-w-0 flex-1 overflow-y-auto py-4 ps-(--chat-lane-inset-start) pe-(--chat-lane-inset-end) [overflow-wrap:anywhere]"
      >
        <div
          ref={content}
          className="mx-auto box-content flex min-w-0 max-w-(--chat-content-max-width) flex-col gap-5 px-3 sm:px-12"
        >
          {missingPoint ? (
            <p role="status">
              Saved reading location unavailable. Showing the nearest retained position.
            </p>
          ) : null}
          {result._tag === "Failure" ? (
            <p role="status">
              Architecture read unavailable. Previously captured content, if any, is retained. Check
              connection and read permission.
            </p>
          ) : null}
          {!summary ? (
            <p role="status">
              {result._tag === "Failure" ? "Table unavailable" : "Loading table…"}
            </p>
          ) : presentation.briefOpen ? (
            <>
              <div data-reading-id="brief" className="flex flex-wrap items-center gap-2">
                <h2 className="text-base font-semibold">Original brief</h2>
                <Button variant="outline" size="sm" onClick={() => update({ briefOpen: false })}>
                  Back to {presentation.tab}
                </Button>
                <Button
                  variant={presentation.source ? "ghost" : "default"}
                  size="sm"
                  aria-pressed={!presentation.source}
                  onClick={() => update({ source: false })}
                >
                  <span className={presentation.source ? "font-normal" : "font-semibold"}>
                    Reading
                  </span>
                </Button>
                <Button
                  variant={presentation.source ? "default" : "ghost"}
                  size="sm"
                  aria-pressed={presentation.source}
                  onClick={() => update({ source: true })}
                >
                  <span className={presentation.source ? "font-semibold" : "font-normal"}>
                    Source
                  </span>
                </Button>
              </div>
              <CapturedContent
                target={target}
                atoms={atoms}
                textId="original"
                captureKey={`${summary.capture}:${summary.observation}`}
              >
                {(text) =>
                  presentation.source ? (
                    <pre className="max-w-full whitespace-pre-wrap break-words font-mono text-sm">
                      {text}
                    </pre>
                  ) : (
                    <Markdown target={target} text={text} />
                  )
                }
              </CapturedContent>
            </>
          ) : presentation.tab === "Summary" ? (
            <>
              <div data-reading-id="summary">
                <Button variant="outline" size="sm" onClick={() => update({ briefOpen: true })}>
                  Read original brief
                </Button>
              </div>
              <h2 className="text-base font-semibold">Outcome and capture</h2>
              {records.filter((record) => record.kind === "outcome").map(textRecord)}
              {!records.some((record) => record.kind === "outcome") ? (
                <p>No confirmed outcome captured.</p>
              ) : null}
              <p>
                Execution outcome, native closure and capture completeness are separate
                observations.
              </p>
              {records.filter((record) => record.kind === "failure").map(textRecord)}
              <h2 className="text-base font-semibold">Later instructions</h2>
              {records
                .filter((record) => record.kind === "control")
                .toSorted((a, b) => a.order - b.order)
                .map(textRecord)}
              <p>To give instructions or act on this work, return to the coordinator.</p>
            </>
          ) : presentation.tab === "Debate" ? (
            <>
              <div
                data-reading-id="preparation"
                role="toolbar"
                aria-label="Debate actions"
                className="flex flex-wrap items-center gap-2"
              >
                <Button variant="ghost" size="xs" onClick={() => update({ briefOpen: true })}>
                  Read original brief
                </Button>
                {groups.length ? (
                  <Button
                    variant="ghost"
                    size="xs"
                    onClick={() => {
                      const latest = groups.at(-1)!;
                      update({ expanded: { ...presentation.expanded, [latest.id]: true } });
                      requestAnimationFrame(() =>
                        pane.current?.scrollTo({ top: pane.current.scrollHeight }),
                      );
                    }}
                  >
                    Go to latest contributions
                  </Button>
                ) : null}
              </div>
              {groups.length === 0 ? <p>No settled contributions captured.</p> : null}
              {groups.map((group, index) => {
                const expanded = presentation.expanded[group.id] ?? index === groups.length - 1;
                return (
                  <section
                    key={group.id}
                    data-reading-id={`group:${group.id}`}
                    className="min-w-0 rounded-lg border p-3"
                  >
                    <Button
                      variant="ghost"
                      size="sm"
                      aria-expanded={expanded}
                      onClick={() =>
                        update({ expanded: { ...presentation.expanded, [group.id]: !expanded } })
                      }
                    >
                      {group.id}
                    </Button>
                    {expanded
                      ? group.publications.map((publication) => {
                          const member = members.find(
                            (member) => member.id === publication.memberId,
                          );
                          const technicalKey = `technical:${publication.messageId}`;
                          const technicalOpen = presentation.expanded[technicalKey] === true;
                          return (
                            <article
                              key={publication.id}
                              data-reading-id={`publication:${publication.messageId}`}
                              id={`publication-${publication.messageId}`}
                              tabIndex={-1}
                              className="mt-4 min-w-0"
                            >
                              <h3 className="font-semibold">
                                {member ? participantLabel(member) : "Author unavailable"}
                                {" · "}
                                {
                                  {
                                    composition: "Composition",
                                    preparation: "Preparation",
                                    contribution: "Contribution",
                                    decision: "Decision",
                                    answer: "Answer",
                                    "consultation-plan": "Consultation plan",
                                    "consultation-response": "Consultation response",
                                    knowledge: "Knowledge",
                                    synthesis: "Synthesis",
                                  }[publication.stage]
                                }
                              </h3>
                              <details
                                open={technicalOpen}
                                onToggle={(event) =>
                                  update({
                                    expanded: {
                                      ...presentation.expanded,
                                      [technicalKey]: event.currentTarget.open,
                                    },
                                  })
                                }
                                className="text-xs text-muted-foreground"
                              >
                                <summary className="cursor-pointer">
                                  Contribution technical details
                                </summary>
                                {technicalOpen ? (
                                  <div className="break-all">
                                    <p>
                                      Publication ID: {publication.id} · Member ID:{" "}
                                      {publication.memberId}
                                      {" · "}Assignment ID: {publication.assignmentId} · Message ID:{" "}
                                      {publication.messageId}
                                    </p>
                                    <p>
                                      Snapshot {publication.snapshotId} · turn {publication.turnId}{" "}
                                      · attempt {publication.attemptId}
                                    </p>
                                  </div>
                                ) : null}
                              </details>
                              {textRecord(publication)}
                            </article>
                          );
                        })
                      : null}
                  </section>
                );
              })}
              {records.filter((record) => record.kind === "failure").map(textRecord)}
            </>
          ) : (
            <>
              <h2 className="text-base font-semibold">Participants</h2>
              {members.length === 0 ? <p>No confirmed participants captured.</p> : null}
              {members
                .toSorted((a, b) => a.ordinal - b.ordinal)
                .map((member) => (
                  <article
                    key={member.id}
                    data-reading-id={`member:${member.id}`}
                    className="min-w-0 rounded-lg border p-3"
                  >
                    <h3 className="font-semibold">
                      {member.roleId} · {member.id}
                    </h3>
                    <p>
                      {member.function} · {member.execution}
                      {member.supersededByCompositionId ? " · replaced" : ""}
                    </p>
                    <p>{participantModel(member)}</p>
                    {textRecord(member)}
                    {member.mandateSource === "root-workbrief" ? (
                      <Button variant="ghost" size="sm" onClick={() => update({ briefOpen: true })}>
                        Read original brief
                      </Button>
                    ) : null}
                    <div className="mt-2">
                      {member.workerThreadId && member.bindingCount > 0 ? (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => onParticipant(member.id)}
                        >
                          Read conversation: {member.roleId} · {member.id}
                        </Button>
                      ) : (
                        <p>Conversation unavailable · {member.bindingCount} confirmed bindings</p>
                      )}
                    </div>
                  </article>
                ))}
            </>
          )}
        </div>
      </div>
    </section>
  );
}

function Markdown({ target, text }: { readonly target: TableTarget; readonly text: string }) {
  return (
    <div className="min-w-0 max-w-full overflow-x-auto">
      <ChatMarkdown text={text} cwd={undefined} environmentId={target.environmentId} />
    </div>
  );
}
export function CapturedContent(props: {
  readonly target: TableTarget;
  readonly atoms: PiArchitectureAtoms;
  readonly textId: string;
  readonly captureKey?: string;
  readonly quietComplete?: boolean;
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
        <p role="status">
          Captured text unavailable or stale. No pages have been combined across revisions.
        </p>
      ) : null}
      {page?.kind !== "text" ? (
        result._tag !== "Failure" ? (
          <p role="status">Loading captured text pages…</p>
        ) : null
      ) : (
        <>
          {props.quietComplete && page.complete && page.source !== "unavailable" ? (
            <details className="text-xs text-muted-foreground">
              <summary className="cursor-pointer">Capture details</summary>
              {page.source} · Complete capture
            </details>
          ) : (
            <p className="text-sm text-muted-foreground">
              {page.source} · {page.complete ? "Complete capture" : "Partial native capture"}
            </p>
          )}
          {page.source === "unavailable" ? (
            <p>Original brief unavailable.</p>
          ) : (
            props.children(page.text)
          )}
        </>
      )}
    </>
  );
}
function RecordContent(props: {
  readonly target: TableTarget;
  readonly atoms: PiArchitectureAtoms;
  readonly record: PiArchitectureDetailRecord;
  readonly records: readonly PiArchitectureDetailRecord[];
  readonly onReference: (memberId: string, messageId: string) => void;
  readonly expanded: boolean;
  readonly onExpandedChange: (expanded: boolean) => void;
}) {
  if (!("textId" in props.record)) return null;
  return (
    <CapturedContent
      target={props.target}
      atoms={props.atoms}
      textId={props.record.textId}
      quietComplete={props.record.kind === "publication"}
    >
      {(text) => {
        const sections = recordSections(props.record, text);
        const long =
          props.record.kind === "publication" &&
          sections.reduce((length, section) => length + section.text.length, 0) > 1000;
        if (long && !props.expanded) {
          const first = sections[0]!;
          const excerpt = [];
          // Bound the excerpt at grapheme boundaries, not inside a Unicode character.
          for (const { segment } of new Intl.Segmenter(undefined, {
            granularity: "grapheme",
          }).segment(first.text)) {
            if (excerpt.length === 600) break;
            excerpt.push(segment);
          }
          return (
            <div className="my-3">
              <h4 className="text-sm font-semibold">Contribution excerpt · {first.title}</h4>
              <p className="whitespace-pre-wrap">{excerpt.join("")}…</p>
              <Button
                variant="link"
                size="xs"
                aria-expanded={false}
                onClick={() => props.onExpandedChange(true)}
              >
                Read full contribution
              </Button>
            </div>
          );
        }
        return (
          <>
            {sections.map((section, index) => {
              const referenced =
                section.reference &&
                props.records.find(
                  (record) =>
                    record.kind === "publication" &&
                    record.memberId === section.reference?.memberId &&
                    record.messageId === section.reference.messageId,
                );
              return (
                <section key={index} className="my-3 min-w-0">
                  <h4 className="text-sm font-semibold">{section.title}</h4>
                  {section.reference ? (
                    referenced ? (
                      <Button
                        variant="link"
                        size="sm"
                        onClick={() => {
                          if (section.reference)
                            props.onReference(
                              section.reference.memberId,
                              section.reference.messageId,
                            );
                        }}
                      >
                        Read referenced contribution
                      </Button>
                    ) : (
                      <p className="text-sm">
                        Reference unavailable: {section.reference.memberId} ·{" "}
                        {section.reference.messageId}
                      </p>
                    )
                  ) : null}
                  <Markdown target={props.target} text={section.text} />
                </section>
              );
            })}
            {long ? (
              <Button
                variant="link"
                size="xs"
                aria-expanded={true}
                onClick={() => props.onExpandedChange(false)}
              >
                Show less
              </Button>
            ) : null}
          </>
        );
      }}
    </CapturedContent>
  );
}
