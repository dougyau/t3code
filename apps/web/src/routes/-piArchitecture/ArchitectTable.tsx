import { RegistryContext, useAtomValue } from "@effect/atom-react";
import {
  debateGroups,
  participantModel,
  recordSections,
  tableKey,
  tableObservation,
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
import { COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS } from "../../workspaceTitlebar";
import { piArchitecture } from "./state";

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
      expanded: { ...presentation.expanded, [group.id]: true },
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
      <RecordContent {...{ target, atoms, record }} records={records} onReference={openReference} />
    </div>
  );
  return (
    <section
      className="flex h-full min-h-0 min-w-0 flex-1 flex-col overflow-hidden"
      aria-label="Architect table"
    >
      <header className={cn("shrink-0 border-b p-4", COLLAPSED_SIDEBAR_TITLEBAR_INSET_CLASS)}>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h1 ref={heading} tabIndex={-1} className="text-lg font-semibold">
            Architect table
          </h1>
          <Button variant="outline" size="sm" onClick={onCoordinator}>
            Return to coordinator
          </Button>
        </div>
        <p className="mt-2 break-all text-sm text-muted-foreground">{target.tableId}</p>
        {summary ? <p className="mt-2 text-sm">{tableObservation(summary)}</p> : null}
        {summary?.gaps.length ? <p role="status">Capture gaps: {summary.gaps.join(", ")}</p> : null}
      </header>
      <div
        role="tablist"
        aria-label="Architect table sections"
        className="flex shrink-0 gap-2 border-b p-2"
      >
        {tableTabs.map((tab, index) => (
          <Button
            key={tab}
            role="tab"
            id={`architect-tab-${tab}`}
            aria-controls="architect-reading-pane"
            aria-selected={presentation.tab === tab}
            tabIndex={presentation.tab === tab ? 0 : -1}
            variant={presentation.tab === tab ? "secondary" : "ghost"}
            size="sm"
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
          </Button>
        ))}
      </div>
      <div
        ref={pane}
        id="architect-reading-pane"
        role="tabpanel"
        onScroll={saveReadingPoint}
        aria-labelledby={`architect-tab-${presentation.tab}`}
        tabIndex={0}
        className="scrollbar-gutter-both min-h-0 min-w-0 flex-1 overflow-y-auto p-4 [overflow-wrap:anywhere]"
      >
        <div ref={content} className="mx-auto flex min-w-0 max-w-4xl flex-col gap-5">
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
                  variant="ghost"
                  size="sm"
                  aria-pressed={!presentation.source}
                  onClick={() => update({ source: false })}
                >
                  Reading
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  aria-pressed={presentation.source}
                  onClick={() => update({ source: true })}
                >
                  Source
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
              <div data-reading-id="preparation">
                <Button variant="outline" size="sm" onClick={() => update({ briefOpen: true })}>
                  Read original brief
                </Button>
              </div>
              {groups.length ? (
                <div>
                  <Button
                    variant="outline"
                    size="sm"
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
                </div>
              ) : null}
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
                      ? group.publications.map((publication) => (
                          <article
                            key={publication.id}
                            data-reading-id={`publication:${publication.messageId}`}
                            id={`publication-${publication.messageId}`}
                            tabIndex={-1}
                            className="mt-4 min-w-0"
                          >
                            <h3 className="font-semibold">
                              {members.find((member) => member.id === publication.memberId)
                                ?.roleId ?? publication.memberId}
                              {" · "}
                              {publication.memberId}
                              {" · "}
                              {publication.stage}
                            </h3>
                            <p className="break-all text-xs text-muted-foreground">
                              Snapshot {publication.snapshotId} · turn {publication.turnId} ·
                              attempt {publication.attemptId}
                            </p>
                            {textRecord(publication)}
                          </article>
                        ))
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
          <p className="text-sm text-muted-foreground">
            {page.source} · {page.complete ? "Complete capture" : "Partial native capture"}
          </p>
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
}) {
  if (!("textId" in props.record)) return null;
  return (
    <CapturedContent target={props.target} atoms={props.atoms} textId={props.record.textId}>
      {(text) =>
        recordSections(props.record, text).map((section, index) => {
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
                        props.onReference(section.reference.memberId, section.reference.messageId);
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
        })
      }
    </CapturedContent>
  );
}
