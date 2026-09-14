import type { SessionHostTarget } from './session-host-backend';

export function collectHerdrPaneIdsForLiveness(input: {
  runtimePaneIds: string[];
  pendingPaneIds: string[];
  knownTargets: SessionHostTarget[];
}): string[] {
  const paneIds = new Set<string>([...input.runtimePaneIds, ...input.pendingPaneIds]);
  for (const target of input.knownTargets) {
    if (target.kind === 'herdr') paneIds.add(target.paneId);
  }
  return [...paneIds];
}

/**
 * Drop cached prompt status for panes that are no longer live.
 *
 * Only `refreshHerdrPrompt` deleted a cache entry, and only for a pane it was
 * actually asked to refresh — which liveness had already narrowed to the
 * still-live set. A pane that goes dead therefore stops being asked about and
 * its last-known status (however stale, however wrong) never gets deleted:
 * every subsequent poll keeps forwarding it to `onHerdrStatusChange` as if the
 * pane were still there. Called from the same liveness pass that recomputes
 * which panes are live, so a dead pane's entry is gone before that pass's own
 * status-forwarding loop runs.
 */
export function pruneDeadHerdrPrompts<T>(
  herdrPrompt: Map<string, T>,
  liveHerdrPanes: ReadonlySet<string>
): void {
  for (const paneId of herdrPrompt.keys()) {
    if (!liveHerdrPanes.has(paneId)) herdrPrompt.delete(paneId);
  }
}
