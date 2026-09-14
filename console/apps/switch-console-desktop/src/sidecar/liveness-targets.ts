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
