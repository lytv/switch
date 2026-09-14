import { describe, expect, it } from 'vitest';
import { collectHerdrPaneIdsForLiveness } from './liveness-targets';

describe('collectHerdrPaneIdsForLiveness', () => {
  it('includes restored herdr targets even when they are not in runtime or pending sets', () => {
    const paneIds = collectHerdrPaneIdsForLiveness({
      runtimePaneIds: [],
      pendingPaneIds: [],
      knownTargets: [{ kind: 'herdr', paneId: 'pane-restored', tabId: 'tab-1', workspaceId: 'ws-1' }],
    });
    expect(paneIds).toEqual(['pane-restored']);
  });

  it('deduplicates runtime, pending, and restored herdr ids', () => {
    const paneIds = collectHerdrPaneIdsForLiveness({
      runtimePaneIds: ['pane-a', 'pane-b'],
      pendingPaneIds: ['pane-b', 'pane-c'],
      knownTargets: [
        { kind: 'herdr', paneId: 'pane-c', tabId: 'tab-1', workspaceId: 'ws-1' },
        { kind: 'herdr', paneId: 'pane-d', tabId: 'tab-2', workspaceId: 'ws-1' },
      ],
    });
    expect(new Set(paneIds)).toEqual(new Set(['pane-a', 'pane-b', 'pane-c', 'pane-d']));
  });

  it('ignores tmux targets from durable state', () => {
    const paneIds = collectHerdrPaneIdsForLiveness({
      runtimePaneIds: [],
      pendingPaneIds: [],
      knownTargets: [{ kind: 'tmux', tmuxTarget: 'switchdash-session-a' }],
    });
    expect(paneIds).toEqual([]);
  });
});
