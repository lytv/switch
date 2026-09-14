import { describe, expect, it } from 'vitest';
import { collectHerdrPaneIdsForLiveness, pruneDeadHerdrPrompts } from './liveness-targets';

describe('collectHerdrPaneIdsForLiveness', () => {
  it('includes restored herdr targets even when they are not in runtime or pending sets', () => {
    const paneIds = collectHerdrPaneIdsForLiveness({
      runtimePaneIds: [],
      pendingPaneIds: [],
      knownTargets: [
        { kind: 'herdr', paneId: 'pane-restored', tabId: 'tab-1', workspaceId: 'ws-1' },
      ],
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

describe('pruneDeadHerdrPrompts', () => {
  it('deletes cached entries for panes no longer live', () => {
    const herdrPrompt = new Map([
      ['pane-live', { status: 'working' }],
      ['pane-dead', { status: 'working' }],
    ]);
    pruneDeadHerdrPrompts(herdrPrompt, new Set(['pane-live']));
    expect([...herdrPrompt.keys()]).toEqual(['pane-live']);
  });

  it('leaves every entry alone when all its panes are still live', () => {
    const herdrPrompt = new Map([
      ['pane-a', { status: 'working' }],
      ['pane-b', { status: 'idle' }],
    ]);
    pruneDeadHerdrPrompts(herdrPrompt, new Set(['pane-a', 'pane-b']));
    expect([...herdrPrompt.entries()]).toEqual([
      ['pane-a', { status: 'working' }],
      ['pane-b', { status: 'idle' }],
    ]);
  });

  it('empties the cache when no pane is live', () => {
    const herdrPrompt = new Map([['pane-a', { status: 'working' }]]);
    pruneDeadHerdrPrompts(herdrPrompt, new Set());
    expect(herdrPrompt.size).toBe(0);
  });
});
