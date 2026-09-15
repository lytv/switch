import { describe, expect, it, vi } from 'vitest';

vi.mock('@renderer/lib/ipc', () => ({ rpc: {} }));

import { mergeServerAgents, type SyncedServerAgent } from './server-agents-store';

function agent(id: string, name: string, iconUrl: string | null = null): SyncedServerAgent {
  return {
    id,
    name,
    description: '',
    connectorType: 'agent',
    ownerId: null,
    ownerName: null,
    knownAgentType: 'codex',
    addressingPolicy: null,
    iconUrl,
    displayName: null,
    createdAt: '2026-01-01T00:00:00Z',
    missing: false,
  };
}

describe('mergeServerAgents', () => {
  it('matches by server agent id and updates server metadata', () => {
    const merged = mergeServerAgents(
      [agent('a', 'old', null)],
      [agent('a', 'new', 'https://x/icon')]
    );

    expect(merged).toEqual([
      expect.objectContaining({ id: 'a', name: 'new', iconUrl: 'https://x/icon', missing: false }),
    ]);
  });

  it('keeps identities missing from the latest response as orphaned', () => {
    const merged = mergeServerAgents([agent('local-only', 'local')], []);

    expect(merged).toEqual([expect.objectContaining({ id: 'local-only', missing: true })]);
  });
});
