import { describe, expect, it, vi } from 'vitest';

vi.mock('@renderer/lib/ipc', () => ({ rpc: {} }));

import type { Agent } from '@shared/core/agents/agents';
import {
  mergeServerAgents,
  serverAgentAutoLinks,
  type SyncedServerAgent,
} from './server-agents-store';

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

function localAgent(
  id: string,
  name: string,
  switchAgentId: string | null = null,
  locationId = 'location-1'
): Agent {
  return { id, name, switchAgentId, locationId } as Agent;
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

describe('serverAgentAutoLinks', () => {
  it('links one unbound local agent with the exact same name', () => {
    expect(
      serverAgentAutoLinks([agent('switch-1', 'atlas')], [localAgent('local-1', 'atlas')])
    ).toEqual([{ localAgentId: 'local-1', switchAgentId: 'switch-1' }]);
  });

  it('does not link an ambiguous local name', () => {
    expect(
      serverAgentAutoLinks(
        [agent('switch-1', 'atlas')],
        [localAgent('local-1', 'atlas'), localAgent('local-2', 'atlas')]
      )
    ).toEqual([]);
  });

  it('does not replace a different Switch identity', () => {
    expect(
      serverAgentAutoLinks(
        [agent('switch-1', 'atlas')],
        [localAgent('local-1', 'atlas', 'switch-2')]
      )
    ).toEqual([]);
  });

  it('does not link a local agent without a folder', () => {
    expect(
      serverAgentAutoLinks([agent('switch-1', 'atlas')], [localAgent('local-1', 'atlas', null, '')])
    ).toEqual([]);
  });

  it('requires an exact, case-sensitive name match', () => {
    expect(
      serverAgentAutoLinks([agent('switch-1', 'Atlas')], [localAgent('local-1', 'atlas')])
    ).toEqual([]);
  });
});
