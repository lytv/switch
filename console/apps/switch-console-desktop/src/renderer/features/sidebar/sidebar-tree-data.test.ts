import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * The sidebar's Reload button has no logic of its own — it calls
 * `reloadRoomsAndAgents`, which is what actually adopts pending-location
 * folders and re-reads rooms and agents. This is the path the button click
 * goes through.
 */

const toast = vi.fn();
const processPendingLocations = vi.fn().mockResolvedValue(undefined);
const backfillAgentIcons = vi.fn().mockResolvedValue({ kind: 'written', written: 0 });
const load = vi.fn().mockResolvedValue(undefined);
const reloadLocations = vi.fn().mockResolvedValue(undefined);
const ensureMembershipsFor = vi.fn().mockResolvedValue(undefined);
const loadRoomNames = vi.fn().mockResolvedValue(undefined);

vi.mock('@renderer/lib/hooks/use-toast', () => ({ toast }));
vi.mock('@renderer/lib/ipc', () => ({
  rpc: {
    locations: { processPendingLocations },
    switchServers: { backfillAgentIcons },
  },
}));
vi.mock('@renderer/features/locations/stores/agents-store', () => ({
  agentsStore: { load, byLocation: new Map(), agentsOnServerAtLocation: () => [] },
}));
vi.mock('@renderer/features/locations/stores/location-selectors', () => ({
  getLocationManagerStore: () => ({ reload: reloadLocations }),
}));
vi.mock('@renderer/features/switch-servers/switch-rooms-store', () => ({
  switchRoomsStore: { ensureMembershipsFor, loadRoomNames },
}));
vi.mock('@renderer/features/switch-servers/switch-servers-store', () => ({
  switchServersStore: { activeServerId: null },
}));
vi.mock('@renderer/lib/stores/app-state', () => ({
  sidebarStore: {
    orderedLocations: [],
    filteredLocations: [],
    visibleSessionsForLocation: () => [],
    orderAgents: (entries: unknown[]) => entries,
  },
}));
vi.mock('@renderer/utils/logger', () => ({
  log: { warn: vi.fn(), info: vi.fn(), debug: vi.fn() },
}));

describe('reloadRoomsAndAgents', () => {
  beforeEach(() => {
    processPendingLocations.mockClear();
    load.mockClear();
    reloadLocations.mockClear();
    ensureMembershipsFor.mockClear();
    loadRoomNames.mockClear();
  });

  it('adopts pending-location folders, mounts new locations, then reloads agents and rooms', async () => {
    const { reloadRoomsAndAgents } = await import('./sidebar-tree-data');

    await reloadRoomsAndAgents();

    expect(processPendingLocations).toHaveBeenCalledTimes(1);
    expect(reloadLocations).toHaveBeenCalledTimes(1);
    expect(load).toHaveBeenCalledTimes(1);
    expect(ensureMembershipsFor).toHaveBeenCalledWith([], { force: true });
    expect(loadRoomNames).toHaveBeenCalledTimes(1);
  });

  it('propagates a failure instead of swallowing it', async () => {
    processPendingLocations.mockRejectedValueOnce(new Error('server unreachable'));
    const { reloadRoomsAndAgents } = await import('./sidebar-tree-data');

    await expect(reloadRoomsAndAgents()).rejects.toThrow('server unreachable');
    expect(load).not.toHaveBeenCalled();
  });
});
