import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { FileWatchEvent } from '@shared/core/fs/fs';
import type { Location } from '@shared/core/locations/locations';

type DiscoveredStub = { name: string; switchAgentId: string; apiEndpoint: string };
type ServerStub = { id: string; apiUrl: string };

function loc(overrides: Partial<Location> = {}): Location {
  return {
    id: 'loc-1',
    name: 'repo',
    dir: '/repo',
    sshHost: null,
    createdAt: '2024-01-01',
    updatedAt: '2024-01-01',
    ...overrides,
  };
}

const h = vi.hoisted(() => {
  const state = {
    hooks: {
      locationOpened: [] as Array<(id: string) => void>,
      locationClosed: [] as Array<(id: string) => void>,
    },
    locations: [] as Location[],
    locationById: new Map<string, Location>(),
    discoveredByDir: new Map<string, DiscoveredStub[]>(),
    serversByEndpoint: new Map<string, ServerStub>(),
    watchCallbacks: new Map<string, (events: FileWatchEvent[]) => void>(),
    stops: new Map<string, ReturnType<typeof vi.fn>>(),
  };
  return {
    state,
    on: vi.fn((name: 'locationOpened' | 'locationClosed', handler: (id: string) => void) => {
      state.hooks[name].push(handler);
    }),
    getLocations: vi.fn(async () => state.locations),
    getLocationById: vi.fn(async (id: string) => state.locationById.get(id)),
    subscribeToLocationWatch: vi.fn(
      (locationId: string, _label: string, callback: (events: FileWatchEvent[]) => void) => {
        state.watchCallbacks.set(locationId, callback);
        const stop = vi.fn();
        state.stops.set(locationId, stop);
        return stop;
      }
    ),
    discoverConfiguredAgents: vi.fn(
      async ({ dir }: { dir: string }) => state.discoveredByDir.get(dir) ?? []
    ),
    findServerByEndpoint: vi.fn(
      async (endpoint: string) => state.serversByEndpoint.get(endpoint) ?? null
    ),
    adoptConfiguredAgent: vi.fn(async () => ({ success: true, data: null })),
    warn: vi.fn(),
  };
});

vi.mock('@main/core/locations/location-manager', () => ({
  locationManager: { on: h.on },
}));
vi.mock('@main/core/locations/store', () => ({
  getLocations: h.getLocations,
  getLocationById: h.getLocationById,
}));
vi.mock('@main/core/fs/controller', () => ({
  subscribeToLocationWatch: h.subscribeToLocationWatch,
}));
vi.mock('@main/core/agents/discover-configured-agents', () => ({
  discoverConfiguredAgents: h.discoverConfiguredAgents,
}));
vi.mock('@main/core/switch-servers/servers-store', () => ({
  findServerByEndpoint: h.findServerByEndpoint,
}));
vi.mock('@main/core/agents/attach-configured-agents', () => ({
  adoptConfiguredAgent: h.adoptConfiguredAgent,
}));
vi.mock('@main/lib/logger', () => ({ log: { info: vi.fn(), warn: h.warn, error: vi.fn() } }));

async function freshService() {
  vi.resetModules();
  const mod = await import('./configured-agent-discovery-service');
  return mod.configuredAgentDiscoveryService;
}

beforeEach(() => {
  vi.clearAllMocks();
  h.state.hooks.locationOpened = [];
  h.state.hooks.locationClosed = [];
  h.state.locations = [];
  h.state.locationById.clear();
  h.state.discoveredByDir.clear();
  h.state.serversByEndpoint.clear();
  h.state.watchCallbacks.clear();
  h.state.stops.clear();
  h.adoptConfiguredAgent.mockResolvedValue({ success: true, data: null });
});

describe('configuredAgentDiscoveryService', () => {
  it('adopts an agent discovered at startup for a local location with a matching server', async () => {
    const location = loc();
    h.state.locations = [location];
    h.state.discoveredByDir.set('/repo', [
      { name: 'theirs', switchAgentId: 'sw-theirs', apiEndpoint: 'https://switch.example.com' },
    ]);
    h.state.serversByEndpoint.set('https://switch.example.com', {
      id: 'srv-1',
      apiUrl: 'https://switch.example.com',
    });

    const service = await freshService();
    service.initialize();

    await vi.waitFor(() => expect(h.adoptConfiguredAgent).toHaveBeenCalledTimes(1));
    expect(h.adoptConfiguredAgent).toHaveBeenCalledWith(
      expect.objectContaining({
        serverId: 'srv-1',
        discovered: expect.objectContaining({ switchAgentId: 'sw-theirs' }),
      })
    );
  });

  it('does not scan a location on an SSH host at startup', async () => {
    h.state.locations = [loc({ id: 'loc-ssh', sshHost: 'vm-1', dir: '/remote' })];

    const service = await freshService();
    service.initialize();
    await vi.waitFor(() => expect(h.getLocations).toHaveBeenCalled());
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(h.discoverConfiguredAgents).not.toHaveBeenCalled();
  });

  it('warns and does not adopt when no configured server matches the credential endpoint', async () => {
    h.state.locations = [loc()];
    h.state.discoveredByDir.set('/repo', [
      { name: 'orphan', switchAgentId: 'sw-orphan', apiEndpoint: 'https://unknown.example.com' },
    ]);

    const service = await freshService();
    service.initialize();

    await vi.waitFor(() => expect(h.warn).toHaveBeenCalled());
    expect(h.adoptConfiguredAgent).not.toHaveBeenCalled();
  });

  it('subscribes to the native watcher when a local location opens, and unsubscribes when it closes', async () => {
    const location = loc({ id: 'loc-2', dir: '/repo2' });
    h.state.locationById.set('loc-2', location);

    const service = await freshService();
    service.initialize();
    h.state.hooks.locationOpened.forEach((handler) => handler('loc-2'));
    await vi.waitFor(() =>
      expect(h.subscribeToLocationWatch).toHaveBeenCalledWith(
        'loc-2',
        expect.stringContaining('loc-2'),
        expect.any(Function)
      )
    );

    const stop = h.state.stops.get('loc-2');
    expect(stop).toBeDefined();
    h.state.hooks.locationClosed.forEach((handler) => handler('loc-2'));
    expect(stop).toHaveBeenCalledTimes(1);
  });

  it('does not watch a location opened on an SSH host', async () => {
    h.state.locationById.set('loc-ssh', loc({ id: 'loc-ssh', sshHost: 'vm-1', dir: '/remote' }));

    const service = await freshService();
    service.initialize();
    h.state.hooks.locationOpened.forEach((handler) => handler('loc-ssh'));
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(h.subscribeToLocationWatch).not.toHaveBeenCalled();
  });

  it('reconciles only on a create/modify event under .switch/agents/*.json', async () => {
    const location = loc({ id: 'loc-3', dir: '/repo3' });
    h.state.locationById.set('loc-3', location);
    h.state.discoveredByDir.set('/repo3', [
      { name: 'theirs', switchAgentId: 'sw-theirs', apiEndpoint: 'https://switch.example.com' },
    ]);
    h.state.serversByEndpoint.set('https://switch.example.com', {
      id: 'srv-1',
      apiUrl: 'https://switch.example.com',
    });

    const service = await freshService();
    service.initialize();
    h.state.hooks.locationOpened.forEach((handler) => handler('loc-3'));
    await vi.waitFor(() => expect(h.state.watchCallbacks.has('loc-3')).toBe(true));

    const callback = h.state.watchCallbacks.get('loc-3');
    if (!callback) throw new Error('watch callback not registered');

    callback([{ type: 'modify', entryType: 'file', path: 'src/index.ts' }]);
    callback([{ type: 'delete', entryType: 'file', path: '.switch/agents/theirs.json' }]);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(h.adoptConfiguredAgent).not.toHaveBeenCalled();

    callback([{ type: 'create', entryType: 'file', path: '.switch/agents/theirs.json' }]);
    await vi.waitFor(() => expect(h.adoptConfiguredAgent).toHaveBeenCalledTimes(1));
  });

  it('does not adopt the same identity twice when reconciliation overlaps', async () => {
    const location = loc({ id: 'loc-4', dir: '/repo4' });
    h.state.locationById.set('loc-4', location);
    h.state.discoveredByDir.set('/repo4', [
      { name: 'theirs', switchAgentId: 'sw-theirs', apiEndpoint: 'https://switch.example.com' },
    ]);
    h.state.serversByEndpoint.set('https://switch.example.com', {
      id: 'srv-1',
      apiUrl: 'https://switch.example.com',
    });
    let releaseAdopt: (() => void) | undefined;
    h.adoptConfiguredAgent.mockImplementation(
      () =>
        new Promise((resolve) => {
          releaseAdopt = () => resolve({ success: true, data: null });
        })
    );

    const service = await freshService();
    service.initialize();
    h.state.hooks.locationOpened.forEach((handler) => handler('loc-4'));
    await vi.waitFor(() => expect(h.state.watchCallbacks.has('loc-4')).toBe(true));

    const callback = h.state.watchCallbacks.get('loc-4');
    if (!callback) throw new Error('watch callback not registered');

    const event: FileWatchEvent = {
      type: 'create',
      entryType: 'file',
      path: '.switch/agents/theirs.json',
    };
    callback([event]);
    callback([event]);

    await vi.waitFor(() => expect(h.adoptConfiguredAgent).toHaveBeenCalledTimes(1));
    releaseAdopt?.();
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(h.adoptConfiguredAgent).toHaveBeenCalledTimes(1);
  });
});
