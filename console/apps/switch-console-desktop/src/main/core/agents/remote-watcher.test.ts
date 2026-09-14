import { beforeEach, describe, expect, it, vi } from 'vitest';

const ensureAgentSidecar = vi.hoisted(() => vi.fn(async () => ({})));
const writeWatchEnabled = vi.hoisted(() => vi.fn(async () => {}));
const listAutoSessionAgentIds = vi.fn(async () => ['agent-1']);
const getAgentById = vi.fn();
const getRemoteAgentLocation = vi.fn();
const connectRemoteAgent = vi.fn();
const getSettings = vi.fn();

vi.mock('@main/core/agent-runtime/impl/ensure-agent-sidecar', () => ({
  ensureAgentSidecar,
}));
vi.mock('@main/core/agent-runtime/impl/remote-sidecar-launcher', () => ({
  writeWatchEnabled,
}));
vi.mock('@main/core/fs/impl/ssh-fs', () => ({ SshFileSystem: vi.fn() }));
vi.mock('@main/core/locations/settings/providers/remote-location-settings-provider', () => ({
  RemoteLocationSettingsProvider: vi.fn(function () {
    return { get: () => getSettings() };
  }),
}));
vi.mock('@main/core/switch-rooms/auto-session-store', () => ({
  listAutoSessionAgentIds: () => listAutoSessionAgentIds(),
}));
vi.mock('@main/lib/logger', () => ({ log: { info: vi.fn(), warn: vi.fn() } }));
vi.mock('./agent-launch-config', () => ({
  agentLaunchSpecialization: vi.fn(async () => undefined),
}));
vi.mock('./agent-location', () => ({
  getRemoteAgentLocation: () => getRemoteAgentLocation(),
}));
vi.mock('./connect-remote-agent', () => ({
  connectRemoteAgent: () => connectRemoteAgent(),
}));
vi.mock('./getAgentById', () => ({ getAgentById: () => getAgentById() }));
vi.mock('./getAgents', () => ({ getAgents: vi.fn(async () => []) }));
vi.mock('./reap-stale-sidecars', () => ({ reapStaleSidecarsForAgent: vi.fn(async () => {}) }));
vi.mock('./remote-session-reconciler', () => ({ remoteSessionReconciler: { start: vi.fn() } }));

import { ensureRemoteWatcher } from './remote-watcher';

describe('ensureRemoteWatcher', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    getAgentById.mockResolvedValue({
      id: 'agent-1',
      providerId: 'codex',
      autoApprove: false,
      name: 'herdr-agent',
      switchAgentId: 'switch-1',
    });
    getRemoteAgentLocation.mockResolvedValue({ id: 'location-1', dir: '/repo', sshHost: 'host' });
    connectRemoteAgent.mockResolvedValue({
      ctx: {},
      proxy: {},
      connectionId: 'connection-1',
      remoteRepoDir: '/repo',
      host: {},
    });
    getSettings.mockResolvedValue({
      sessionHost: 'herdr',
      herdr: {
        sessionName: 'switchdash',
        protocolMin: 14,
        preferAgentPrompt: true,
        workspaceMode: 'flat',
      },
    });
  });

  it('writes the configured Herdr host into the watcher sidecar spec', async () => {
    await ensureRemoteWatcher('agent-1');

    expect(ensureAgentSidecar).toHaveBeenCalledWith(
      expect.objectContaining({
        sessionHost: 'herdr',
        herdr: expect.objectContaining({ protocolMin: 14 }),
      })
    );
  });
});
