import type { PluginFs } from '@switch-console/core/agents/plugins';
import { beforeEach, describe, expect, it, vi } from 'vitest';

/**
 * A {@link PluginFs} that fails any write. Attaching adopts another install's
 * directory, so every write is a bug — this turns "should not write" into a
 * test failure at the point of the write rather than an assertion after it.
 */
function readOnlyFs(seed: Record<string, string>): PluginFs {
  const files = new Map<string, string>(Object.entries(seed));
  return {
    read: (p) => Promise.resolve(files.get(p) ?? null),
    write: (p) => Promise.reject(new Error(`attach wrote to the workspace: ${p}`)),
    delete: (p) => Promise.reject(new Error(`attach deleted from the workspace: ${p}`)),
    exists: (p) => Promise.resolve(files.has(p)),
    list: (dir) => {
      const prefix = `${dir}/`;
      const entries = new Set<string>();
      for (const p of files.keys()) {
        if (!p.startsWith(prefix)) continue;
        const rest = p.slice(prefix.length);
        const slash = rest.indexOf('/');
        entries.add(slash === -1 ? rest : rest.slice(0, slash));
      }
      return Promise.resolve([...entries]);
    },
  };
}

function creds(agentId: string, endpoint = 'https://switch.example.com') {
  return JSON.stringify({
    env: {
      SWITCH_API_ENDPOINT: endpoint,
      SWITCH_API_TOKEN: 'tok-secret',
      SWITCH_AGENT_ID: agentId,
    },
  });
}

const h = vi.hoisted(() => {
  class GatewayError extends Error {
    constructor(
      readonly kind: string,
      readonly status?: number
    ) {
      super(kind);
    }
  }
  const state: {
    workspace: PluginFs | null;
    /** Agent-row names already in the directory, per Switch server. */
    agentNamesByServer: Record<string, Array<{ name: string; switchAgentId?: string }>>;
    existsOnServer: boolean;
    existsThrows: Error | null;
    knownAgentType: string | null;
  } = {
    workspace: null,
    agentNamesByServer: {},
    existsOnServer: true,
    existsThrows: null,
    knownAgentType: 'codex',
  };
  return {
    state,
    GatewayError,
    warn: vi.fn(),
    createAgent: vi.fn(async (input: Record<string, unknown>) => ({ ...input })),
    agentExistsOnServer: vi.fn(async () => {
      if (h.state.existsThrows) throw h.state.existsThrows;
      return h.state.existsOnServer;
    }),
    getAgents: vi.fn(
      async (): Promise<Array<{ id: string; locationId: string; autoApprove: boolean }>> => []
    ),
    fetchAgentDetail: vi.fn(async () => {
      if (h.state.existsThrows) throw h.state.existsThrows;
      if (!h.state.existsOnServer) throw new GatewayError('http', 404);
      return { knownAgentType: h.state.knownAgentType };
    }),
    openLocation: vi.fn(async () => {}),
    emit: vi.fn(),
  };
});

vi.mock('@main/core/locations/store', () => ({
  getLocationByHostDir: vi.fn(async () => ({ id: 'loc-1' })),
  ensureLocation: vi.fn(async (params: { sshHost: string | null }) => ({
    id: 'loc-1',
    sshHost: params.sshHost,
  })),
}));
vi.mock('./getAgents', () => ({
  getAgents: h.getAgents,
  getLocationAgentsOnServer: vi.fn(
    async (_locationId: string, serverId: string) => h.state.agentNamesByServer[serverId] ?? []
  ),
}));
vi.mock('./agent-workspace-fs', () => ({
  resolveWorkspaceFsFor: vi.fn(async () => ({
    fs: h.state.workspace as PluginFs,
    homeFs: null,
    close: vi.fn(),
  })),
}));
vi.mock('@main/core/providers/plugin-registry', () => ({
  listPlugins: () => [{ metadata: { id: 'codex' }, behavior: {} }],
}));
vi.mock('@main/core/switch-servers/gateway-client', () => ({
  fetchAgentDetail: h.fetchAgentDetail,
  GatewayError: h.GatewayError,
}));
vi.mock('@main/core/switch-servers/servers-store', () => ({
  getServer: vi.fn(async () => ({
    id: 'srv-1',
    name: 'Switch',
    apiUrl: 'https://switch.example.com',
  })),
}));
vi.mock('./createAgent', () => ({ createAgent: h.createAgent }));
vi.mock('@main/core/locations/path-utils', () => ({ checkIsValidDirectory: () => true }));
vi.mock('@main/core/locations/location-manager', () => ({
  locationManager: { openLocation: h.openLocation },
}));
vi.mock('./setAgentAutoSession', () => ({
  reconcileAgentAutoSessionFromGateway: vi.fn(async () => {}),
}));
vi.mock('./agent-events', () => ({ agentEvents: { _emit: h.emit } }));
vi.mock('@main/lib/logger', () => ({ log: { info: vi.fn(), warn: h.warn, error: vi.fn() } }));

const { attachConfiguredAgents, adoptConfiguredAgent } = await import('./attach-configured-agents');

function params(
  agents: Array<{ name: string; providerId: 'codex' | 'claude' }>,
  sshHost: string | null = 'vm-1'
) {
  return { sshHost, dir: '/repo', serverId: 'srv-1', agents };
}

describe('attachConfiguredAgents', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    h.state.agentNamesByServer = {};
    h.state.existsOnServer = true;
    h.state.existsThrows = null;
    h.state.knownAgentType = 'codex';
    h.state.workspace = readOnlyFs({ '.switch/agents/theirs.json': creds('sw-theirs') });
    h.getAgents.mockResolvedValue([]);
  });

  it('adopts the existing Switch identity instead of minting a new one', async () => {
    const result = await attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }]));

    expect(result.success).toBe(true);
    expect(h.createAgent).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'theirs',
        providerId: 'codex',
        switchAgentId: 'sw-theirs',
        apiEndpoint: 'https://switch.example.com',
        locationId: 'loc-1',
      })
    );
  });

  it('creates one row when concurrent paths adopt the same identity', async () => {
    const results = await Promise.all([
      attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }])),
      attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }])),
    ]);

    expect(results).toMatchObject([{ success: true }, { success: true }]);
    expect(h.createAgent).toHaveBeenCalledTimes(1);
  });

  it('writes nothing to the working directory', async () => {
    // The workspace belongs to whichever install set the agent up. Any write
    // here rejects, so this passing means attach touched none of their state.
    await expect(
      attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }]))
    ).resolves.toMatchObject({ success: true });
  });

  it('fails loudly when the identity no longer exists on the server', async () => {
    // Minting a replacement here would create exactly the duplicate agent this
    // feature exists to avoid.
    h.state.existsOnServer = false;

    const result = await attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }]));

    expect(result).toMatchObject({
      success: false,
      error: { type: 'switch-agent-not-on-server', agentId: 'sw-theirs' },
    });
    expect(h.createAgent).not.toHaveBeenCalled();
  });

  it('reports an unauthenticated server rather than throwing', async () => {
    h.state.existsThrows = new h.GatewayError('unauthorized');

    expect(
      await attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }]))
    ).toMatchObject({ success: false, error: { type: 'switch-server-unauthenticated' } });
    expect(h.createAgent).not.toHaveBeenCalled();
  });

  it('refuses a name that is no longer configured in the directory', async () => {
    const result = await attachConfiguredAgents(params([{ name: 'ghost', providerId: 'codex' }]));

    expect(result.success).toBe(false);
    expect(h.createAgent).not.toHaveBeenCalled();
  });

  it('skips an agent this Switch Console already has', async () => {
    h.state.agentNamesByServer = { 'srv-1': [{ name: 'theirs', switchAgentId: 'sw-theirs' }] };

    const result = await attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }]));

    expect(result.success).toBe(false);
    expect(h.createAgent).not.toHaveBeenCalled();
  });

  it('attaches an agent already attached to a different server (CHOO-2044)', async () => {
    // Same directory, same name, other server — a separate agent, not a duplicate.
    h.state.agentNamesByServer = { 'srv-other': [{ name: 'theirs', switchAgentId: 'sw-theirs' }] };

    const result = await attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }]));

    expect(result.success).toBe(true);
    expect(h.createAgent).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'theirs', serverId: 'srv-1' })
    );
  });

  it('inherits autoApprove from an existing sibling at the same location', async () => {
    h.getAgents.mockResolvedValue([
      { id: 'agent-sibling', locationId: 'loc-1', autoApprove: true },
    ]);

    await attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }], null));

    expect(h.createAgent).toHaveBeenCalledWith(expect.objectContaining({ autoApprove: true }));
  });

  it('defaults autoApprove to false for a local attach with no sibling opted in', async () => {
    h.getAgents.mockResolvedValue([
      { id: 'agent-sibling', locationId: 'loc-1', autoApprove: false },
    ]);

    await attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }], null));

    expect(h.createAgent).toHaveBeenCalledWith(expect.objectContaining({ autoApprove: false }));
  });

  it('defaults autoApprove to true for a manual SSH-remote attach with no siblings', async () => {
    // A headless tmux session on a remote host has no easy way to answer an
    // interactive permission prompt, so a remote agent gets unattended
    // operation on attach even before it has any siblings there.
    await attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }], 'vm-1'));

    expect(h.createAgent).toHaveBeenCalledWith(expect.objectContaining({ autoApprove: true }));
  });

  it('honors the caller-chosen provider even when the gateway reports no known agent type', async () => {
    // Discovery can't infer a provider from disk for an agent registered
    // through the plain Agent Bridge, so the caller (the user, via the UI)
    // picks one. That pick must win over an unsupported/missing gateway type
    // rather than the agent being silently dropped.
    h.state.knownAgentType = null;

    const result = await attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }]));

    expect(result.success).toBe(true);
    expect(h.createAgent).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'theirs', providerId: 'codex' })
    );
    expect(h.warn).not.toHaveBeenCalled();
  });

  it('drops an automatically discovered agent with no caller-supplied provider when the gateway type is unsupported', async () => {
    h.state.knownAgentType = null;

    const result = await adoptConfiguredAgent({
      location: { id: 'loc-1', name: 'repo', dir: '/repo', sshHost: null },
      serverId: 'srv-1',
      discovered: {
        name: 'theirs',
        switchAgentId: 'sw-theirs',
        apiEndpoint: 'https://switch.example.com',
        providerId: null,
        providerSource: 'unknown',
        alreadyAgent: false,
      },
    });

    expect(result).toMatchObject({ success: true, data: null });
    expect(h.createAgent).not.toHaveBeenCalled();
    expect(h.warn).toHaveBeenCalledWith(
      expect.stringContaining('unsupported or missing known agent type'),
      expect.anything()
    );
  });

  it('keeps the directory endpoint and warns when it differs from the chosen server', async () => {
    // One Switch server can be reachable at two URLs. The launch path reads the
    // endpoint from the same file as the token, so the directory's value is what
    // the session will really use — surfaced, never corrected.
    h.state.workspace = readOnlyFs({
      '.switch/agents/theirs.json': creds('sw-theirs', 'https://switch.internal:8443'),
    });

    await attachConfiguredAgents(params([{ name: 'theirs', providerId: 'codex' }]));

    expect(h.createAgent).toHaveBeenCalledWith(
      expect.objectContaining({ apiEndpoint: 'https://switch.internal:8443' })
    );
    expect(h.warn).toHaveBeenCalledWith(
      expect.stringContaining('endpoint differs'),
      expect.anything()
    );
  });
});
