import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const h = vi.hoisted(() => ({
  definitionNamesByProvider: {} as Record<string, string[]>,
}));

vi.mock('../store', () => ({
  getLocationByHostDir: vi.fn(async () => undefined),
}));

vi.mock('@main/core/agents/getAgents', () => ({
  getLocationAgentsOnServer: vi.fn(async () => []),
}));

vi.mock('@main/core/agents/agent-workspace-fs', async () => {
  const { createPluginFs } = await import('@main/core/providers/plugin-fs');
  return {
    resolveWorkspaceFsFor: async (_sshHost: string | null, dir: string) => ({
      fs: createPluginFs(dir),
      homeFs: createPluginFs(dir),
      close: () => {},
    }),
  };
});

vi.mock('@main/core/providers/plugin-registry', () => ({
  listPlugins: () =>
    Object.entries(h.definitionNamesByProvider).map(([id, names]) => ({
      metadata: { id },
      behavior: {
        repoAgents: {
          discoverDefinitions: async () => names.map((name) => ({ name, description: null })),
        },
      },
    })),
}));

vi.mock('@main/lib/logger', () => ({
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

const { inspectLocationPath } = await import('./inspect-location-path');

async function writeStoreEntry(dir: string, name: string, agentId: string): Promise<void> {
  const storeDir = path.join(dir, '.switch', 'agents');
  await fs.mkdir(storeDir, { recursive: true });
  await fs.writeFile(
    path.join(storeDir, `${name}.json`),
    JSON.stringify({
      env: {
        SWITCH_API_ENDPOINT: 'https://switch.example.com',
        SWITCH_API_TOKEN: 'tok',
        SWITCH_AGENT_ID: agentId,
      },
    }),
    'utf8'
  );
}

async function writeLaunchSpec(dir: string, name: string, providerId: string): Promise<void> {
  const specDir = path.join(dir, '.switchdash', 'agents', name);
  await fs.mkdir(specDir, { recursive: true });
  await fs.writeFile(
    path.join(specDir, 'agent-launch-spec.json'),
    JSON.stringify({ command: providerId, args: [], env: {}, cwd: dir, providerId }),
    'utf8'
  );
}

describe('inspectLocationPath provider inference (unit, on-disk signals only)', () => {
  let dir: string;

  beforeEach(async () => {
    dir = await fs.mkdtemp(path.join(os.tmpdir(), 'switch-console-inspect-'));
    h.definitionNamesByProvider = {};
  });

  afterEach(async () => {
    await fs.rm(dir, { recursive: true, force: true });
  });

  it('infers codex from a launch spec on disk', async () => {
    await writeStoreEntry(dir, 'codex-hoot', 'sw-codex');
    await writeLaunchSpec(dir, 'codex-hoot', 'codex');

    await expect(inspectLocationPath({ path: dir })).resolves.toMatchObject({
      isDirectory: true,
      switchAgent: { agentId: 'sw-codex', apiEndpoint: 'https://switch.example.com', dir },
      providerId: 'codex',
    });
  });

  it('infers opencode from a launch spec on disk', async () => {
    await writeStoreEntry(dir, 'open-hoot', 'sw-open');
    await writeLaunchSpec(dir, 'open-hoot', 'opencode');

    await expect(inspectLocationPath({ path: dir })).resolves.toMatchObject({
      providerId: 'opencode',
      switchAgent: { agentId: 'sw-open' },
    });
  });

  it('returns null when nothing on disk names a provider', async () => {
    await writeStoreEntry(dir, 'mystery', 'sw-my');

    await expect(inspectLocationPath({ path: dir })).resolves.toMatchObject({
      switchAgent: { agentId: 'sw-my' },
      providerId: null,
    });
  });

  it('falls back to the owning provider definition when there is no launch spec', async () => {
    h.definitionNamesByProvider = { opencode: ['open-hoot'] };
    await writeStoreEntry(dir, 'open-hoot', 'sw-open');

    await expect(inspectLocationPath({ path: dir })).resolves.toMatchObject({
      providerId: 'opencode',
    });
  });

  it('prefers the launch spec over a definition', async () => {
    h.definitionNamesByProvider = { claude: ['hoot'] };
    await writeStoreEntry(dir, 'hoot', 'sw-1');
    await writeLaunchSpec(dir, 'hoot', 'codex');

    await expect(inspectLocationPath({ path: dir })).resolves.toMatchObject({
      providerId: 'codex',
    });
  });

  it('returns no provider when the directory is not a Switch agent', async () => {
    await expect(inspectLocationPath({ path: dir })).resolves.toEqual({
      isDirectory: true,
      existingLocation: undefined,
      switchAgent: null,
      providerId: null,
    });
  });
});
