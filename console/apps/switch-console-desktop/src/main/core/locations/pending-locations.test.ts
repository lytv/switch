import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import type os from 'node:os';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mockOpenLocationFolder = vi.fn();
vi.mock('./open-location', () => ({ openLocationFolder: mockOpenLocationFolder }));
vi.mock('@main/lib/logger', () => ({
  log: { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} },
}));

const state = vi.hoisted(() => ({ home: '' }));
vi.mock('node:os', async (importOriginal) => {
  const actual = await importOriginal<typeof os>();
  return { ...actual, default: { ...actual, homedir: () => state.home } };
});

const { processPendingLocations } = await import('./pending-locations');

function pendingFilePath(): string {
  return join(state.home, '.config', 'switch-axi', 'pending-locations.json');
}

async function writePending(dirs: string[]): Promise<void> {
  await mkdir(join(state.home, '.config', 'switch-axi'), { recursive: true });
  await writeFile(pendingFilePath(), JSON.stringify({ dirs }), 'utf8');
}

beforeEach(async () => {
  vi.clearAllMocks();
  state.home = await mkdtemp(join(tmpdir(), 'switch-console-home-'));
});

describe('processPendingLocations', () => {
  it('does nothing when no pending-locations.json exists', async () => {
    await expect(processPendingLocations()).resolves.toBeUndefined();
    expect(mockOpenLocationFolder).not.toHaveBeenCalled();
  });

  it('opens each pending dir and removes the file once all succeed', async () => {
    await writePending(['/a', '/b']);
    mockOpenLocationFolder.mockResolvedValue({ success: true, data: {} });

    await processPendingLocations();

    expect(mockOpenLocationFolder).toHaveBeenCalledWith('/a');
    expect(mockOpenLocationFolder).toHaveBeenCalledWith('/b');
    expect(existsSync(pendingFilePath())).toBe(false);
  });

  it('leaves a failed dir in the list and drops the succeeded one', async () => {
    await writePending(['/a', '/b']);
    mockOpenLocationFolder.mockImplementation(async (dir: string) =>
      dir === '/a'
        ? { success: false, error: { type: 'invalid-directory', dir } }
        : { success: true, data: {} }
    );

    await processPendingLocations();

    const remaining = JSON.parse(await readFile(pendingFilePath(), 'utf8'));
    expect(remaining).toEqual({ dirs: ['/a'] });
  });

  it('leaves the file untouched on invalid JSON', async () => {
    await mkdir(join(state.home, '.config', 'switch-axi'), { recursive: true });
    await writeFile(pendingFilePath(), 'not json', 'utf8');

    await processPendingLocations();

    expect(mockOpenLocationFolder).not.toHaveBeenCalled();
    expect(await readFile(pendingFilePath(), 'utf8')).toBe('not json');
  });
});
