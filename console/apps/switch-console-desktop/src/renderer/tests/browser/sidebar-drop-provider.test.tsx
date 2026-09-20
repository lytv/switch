/**
 * Sidebar folder-drop provider id.
 *
 * inspectLocationPath's inference is unit-tested in Node. This file checks the
 * renderer mapping: the real drop hook, a simulated Files drop, and the
 * providerId handed to createAgent. It uses the browser (Playwright/Chromium)
 * project already used by sibling renderer tests, not a live Electron app.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useSidebarDrop } from '@renderer/features/sidebar/use-sidebar-drop';
import type { LocationPathInspection } from '@shared/core/locations/locations';

const mocks = vi.hoisted(() => ({
  inspectLocationPath: vi.fn(),
  createAgent: vi.fn(),
  toast: vi.fn(),
  navigate: vi.fn(),
  activeServerId: 'server-1' as string | null,
}));

vi.mock('@renderer/lib/ipc', () => ({
  events: { on: vi.fn() },
  rpc: {
    locations: { inspectLocationPath: mocks.inspectLocationPath },
  },
}));

vi.mock('@renderer/features/locations/stores/location-selectors', () => ({
  getLocationManagerStore: () => ({ createAgent: mocks.createAgent }),
}));

vi.mock('@renderer/features/switch-servers/switch-servers-store', () => ({
  switchServersStore: {
    get activeServerId() {
      return mocks.activeServerId;
    },
  },
}));

vi.mock('@renderer/lib/layout/navigation-provider', () => ({
  useNavigate: () => ({ navigate: mocks.navigate }),
}));

vi.mock('@renderer/lib/hooks/use-toast', () => ({
  useToast: () => ({ toast: mocks.toast }),
}));

vi.mock('@renderer/utils/logger', () => ({
  log: { error: vi.fn(), warn: vi.fn(), info: vi.fn() },
}));

let container: HTMLDivElement | null = null;
let root: Root | null = null;

function DropSurface() {
  const { onDrop, onDragOver, onDragEnter, onDragLeave } = useSidebarDrop();
  return (
    <div
      data-testid="drop-surface"
      onDrop={onDrop}
      onDragOver={onDragOver}
      onDragEnter={onDragEnter}
      onDragLeave={onDragLeave}
    />
  );
}

async function renderSurface(): Promise<HTMLDivElement> {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
  await act(async () => {
    root!.render(<DropSurface />);
  });
  return container;
}

function inspection(overrides: Partial<LocationPathInspection> = {}): LocationPathInspection {
  return {
    isDirectory: true,
    switchAgent: {
      agentId: 'sw-1',
      apiEndpoint: 'https://switch.example.com',
      dir: '/tmp/agent',
    },
    providerId: null,
    ...overrides,
  };
}

function dropFiles(surface: HTMLElement, paths: string[]): void {
  const dt = new DataTransfer();
  for (const filePath of paths) {
    const name = filePath.split('/').pop() ?? filePath;
    dt.items.add(new File([''], name));
  }
  surface.dispatchEvent(
    new DragEvent('drop', { bubbles: true, cancelable: true, dataTransfer: dt })
  );
}

describe('sidebar drop maps inspect.providerId onto createAgent', () => {
  beforeEach(() => {
    mocks.activeServerId = 'server-1';
    mocks.inspectLocationPath.mockReset();
    mocks.createAgent.mockReset();
    mocks.toast.mockReset();
    mocks.navigate.mockReset();
    mocks.createAgent.mockResolvedValue('loc-1');
    Object.assign(window.electronAPI, {
      getPathForFile: (file: File) => `/tmp/${file.name}`,
    });
  });

  afterEach(async () => {
    if (root) await act(async () => root!.unmount());
    container?.remove();
    container = null;
    root = null;
  });

  async function dropOnSurface(path: string): Promise<void> {
    const el = await renderSurface();
    const surface = el.querySelector('[data-testid="drop-surface"]');
    if (!surface) throw new Error('drop surface not rendered');
    await act(async () => {
      dropFiles(surface as HTMLElement, [path]);
    });
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
  }

  it('passes an inferred codex providerId through to createAgent', async () => {
    mocks.inspectLocationPath.mockResolvedValue(inspection({ providerId: 'codex' }));

    await dropOnSurface('/tmp/agent');

    expect(mocks.createAgent).toHaveBeenCalledWith({
      mode: 'pick',
      name: 'agent',
      path: '/tmp/agent',
      serverId: 'server-1',
      providerId: 'codex',
    });
  });

  it('passes an inferred opencode providerId through to createAgent', async () => {
    mocks.inspectLocationPath.mockResolvedValue(
      inspection({
        providerId: 'opencode',
        switchAgent: {
          agentId: 'sw-open',
          apiEndpoint: 'https://switch.example.com',
          dir: '/tmp/open-hoot',
        },
      })
    );

    await dropOnSurface('/tmp/open-hoot');

    expect(mocks.createAgent).toHaveBeenCalledWith(
      expect.objectContaining({ path: '/tmp/open-hoot', providerId: 'opencode' })
    );
  });

  it('uses claude only when inspect.providerId is null', async () => {
    mocks.inspectLocationPath.mockResolvedValue(inspection({ providerId: null }));

    await dropOnSurface('/tmp/agent');

    expect(mocks.createAgent).toHaveBeenCalledWith(
      expect.objectContaining({ providerId: 'claude' })
    );
  });
});
