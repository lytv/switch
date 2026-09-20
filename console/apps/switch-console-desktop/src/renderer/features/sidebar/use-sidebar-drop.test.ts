import { JSDOM } from 'jsdom';
import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { LocationPathInspection } from '@shared/core/locations/locations';

(
  globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT?: boolean }
).IS_REACT_ACT_ENVIRONMENT = true;

const mocks = vi.hoisted(() => ({
  inspectLocationPath: vi.fn(),
  createAgent: vi.fn(),
  toast: vi.fn(),
  navigate: vi.fn(),
  getDraggedFilePaths: vi.fn(),
  activeServerId: 'server-1' as string | null,
}));

vi.mock('@renderer/lib/ipc', () => ({
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

vi.mock('@renderer/lib/drag-files', () => ({
  hasDraggedFiles: () => true,
  getDraggedFilePaths: mocks.getDraggedFilePaths,
}));

vi.mock('@renderer/utils/logger', () => ({
  log: { error: vi.fn(), warn: vi.fn(), info: vi.fn() },
}));

const { useSidebarDrop } = await import('./use-sidebar-drop');

type SidebarDrop = ReturnType<typeof useSidebarDrop>;

function dropEvent(): React.DragEvent {
  return {
    preventDefault: vi.fn(),
    dataTransfer: { files: [] },
  } as unknown as React.DragEvent;
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

describe('useSidebarDrop provider id', () => {
  let dom: JSDOM;
  let root: Root;
  let container: HTMLDivElement;
  let latest: SidebarDrop | null;

  function Probe() {
    latest = useSidebarDrop();
    return null;
  }

  beforeEach(() => {
    latest = null;
    mocks.activeServerId = 'server-1';
    mocks.inspectLocationPath.mockReset();
    mocks.createAgent.mockReset();
    mocks.toast.mockReset();
    mocks.navigate.mockReset();
    mocks.getDraggedFilePaths.mockReset();
    mocks.createAgent.mockResolvedValue('loc-1');
    mocks.getDraggedFilePaths.mockReturnValue(['/tmp/agent']);

    dom = new JSDOM('<!doctype html><html><body><div id="root"></div></body></html>');
    vi.stubGlobal('window', dom.window);
    vi.stubGlobal('document', dom.window.document);
    vi.stubGlobal('HTMLElement', dom.window.HTMLElement);
    container = dom.window.document.getElementById('root') as HTMLDivElement;
    root = createRoot(container);
  });

  afterEach(async () => {
    await act(async () => {
      root.unmount();
    });
    vi.unstubAllGlobals();
    dom.window.close();
  });

  async function dropFolder(): Promise<void> {
    await act(async () => {
      root.render(React.createElement(Probe));
    });
    await act(async () => {
      latest!.onDrop(dropEvent());
    });
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
  }

  it('registers a dropped Codex folder with the inferred provider, not claude', async () => {
    mocks.inspectLocationPath.mockResolvedValue(inspection({ providerId: 'codex' }));

    await dropFolder();

    expect(mocks.createAgent).toHaveBeenCalledWith({
      mode: 'pick',
      name: 'agent',
      path: '/tmp/agent',
      serverId: 'server-1',
      providerId: 'codex',
    });
  });

  it('registers a dropped OpenCode folder with the inferred provider', async () => {
    mocks.getDraggedFilePaths.mockReturnValue(['/tmp/open-hoot']);
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

    await dropFolder();

    expect(mocks.createAgent).toHaveBeenCalledWith(
      expect.objectContaining({ path: '/tmp/open-hoot', providerId: 'opencode' })
    );
  });

  it('falls back to claude when inspect finds a Switch agent but no provider', async () => {
    mocks.inspectLocationPath.mockResolvedValue(inspection({ providerId: null }));

    await dropFolder();

    expect(mocks.createAgent).toHaveBeenCalledWith(
      expect.objectContaining({ providerId: 'claude' })
    );
  });
});
