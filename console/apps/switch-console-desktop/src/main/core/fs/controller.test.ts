import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { FileWatchEvent } from '@shared/core/fs/fs';
import type { FileWatcher } from './types';

type WatchFn = (callback: (events: FileWatchEvent[]) => void) => FileWatcher;

const h = vi.hoisted(() => {
  return {
    runtimes: new Map<string, { fs: { watch?: WatchFn } }>(),
    resolveLocationRuntime: vi.fn((locationId: string) => h.runtimes.get(locationId) ?? null),
    emit: vi.fn(),
  };
});

vi.mock('@main/core/locations/utils', () => ({
  resolveLocationRuntime: h.resolveLocationRuntime,
}));
vi.mock('@main/lib/events', () => ({
  events: { emit: h.emit, on: vi.fn(() => () => {}) },
}));

function fakeWatcher() {
  let handler: ((events: FileWatchEvent[]) => void) | undefined;
  const close = vi.fn();
  const update = vi.fn();
  return {
    watch: vi.fn((callback: (events: FileWatchEvent[]) => void) => {
      handler = callback;
      return { update, close };
    }),
    close,
    update,
    fire: (events: FileWatchEvent[]) => handler?.(events),
  };
}

async function freshController() {
  vi.resetModules();
  return import('./controller');
}

describe('fs controller watcher lifecycle', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    h.runtimes.clear();
  });

  it('creates one underlying watcher shared by multiple subscribers on the same location', async () => {
    const { subscribeToLocationWatch } = await freshController();
    const watcher = fakeWatcher();
    h.runtimes.set('loc-1', { fs: { watch: watcher.watch } });

    subscribeToLocationWatch('loc-1', 'sub-a', vi.fn());
    subscribeToLocationWatch('loc-1', 'sub-b', vi.fn());

    expect(watcher.watch).toHaveBeenCalledTimes(1);
  });

  it('delivers native events to every subscriber and to the global event bus', async () => {
    const { subscribeToLocationWatch } = await freshController();
    const watcher = fakeWatcher();
    h.runtimes.set('loc-1', { fs: { watch: watcher.watch } });
    const subA = vi.fn();
    const subB = vi.fn();
    subscribeToLocationWatch('loc-1', 'sub-a', subA);
    subscribeToLocationWatch('loc-1', 'sub-b', subB);

    const events: FileWatchEvent[] = [
      { type: 'modify', entryType: 'file', path: '.switch/agents/theirs.json' },
    ];
    watcher.fire(events);

    expect(subA).toHaveBeenCalledWith(events);
    expect(subB).toHaveBeenCalledWith(events);
    expect(h.emit).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ locationId: 'loc-1', events })
    );
  });

  it('returns null and subscribes nobody when the location has no watch support', async () => {
    const { subscribeToLocationWatch } = await freshController();
    h.runtimes.set('loc-1', { fs: {} });

    const stop = subscribeToLocationWatch('loc-1', 'sub-a', vi.fn());

    expect(stop).toBeNull();
  });

  it('does not close the shared watcher while another subscriber is still attached', async () => {
    const { subscribeToLocationWatch } = await freshController();
    const watcher = fakeWatcher();
    h.runtimes.set('loc-1', { fs: { watch: watcher.watch } });
    const stopA = subscribeToLocationWatch('loc-1', 'sub-a', vi.fn());
    subscribeToLocationWatch('loc-1', 'sub-b', vi.fn());

    stopA?.();

    expect(watcher.close).not.toHaveBeenCalled();
  });

  it('closes the shared watcher once the last subscriber unsubscribes', async () => {
    const { subscribeToLocationWatch } = await freshController();
    const watcher = fakeWatcher();
    h.runtimes.set('loc-1', { fs: { watch: watcher.watch } });
    const stopA = subscribeToLocationWatch('loc-1', 'sub-a', vi.fn());
    const stopB = subscribeToLocationWatch('loc-1', 'sub-b', vi.fn());

    stopA?.();
    stopB?.();

    expect(watcher.close).toHaveBeenCalledTimes(1);
  });

  it('keeps the watcher open for a subscriber while watchSetPaths still holds a label on it', async () => {
    const { filesController, subscribeToLocationWatch } = await freshController();
    const watcher = fakeWatcher();
    h.runtimes.set('loc-1', { fs: { watch: watcher.watch } });
    const stopSubscriber = subscribeToLocationWatch('loc-1', 'sub-a', vi.fn());
    await filesController.watchSetPaths('loc-1', ['src'], 'browser-tab');

    stopSubscriber?.();

    expect(watcher.close).not.toHaveBeenCalled();

    await filesController.watchStop('loc-1', 'browser-tab');

    expect(watcher.close).toHaveBeenCalledTimes(1);
  });

  it('keeps the watcher open for watchSetPaths while a subscriber is still attached', async () => {
    const { filesController, subscribeToLocationWatch } = await freshController();
    const watcher = fakeWatcher();
    h.runtimes.set('loc-1', { fs: { watch: watcher.watch } });
    await filesController.watchSetPaths('loc-1', ['src'], 'browser-tab');
    const stopSubscriber = subscribeToLocationWatch('loc-1', 'sub-a', vi.fn());

    await filesController.watchStop('loc-1', 'browser-tab');

    expect(watcher.close).not.toHaveBeenCalled();

    stopSubscriber?.();

    expect(watcher.close).toHaveBeenCalledTimes(1);
  });

  it('updates the watcher with the union of every label group path set', async () => {
    const { filesController } = await freshController();
    const watcher = fakeWatcher();
    h.runtimes.set('loc-1', { fs: { watch: watcher.watch } });

    await filesController.watchSetPaths('loc-1', ['a'], 'tab-1');
    await filesController.watchSetPaths('loc-1', ['b'], 'tab-2');

    expect(watcher.update).toHaveBeenLastCalledWith(['a', 'b']);

    await filesController.watchStop('loc-1', 'tab-1');

    expect(watcher.update).toHaveBeenLastCalledWith(['b']);
  });

  it('reports unsupported when the location filesystem cannot watch', async () => {
    const { filesController } = await freshController();
    h.runtimes.set('loc-1', { fs: {} });

    const result = await filesController.watchSetPaths('loc-1', ['src']);

    expect(result).toMatchObject({ success: true, data: { supported: false } });
  });

  it('reports not_found when the location has no runtime at all', async () => {
    const { filesController } = await freshController();

    const result = await filesController.watchSetPaths('unknown-loc', ['src']);

    expect(result).toMatchObject({
      success: false,
      error: { type: 'not_found', entity: 'filesystem' },
    });
  });
});
