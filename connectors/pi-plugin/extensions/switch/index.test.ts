import { describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  connectSwitchRuntime: vi.fn(),
  registerSwitchTools: vi.fn(),
}));

vi.mock('./mcp-bridge', () => mocks);

import switchExtension from './index';

describe('switchExtension', () => {
  it('handles a room event during runtime startup', async () => {
    const bridge = { client: { listTools: vi.fn() }, sessionPid: 1, close: vi.fn() };
    mocks.connectSwitchRuntime.mockImplementation((onEvent) => {
      onEvent('ready', { room_id: 'room' });
      return Promise.resolve(bridge);
    });
    mocks.registerSwitchTools.mockResolvedValue(undefined);

    const handlers = new Map<string, (...args: never[]) => unknown>();
    const pi = {
      on: vi.fn((event: string, handler: (...args: never[]) => unknown) => handlers.set(event, handler)),
      registerCommand: vi.fn(),
      sendUserMessage: vi.fn(),
    };
    switchExtension(pi as never);

    await handlers.get('session_start')?.({} as never, { ui: { notify: vi.fn() } } as never);

    expect(mocks.connectSwitchRuntime).toHaveBeenCalledBefore(mocks.registerSwitchTools);
    expect(pi.sendUserMessage).toHaveBeenCalledWith('[Switch] room room: ready');
  });
});
