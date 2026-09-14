import { describe, expect, it, vi } from 'vitest';
import { isHerdrPaneLive, mapHerdrAgentStatus, readHerdrPromptStatus } from './herdr-session-host';

describe('mapHerdrAgentStatus', () => {
  it.each([
    ['idle', 'idle'],
    ['working', 'working'],
    ['blocked', 'awaiting-input'],
    ['done', 'completed'],
    ['unknown', 'error'],
  ] as const)('maps %s to %s', (input, expected) => {
    expect(mapHerdrAgentStatus(input)).toBe(expected);
  });
});

describe('readHerdrPromptStatus', () => {
  it('checks liveness with the exact pane id', async () => {
    const exec = vi.fn(async () => ({
      stdout: JSON.stringify({ result: { pane: { pane_id: 'pane-1' } } }),
      stderr: '',
    }));

    await expect(isHerdrPaneLive(exec, 'pane-1')).resolves.toBe(true);
    expect(exec).toHaveBeenCalledWith('herdr', ['pane', 'get', 'pane-1']);
  });

  it('reads the current agent state from the exact pane', async () => {
    const exec = vi.fn(async () => ({
      stdout: JSON.stringify({
        result: {
          agent: { agent: 'codex', agent_status: 'working', pane_id: 'pane-1' },
        },
      }),
      stderr: '',
    }));

    await expect(readHerdrPromptStatus(exec, 'pane-1')).resolves.toEqual({
      agentId: 'pane-1',
      recognizedKind: true,
      blocked: false,
      runtimeStatus: 'working',
    });
    expect(exec).toHaveBeenCalledWith('herdr', ['agent', 'get', 'pane-1']);
  });

  it('fails closed when Herdr reports an unknown state', async () => {
    const exec = vi.fn(async () => ({
      stdout: JSON.stringify({ result: { agent: { agent: 'codex', agent_status: 'unknown' } } }),
      stderr: '',
    }));

    await expect(readHerdrPromptStatus(exec, 'pane-1')).resolves.toMatchObject({
      blocked: true,
      runtimeStatus: 'error',
    });
  });
});
