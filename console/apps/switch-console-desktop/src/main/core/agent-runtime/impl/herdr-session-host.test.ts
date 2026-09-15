import { describe, expect, it, vi } from 'vitest';
import {
  createHerdrPane,
  isHerdrPaneLive,
  mapHerdrAgentStatus,
  readHerdrPromptStatus,
} from './herdr-session-host';

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

describe('createHerdrPane', () => {
  it.each([
    ['flat', 'switchdash'],
    ['per-agent', 'switchdash-agent-1'],
    ['per-room', 'switchdash-room-1'],
    ['per-task', 'switchdash-session-1'],
  ] as const)('uses %s workspace isolation', async (workspaceMode, expectedWorkspace) => {
    const exec = vi.fn(async (_command: string, args: string[]) => {
      if (args[0] === 'workspace')
        return {
          stdout: JSON.stringify({
            result: {
              workspace: { workspace_id: 'ws-1', label: expectedWorkspace },
              tab: { tab_id: 'tab-1' },
              root_pane: { pane_id: 'pane-1', tab_id: 'tab-1', workspace_id: 'ws-1' },
            },
          }),
          stderr: '',
        };
      return {
        stdout: JSON.stringify({
          result: {
            tab: { tab_id: 'tab-2' },
            root_pane: { pane_id: 'pane-2', tab_id: 'tab-2', workspace_id: 'ws-1' },
          },
        }),
        stderr: '',
      };
    });

    await createHerdrPane(
      exec,
      {
        sessionName: 'switchdash',
        protocolMin: 14,
        preferAgentPrompt: true,
        workspaceMode,
      },
      {
        cwd: '/repo',
        tabLabel: 'switchdash-session-1',
        agentSlug: 'agent-1',
        roomId: 'room-1',
        sessionId: 'session-1',
      }
    );

    expect(exec).toHaveBeenCalledWith('herdr', [
      'workspace',
      'create',
      '--label',
      expectedWorkspace,
      '--cwd',
      '/repo',
      '--no-focus',
    ]);
  });
});
