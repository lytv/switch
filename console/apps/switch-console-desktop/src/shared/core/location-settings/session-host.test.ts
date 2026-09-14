import { describe, expect, it } from 'vitest';
import { resolveSessionHostForTransport } from './session-host';

describe('resolveSessionHostForTransport', () => {
  it('defaults local locations to pty', () => {
    const resolved = resolveSessionHostForTransport('local', {});
    expect(resolved.host).toBe('pty');
  });

  it('defaults ssh locations to tmux', () => {
    const resolved = resolveSessionHostForTransport('ssh', {});
    expect(resolved.host).toBe('tmux');
  });

  it('respects explicit legacy local tmux opt-in', () => {
    const resolved = resolveSessionHostForTransport('local', { tmux: true });
    expect(resolved.host).toBe('tmux');
  });

  it('throws when herdr is selected for a local transport', () => {
    expect(() =>
      resolveSessionHostForTransport('local', {
        sessionHost: 'herdr',
      })
    ).toThrow(/only for SSH locations/i);
  });

  it('resolves herdr settings defaults when host is herdr', () => {
    const resolved = resolveSessionHostForTransport('ssh', { sessionHost: 'herdr' });
    expect(resolved.host).toBe('herdr');
    expect(resolved.herdr).toEqual({
      sessionName: 'switchdash',
      protocolMin: 14,
      preferAgentPrompt: true,
      workspaceMode: 'flat',
    });
  });
});
