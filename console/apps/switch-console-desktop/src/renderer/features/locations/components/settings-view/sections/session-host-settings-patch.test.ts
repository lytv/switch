import { describe, expect, it } from 'vitest';
import type { LocationSettings } from '@shared/core/location-settings/location-settings';
import { withSessionHostPatch } from './session-host-settings-patch';

function baseSettings(overrides: Partial<LocationSettings> = {}): LocationSettings {
  return {
    worktreeDirectory: '/repo',
    preservePatterns: ['.env'],
    ...overrides,
  };
}

describe('withSessionHostPatch', () => {
  it('sets sessionHost without touching unrelated fields', () => {
    const current = baseSettings();
    const next = withSessionHostPatch(current, { sessionHost: 'herdr' });
    expect(next.sessionHost).toBe('herdr');
    expect(next.worktreeDirectory).toBe('/repo');
    expect(next.preservePatterns).toEqual(['.env']);
  });

  it('merges a herdr patch into the existing herdr settings rather than replacing them', () => {
    const current = baseSettings({
      sessionHost: 'herdr',
      herdr: { sessionName: 'mine', preferAgentPrompt: false },
    });
    const next = withSessionHostPatch(current, { herdr: { protocolMin: 16 } });
    expect(next.herdr).toEqual({
      sessionName: 'mine',
      preferAgentPrompt: false,
      protocolMin: 16,
    });
  });

  it('clears a herdr field back to default when patched with undefined', () => {
    const current = baseSettings({ sessionHost: 'herdr', herdr: { sessionName: 'mine' } });
    const next = withSessionHostPatch(current, { herdr: { sessionName: undefined } });
    expect(next.herdr?.sessionName).toBeUndefined();
  });

  it('leaves herdr settings untouched when only sessionHost changes', () => {
    const current = baseSettings({ sessionHost: 'tmux', herdr: { sessionName: 'mine' } });
    const next = withSessionHostPatch(current, { sessionHost: 'herdr' });
    expect(next.herdr).toEqual({ sessionName: 'mine' });
  });

  it('is a no-op copy when the patch is empty', () => {
    const current = baseSettings({ sessionHost: 'pty' });
    expect(withSessionHostPatch(current, {})).toEqual(current);
  });
});
