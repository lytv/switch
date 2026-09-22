import { describe, expect, it } from 'vitest';
import { toStringEnv } from './env';

describe('toStringEnv', () => {
  it('drops undefined values', () => {
    expect(toStringEnv({ PATH: '/usr/bin', UNSET: undefined })).toEqual({ PATH: '/usr/bin' });
  });

  it('keeps every string value, including SWITCH_* credentials', () => {
    const env = {
      SWITCH_API_ENDPOINT: 'https://switch.example',
      SWITCH_API_TOKEN: 'tok',
      SWITCH_AGENT_ID: 'agent-1',
      PATH: '/usr/bin',
    };

    expect(toStringEnv(env)).toEqual(env);
  });

  it('returns an empty object for an empty environment', () => {
    expect(toStringEnv({})).toEqual({});
  });
});
