import { describe, expect, it } from 'vitest';
import {
  chooseDeliveryMode,
  drainLines,
  encodeJsonRpc,
  formatChannelEvent,
  hookPathForTool,
  isConsoleManaged,
  mapMcpToolResult,
  RUNTIME_PACKAGE,
  SwitchToolError,
} from './switch-connector';

describe('console extension framing', () => {
  it('encodes one JSON value per line', () => {
    expect(encodeJsonRpc({ jsonrpc: '2.0', id: 1 })).toBe('{"jsonrpc":"2.0","id":1}\n');
  });

  it('splits complete lines and keeps a partial tail', () => {
    expect(drainLines('{"a":1}\n{"b":2')).toEqual({ lines: ['{"a":1}'], rest: '{"b":2' });
  });

  it('drops blank lines and carriage returns', () => {
    expect(drainLines('\n{"a":1}\r\n')).toEqual({ lines: ['{"a":1}'], rest: '' });
  });
});

describe('console extension tool results', () => {
  it('passes text and image content through', () => {
    expect(
      mapMcpToolResult({
        content: [
          { type: 'text', text: 'hello' },
          { type: 'image', data: 'abc', mimeType: 'image/png' },
        ],
        structuredContent: { room: 'x' },
      })
    ).toEqual({
      content: [
        { type: 'text', text: 'hello' },
        { type: 'image', data: 'abc', mimeType: 'image/png' },
      ],
      details: { room: 'x' },
    });
  });

  it('renders unknown content as text rather than dropping it', () => {
    const item = { type: 'resource', uri: 'file:///x' };
    expect(mapMcpToolResult({ content: [item] }).content).toEqual([
      { type: 'text', text: JSON.stringify(item) },
    ]);
  });

  it('throws on error results instead of returning them', () => {
    expect(() => mapMcpToolResult({ content: [{ type: 'text', text: 'nope' }], isError: true })).toThrow(
      SwitchToolError
    );
  });
});

describe('console extension events', () => {
  it('frames the channel line with the room id', () => {
    expect(formatChannelEvent('hi', { room_id: 'room1' })).toBe('[Switch] room room1: hi');
  });

  it('appends attachment paths instead of dropping them', () => {
    expect(formatChannelEvent('hi', { room_id: 'r', image_path: '/tmp/a.png' })).toContain(
      'image_path=/tmp/a.png'
    );
  });

  it('delivers fresh turns when idle and steers into running ones', () => {
    expect(chooseDeliveryMode(true)).toBe('immediate');
    expect(chooseDeliveryMode(false)).toBe('steer');
  });
});

describe('console extension hooks', () => {
  it('maps only the tools with a post-call hook', () => {
    expect(hookPathForTool('read_context')).toBe('/read-context');
    expect(hookPathForTool('assume_role')).toBe('/assume-role');
    expect(hookPathForTool('release_role')).toBe('/release-role');
    expect(hookPathForTool('connect_to_room')).toBeNull();
    expect(hookPathForTool('post_message')).toBeNull();
  });

  it('treats Console-launched sessions as managed', () => {
    expect(isConsoleManaged({ SWITCHDASH_HOOK_PORT: '1234' })).toBe(true);
    expect(isConsoleManaged({ SWITCHDASH_PTY_ID: 'pty-1' })).toBe(true);
    expect(isConsoleManaged({})).toBe(false);
  });

  it('pins an exact runtime version', () => {
    expect(RUNTIME_PACKAGE).toMatch(/^@sandboxaq\/switch-agent-runtime@\d+\.\d+\.\d+$/);
  });
});
