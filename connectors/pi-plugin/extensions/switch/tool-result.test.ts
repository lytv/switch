import { describe, expect, it } from 'vitest';
import { mapMcpToolResult, SwitchToolError } from './tool-result';

describe('mapMcpToolResult', () => {
  it('maps text content and structuredContent through to pi details', () => {
    const result = mapMcpToolResult({
      content: [{ type: 'text', text: 'posted' }],
      structuredContent: { room_id: '!room:example.org', agent_id: 'a1' },
    });

    expect(result).toEqual({
      content: [{ type: 'text', text: 'posted' }],
      details: { room_id: '!room:example.org', agent_id: 'a1' },
    });
  });

  it('defaults details to an empty object when there is no structuredContent', () => {
    const result = mapMcpToolResult({ content: [{ type: 'text', text: 'ok' }] });

    expect(result.details).toEqual({});
  });

  it('passes image content through unchanged (pi and MCP share the same shape)', () => {
    const result = mapMcpToolResult({
      content: [{ type: 'image', data: 'QUJD', mimeType: 'image/png' }],
    });

    expect(result.content).toEqual([{ type: 'image', data: 'QUJD', mimeType: 'image/png' }]);
  });

  it('falls back to a JSON text rendering for content types pi has no shape for', () => {
    const result = mapMcpToolResult({
      content: [{ type: 'resource', resource: { uri: 'mxc://x', text: 'y' } }],
    });

    expect(result.content).toHaveLength(1);
    const [item] = result.content;
    expect(item.type).toBe('text');
    expect(item.type === 'text' && item.text).toContain('mxc://x');
  });

  it('treats a missing content array as empty rather than throwing', () => {
    const result = mapMcpToolResult({});

    expect(result.content).toEqual([]);
  });

  it('throws SwitchToolError with the text content when isError is set', () => {
    expect(() =>
      mapMcpToolResult({
        content: [{ type: 'text', text: 'room not found' }],
        isError: true,
      })
    ).toThrow(SwitchToolError);

    try {
      mapMcpToolResult({ content: [{ type: 'text', text: 'room not found' }], isError: true });
      expect.unreachable();
    } catch (err) {
      expect((err as Error).message).toBe('room not found');
    }
  });

  it('throws a generic message when an error result carries no text content', () => {
    expect(() => mapMcpToolResult({ isError: true })).toThrow('Switch tool call failed');
  });
});
