/**
 * Maps an MCP `CallToolResult` (from the Switch agent runtime) to the shape
 * pi's `execute()` expects.
 *
 * Kept separate from the MCP SDK's own types so this stays a plain structural
 * mapping that tests can exercise without spawning a real client.
 */

export interface McpContentItem {
  type: string;
  text?: string;
  data?: string;
  mimeType?: string;
  [key: string]: unknown;
}

export interface McpToolResult {
  content?: McpContentItem[];
  structuredContent?: Record<string, unknown>;
  isError?: boolean;
}

// Pi's own `TextContent` / `ImageContent` (from `@earendil-works/pi-ai`, as
// re-exported through `AgentToolResult` in `@earendil-works/pi-agent-core`)
// happen to use the same field names MCP's content blocks do - `{type:
// "image", data, mimeType}`, not the `{source: {type: "base64", ...}}` shape
// used for multimodal *user message* content elsewhere in pi's API - so this
// mapping is close to the identity function for the two content types pi
// tool results support.
export type PiContentItem = { type: 'text'; text: string } | { type: 'image'; data: string; mimeType: string };

export interface PiToolResult {
  content: PiContentItem[];
  details: Record<string, unknown>;
}

/** A tool result the MCP server reported as an error - thrown, never returned. */
export class SwitchToolError extends Error {}

function mapContentItem(item: McpContentItem): PiContentItem {
  if (item.type === 'text' && typeof item.text === 'string') {
    return { type: 'text', text: item.text };
  }
  if (item.type === 'image' && typeof item.data === 'string' && typeof item.mimeType === 'string') {
    return { type: 'image', data: item.data, mimeType: item.mimeType };
  }
  // Anything else (resource, resource_link, audio, ...) has no pi equivalent;
  // fall back to a text rendering rather than dropping it silently.
  return { type: 'text', text: JSON.stringify(item) };
}

/**
 * Convert a Switch MCP tool result into pi's tool result shape.
 *
 * pi tools signal failure by throwing, not by an `isError` field on the
 * return value (see `execute()` in pi's `ToolDefinition`), so an
 * `isError: true` result is thrown here as `SwitchToolError` rather than
 * returned.
 */
export function mapMcpToolResult(result: McpToolResult): PiToolResult {
  const content = (result.content ?? []).map(mapContentItem);

  if (result.isError) {
    const message = content
      .map((item) => (item.type === 'text' ? item.text : JSON.stringify(item)))
      .join('\n');
    throw new SwitchToolError(message || 'Switch tool call failed');
  }

  return { content, details: result.structuredContent ?? {} };
}
