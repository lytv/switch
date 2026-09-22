/**
 * Wires the Switch agent runtime into pi as a native tool surface.
 *
 * pi has no built-in MCP client (see `docs/usage.md`: "It intentionally does
 * not include built-in MCP..."), unlike OpenCode, Codex and Claude Code,
 * which all register `@sandboxaq/switch-agent-runtime` as an MCP server
 * through host-native config. So this module IS the MCP client: it spawns
 * the runtime's `./bin` entry over stdio with the MCP SDK's own
 * `StdioClientTransport`, fetches its tool catalog, and re-registers each
 * tool as a native pi tool that proxies to `client.callTool()`.
 *
 * This reuses the runtime wholesale - the Agent Protocol, its SSE stream,
 * credential resolution and the operation catalog all stay inside the
 * spawned process. Nothing here re-implements any of it; this module only
 * bridges MCP tool calls and MCP notifications to pi's own tool and message
 * APIs.
 */

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import type { Notification, Tool } from '@modelcontextprotocol/sdk/types.js';
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
import { toStringEnv } from './env';
import { hookPathForTool, notifyRuntimeHook } from './hooks';
import { mapMcpToolResult, type McpToolResult } from './tool-result';

/**
 * Pinned to the same version the other connectors register
 * (`connectors/{claude-code,codex,opencode}-plugin`). Bump all four together.
 */
export const RUNTIME_PACKAGE = '@sandboxaq/switch-agent-runtime@0.4.1';

export interface SwitchBridge {
  client: Client;
  transport: StdioClientTransport;
  /** This process's pid - the session id the spawned runtime resolves its hook-listener directory from. */
  sessionPid: number;
  close(): Promise<void>;
}

/** Spawn the runtime and connect to it as an MCP client. */
export async function connectSwitchRuntime(): Promise<SwitchBridge> {
  const transport = new StdioClientTransport({
    command: 'npx',
    args: ['-y', RUNTIME_PACKAGE],
    // See env.ts: without this the child gets a bare safe-list environment,
    // not this process's SWITCH_* credentials or even PATH for npx itself.
    env: toStringEnv(process.env),
  });

  const client = new Client({ name: 'pi-switch-connector', version: '0.1.0' }, { capabilities: {} });
  await client.connect(transport);

  return {
    client,
    transport,
    sessionPid: process.pid,
    async close() {
      await client.close();
    },
  };
}

/**
 * Register every tool the runtime reports as a native pi tool.
 *
 * The runtime already assembles the whole surface a host should offer,
 * including `select_agent` / `switch_unavailable` (degraded states) and its
 * own `send_attachment` / `download_attachment` (see `bin.ts`'s
 * `ListToolsRequestSchema` handler) - so nothing here special-cases any tool
 * by name beyond the post-call hooks in `hooks.ts`.
 */
export async function registerSwitchTools(pi: ExtensionAPI, bridge: SwitchBridge): Promise<void> {
  const { tools } = await bridge.client.listTools();
  for (const tool of tools) {
    pi.registerTool(buildToolDefinition(bridge, tool));
  }
}

function buildToolDefinition(bridge: SwitchBridge, tool: Tool) {
  return {
    name: tool.name,
    label: tool.name,
    description: tool.description ?? '',
    // The runtime's tools carry plain JSON Schema (fetched from the Switch
    // server's operation catalog, or the runtime's own fixed tool defs), not
    // a TypeBox schema. pi only uses this for parameter validation and
    // system-prompt rendering, both of which work against JSON Schema
    // directly, so no TypeBox construction is needed.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    parameters: (tool.inputSchema ?? { type: 'object', properties: {} }) as any,
    async execute(_toolCallId: string, params: Record<string, unknown>) {
      const result = (await bridge.client.callTool({
        name: tool.name,
        arguments: params,
      })) as McpToolResult;
      const mapped = mapMcpToolResult(result);

      const hookPath = hookPathForTool(tool.name);
      if (hookPath) void notifyRuntimeHook(bridge.sessionPid, hookPath);

      return mapped;
    },
  };
}

/**
 * Deliver the runtime's `notifications/claude/channel` events (its one
 * custom notification method - see `emitNotification` in `bin.ts`) to
 * `onEvent`.
 *
 * The MCP client SDK routes any notification with no registered schema
 * handler to `fallbackNotificationHandler`, which is exactly this method:
 * Claude Code's own `claude/channel` experimental capability is how the
 * runtime was designed to be consumed, and a generic MCP client receives the
 * same notification just as generically.
 */
export function onChannelNotification(
  bridge: SwitchBridge,
  onEvent: (content: string, meta: Record<string, string>) => void
): void {
  bridge.client.fallbackNotificationHandler = async (notification: Notification) => {
    if (notification.method !== 'notifications/claude/channel') return;
    const params = notification.params as { content?: unknown; meta?: unknown } | undefined;
    if (typeof params?.content !== 'string') return;
    const meta = (params.meta ?? {}) as Record<string, string>;
    onEvent(params.content, meta);
  };
}
