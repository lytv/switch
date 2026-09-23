/**
 * The Switch connector Switch Console writes for pi (pi.dev).
 *
 * Source of truth for what Console installs; Console embeds this file verbatim
 * (`console/packages/plugins/src/agents/impl/pi/`) and its drift guard fails
 * if the two disagree. Edit here, not there.
 *
 * pi has no built-in MCP client, so this extension IS the client — but unlike
 * `../extensions/switch/mcp-bridge.ts` (the manually-installed connector,
 * which uses the MCP TypeScript SDK) this file uses only node builtins plus
 * the pi extension API. It has to: Console writes it into
 * `~/.pi/agent/extensions/`, where no `node_modules` with the SDK exists and
 * nothing can run `npm install`. So the MCP stdio framing below is hand-rolled
 * (one JSON-RPC value per line, as the stdio transport defines it). It does
 * not re-implement the Agent Protocol — the spawned runtime owns the wire
 * protocol, credentials, SSE stream and operation catalog wholesale.
 *
 * Event delivery: the runtime pushes room events as
 * `notifications/claude/channel`. In a Switch Console-managed session Console
 * itself injects `[Switch]` lines into the PTY, so bridging them here too
 * would deliver every event twice — when `SWITCHDASH_HOOK_PORT` or
 * `SWITCHDASH_PTY_ID` is set (Console always sets both for sessions it
 * launches) this extension registers tools only. In a standalone session using
 * a Console-installed connector, nothing else delivers events, so each channel
 * notification becomes a pi turn via `pi.sendUserMessage()`.
 */

import { type ChildProcess, spawn } from 'node:child_process';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';

/** Pinned to the same version every other connector runs. Bump all together. */
export const RUNTIME_PACKAGE = '@sandboxaq/switch-agent-runtime@0.4.1';

const INITIALIZE_TIMEOUT_MS = 60_000;
const MAX_BUFFER_BYTES = 10 * 1024 * 1024;
const CHANNEL_METHOD = 'notifications/claude/channel';

/** Set by Switch Console for every session it launches. */
export function isConsoleManaged(env: NodeJS.ProcessEnv = process.env): boolean {
  return Boolean(env.SWITCHDASH_HOOK_PORT ?? env.SWITCHDASH_PTY_ID);
}

/** One JSON-RPC message, plus the newline the stdio transport reads on. */
export function encodeJsonRpc(message: object): string {
  return `${JSON.stringify(message)}\n`;
}

/** Split a growing stdout buffer into complete lines, keeping a partial tail. */
export function drainLines(buffer: string): { lines: string[]; rest: string } {
  const parts = buffer.split('\n');
  const rest = parts.pop() ?? '';
  const lines = parts.map((line) => line.replace(/\r$/, '')).filter((line) => line.length > 0);
  return { lines, rest };
}

export interface McpToolInfo {
  name: string;
  description?: string;
  inputSchema?: Record<string, unknown>;
}

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

export type PiContentItem =
  | { type: 'text'; text: string }
  | { type: 'image'; data: string; mimeType: string };

export interface PiToolResult {
  content: PiContentItem[];
  details: Record<string, unknown>;
}

/** A tool result the runtime reported as an error - thrown, never returned. */
export class SwitchToolError extends Error {}

function mapContentItem(item: McpContentItem): PiContentItem {
  if (item.type === 'text' && typeof item.text === 'string') {
    return { type: 'text', text: item.text };
  }
  if (item.type === 'image' && typeof item.data === 'string' && typeof item.mimeType === 'string') {
    return { type: 'image', data: item.data, mimeType: item.mimeType };
  }
  return { type: 'text', text: JSON.stringify(item) };
}

/**
 * Convert a runtime tool result into pi's tool result shape. pi signals
 * failure by throwing from `execute()`, so `isError` throws here.
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

/** Render a channel notification as the `[Switch]` line the skill promises. */
export function formatChannelEvent(content: string, meta: Record<string, string>): string {
  const roomId = meta.room_id ?? 'unknown-room';
  const attachments: string[] = [];
  if (meta.image_path) attachments.push(`image_path=${meta.image_path}`);
  if (meta.file_path) attachments.push(`file_path=${meta.file_path}`);
  if (meta.failed_attachments) attachments.push(`failed_attachments=${meta.failed_attachments}`);
  const suffix = attachments.length > 0 ? ` (${attachments.join(', ')})` : '';
  return `[Switch] room ${roomId}: ${content}${suffix}`;
}

/** Fresh turn when idle; `sendUserMessage` requires `deliverAs` while streaming. */
export function chooseDeliveryMode(isIdle: boolean): 'immediate' | 'steer' {
  return isIdle ? 'immediate' : 'steer';
}

/** Hook route to call after a Switch tool succeeds, keyed by tool name. */
export function hookPathForTool(toolName: string): string | null {
  // read_context clears the unaddressed-message tally; assume/release_role
  // start and stop the exclusive-role lease-renewal loop. connect_to_room
  // needs none: the runtime tracks its connected room in-process on every
  // successful call, regardless of client.
  if (toolName === 'read_context') return '/read-context';
  if (toolName === 'assume_role') return '/assume-role';
  if (toolName === 'release_role') return '/release-role';
  return null;
}

/**
 * Where the runtime publishes its hook listener port. The runtime resolves its
 * session directory from `process.ppid` — its parent — which is this process,
 * so the session id is `process.pid`, not the runtime's own pid.
 */
export function hookPortFilePath(sessionPid: number, home: string = os.homedir()): string {
  return path.join(home, '.switch', 'sessions', String(sessionPid), 'port');
}

/** POST to the runtime's local hook listener. Best-effort: never throws. */
export async function notifyRuntimeHook(
  sessionPid: number,
  hookPath: string,
  home?: string
): Promise<void> {
  let port: number | null = null;
  try {
    const raw = await fs.readFile(hookPortFilePath(sessionPid, home), 'utf8');
    const parsed = Number(raw.trim());
    if (Number.isInteger(parsed) && parsed > 0) port = parsed;
  } catch {
    return;
  }
  try {
    await fetch(`http://127.0.0.1:${port}${hookPath}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{}',
    });
  } catch {
    // A hook arriving before the listener is up, or after the runtime exited,
    // is not something the caller should fail over.
  }
}

interface Waiter {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout> | undefined;
}

export interface StdioMcpClient {
  listTools(): Promise<{ tools: McpToolInfo[] }>;
  callTool(params: { name: string; arguments: Record<string, unknown> }): Promise<McpToolResult>;
  onNotification(handler: (method: string, params: unknown) => void): void;
  close(): Promise<void>;
}

class StdioSession {
  private readonly waiters = new Map<number, Waiter>();
  private readonly notifications: Array<(method: string, params: unknown) => void> = [];
  private nextId = 1;
  private buffer = '';
  private failed: Error | null = null;
  private closed = false;

  constructor(private readonly child: ChildProcess) {
    this.child.on('error', (err) => {
      this.failAll(err instanceof Error ? err : new Error(String(err)));
    });
    this.child.on('close', (code) => {
      if (this.waiters.size > 0) {
        this.failAll(new Error(`Switch runtime exited (${code ?? 'no code'}) before answering`));
      }
    });
    this.child.stdout!.on('data', (chunk: Buffer) => {
      this.onStdout(chunk);
    });
  }

  onNotification(handler: (method: string, params: unknown) => void): void {
    this.notifications.push(handler);
  }

  notify(method: string, params?: unknown): void {
    this.write({ jsonrpc: '2.0', method, params });
  }

  request(method: string, params: unknown, timeoutMs?: number): Promise<unknown> {
    if (this.failed) return Promise.reject(this.failed);
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const timer =
        timeoutMs === undefined
          ? undefined
          : setTimeout(() => {
              this.waiters.delete(id);
              reject(new Error(`Switch runtime timed out waiting for ${method}`));
            }, timeoutMs);
      this.waiters.set(id, { resolve, reject, timer });
      try {
        this.write({ jsonrpc: '2.0', id, method, params });
      } catch (err) {
        this.waiters.delete(id);
        if (timer) clearTimeout(timer);
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    });
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.failAll(new Error('Switch runtime connection closed'));
    this.child.stdin?.end();
    this.child.kill();
  }

  private write(message: object): void {
    if (this.failed || !this.child.stdin) {
      throw this.failed ?? new Error('Switch runtime stdin is closed');
    }
    this.child.stdin.write(encodeJsonRpc(message));
  }

  private onStdout(chunk: Buffer): void {
    this.buffer += chunk.toString('utf8');
    if (this.buffer.length > MAX_BUFFER_BYTES) {
      this.failAll(
        new Error(`Switch runtime stdout exceeded ${MAX_BUFFER_BYTES} bytes without a complete message`)
      );
      return;
    }
    const drained = drainLines(this.buffer);
    this.buffer = drained.rest;
    for (const line of drained.lines) {
      let message: {
        jsonrpc?: string;
        id?: number | string;
        method?: string;
        params?: unknown;
        result?: unknown;
        error?: { code?: number; message?: string };
      };
      try {
        message = JSON.parse(line) as typeof message;
      } catch {
        this.failAll(new Error(`Switch runtime sent a non-JSON line: ${line}`));
        return;
      }
      this.onMessage(message);
    }
  }

  private onMessage(message: {
    id?: number | string;
    method?: string;
    params?: unknown;
    result?: unknown;
    error?: { code?: number; message?: string };
  }): void {
    if (message.id !== undefined && message.method) {
      this.write({
        jsonrpc: '2.0',
        id: message.id,
        error: { code: -32601, message: `Method not found: ${message.method}` },
      });
      return;
    }
    if (message.id !== undefined) {
      const pending = this.waiters.get(Number(message.id));
      if (!pending) return;
      this.waiters.delete(Number(message.id));
      if (pending.timer) clearTimeout(pending.timer);
      if (message.error) {
        pending.reject(new Error(message.error.message ?? 'Switch runtime request failed'));
      } else {
        pending.resolve(message.result);
      }
      return;
    }
    if (message.method) {
      for (const handler of this.notifications) handler(message.method, message.params);
    }
  }

  private failAll(error: Error): void {
    if (!this.failed) this.failed = error;
    this.buffer = '';
    for (const pending of this.waiters.values()) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.waiters.clear();
  }
}

/** Spawn the runtime and connect over its stdio. Inherits this process's env (SWITCH_* and PATH). */
export async function connectSwitchRuntime(): Promise<StdioMcpClient> {
  const child = spawn('npx', ['-y', RUNTIME_PACKAGE], {
    stdio: ['pipe', 'pipe', 'inherit'],
    shell: false,
    windowsHide: process.platform === 'win32',
  });
  if (!child.stdin || !child.stdout) {
    child.kill();
    throw new Error('Failed to open stdio pipes to the Switch runtime');
  }
  const session = new StdioSession(child);
  try {
    const result = (await session.request(
      'initialize',
      {
        protocolVersion: '2025-11-25',
        capabilities: {},
        clientInfo: { name: 'pi-switch-connector', version: '0.1.0' },
      },
      INITIALIZE_TIMEOUT_MS
    )) as { protocolVersion?: unknown } | undefined;
    if (!result || typeof result.protocolVersion !== 'string') {
      throw new Error('Switch runtime sent an invalid initialize result');
    }
    session.notify('notifications/initialized');
  } catch (err) {
    await session.close();
    throw err;
  }
  return {
    listTools: () => session.request('tools/list', {}) as Promise<{ tools: McpToolInfo[] }>,
    callTool: (params) => session.request('tools/call', params) as Promise<McpToolResult>,
    onNotification: (handler) => session.onNotification(handler),
    close: () => session.close(),
  };
}

export default function switchExtension(pi: ExtensionAPI) {
  let client: StdioMcpClient | null = null;
  const sessionPid = process.pid;
  // Tracked from pi's own turn lifecycle: the channel handler fires outside
  // any event dispatch, so there is no ctx in scope to call isIdle() on.
  let busy = false;
  // In a Console-managed session Console injects [Switch] lines into the PTY
  // itself; bridging the same events here would deliver every one twice.
  const managed = isConsoleManaged();

  pi.on('agent_start', () => {
    busy = true;
  });
  pi.on('agent_settled', () => {
    busy = false;
    if (client) void notifyRuntimeHook(sessionPid, '/turn-end');
  });

  pi.on('session_start', async (_event, ctx) => {
    try {
      client = await connectSwitchRuntime();
      const { tools } = await client.listTools();
      for (const tool of tools) {
        pi.registerTool({
          name: tool.name,
          label: tool.name,
          description: tool.description ?? '',
          // The runtime's tools carry plain JSON Schema, not a TypeBox
          // schema; pi uses this for parameter validation and prompt
          // rendering, both of which work against JSON Schema directly.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          parameters: (tool.inputSchema ?? { type: 'object', properties: {} }) as any,
          async execute(_toolCallId: string, params: Record<string, unknown>) {
            const result = await client!.callTool({ name: tool.name, arguments: params });
            const mapped = mapMcpToolResult(result);
            const hookPath = hookPathForTool(tool.name);
            if (hookPath) void notifyRuntimeHook(sessionPid, hookPath);
            return mapped;
          },
        });
      }
      if (!managed) {
        client.onNotification((method, params) => {
          if (method !== CHANNEL_METHOD) return;
          const p = params as { content?: unknown; meta?: unknown } | undefined;
          if (typeof p?.content !== 'string') return;
          const text = formatChannelEvent(p.content, (p.meta ?? {}) as Record<string, string>);
          if (chooseDeliveryMode(!busy) === 'immediate') {
            pi.sendUserMessage(text);
          } else {
            pi.sendUserMessage(text, { deliverAs: 'steer' });
          }
        });
      }
    } catch (err) {
      client = null;
      ctx.ui.notify(`Switch connector failed to start: ${err instanceof Error ? err.message : err}`, 'error');
    }
  });

  pi.on('session_shutdown', async () => {
    if (!client) return;
    const current = client;
    client = null;
    await notifyRuntimeHook(sessionPid, '/disconnect');
    await current.close();
  });

  pi.registerCommand('switch', {
    description: 'Show the Switch connector status',
    handler: async (_args, ctx) => {
      if (!client) {
        ctx.ui.notify(
          'Switch connector: not connected. Check the pi log for a startup error, or restart the session.',
          'error'
        );
        return;
      }
      const { tools } = await client.listTools();
      ctx.ui.notify(
        `Switch connector: connected, ${tools.length} tool(s) registered ` +
          '(list_rooms / connect_to_room / select_agent tell you the rest - identity and room state ' +
          'live in the Switch server, not this command).',
        'info'
      );
    },
  });
}
