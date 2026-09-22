/**
 * Talks to the Switch agent runtime's local hook listener
 * (`startHookListener` in `switch-agent-runtime/src/bin.ts`).
 *
 * That listener exists for hosts that cannot hook into the runtime's own MCP
 * tool-call handling - Claude Code's separate `PostToolUse` hook, OpenCode's
 * reporting plugin, and (here) this pi extension, which spawns the runtime as
 * a child process but calls its tools through a generic MCP client rather
 * than through code that could special-case them in-process.
 *
 * `connect_to_room` needs no entry here: the runtime's own MCP tool-call
 * handler already tracks the connected room for every host (see the
 * `connect_to_room` special case in `bin.ts`'s `CallToolRequestSchema`
 * handler), so a room switch is picked up regardless of which client made the
 * call. Only the routes below have no such in-process equivalent.
 */

import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';

/** Hook route to call after a Switch tool succeeds, keyed by tool name. */
export const POST_CALL_HOOKS: Record<string, string> = {
  // Clears the runtime's unaddressed-message tally, so a later notification
  // does not claim this session is behind on messages it just read.
  read_context: '/read-context',
  // Starts/stops the exclusive-role lease-renewal loop, so the lease is held
  // only while a live session holds the role.
  assume_role: '/assume-role',
  release_role: '/release-role',
};

/** The hook route to call after `toolName` succeeds, or null for no hook. */
export function hookPathForTool(toolName: string): string | null {
  return POST_CALL_HOOKS[toolName] ?? null;
}

/**
 * Where the runtime publishes its hook listener port.
 *
 * The runtime resolves its session directory from `process.ppid` - its
 * parent's pid. Since this extension spawns the runtime directly, that
 * parent is this process, so `sessionPid` is `process.pid`, not the
 * runtime's own pid.
 */
export function hookPortFilePath(sessionPid: number, home: string = os.homedir()): string {
  return path.join(home, '.switch', 'sessions', String(sessionPid), 'port');
}

/** The runtime's hook listener port, or null if it has not published one (yet, or at all). */
export async function readHookPort(sessionPid: number, home?: string): Promise<number | null> {
  try {
    const raw = await fs.readFile(hookPortFilePath(sessionPid, home), 'utf8');
    const port = Number(raw.trim());
    return Number.isInteger(port) && port > 0 ? port : null;
  } catch {
    return null;
  }
}

/**
 * POST to one of the runtime's local hook routes.
 *
 * Best-effort, matching every other consumer of this listener
 * (`switch_hook.py`, `switch-notifications.js`): a hook that arrives before
 * the listener is up, or after the runtime has exited, is not something the
 * caller should fail over.
 */
export async function postHook(
  port: number,
  hookPath: string,
  body: unknown = {}
): Promise<void> {
  try {
    await fetch(`http://127.0.0.1:${port}${hookPath}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch {
    // Best-effort; see the module doc comment.
  }
}

/** Resolve the runtime's hook port and POST to it in one call. A no-op if the port is not published. */
export async function notifyRuntimeHook(
  sessionPid: number,
  hookPath: string,
  body: unknown = {},
  home?: string
): Promise<void> {
  const port = await readHookPort(sessionPid, home);
  if (port === null) return;
  await postHook(port, hookPath, body);
}
