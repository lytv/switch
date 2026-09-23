/**
 * `StdioClientTransport` spawns its child with `getDefaultEnvironment()`
 * (a curated safe allowlist) unless an `env` is passed explicitly - it does
 * NOT inherit the parent's environment the way OpenCode's and Codex's own
 * spawns do. Since this extension is the one spawning the Switch agent
 * runtime, it has to pass the parent environment through itself, or
 * `SWITCH_API_ENDPOINT` / `SWITCH_API_TOKEN` / `SWITCH_AGENT_ID` (and `PATH`,
 * needed for `npx` to resolve at all) never reach the runtime.
 */

/** `process.env` with `undefined` values dropped, as `child_process`/MCP transports expect. */
export function toStringEnv(env: NodeJS.ProcessEnv): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(env)) {
    if (value !== undefined) result[key] = value;
  }
  return result;
}
