import { quoteShellArg } from '@main/utils/shellEscape';
import type { HerdrWorkspaceMode } from '@shared/core/location-settings/location-settings';
import type { AgentStatus } from '@shared/core/providers/agentEvents';
import { existsSync } from 'node:fs';
import { delimiter, join } from 'node:path';

/** Resolve the herdr binary to an absolute path.
 *
 * GUI-launched Electron often has a stripped PATH (no Homebrew). Bare `herdr`
 * then fails inside `/bin/zsh -lc` attach PTYs with "command not found". Prefer
 * well-known install locations, then PATH.
 */
export function resolveHerdrBin(env: NodeJS.ProcessEnv = process.env): string {
  const candidates = [
    env.HERDR_BIN,
    '/opt/homebrew/bin/herdr',
    '/usr/local/bin/herdr',
    join(env.HOME ?? '', '.local/bin/herdr'),
  ].filter((value): value is string => Boolean(value && value.trim()));

  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
  }

  const pathEnv = env.PATH ?? '';
  for (const dir of pathEnv.split(delimiter)) {
    if (!dir) continue;
    const candidate = join(dir, 'herdr');
    if (existsSync(candidate)) return candidate;
  }

  // Last resort: leave the bare name so the OS error stays readable.
  return 'herdr';
}

function herdrArgs(args: string[], env?: NodeJS.ProcessEnv): { bin: string; args: string[] } {
  return { bin: resolveHerdrBin(env), args };
}


export type HerdrExec = (
  command: string,
  args: string[]
) => Promise<{ stdout: string; stderr: string }>;

export type HerdrSessionHostConfig = {
  sessionName: string;
  protocolMin: number;
  preferAgentPrompt: boolean;
  workspaceMode: HerdrWorkspaceMode;
};

export type HerdrPaneRef = {
  workspaceId: string;
  tabId: string;
  paneId: string;
  /** Herdr agent name used for `agent attach` / `agent prompt` (not a pane id). */
  agentName: string;
};

const BLOCKED_AGENT_STATES = new Set([
  'blocked',
  'awaiting_input',
  'awaiting-input',
  'stalled',
  'waiting_for_input',
  'waiting-for-input',
]);

const RECOGNIZED_AGENT_KINDS = new Set(['claude', 'codex', 'opencode']);

function parseJson(stdout: string, commandLabel: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(stdout) as unknown;
    if (!parsed || typeof parsed !== 'object') {
      throw new Error(`${commandLabel} did not return an object`);
    }
    const root = parsed as Record<string, unknown>;
    // Herdr 0.9 CLI wraps payloads as { id, result }.
    if (root.result && typeof root.result === 'object') {
      return root.result as Record<string, unknown>;
    }
    return root;
  } catch (error) {
    throw new Error(`${commandLabel} returned invalid JSON: ${String(error)}`);
  }
}

function asId(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function readId(parsed: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = asId(parsed[key]);
    if (value) return value;
  }
  return null;
}

function hasNotFound(detail: string, kind: 'pane' | 'agent'): boolean {
  return detail.includes(`${kind}_not_found`) || detail.toLowerCase().includes(`${kind} not found`);
}

function workspaceLabel(
  config: HerdrSessionHostConfig,
  opts: { agentSlug: string; roomId: string | null; sessionId: string }
): string {
  if (config.workspaceMode === 'per-agent') return `${config.sessionName}-${opts.agentSlug}`;
  if (config.workspaceMode === 'per-room' && opts.roomId) {
    return `${config.sessionName}-${opts.roomId}`;
  }
  if (config.workspaceMode === 'per-task') return `${config.sessionName}-${opts.sessionId}`;
  return config.sessionName;
}

export async function readHerdrProtocol(exec: HerdrExec): Promise<number> {
  const { stdout } = await exec(resolveHerdrBin(), ['status', '--json']);
  const parsed = parseJson(stdout, 'herdr status --json');
  const client =
    parsed.client && typeof parsed.client === 'object'
      ? (parsed.client as Record<string, unknown>)
      : null;
  const protocol =
    client && typeof client.protocol === 'number' && Number.isInteger(client.protocol)
      ? client.protocol
      : null;
  if (protocol === null) throw new Error('herdr status did not include client.protocol');
  return protocol;
}

export async function ensureHerdrProtocol(exec: HerdrExec, minProtocol: number): Promise<number> {
  const protocol = await readHerdrProtocol(exec);
  if (protocol < minProtocol) {
    throw new Error(
      `herdr protocol ${protocol} is below the required floor ${minProtocol}; upgrade herdr on the host`
    );
  }
  return protocol;
}

export function mapProviderToHerdrKind(providerId: string): string | null {
  switch (providerId) {
    case 'claude':
    case 'claude-code':
      return 'claude';
    case 'codex':
      return 'codex';
    case 'opencode':
      return 'opencode';
    case 'cursor':
      return 'cursor';
    case 'gemini':
      return 'gemini';
    case 'pi':
      return 'pi';
    case 'grok':
      return 'grok';
    case 'kimi':
      return 'kimi';
    case 'omp':
      return 'omp';
    default:
      return null;
  }
}

export async function createHerdrPane(
  exec: HerdrExec,
  config: HerdrSessionHostConfig,
  opts: {
    cwd: string;
    tabLabel: string;
    agentSlug: string;
    agentName: string;
    roomId: string | null;
    sessionId: string;
    env?: Record<string, string>;
  }
): Promise<HerdrPaneRef> {
  const wsLabel = workspaceLabel(config, opts);
  // Herdr 0.9: workspace create takes --label (not --name) and always prints JSON
  // (no --json flag). It also creates the first tab + root pane in one shot.
  const createArgs = [
    'workspace',
    'create',
    '--label',
    wsLabel,
    '--cwd',
    opts.cwd,
    '--no-focus',
  ];
  for (const [key, value] of Object.entries(opts.env ?? {})) {
    // Skip empty values; herdr --env requires KEY=VALUE.
    if (!key || value === undefined || value === null) continue;
    createArgs.push('--env', `${key}=${value}`);
  }
  const created = parseJson(
    (await exec(resolveHerdrBin(), createArgs)).stdout,
    'herdr workspace create'
  );
  const workspaceObj =
    created.workspace && typeof created.workspace === 'object'
      ? (created.workspace as Record<string, unknown>)
      : created;
  const rootPane =
    created.root_pane && typeof created.root_pane === 'object'
      ? (created.root_pane as Record<string, unknown>)
      : {};
  const tabObj =
    created.tab && typeof created.tab === 'object'
      ? (created.tab as Record<string, unknown>)
      : {};

  const workspaceId =
    readId(workspaceObj, ['workspace_id', 'id']) ?? readId(rootPane, ['workspace_id']);
  const tabId = readId(tabObj, ['tab_id', 'id']) ?? readId(rootPane, ['tab_id']);
  const paneId =
    readId(rootPane, ['pane_id', 'id']) ??
    readId(tabObj, ['pane_id', 'root_pane_id']);

  if (!workspaceId) throw new Error('herdr workspace create returned no workspace id');
  if (!tabId || !paneId) {
    throw new Error('herdr workspace/tab create returned no tab/pane id');
  }
  return { workspaceId, tabId, paneId, agentName: opts.agentName };
}

/** Start a recognized agent in an existing shell pane (Herdr 0.9). */
export async function startHerdrAgent(
  exec: HerdrExec,
  opts: {
    paneId: string;
    name: string;
    kind: string;
    args: string[];
    timeoutMs?: number;
  }
): Promise<void> {
  await exec(resolveHerdrBin(), [
    'agent',
    'start',
    opts.name,
    '--kind',
    opts.kind,
    '--pane',
    opts.paneId,
    '--timeout',
    String(opts.timeoutMs ?? 90_000),
    '--',
    ...opts.args,
  ]);
}

export async function runHerdrPaneCommand(
  exec: HerdrExec,
  paneId: string,
  env: Record<string, string>,
  command: string,
  args: string[]
): Promise<void> {
  const envPrefix = Object.entries(env)
    .map(([key, value]) => `${key}=${quoteShellArg(value)}`)
    .join(' ');
  const commandLine = [command, ...args].map(quoteShellArg).join(' ');
  const inner = envPrefix ? `${envPrefix} exec ${commandLine}` : `exec ${commandLine}`;
  // Herdr 0.9: positional pane id + command (no --pane / --).
  await exec(resolveHerdrBin(), ['pane', 'run', paneId, inner]);
}

export async function isHerdrPaneLive(exec: HerdrExec, paneId: string): Promise<boolean> {
  try {
    const { stdout } = await exec(resolveHerdrBin(), ['pane', 'get', paneId]);
    const parsed = parseJson(stdout, 'herdr pane get');
    const result =
      parsed.result && typeof parsed.result === 'object'
        ? (parsed.result as Record<string, unknown>)
        : parsed;
    const pane =
      result.pane && typeof result.pane === 'object'
        ? (result.pane as Record<string, unknown>)
        : result;
    const found = readId(pane, ['pane_id', 'id']);
    if (!found) return false;
    return found === paneId;
  } catch (error) {
    const detail = String(error);
    if (hasNotFound(detail, 'pane')) return false;
    throw error;
  }
}

export async function closeHerdrPane(exec: HerdrExec, paneId: string): Promise<void> {
  try {
    await exec(resolveHerdrBin(), ['pane', 'close', paneId]);
  } catch (error) {
    const detail = String(error);
    if (hasNotFound(detail, 'pane')) return;
    throw error;
  }
}

export type HerdrPromptStatus = {
  agentId: string;
  recognizedKind: boolean;
  blocked: boolean;
  runtimeStatus: AgentStatus;
  detail?: string;
};

export function mapHerdrAgentStatus(status: unknown): AgentStatus {
  switch (typeof status === 'string' ? status.toLowerCase() : 'unknown') {
    case 'idle':
      return 'idle';
    case 'working':
      return 'working';
    case 'blocked':
    case 'awaiting_input':
    case 'awaiting-input':
    case 'stalled':
    case 'waiting_for_input':
    case 'waiting-for-input':
      return 'awaiting-input';
    case 'done':
      return 'completed';
    default:
      return 'error';
  }
}

export async function readHerdrPromptStatus(
  exec: HerdrExec,
  paneId: string
): Promise<HerdrPromptStatus | null> {
  try {
    const { stdout } = await exec(resolveHerdrBin(), ['agent', 'get', paneId]);
    const parsed = parseJson(stdout, 'herdr agent get');
    const result =
      parsed.result && typeof parsed.result === 'object'
        ? (parsed.result as Record<string, unknown>)
        : parsed;
    const agent =
      result.agent && typeof result.agent === 'object'
        ? (result.agent as Record<string, unknown>)
        : parsed.result
          ? null
          : result;
    if (!agent) return null;
    const agentId = readId(agent, ['pane_id', 'agent_id', 'id']) ?? paneId;
    if (!agentId) return null;
    const kindRaw =
      typeof agent.kind === 'string'
        ? agent.kind
        : typeof agent.agent_kind === 'string'
          ? agent.agent_kind
          : typeof agent.agent === 'string'
            ? agent.agent
            : '';
    const stateRaw =
      typeof agent.state === 'string'
        ? agent.state
        : typeof agent.status === 'string'
          ? agent.status
          : typeof agent.agent_status === 'string'
            ? agent.agent_status
            : 'unknown';
    const runtimeStatus = mapHerdrAgentStatus(stateRaw);
    return {
      agentId,
      recognizedKind: RECOGNIZED_AGENT_KINDS.has(kindRaw.toLowerCase()),
      blocked: runtimeStatus === 'error' || BLOCKED_AGENT_STATES.has(stateRaw.toLowerCase()),
      runtimeStatus,
      ...(runtimeStatus === 'error' ? { detail: `Herdr reported agent status '${stateRaw}'` } : {}),
    };
  } catch (error) {
    const detail = String(error);
    if (hasNotFound(detail, 'agent') || hasNotFound(detail, 'pane')) return null;
    throw error;
  }
}
