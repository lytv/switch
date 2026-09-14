import { quoteShellArg } from '@main/utils/shellEscape';
import type { HerdrWorkspaceMode } from '@shared/core/location-settings/location-settings';
import type { AgentStatus } from '@shared/core/providers/agentEvents';

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
    return parsed as Record<string, unknown>;
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
  opts: { agentSlug: string; roomId: string | null }
): string {
  if (config.workspaceMode === 'per-agent') return `${config.sessionName}-${opts.agentSlug}`;
  if (config.workspaceMode === 'per-room' && opts.roomId) {
    return `${config.sessionName}-${opts.roomId}`;
  }
  return config.sessionName;
}

export async function readHerdrProtocol(exec: HerdrExec): Promise<number> {
  const { stdout } = await exec('herdr', ['status', '--json']);
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

export async function createHerdrPane(
  exec: HerdrExec,
  config: HerdrSessionHostConfig,
  opts: { cwd: string; tabLabel: string; agentSlug: string; roomId: string | null }
): Promise<HerdrPaneRef> {
  const wsLabel = workspaceLabel(config, opts);
  const ws = parseJson(
    (await exec('herdr', ['workspace', 'create', '--name', wsLabel, '--json'])).stdout,
    'herdr workspace create'
  );
  const workspaceId = readId(ws, ['workspace_id', 'id']);
  if (!workspaceId) throw new Error('herdr workspace create returned no workspace id');

  const tab = parseJson(
    (
      await exec('herdr', [
        'tab',
        'create',
        '--workspace',
        workspaceId,
        '--name',
        opts.tabLabel,
        '--cwd',
        opts.cwd,
        '--json',
      ])
    ).stdout,
    'herdr tab create'
  );
  const tabId = readId(tab, ['tab_id', 'id']);
  const paneId =
    readId(tab, ['pane_id', 'root_pane_id']) ??
    readId((tab.root_pane as Record<string, unknown>) ?? {}, ['pane_id', 'id']) ??
    readId((tab.pane as Record<string, unknown>) ?? {}, ['pane_id', 'id']);
  if (!tabId || !paneId) {
    throw new Error('herdr tab create returned no tab/pane id');
  }
  return { workspaceId, tabId, paneId };
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
  await exec('herdr', ['pane', 'run', '--pane', paneId, '--', inner]);
}

export async function isHerdrPaneLive(exec: HerdrExec, paneId: string): Promise<boolean> {
  try {
    const { stdout } = await exec('herdr', ['pane', 'get', paneId]);
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
    await exec('herdr', ['pane', 'close', '--pane', paneId]);
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
    const { stdout } = await exec('herdr', ['agent', 'get', paneId]);
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
