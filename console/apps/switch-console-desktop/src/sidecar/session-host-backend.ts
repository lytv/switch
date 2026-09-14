import {
  closeHerdrPane,
  createHerdrPane,
  ensureHerdrProtocol,
  isHerdrPaneLive,
  runHerdrPaneCommand,
  type HerdrExec,
  type HerdrSessionHostConfig,
} from '@main/core/agent-runtime/impl/herdr-session-host';
import { quoteShellArg } from '@main/utils/shellEscape';
import { exactTmuxTarget, makeAgentTmuxSessionName } from './vm-tmux';

export type SessionHostTarget =
  | { kind: 'tmux'; tmuxTarget: string }
  | { kind: 'herdr'; paneId: string; tabId: string; workspaceId: string };

export interface SessionHostBackend {
  readonly host: SessionHostTarget['kind'];
  ensureDeps(): Promise<void>;
  create(input: {
    sessionId: string;
    cwd: string;
    env: Record<string, string>;
    command: string;
    args: string[];
    roomId: string;
    agentSlug: string;
  }): Promise<SessionHostTarget>;
  stop(target: SessionHostTarget): Promise<void>;
  attach(target: SessionHostTarget): { command: string; args: string[] };
  dehydrate(_target: SessionHostTarget): Promise<void>;
  inject(target: SessionHostTarget, text: string): Promise<void>;
  isLive(target: SessionHostTarget): Promise<boolean>;
}

export class TmuxSessionHostBackend implements SessionHostBackend {
  readonly host = 'tmux' as const;

  constructor(
    private readonly exec: HerdrExec,
    private readonly isPaneLive: (tmuxTarget: string) => boolean
  ) {}

  async ensureDeps(): Promise<void> {
    await this.exec('tmux', ['-V']);
  }

  async create(input: {
    sessionId: string;
    cwd: string;
    env: Record<string, string>;
    command: string;
    args: string[];
  }): Promise<SessionHostTarget> {
    const tmuxTarget = makeAgentTmuxSessionName(input.sessionId);
    const envPrefix = Object.entries(input.env)
      .map(([key, value]) => `${key}=${quoteShellArg(value)}`)
      .join(' ');
    const commandLine = [input.command, ...input.args].map(quoteShellArg).join(' ');
    const inner = `${envPrefix} exec ${commandLine}`;
    await this.exec('tmux', ['new-session', '-d', '-s', tmuxTarget, '-c', input.cwd, inner]);
    return { kind: 'tmux', tmuxTarget };
  }

  async stop(target: SessionHostTarget): Promise<void> {
    if (target.kind !== 'tmux') return;
    await this.exec('tmux', ['kill-session', '-t', exactTmuxTarget(target.tmuxTarget)]);
  }

  attach(target: SessionHostTarget): { command: string; args: string[] } {
    if (target.kind !== 'tmux') throw new Error('tmux backend received a non-tmux target');
    return { command: 'tmux', args: ['attach-session', '-t', exactTmuxTarget(target.tmuxTarget)] };
  }

  async dehydrate(_target: SessionHostTarget): Promise<void> {}

  async inject(target: SessionHostTarget, text: string): Promise<void> {
    if (target.kind !== 'tmux') return;
    await this.exec('tmux', ['send-keys', '-t', target.tmuxTarget, '-l', '--', text]);
  }

  async isLive(target: SessionHostTarget): Promise<boolean> {
    if (target.kind !== 'tmux') return false;
    return this.isPaneLive(target.tmuxTarget);
  }
}

export class HerdrSessionHostBackend implements SessionHostBackend {
  readonly host = 'herdr' as const;

  constructor(
    private readonly exec: HerdrExec,
    private readonly config: HerdrSessionHostConfig,
    private readonly isPaneLive: (paneId: string) => boolean
  ) {}

  async ensureDeps(): Promise<void> {
    await ensureHerdrProtocol(this.exec, this.config.protocolMin);
  }

  async create(input: {
    sessionId: string;
    cwd: string;
    env: Record<string, string>;
    command: string;
    args: string[];
    roomId: string;
    agentSlug: string;
  }): Promise<SessionHostTarget> {
    const ref = await createHerdrPane(this.exec, this.config, {
      cwd: input.cwd,
      tabLabel: `switchdash-${input.sessionId}`,
      agentSlug: input.agentSlug,
      roomId: input.roomId,
    });
    try {
      await runHerdrPaneCommand(this.exec, ref.paneId, input.env, input.command, input.args);
    } catch (error) {
      await closeHerdrPane(this.exec, ref.paneId).catch(() => {});
      throw error;
    }
    return { kind: 'herdr', paneId: ref.paneId, tabId: ref.tabId, workspaceId: ref.workspaceId };
  }

  async stop(target: SessionHostTarget): Promise<void> {
    if (target.kind !== 'herdr') return;
    await closeHerdrPane(this.exec, target.paneId);
  }

  attach(target: SessionHostTarget): { command: string; args: string[] } {
    if (target.kind !== 'herdr') throw new Error('herdr backend received a non-herdr target');
    return { command: 'herdr', args: ['pane', 'attach', '--pane', target.paneId] };
  }

  async dehydrate(_target: SessionHostTarget): Promise<void> {}

  async inject(target: SessionHostTarget, text: string): Promise<void> {
    if (target.kind !== 'herdr') return;
    await this.exec('herdr', ['pane', 'send-text', '--pane', target.paneId, '--text', text]);
  }

  async isLive(target: SessionHostTarget): Promise<boolean> {
    if (target.kind !== 'herdr') return false;
    if (this.isPaneLive(target.paneId)) return true;
    return isHerdrPaneLive(this.exec, target.paneId);
  }
}
