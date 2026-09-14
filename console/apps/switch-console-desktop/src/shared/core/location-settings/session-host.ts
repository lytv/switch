import type { LocationSettings } from './location-settings';
import {
  DEFAULT_HERDR_PROTOCOL_MIN,
  type HerdrWorkspaceMode,
  type SessionHost,
} from './location-settings';

export const DEFAULT_HERDR_SESSION_NAME = 'switchdash';
export const DEFAULT_HERDR_WORKSPACE_MODE: HerdrWorkspaceMode = 'flat';

export type ResolvedHerdrSettings = {
  sessionName: string;
  protocolMin: number;
  preferAgentPrompt: boolean;
  workspaceMode: HerdrWorkspaceMode;
};

export type ResolvedSessionHost = {
  host: SessionHost;
  herdr: ResolvedHerdrSettings;
};

export function resolveSessionHostForTransport(
  transportKind: 'local' | 'ssh',
  settings: Pick<LocationSettings, 'sessionHost' | 'tmux' | 'herdr'>
): ResolvedSessionHost {
  const configured = settings.sessionHost;
  const host: SessionHost =
    configured ??
    (transportKind === 'ssh' ? 'tmux' : (settings.tmux ?? false) ? 'tmux' : 'pty');
  if (host === 'herdr' && transportKind !== 'ssh') {
    throw new Error("sessionHost 'herdr' is supported only for SSH locations in P0");
  }
  return {
    host,
    herdr: {
      sessionName: settings.herdr?.sessionName?.trim() || DEFAULT_HERDR_SESSION_NAME,
      protocolMin: settings.herdr?.protocolMin ?? DEFAULT_HERDR_PROTOCOL_MIN,
      preferAgentPrompt: settings.herdr?.preferAgentPrompt ?? true,
      workspaceMode: settings.herdr?.workspaceMode ?? DEFAULT_HERDR_WORKSPACE_MODE,
    },
  };
}
