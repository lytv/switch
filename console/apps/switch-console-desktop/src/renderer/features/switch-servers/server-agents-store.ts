import { makeAutoObservable, runInAction } from 'mobx';
import { failureText } from '@renderer/lib/errors/describe-failure';
import { rpc } from '@renderer/lib/ipc';
import type { RemoteAgentSummary } from '@shared/core/switch-servers/switch-servers';

export type SyncedServerAgent = RemoteAgentSummary & { missing: boolean };

/**
 * Merge the latest server list without throwing away an identity that vanished
 * upstream. A missing identity stays visible so its local setup can be repaired
 * instead of silently losing sessions or configuration.
 */
export function mergeServerAgents(
  current: SyncedServerAgent[],
  remote: RemoteAgentSummary[]
): SyncedServerAgent[] {
  const previous = new Map(current.map((agent) => [agent.id, agent]));
  const merged = remote.map((agent) => ({ ...previous.get(agent.id), ...agent, missing: false }));
  const present = new Set(remote.map((agent) => agent.id));
  for (const agent of current) {
    if (!present.has(agent.id)) merged.push({ ...agent, missing: true });
  }
  return merged.sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * Local mirror of the server identities. It deliberately does not manufacture
 * runnable agents: only a configured folder contains the agent token needed to
 * start a session. The existing attach flow supplies that folder and then its
 * local agent row joins this mirror by `switchAgentId`.
 */
export class ServerAgentsStore {
  readonly byServer = new Map<string, SyncedServerAgent[]>();
  readonly refreshing = new Set<string>();
  readonly errors = new Map<string, string>();

  constructor() {
    makeAutoObservable(this);
  }

  agentsOnServer(serverId: string): SyncedServerAgent[] {
    return this.byServer.get(serverId) ?? [];
  }

  async refresh(serverId: string): Promise<void> {
    if (this.refreshing.has(serverId)) return;
    runInAction(() => this.refreshing.add(serverId));
    try {
      const remote = await rpc.switchServers.listRemoteAgents(serverId);
      runInAction(() => {
        this.byServer.set(serverId, mergeServerAgents(this.byServer.get(serverId) ?? [], remote));
        this.errors.delete(serverId);
      });
    } catch (cause) {
      runInAction(() => {
        this.errors.set(serverId, failureText(cause, 'Could not sync the server agents.'));
      });
    } finally {
      runInAction(() => this.refreshing.delete(serverId));
    }
  }
}

export const serverAgentsStore = new ServerAgentsStore();
