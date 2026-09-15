import { makeAutoObservable, runInAction } from 'mobx';
import { agentsStore } from '@renderer/features/locations/stores/agents-store';
import { failureText } from '@renderer/lib/errors/describe-failure';
import { rpc } from '@renderer/lib/ipc';
import type { Agent } from '@shared/core/agents/agents';
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

export type ServerAgentAutoLink = { localAgentId: string; switchAgentId: string };

/** Names match only when their exact, case-sensitive strings match. */
export function serverAgentAutoLinks(
  remoteAgents: RemoteAgentSummary[],
  localAgents: Agent[]
): ServerAgentAutoLink[] {
  const localByName = new Map<string, Agent[]>();
  const remoteNameCounts = new Map<string, number>();
  for (const local of localAgents) {
    const matches = localByName.get(local.name);
    if (matches) matches.push(local);
    else localByName.set(local.name, [local]);
  }
  for (const remote of remoteAgents) {
    remoteNameCounts.set(remote.name, (remoteNameCounts.get(remote.name) ?? 0) + 1);
  }

  const links: ServerAgentAutoLink[] = [];
  for (const remote of remoteAgents) {
    const matches = localByName.get(remote.name) ?? [];
    if (remoteNameCounts.get(remote.name) !== 1 || matches.length !== 1) continue;
    const local = matches[0]!;
    if (!local.locationId || local.switchAgentId !== null) continue;
    links.push({ localAgentId: local.id, switchAgentId: remote.id });
  }
  return links;
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
      if (!agentsStore.loaded) await agentsStore.load();
      const links = serverAgentAutoLinks(remote, agentsStore.agentsForServer(serverId));
      if (links.length > 0) {
        await Promise.all(
          links.map(async (link) => {
            const agent = await rpc.agents.updateAgent({
              agentId: link.localAgentId,
              switchAgentId: link.switchAgentId,
            });
            if (!agent) throw new Error(`Local agent ${link.localAgentId} was not found`);
          })
        );
        await agentsStore.load();
      }
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
