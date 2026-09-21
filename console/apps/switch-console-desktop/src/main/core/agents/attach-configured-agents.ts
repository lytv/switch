import { randomUUID } from 'node:crypto';
import { err, ok } from '@switch-console/shared';
import type { Result } from '@switch-console/shared';
import { locationManager } from '@main/core/locations/location-manager';
import { checkIsValidDirectory } from '@main/core/locations/path-utils';
import { ensureLocation } from '@main/core/locations/store';
import { fetchAgentDetail, GatewayError } from '@main/core/switch-servers/gateway-client';
import { getServer } from '@main/core/switch-servers/servers-store';
import { log } from '@main/lib/logger';
import type { Agent } from '@shared/core/agents/agents';
import type { OnboardAgentError } from '@shared/core/agents/onboarding';
import type { AgentProviderId } from '@shared/core/providers/agent-provider-registry';
import { sameApiEndpoint } from '@shared/core/switch-servers/switch-servers';
import { basenameFromAnyPath } from '@shared/path-name';
import { agentEvents } from './agent-events';
import { createAgent } from './createAgent';
import {
  type DiscoveredConfiguredAgent,
  discoverConfiguredAgents,
} from './discover-configured-agents';
import { getAgents } from './getAgents';
import { providerForKnownAgentType } from './known-agent-type';
import { reconcileAgentAutoSessionFromGateway } from './setAgentAutoSession';

export type AttachConfiguredAgentsParams = {
  sshHost: string | null;
  dir: string;
  locationName?: string;
  /** The registered Switch server the discovered identities are verified against. */
  serverId: string;
  /**
   * The agents to attach. `providerId` is supplied by the caller rather than
   * taken from the scan: discovery infers it best-effort and reports `null` when
   * the directory names none, in which case the user picks.
   */
  agents: Array<{ name: string; providerId: AgentProviderId }>;
};

export type AttachConfiguredAgentsResult = Result<Agent[], OnboardAgentError>;

const pendingAdoptions = new Map<string, Promise<Result<Agent | null, OnboardAgentError>>>();

export function adoptConfiguredAgent(params: {
  location: { id: string; name: string; dir: string };
  serverId: string;
  discovered: DiscoveredConfiguredAgent;
  /** Caller-chosen provider, for the manual attach path where a user picked
   * one. When absent (automatic discovery has no one to ask), the provider is
   * derived from the gateway's known agent type instead. */
  providerId?: AgentProviderId;
}): Promise<Result<Agent | null, OnboardAgentError>> {
  const key = JSON.stringify([
    params.location.id,
    params.serverId,
    params.discovered.switchAgentId,
  ]);
  const pending = pendingAdoptions.get(key);
  if (pending) return pending;

  const adoption = adoptConfiguredAgentOnce(params).finally(() => pendingAdoptions.delete(key));
  pendingAdoptions.set(key, adoption);
  return adoption;
}

async function adoptConfiguredAgentOnce(params: {
  location: { id: string; name: string; dir: string };
  serverId: string;
  discovered: DiscoveredConfiguredAgent;
  providerId?: AgentProviderId;
}): Promise<Result<Agent | null, OnboardAgentError>> {
  const server = await getServer(params.serverId);
  if (!server) throw new Error(`No Switch server with id ${params.serverId}`);

  const siblings = await getAgents(params.location.id);
  if (
    siblings.some(
      (agent) =>
        agent.serverId === params.serverId &&
        agent.switchAgentId === params.discovered.switchAgentId
    )
  ) {
    return ok(null);
  }

  let remote;
  try {
    remote = await fetchAgentDetail(server, params.discovered.switchAgentId);
  } catch (cause) {
    if (cause instanceof GatewayError && cause.kind === 'unauthorized') {
      return err({
        type: 'switch-server-unauthenticated',
        dir: params.location.dir,
        serverId: server.id,
        serverName: server.name,
      });
    }
    if (cause instanceof GatewayError && cause.kind === 'http' && cause.status === 404) {
      return err({
        type: 'switch-agent-not-on-server',
        dir: params.location.dir,
        serverId: server.id,
        serverName: server.name,
        agentId: params.discovered.switchAgentId,
      });
    }
    throw cause;
  }
  const providerId = params.providerId ?? providerForKnownAgentType(remote.knownAgentType);
  if (!providerId) {
    log.warn('attachConfiguredAgents: unsupported or missing known agent type', {
      agentId: params.discovered.switchAgentId,
      knownAgentType: remote.knownAgentType,
    });
    return ok(null);
  }
  if (!sameApiEndpoint(params.discovered.apiEndpoint, server.apiUrl)) {
    log.warn('attachConfiguredAgents: directory endpoint differs from the chosen server', {
      name: params.discovered.name,
      dirEndpoint: params.discovered.apiEndpoint,
      serverEndpoint: server.apiUrl,
      serverId: server.id,
    });
  }
  const agent = await createAgent({
    id: randomUUID(),
    locationId: params.location.id,
    name: params.discovered.name,
    providerId,
    switchAgentId: params.discovered.switchAgentId,
    apiEndpoint: params.discovered.apiEndpoint,
    serverId: params.serverId,
    autoApprove: siblings.some((sibling) => sibling.autoApprove),
  });
  await reconcileAgentAutoSessionFromGateway(agent.id).catch((error) => {
    log.warn('attachConfiguredAgents: failed to reconcile auto_session', {
      agentId: agent.id,
      error: String(error),
    });
  });
  agentEvents._emit('agent:created', agent, 'unknown');
  return ok(agent);
}

/**
 * Adopt agents already configured in a working directory into this Switch Console,
 * under the Switch identity they already have (CHOO-1937).
 *
 * This is the second half of shared-host onboarding: another install (or another
 * person) set the agent up here, and this one attaches to it rather than
 * creating a duplicate. The identity is read back from disk at attach time
 * rather than trusted from the caller, so a stale scan cannot mint a row
 * pointing at the wrong agent.
 *
 * **Writes nothing to the working directory.** No credentials, no definition, no
 * provider config — the directory is another install's state and this operation
 * treats it as read-only. That is possible because the API token is never needed
 * here: it stays where it already is, and the launch path reads it from disk when
 * a session spawns. This module deliberately imports no workspace writer, so the
 * guarantee is structural rather than a matter of care.
 *
 * An identity that no longer exists on the chosen server fails the attach loudly
 * instead of falling back to minting a fresh one — a silent mint is exactly the
 * duplicate this feature exists to prevent.
 */
export async function attachConfiguredAgents(
  params: AttachConfiguredAgentsParams
): Promise<AttachConfiguredAgentsResult> {
  if (params.sshHost === null && !checkIsValidDirectory(params.dir)) {
    return err({ type: 'invalid-directory', dir: params.dir, message: 'Invalid directory' });
  }
  if (params.agents.length === 0) {
    return err({
      type: 'invalid-directory',
      dir: params.dir,
      message: 'No agents selected to attach.',
    });
  }

  const server = await getServer(params.serverId);
  if (!server) throw new Error(`No Switch server with id ${params.serverId}`);

  const discovered = new Map(
    (
      await discoverConfiguredAgents({
        sshHost: params.sshHost,
        dir: params.dir,
        serverId: params.serverId,
      })
    ).map((d) => [d.name, d])
  );

  const selected: Array<{ name: string; providerId: AgentProviderId }> = [];
  for (const requested of params.agents) {
    const found = discovered.get(requested.name);
    if (!found) {
      return err({
        type: 'invalid-directory',
        dir: params.dir,
        message: `No configured agent named "${requested.name}" in this directory. It may have been removed since the directory was scanned.`,
      });
    }
    // Already attached here — the scan marks these, so re-selecting one is a
    // stale-UI artefact rather than an error worth failing the whole batch for.
    if (!found.alreadyAgent) selected.push(requested);
  }
  if (selected.length === 0) {
    return err({
      type: 'invalid-directory',
      dir: params.dir,
      message: `Every selected agent is already attached to ${server.name} here.`,
    });
  }

  const location = await ensureLocation({
    sshHost: params.sshHost,
    dir: params.dir,
    name: params.locationName ?? basenameFromAnyPath(params.dir) ?? params.dir,
  });

  const created: Agent[] = [];
  for (const { name, providerId } of selected) {
    const found = discovered.get(name);
    if (!found) continue;

    const adopted = await adoptConfiguredAgent({
      location,
      serverId: params.serverId,
      discovered: found,
      providerId,
    });
    if (!adopted.success) return adopted;
    if (adopted.data) created.push(adopted.data);
  }

  await locationManager.openLocation(location);
  return ok(created);
}
