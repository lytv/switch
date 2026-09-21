import { adoptConfiguredAgent } from '@main/core/agents/attach-configured-agents';
import { discoverConfiguredAgents } from '@main/core/agents/discover-configured-agents';
import { subscribeToLocationWatch } from '@main/core/fs/controller';
import { locationManager } from '@main/core/locations/location-manager';
import { getLocationById, getLocations } from '@main/core/locations/store';
import { findServerByEndpoint } from '@main/core/switch-servers/servers-store';
import { log } from '@main/lib/logger';
import type { Location } from '@shared/core/locations/locations';

const WATCH_LABEL_PREFIX = 'configured-agent-discovery:';
const CREDENTIAL_PATH = /^\.switch\/agents\/[^/]+\.json$/;

class ConfiguredAgentDiscoveryService {
  private readonly stops = new Map<string, () => void>();
  private readonly reconciling = new Set<string>();
  private initialized = false;

  initialize(): void {
    if (this.initialized) return;
    this.initialized = true;
    locationManager.on('locationOpened', (locationId) => void this.watchLocation(locationId));
    locationManager.on('locationClosed', (locationId) => this.stopWatching(locationId));
    void this.scanPersistedLocations();
  }

  private async scanPersistedLocations(): Promise<void> {
    for (const location of await getLocations()) {
      if (location.sshHost === null) await this.reconcileLocation(location);
    }
  }

  private async watchLocation(locationId: string): Promise<void> {
    if (this.stops.has(locationId)) return;
    const location = await getLocationById(locationId);
    if (!location || location.sshHost !== null) return;
    const stop = subscribeToLocationWatch(
      locationId,
      `${WATCH_LABEL_PREFIX}${locationId}`,
      (events) => {
        if (
          events.some(
            (event) =>
              (event.type === 'create' || event.type === 'modify') &&
              CREDENTIAL_PATH.test(event.path)
          )
        ) {
          void this.reconcileLocation(location);
        }
      }
    );
    if (stop) this.stops.set(locationId, stop);
  }

  private stopWatching(locationId: string): void {
    this.stops.get(locationId)?.();
    this.stops.delete(locationId);
  }

  private async reconcileLocation(location: Location): Promise<void> {
    if (location.sshHost !== null) return;
    const discovered = await discoverConfiguredAgents({
      sshHost: null,
      dir: location.dir,
      serverId: '',
    });
    for (const agent of discovered) {
      const server = await findServerByEndpoint(agent.apiEndpoint);
      if (!server) {
        log.warn('configured-agent-discovery: no configured server matches credential endpoint', {
          locationId: location.id,
          agentId: agent.switchAgentId,
        });
        continue;
      }
      const key = `${location.id}:${server.id}:${agent.switchAgentId}`;
      if (this.reconciling.has(key)) continue;
      this.reconciling.add(key);
      try {
        const result = await adoptConfiguredAgent({
          location,
          serverId: server.id,
          discovered: agent,
        });
        if (!result.success)
          log.warn('configured-agent-discovery: could not adopt configured agent', result.error);
      } catch (error) {
        log.warn('configured-agent-discovery: could not adopt configured agent', {
          locationId: location.id,
          agentId: agent.switchAgentId,
          error: String(error),
        });
      } finally {
        this.reconciling.delete(key);
      }
    }
  }
}

export const configuredAgentDiscoveryService = new ConfiguredAgentDiscoveryService();
