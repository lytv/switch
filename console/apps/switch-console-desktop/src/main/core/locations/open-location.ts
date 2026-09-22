import { err, ok, type Result } from '@switch-console/shared';
import { configuredAgentDiscoveryService } from '@main/core/agents/configured-agent-discovery-service';
import type { Location } from '@shared/core/locations/locations';
import { basenameFromAnyPath } from '@shared/path-name';
import { locationManager } from './location-manager';
import { checkIsValidDirectory } from './path-utils';
import { ensureLocation } from './store';

export type OpenLocationError = { type: 'invalid-directory'; dir: string };

/**
 * Ensure a local directory is an open location, then adopt whatever
 * `.switch/agents/*.json` credentials already sit in it - the CLI-create
 * counterpart to onboarding through the UI. Idempotent: an already-open
 * location with already-adopted agents is a no-op success.
 */
export async function openLocationFolder(
  dir: string
): Promise<Result<Location, OpenLocationError>> {
  if (!checkIsValidDirectory(dir)) return err({ type: 'invalid-directory', dir });

  const location = await ensureLocation({
    sshHost: null,
    dir,
    name: basenameFromAnyPath(dir) ?? dir,
  });
  await locationManager.openLocation(location);
  await configuredAgentDiscoveryService.reconcileLocation(location);
  return ok(location);
}
