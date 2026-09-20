import { resolveWorkspaceFsFor } from '@main/core/agents/agent-workspace-fs';
import { detectSwitchAgent } from '@main/core/agents/detect';
import {
  definitionOwners,
  providerFromLaunchSpec,
} from '@main/core/agents/discover-configured-agents';
import { SWITCH_AGENTS_DIR_RELATIVE } from '@main/core/agents/switch-settings-paths';
import type {
  InspectLocationPathParams,
  LocationPathInspection,
} from '@shared/core/locations/locations';
import type { AgentProviderId } from '@shared/core/providers/agent-provider-registry';
import { checkIsValidDirectory } from '../path-utils';
import { getLocationByHostDir } from '../store';

export async function inspectLocationPath(
  params: InspectLocationPathParams
): Promise<LocationPathInspection> {
  const [existingLocation, switchAgent] = await Promise.all([
    getLocationByHostDir(null, params.path),
    detectSwitchAgent(params.path),
  ]);

  return {
    isDirectory: checkIsValidDirectory(params.path),
    existingLocation,
    switchAgent,
    providerId: switchAgent ? await inferProviderIdForDir(params.path) : null,
  };
}

/**
 * Best-effort provider for a dropped folder. Uses the same launch-spec then
 * definition scan as configured-agent discovery. Returns null when the
 * directory does not name exactly one credential file, or when neither signal
 * names a provider - callers fall back to `'claude'`.
 */
async function inferProviderIdForDir(dir: string): Promise<AgentProviderId | null> {
  const workspace = await resolveWorkspaceFsFor(null, dir);
  try {
    const names = (await workspace.fs.list(SWITCH_AGENTS_DIR_RELATIVE))
      .filter((entry) => entry.endsWith('.json'))
      .map((entry) => entry.slice(0, -'.json'.length))
      .filter((name) => name.length > 0);
    if (names.length !== 1) return null;

    const name = names[0];
    const fromSpec = await providerFromLaunchSpec(workspace.fs, name);
    if (fromSpec) return fromSpec;
    const owners = await definitionOwners(workspace.fs);
    return owners.get(name) ?? null;
  } finally {
    workspace.close();
  }
}
