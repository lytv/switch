import type { IDisposable } from '@switch-console/shared';
import type { IExecutionContext } from '@main/core/execution-context/types';
import type { FileSystemProvider } from '@main/core/fs/types';
import { resolveSessionHostForTransport } from '@shared/core/location-settings/session-host';
import type { Location } from '@shared/core/locations/locations';
import type { AgentRuntimeProvider } from '../agent-runtime/types';
import { sessionRuntimeManager } from '../sessions/session-runtime-manager';
import { locationRuntimeRegistry } from './location-runtime-registry';
import type { LocationTransport } from './location-transport';
import type { LocationSettingsProvider } from './settings/provider';

export type ProvisionResult = {
  agent: AgentRuntimeProvider;
  persistData: {
    locationId: string;
  };
};

/**
 * The open handle on a location: its execution context, filesystem and
 * settings, built local or SSH according to the location's transport. One
 * provider per open location, managed by the location manager.
 */
export class LocationProvider implements IDisposable {
  readonly location: Location;
  readonly transport: LocationTransport;
  readonly settings: LocationSettingsProvider;
  readonly fs: FileSystemProvider;

  private readonly _ctx: IExecutionContext;

  constructor(
    location: Location,
    transport: LocationTransport,
    deps: {
      ctx: IExecutionContext;
      fs: FileSystemProvider;
      settings: LocationSettingsProvider;
    }
  ) {
    this.location = location;
    this.transport = transport;
    this._ctx = deps.ctx;
    this.settings = deps.settings;
    this.fs = deps.fs;
  }

  get locationId(): string {
    return this.location.id;
  }

  /** The working directory on the location's host. */
  get dir(): string {
    return this.location.dir;
  }

  get ctx(): IExecutionContext {
    return this._ctx;
  }

  async dispose(): Promise<void> {
    const settings = await this.settings.get();
    const sessionHost = resolveSessionHostForTransport(this.transport.kind, settings);
    // Detach (don't terminate) when work should outlive the app: tmux sessions,
    // and remote locations whose on-VM sidecar must keep listening to Switch
    // while Switch Console is closed (CHOO-1059). Terminate only cleans up the
    // local pane.
    const mode =
      this.transport.kind === 'ssh' || sessionHost.host === 'tmux' || sessionHost.host === 'herdr'
        ? 'detach'
        : 'terminate';
    await sessionRuntimeManager.teardownAllForLocation(this.location.id, mode);
    await locationRuntimeRegistry.releaseAll(this.location.id, mode);
  }
}
