import type {
  HerdrLocationSettings,
  LocationSettings,
  SessionHost,
} from '@shared/core/location-settings/location-settings';

export type SessionHostSettingsPatch = {
  sessionHost?: SessionHost;
  herdr?: Partial<HerdrLocationSettings>;
};

/**
 * `updateLocationSettings` replaces the whole settings row (there is no
 * partial `sessionHost`/`herdr` write path), so a save from this form has to
 * carry every field already on the location's settings — not just the ones
 * this form edits — or it would silently drop everything else on save.
 *
 * A `herdr` patch merges into the existing `herdr` object rather than
 * replacing it, so changing one Herdr field (e.g. `protocolMin`) does not
 * clear the others. Pass `undefined` for a field to clear it back to its
 * default.
 */
export function withSessionHostPatch(
  current: LocationSettings,
  patch: SessionHostSettingsPatch
): LocationSettings {
  return {
    ...current,
    ...(patch.sessionHost !== undefined ? { sessionHost: patch.sessionHost } : {}),
    ...(patch.herdr !== undefined ? { herdr: { ...current.herdr, ...patch.herdr } } : {}),
  };
}
