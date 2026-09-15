import { observer } from 'mobx-react-lite';
import { useState } from 'react';
import { InfoTooltip } from '@renderer/features/settings/components/InfoTooltip';
import {
  asMounted,
  getLocationStore,
} from '@renderer/features/locations/stores/location-selectors';
import { Field, FieldDescription, FieldTitle } from '@renderer/lib/ui/field';
import { Input } from '@renderer/lib/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@renderer/lib/ui/select';
import { Switch } from '@renderer/lib/ui/switch';
import { log } from '@renderer/utils/logger';
import {
  DEFAULT_HERDR_PROTOCOL_MIN,
  type HerdrWorkspaceMode,
  type SessionHost,
} from '@shared/core/location-settings/location-settings';
import {
  DEFAULT_HERDR_SESSION_NAME,
  DEFAULT_HERDR_WORKSPACE_MODE,
  resolveSessionHostForTransport,
} from '@shared/core/location-settings/session-host';
import { locationKind } from '@shared/core/locations/locations';
import { withSessionHostPatch, type SessionHostSettingsPatch } from './session-host-settings-patch';

const SESSION_HOST_LABEL: Record<SessionHost, string> = {
  pty: 'PTY',
  tmux: 'tmux',
  herdr: 'Herdr',
};

const WORKSPACE_MODE_LABEL: Record<HerdrWorkspaceMode, string> = {
  flat: 'Flat — one Herdr workspace for the whole location',
  'per-agent': 'Per agent — one workspace per Switch agent',
  'per-room': 'Per room — one workspace per Switch room',
  'per-task': 'Per task — one workspace per session',
};

/**
 * Location-wide session host: whether this location's agent sessions run in a
 * plain PTY, a tmux pane, or a Herdr pane, and (when Herdr) the workspace
 * naming, protocol floor, and prompt-injection preference it launches with.
 *
 * Unlike the per-agent sections around it, this is one location-level setting
 * — `LocationSettingsStore`, not an agent RPC — so it renders the same for
 * every agent tab. Saved immediately on change: `updateLocationSettings`
 * replaces the whole settings row, so every field already on it is carried
 * along (`withSessionHostPatch`) rather than only the one this form touched.
 */
export const SessionHostSettingsSection = observer(function SessionHostSettingsSection({
  locationId,
}: {
  locationId: string;
}) {
  // Hooks must run unconditionally (React #310 if placed after the null return).
  const [pendingHost, setPendingHost] = useState<SessionHost | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);

  const mounted = asMounted(getLocationStore(locationId));
  const store = mounted?.settings ?? null;
  const settings = store?.settings ?? null;
  if (!mounted || !store || !settings) return null;

  const sshHost = mounted.data.sshHost ?? null;
  const transportKind = locationKind(mounted.data);
  const resolved = resolveSessionHostForTransport(transportKind, settings);
  const sessionHost = settings.sessionHost ?? resolved.host;
  const displayedHost = pendingHost ?? sessionHost;

  const save = (patch: SessionHostSettingsPatch) => {
    if (patch.sessionHost) setPendingHost(patch.sessionHost);
    setSaveError(null);
    void store.save(withSessionHostPatch(settings, patch)).then((result) => {
      if (!result.success) {
        setPendingHost(null);
        const message =
          result.error.type === 'invalid-settings'
            ? 'Could not save session host (invalid settings).'
            : result.error.type === 'location-not-found'
              ? 'Location not found.'
              : 'Could not save session host.';
        setSaveError(message);
        log.error('Failed to save session host settings', { locationId, error: result.error });
        return;
      }
      setPendingHost(null);
      setSaveError(null);
    });
  };

  return (
    <Field>
      <FieldTitle>
        <span className="flex items-center gap-1.5">
          Session host
          <InfoTooltip
            label="More info about the session host"
            content="How this location's agent sessions run: a plain PTY, a tmux pane, or a Herdr pane. Remote locations default to tmux; Herdr is opt-in and needs herdr on the host's PATH."
          />
        </span>
      </FieldTitle>
      <FieldDescription className="text-foreground-muted">
        Effective right now: <span className="font-mono">{resolved.host}</span>
        {!settings.sessionHost && ` (this location's default for a ${sshHost ? 'remote' : 'local'} host)`}
      </FieldDescription>
      {/* Buttons instead of Select: Base UI select was easy to mis-click / look
          stuck on tmux when the popup aligned poorly. One click = one host. */}
      <div className="flex flex-wrap gap-2" role="group" aria-label="Session host">
        {(Object.keys(SESSION_HOST_LABEL) as SessionHost[]).map((host) => {
          const selected = displayedHost === host;
          return (
            <button
              key={host}
              type="button"
              aria-pressed={selected}
              disabled={pendingHost !== null && pendingHost !== host}
              onClick={() => {
                if (host === sessionHost && pendingHost === null) return;
                save({ sessionHost: host });
              }}
              className={
                selected
                  ? 'rounded-md border border-ring bg-background-1 px-3 py-1.5 text-sm font-medium text-foreground'
                  : 'rounded-md border border-border bg-transparent px-3 py-1.5 text-sm text-foreground-muted hover:border-border-1 hover:bg-background-1 hover:text-foreground'
              }
            >
              {SESSION_HOST_LABEL[host]}
            </button>
          );
        })}
      </div>
      {saveError && (
        <FieldDescription className="text-destructive">{saveError}</FieldDescription>
      )}

      {displayedHost === 'herdr' && (
        <div className="flex flex-col gap-4 rounded-md border border-border p-3">
          <FieldDescription className="text-foreground-muted">
            Requires <span className="font-mono">herdr</span> on{' '}
            {sshHost ? "the remote host's" : "this machine's"} PATH, protocol ≥{' '}
            {resolved.herdr.protocolMin}.
          </FieldDescription>

          <Field>
            <FieldTitle>Session name</FieldTitle>
            <Input
              key={`session-name-${locationId}`}
              defaultValue={settings.herdr?.sessionName ?? ''}
              placeholder={DEFAULT_HERDR_SESSION_NAME}
              onBlur={(e) => {
                const value = e.currentTarget.value.trim();
                save({ herdr: { sessionName: value.length > 0 ? value : undefined } });
              }}
            />
            <FieldDescription className="text-foreground-muted">
              Prefix for the Herdr workspace(s) this location creates. Defaults to{' '}
              <span className="font-mono">{DEFAULT_HERDR_SESSION_NAME}</span>.
            </FieldDescription>
          </Field>

          <Field>
            <FieldTitle>Protocol floor</FieldTitle>
            <Input
              key={`protocol-min-${locationId}`}
              type="number"
              min={DEFAULT_HERDR_PROTOCOL_MIN}
              defaultValue={settings.herdr?.protocolMin ?? DEFAULT_HERDR_PROTOCOL_MIN}
              onBlur={(e) => {
                const raw = e.currentTarget.value.trim();
                const parsed = Number.parseInt(raw, 10);
                save({
                  herdr: {
                    protocolMin:
                      raw.length > 0 && Number.isFinite(parsed)
                        ? Math.max(parsed, DEFAULT_HERDR_PROTOCOL_MIN)
                        : undefined,
                  },
                });
              }}
            />
            <FieldDescription className="text-foreground-muted">
              Switch Console refuses to launch a session here on an older Herdr. Protocol 16+
              also carries live status events instead of polling.
            </FieldDescription>
          </Field>

          <Field orientation="horizontal">
            <FieldTitle>Prefer agent prompt injection</FieldTitle>
            <Switch
              checked={settings.herdr?.preferAgentPrompt ?? true}
              onCheckedChange={(checked) => save({ herdr: { preferAgentPrompt: checked } })}
            />
          </Field>
          <FieldDescription className="text-foreground-muted">
            Route prompts through <span className="font-mono">herdr agent prompt</span> when
            possible, falling back to <span className="font-mono">herdr pane send-text</span>.
          </FieldDescription>

          <Field>
            <FieldTitle>Workspace mode</FieldTitle>
            <Select
              value={settings.herdr?.workspaceMode ?? DEFAULT_HERDR_WORKSPACE_MODE}
              onValueChange={(next) =>
                save({ herdr: { workspaceMode: next as HerdrWorkspaceMode } })
              }
            >
              <SelectTrigger className="w-full gap-2">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {(Object.keys(WORKSPACE_MODE_LABEL) as HerdrWorkspaceMode[]).map((mode) => (
                  <SelectItem key={mode} value={mode}>
                    {WORKSPACE_MODE_LABEL[mode]}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </Field>
        </div>
      )}
    </Field>
  );
});
