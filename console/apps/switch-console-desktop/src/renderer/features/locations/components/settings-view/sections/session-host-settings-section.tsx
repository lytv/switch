import { observer } from 'mobx-react-lite';
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
  const mounted = asMounted(getLocationStore(locationId));
  const store = mounted?.settings ?? null;
  const settings = store?.settings ?? null;
  if (!mounted || !store || !settings) return null;

  const sshHost = mounted.data.sshHost ?? null;
  const transportKind = locationKind(mounted.data);
  const resolved = resolveSessionHostForTransport(transportKind, settings);
  const sessionHost = settings.sessionHost ?? resolved.host;

  const save = (patch: SessionHostSettingsPatch) => {
    void store.save(withSessionHostPatch(settings, patch)).then((result) => {
      if (!result.success) {
        log.error('Failed to save session host settings', { locationId, error: result.error });
      }
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
      <Select
        value={sessionHost}
        onValueChange={(next) => save({ sessionHost: next as SessionHost })}
      >
        <SelectTrigger className="w-[200px] shrink-0 gap-2">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {(Object.keys(SESSION_HOST_LABEL) as SessionHost[]).map((host) => (
            <SelectItem key={host} value={host}>
              {SESSION_HOST_LABEL[host]}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {sessionHost === 'herdr' && (
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
