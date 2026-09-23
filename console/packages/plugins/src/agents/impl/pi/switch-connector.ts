import type { ISwitchSetupFilesBehavior, PluginFs } from '@switch-console/core/agents/plugins';
import { SWITCH_AGENT_RUNTIME_PIN } from '../../../distribution';
import { PI_SKILL_CONTENT } from './skill-file';
import { PI_SWITCH_EXTENSION_CONTENT } from './switch-extension-file';

/**
 * The room-workflow skill, in pi's global skill directory.
 *
 * Global rather than per-workspace to match the extension it explains: the
 * extension is registered globally below, so every pi session on the machine
 * has the Switch tools whether or not Switch Console launched it. A skill
 * dropped in a workspace would leave those sessions holding the room tools
 * and no instructions for using them.
 *
 * The directory name has to be the skill's own name — pi discovers
 * `skills/<name>/SKILL.md` from `~/.pi/agent/skills/`.
 */
export const PI_SKILL_PATH = '.pi/agent/skills/switch/SKILL.md';

/**
 * The Switch connector extension, in pi's global extension directory.
 *
 * A single self-contained file with no runtime dependencies: pi loads
 * extensions with jiti straight from source, and nothing runs `npm install`
 * in the home directory, so an extension importing the MCP SDK would fail to
 * load. The stdio framing this file hand-rolls is the price of that.
 */
export const PI_EXTENSION_PATH = '.pi/agent/extensions/switch-connector.ts';

/**
 * Records what Switch Console installed, beside the connector rather than
 * inside pi's settings: installs here must never rewrite the user's own
 * `settings.json` to keep bookkeeping.
 */
export const PI_CONNECTOR_MARKER_PATH = '.pi/agent/switch-connector.json';

export function buildPiSwitchConnector(): ISwitchSetupFilesBehavior {
  return {
    async install(fs: PluginFs, { version }: { version: string }): Promise<string[]> {
      await fs.write(PI_EXTENSION_PATH, PI_SWITCH_EXTENSION_CONTENT);
      await fs.write(PI_SKILL_PATH, PI_SKILL_CONTENT);
      await fs.write(
        PI_CONNECTOR_MARKER_PATH,
        `${JSON.stringify({ version, runtime: SWITCH_AGENT_RUNTIME_PIN }, null, 2)}\n`
      );
      return [PI_EXTENSION_PATH, PI_SKILL_PATH, PI_CONNECTOR_MARKER_PATH];
    },

    async uninstall(fs: PluginFs): Promise<void> {
      await fs.delete(PI_EXTENSION_PATH);
      await fs.delete(PI_SKILL_PATH);
      await fs.delete(PI_CONNECTOR_MARKER_PATH);
    },

    async installedVersion(fs: PluginFs): Promise<string | null> {
      const marker = await fs.read(PI_CONNECTOR_MARKER_PATH);
      if (!marker) return null;

      // The marker alone is not proof. Someone deleting the extension by hand
      // leaves the marker behind with no Switch tools — reporting that as
      // installed hides the reason the agent has none.
      if (!(await fs.exists(PI_EXTENSION_PATH))) return null;
      if (!(await fs.exists(PI_SKILL_PATH))) return null;

      try {
        const parsed = JSON.parse(marker) as { version?: unknown };
        return typeof parsed.version === 'string' ? parsed.version : null;
      } catch {
        return null;
      }
    },
  };
}
