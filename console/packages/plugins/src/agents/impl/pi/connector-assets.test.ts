import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { pluginRegistry } from '../../registry';
import { provider } from './index';
import { PI_SKILL_CONTENT } from './skill-file';
import {
  buildPiSwitchConnector,
  PI_CONNECTOR_MARKER_PATH,
  PI_EXTENSION_PATH,
  PI_SKILL_PATH,
} from './switch-connector';
import { PI_SWITCH_EXTENSION_CONTENT } from './switch-extension-file';

// …/console/packages/plugins/src/agents/impl/pi → repo root
const CONNECTOR_DIR = join(
  dirname(fileURLToPath(import.meta.url)),
  '..',
  '..',
  '..',
  '..',
  '..',
  '..',
  '..',
  'connectors',
  'pi-plugin'
);

function connectorFile(...segments: string[]): string {
  return readFileSync(join(CONNECTOR_DIR, ...segments), 'utf8');
}

/**
 * `connectors/pi-plugin/` is the source of truth for what this connector is —
 * but nothing fetches it. Switch Console writes the connector itself, so it
 * carries its own copy of these assets, and the two can drift with nothing to
 * notice.
 *
 * Drift is silent in the worst way: the directory is what a reader reviews and
 * edits, while sessions run whatever the app embedded. Editing the connector
 * and shipping the old behaviour would look, in review, exactly like shipping
 * the new one.
 */
function memoryFs(files: Map<string, string>) {
  return {
    read: async (path: string) => files.get(path) ?? null,
    write: async (path: string, content: string) => void files.set(path, content),
    delete: async (path: string) => void files.delete(path),
    exists: async (path: string) => files.has(path),
    list: async () => [...files.keys()],
  };
}

async function install(): Promise<Map<string, string>> {
  const files = new Map<string, string>();
  await buildPiSwitchConnector().install(memoryFs(files), { version: '1.0.0' });
  return files;
}

describe('pi connector assets', () => {
  it('embeds the Switch extension exactly as the connector ships it', () => {
    expect(PI_SWITCH_EXTENSION_CONTENT).toBe(connectorFile('console', 'switch-connector.ts'));
  });

  it('embeds the room-workflow skill exactly as the connector ships it', () => {
    expect(PI_SKILL_CONTENT).toBe(connectorFile('skills', 'switch', 'SKILL.md'));
  });

  it('writes the extension where pi auto-discovers global extensions', async () => {
    const files = await install();
    expect(files.get(PI_EXTENSION_PATH)).toBe(PI_SWITCH_EXTENSION_CONTENT);
    // pi discovers `~/.pi/agent/extensions/*.ts` — not a subdirectory.
    expect(PI_EXTENSION_PATH).toBe('.pi/agent/extensions/switch-connector.ts');
  });

  /**
   * The tools without the instructions is the failure this guards. Installing
   * the extension is what an install visibly does, and it is easy to call that
   * the whole job: the session then has the room tools and nothing telling it
   * how a room works. Shipping the skill in the connector directory does not
   * deliver it — pi only reads skills it discovers on disk.
   */
  it('writes the skill where pi discovers it', async () => {
    const files = await install();
    expect(files.get(PI_SKILL_PATH)).toBe(PI_SKILL_CONTENT);
  });

  it('removes the extension, skill and marker on uninstall', async () => {
    const files = await install();
    await buildPiSwitchConnector().uninstall(memoryFs(files));

    expect(files.has(PI_EXTENSION_PATH)).toBe(false);
    expect(files.has(PI_SKILL_PATH)).toBe(false);
    expect(files.has(PI_CONNECTOR_MARKER_PATH)).toBe(false);
  });

  it('reports not-installed when the marker is missing', async () => {
    expect(await buildPiSwitchConnector().installedVersion(memoryFs(new Map()))).toBeNull();
  });

  it('reports not-installed when the marker survived but the files did not', async () => {
    const files = new Map<string, string>([
      [PI_CONNECTOR_MARKER_PATH, JSON.stringify({ version: '0.1.0' })],
    ]);
    expect(await buildPiSwitchConnector().installedVersion(memoryFs(files))).toBeNull();
  });

  it('reports the stamped version when installed', async () => {
    const files = await install();
    expect(await buildPiSwitchConnector().installedVersion(memoryFs(files))).toBe('1.0.0');
  });

  it('declares a version for the connector', () => {
    const manifest = JSON.parse(connectorFile('package.json')) as {
      name?: string;
      version?: string;
    };
    expect(manifest.name).toBe('switch-connector-pi');
    expect(manifest.version).toMatch(/^\d+\.\d+\.\d+$/);
  });

});

describe('pi provider wiring', () => {
  it('is registered, so the settings list and the new-agent picker offer it', () => {
    expect(pluginRegistry.get('pi')).toBeDefined();
  });

  // The Settings → Agent providers card and the install/status controls read
  // this descriptor; without `files` pi reports no Switch setup at all.
  it('declares a file-based Switch connector pinned to its own artifact', () => {
    expect(provider.capabilities.switchSetup).toMatchObject({
      kind: 'files',
      connectorName: 'Switch connector',
      artifact: 'switch-connector-pi',
    });
  });

  // A declared descriptor with no behavior installs nothing: the service
  // throws on use, and the card offers a button that always fails.
  it('implements the file behavior its descriptor promises', () => {
    expect(provider.behavior.switchSetup?.files).toBeDefined();
  });
});
