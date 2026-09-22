import { readFile, unlink, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { log } from '@main/lib/logger';
import { openLocationFolder } from './open-location';

function pendingLocationsPath(): string {
  return path.join(os.homedir(), '.config', 'switch-axi', 'pending-locations.json');
}

/**
 * Startup counterpart to the `/locations/open` control route: a CLI create
 * that ran while Console was closed drops the directory here instead. Each
 * dir is opened and adopted the same way; a dir is removed only once that
 * succeeds, so a failure (bad path, DB not ready) is retried on the next launch.
 */
export async function processPendingLocations(): Promise<void> {
  const filePath = pendingLocationsPath();
  let raw: string;
  try {
    raw = await readFile(filePath, 'utf8');
  } catch {
    return;
  }

  let dirs: string[];
  try {
    const parsed: unknown = JSON.parse(raw);
    dirs = Array.isArray((parsed as { dirs?: unknown })?.dirs)
      ? (parsed as { dirs: string[] }).dirs
      : [];
  } catch (error) {
    log.warn('pending-locations: invalid JSON, leaving file untouched', { error: String(error) });
    return;
  }

  const remaining: string[] = [];
  for (const dir of dirs) {
    try {
      const result = await openLocationFolder(dir);
      if (!result.success) {
        log.warn('pending-locations: could not open location', { dir, error: result.error.type });
        remaining.push(dir);
      }
    } catch (error) {
      log.warn('pending-locations: failed to open location', { dir, error: String(error) });
      remaining.push(dir);
    }
  }

  if (remaining.length === 0) {
    await unlink(filePath).catch(() => {});
  } else if (remaining.length !== dirs.length) {
    await writeFile(filePath, JSON.stringify({ dirs: remaining }), 'utf8');
  }
}
