import { randomUUID } from 'node:crypto';
import { and, desc, eq } from 'drizzle-orm';
import { db } from '@main/db/client';
import { locations, type LocationRow } from '@main/db/schema';
import type { Location } from '@shared/core/locations/locations';

function rowToLocation(row: LocationRow): Location {
  return {
    id: row.id,
    name: row.name,
    sshHost: row.sshHost === '' ? null : row.sshHost,
    dir: row.dir,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

export async function getLocations(): Promise<Location[]> {
  const rows = await db.select().from(locations).orderBy(desc(locations.updatedAt));
  return rows.map(rowToLocation);
}

export async function getLocationById(locationId: string): Promise<Location | undefined> {
  const [row] = await db.select().from(locations).where(eq(locations.id, locationId)).limit(1);
  if (!row) return undefined;
  return rowToLocation(row);
}

export async function getLocationByHostDir(
  sshHost: string | null,
  dir: string
): Promise<Location | undefined> {
  const [row] = await db
    .select()
    .from(locations)
    .where(and(eq(locations.sshHost, sshHost ?? ''), eq(locations.dir, dir)))
    .limit(1);
  if (!row) return undefined;
  return rowToLocation(row);
}

/**
 * Find the location for (sshHost, dir), creating it if none exists. The name
 * is only applied on create — an existing location keeps its name.
 */
export async function ensureLocation(params: {
  sshHost: string | null;
  dir: string;
  name: string;
}): Promise<Location> {
  const existing = await getLocationByHostDir(params.sshHost, params.dir);
  if (existing) return existing;
  const [row] = await db
    .insert(locations)
    .values({
      id: randomUUID(),
      name: params.name,
      sshHost: params.sshHost ?? '',
      dir: params.dir,
    })
    .returning();
  return rowToLocation(row!);
}


/** Rewrite a location's working directory. Used when the gateway agent
 * `repo_dir` changes and Console should follow it for new sessions. */
export async function updateLocationDir(
  locationId: string,
  dir: string
): Promise<Location | undefined> {
  const trimmed = dir.trim();
  if (!trimmed) return undefined;
  const current = await getLocationById(locationId);
  if (!current) return undefined;
  if (current.dir === trimmed) return current;

  // (sshHost, dir) is unique. If another location already owns this path, do
  // not clobber — the captain must resolve the collision intentionally.
  const collision = await getLocationByHostDir(current.sshHost, trimmed);
  if (collision && collision.id !== locationId) return undefined;

  const [row] = await db
    .update(locations)
    .set({ dir: trimmed, updatedAt: new Date().toISOString() })
    .where(eq(locations.id, locationId))
    .returning();
  if (!row) return undefined;
  return rowToLocation(row);
}
