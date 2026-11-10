import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { readMigrationFiles } from 'drizzle-orm/migrator';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import type { Pool } from 'pg';

const LEDGER_SCHEMA = 'book_search_migrations';

export function resolveBookSearchMigrationsFolder(baseDirectory = __dirname, cwd = process.cwd()): string {
  const candidates = [
    join(baseDirectory, '..', '..', 'search-migrations'),
    join(baseDirectory, '..', 'db', 'search-migrations'),
    join(baseDirectory, '..', 'src', 'db', 'search-migrations'),
    join(cwd, 'search-migrations'),
    join(cwd, 'src', 'db', 'search-migrations'),
  ];
  const folder = candidates.find((path) => existsSync(join(path, 'meta', '_journal.json')));
  if (!folder) throw new Error('Book search migration assets are missing');
  return folder;
}

export async function applyBookSearchMigrations(pool: Pool) {
  const migrationsFolder = resolveBookSearchMigrationsFolder();
  const files = readMigrationFiles({ migrationsFolder });
  const client = await pool.connect();
  let locked = false;
  let discard = false;
  try {
    const lock = await client.query<{ locked: boolean }>('SELECT pg_try_advisory_lock(724073,3) AS locked');
    locked = lock.rows[0]?.locked === true;
    if (!locked) throw new Error('Book search migration is already running');
    const ownership = await client.query<{ schema_present: boolean; ledger_present: boolean }>(
      "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='book_search') AS schema_present, to_regclass('book_search_migrations.__drizzle_migrations') IS NOT NULL AS ledger_present",
    );
    const state = ownership.rows[0];
    const applied = state.ledger_present
      ? (
          await client.query<{ hash: string; created_at: string }>(
            'SELECT hash,created_at FROM book_search_migrations.__drizzle_migrations ORDER BY created_at,id',
          )
        ).rows
      : [];
    if (state.schema_present !== applied.length > 0) throw new Error('Book search schema requires a managed migration history');
    if (
      applied.length > files.length ||
      applied.some((row, index) => row.hash !== files[index].hash || Number(row.created_at) !== files[index].folderMillis)
    )
      throw new Error('Book search migration history does not match the packaged files');
    await migrate(drizzle(client), { migrationsFolder, migrationsSchema: LEDGER_SCHEMA, migrationsTable: '__drizzle_migrations' });
    return { migrationCount: files.length, appliedCount: files.length - applied.length };
  } finally {
    if (locked) {
      try {
        await client.query('SELECT pg_advisory_unlock(724073,3)');
      } catch {
        discard = true;
      }
    }
    client.release(discard);
  }
}
