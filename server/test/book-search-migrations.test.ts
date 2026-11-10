import { Pool } from 'pg';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { join } from 'node:path';
import { applyBookSearchMigrations } from '../src/scripts/book-search-migrations';

const connectionString = process.env.BOOK_SEARCH_TEST_DATABASE_URL;
describe.runIf(Boolean(connectionString))('managed search migrations on a separate empty disposable database', () => {
  let admin: Pool;
  let pool: Pool;
  let url: URL;
  beforeAll(async () => {
    url = new URL(connectionString!);
    if (url.hostname !== '127.0.0.1' || url.port !== '55432' || url.pathname !== '/book_search_test')
      throw new Error('Dedicated disposable database required');
    admin = new Pool({ connectionString, max: 1 });
    await admin.query('CREATE DATABASE book_search_migration_test');
    url.pathname = '/book_search_migration_test';
    pool = new Pool({ connectionString: url.toString(), max: 2, statement_timeout: 5000 });
    expect((await pool.query('SELECT current_database() AS name')).rows[0].name).toBe('book_search_migration_test');
    await pool.query(
      "CREATE SCHEMA drizzle; CREATE TABLE drizzle.__drizzle_migrations(id serial PRIMARY KEY,hash text NOT NULL,created_at bigint); INSERT INTO drizzle.__drizzle_migrations(hash,created_at) VALUES('native-ledger-sentinel',123)",
    );
  });
  beforeEach(async () => {
    await pool.query('DROP SCHEMA IF EXISTS book_search CASCADE; DROP SCHEMA IF EXISTS book_search_migrations CASCADE');
  });
  afterAll(async () => {
    await pool?.end();
    if (admin) {
      await admin.query('DROP DATABASE book_search_migration_test');
      await admin.end();
    }
  });

  it('installs the generated schema and replays via CLI without changing native migration history or queue data', async () => {
    expect(await applyBookSearchMigrations(pool)).toEqual({ migrationCount: 5, appliedCount: 5 });
    await pool.query('INSERT INTO book_search.state(id,revision) VALUES(1,7); INSERT INTO book_search.pending(book_id,revision) VALUES(42,7)');
    expect(await applyBookSearchMigrations(pool)).toEqual({ migrationCount: 5, appliedCount: 0 });
    const result = await promisify(execFile)(
      process.execPath,
      [join(process.cwd(), 'node_modules/tsx/dist/cli.mjs'), 'src/scripts/book-search-maintain.ts', 'migrate'],
      {
        timeout: 15000,
        maxBuffer: 64 * 1024,
        env: { ...process.env, DATABASE_URL: url.toString(), BOOK_SEARCH_ENABLED: 'true', BOOK_SEARCH_WORKER_ENABLED: 'false' },
      },
    );
    expect(JSON.parse(result.stdout.trim().split('\n').at(-1)!)).toMatchObject({ phase: 'migrated', migrationCount: 5, appliedCount: 0 });
    expect((await pool.query('SELECT book_id,revision::text FROM book_search.pending')).rows).toEqual([{ book_id: 42, revision: '7' }]);
    expect((await pool.query('SELECT hash,created_at::text FROM drizzle.__drizzle_migrations')).rows).toEqual([
      { hash: 'native-ledger-sentinel', created_at: '123' },
    ]);
    expect((await pool.query('SELECT count(*)::integer AS count FROM book_search_migrations.__drizzle_migrations')).rows[0].count).toBe(5);
  });

  it('refuses unmanaged or altered migration history instead of adopting or rewriting it', async () => {
    await pool.query('CREATE SCHEMA book_search');
    await expect(applyBookSearchMigrations(pool)).rejects.toThrow('managed migration history');
    expect((await pool.query("SELECT to_regnamespace('book_search_migrations') IS NULL AS absent")).rows[0].absent).toBe(true);
    await pool.query('DROP SCHEMA book_search');
    await applyBookSearchMigrations(pool);
    await pool.query("UPDATE book_search_migrations.__drizzle_migrations SET hash='altered-history' WHERE id=1");
    await expect(applyBookSearchMigrations(pool)).rejects.toThrow('history does not match');
    expect((await pool.query('SELECT hash FROM book_search_migrations.__drizzle_migrations WHERE id=1')).rows[0].hash).toBe('altered-history');
  });

  it('does not run concurrently with another migration holder and succeeds after that holder leaves', async () => {
    const holder = await pool.connect();
    try {
      await holder.query('SELECT pg_advisory_lock(724073,3)');
      await expect(applyBookSearchMigrations(pool)).rejects.toThrow('already running');
      expect((await holder.query("SELECT to_regnamespace('book_search_migrations') IS NULL AS absent")).rows[0].absent).toBe(true);
    } finally {
      await holder.query('SELECT pg_advisory_unlock(724073,3)');
      holder.release();
    }
    expect(await applyBookSearchMigrations(pool)).toMatchObject({ appliedCount: 5 });
  });
});
