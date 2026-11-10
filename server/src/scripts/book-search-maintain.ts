import 'reflect-metadata';
import { Pool } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { appConfig, dbConfig } from '../config/config';
import { createPostgresClientConfig } from '../db/postgres-connection-config';
import * as schema from '../db/schema';
import { BookSearchBootstrapRepository } from '../modules/book/search/book-search-bootstrap.repository';
import { BookSearchSyncRepository } from '../modules/book/search/book-search-sync.repository';
import { BookSearchDocumentProjector } from '../modules/book/search/book-search-document-projector';
import { BookSearchSyncService } from '../modules/book/search/book-search-sync.service';
import { BookSearchCoordinatorService } from '../modules/book/search/book-search-coordinator.service';
import { ZincBookIndexWriter } from '../modules/book/search/zinc-book-index-writer';
import { ZincBookIndexGeneration } from '../modules/book/search/zinc-book-index-generation';
import { applyBookSearchMigrations } from './book-search-migrations';

async function main(): Promise<void> {
  const command = process.argv[2];
  if (!['status', 'once', 'reconcile', 'migrate', 'activate'].includes(command)) {
    console.error(
      'Usage: node dist/scripts/book-search-maintain.js status|once|reconcile|migrate|activate <capture-session> --source-coverage-confirmed',
    );
    process.exitCode = 2;
    return;
  }
  if (
    command === 'activate' &&
    (process.argv.length !== 5 ||
      process.argv[4] !== '--source-coverage-confirmed' ||
      !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(process.argv[3]))
  ) {
    console.error('Book search activation requires the current capture session and --source-coverage-confirmed');
    process.exitCode = 2;
    return;
  }
  const config = appConfig();
  if (command !== 'status' && (!config.bookSearch.enabled || (command !== 'migrate' && !config.bookSearch.workerEnabled))) {
    console.error('Book search maintenance requires BOOK_SEARCH_ENABLED and BOOK_SEARCH_WORKER_ENABLED');
    process.exitCode = 2;
    return;
  }
  const pool = new Pool(createPostgresClientConfig(dbConfig().url, { max: 2, connectionTimeoutMillis: 5000, statement_timeout: 30_000 }));
  try {
    if (command === 'migrate') {
      const result = await applyBookSearchMigrations(pool);
      console.log(JSON.stringify({ workerPid: process.pid, phase: 'migrated', ...result }));
      return;
    }
    const db = drizzle(pool, { schema });
    const baseline = new BookSearchBootstrapRepository(db);
    const queue = new BookSearchSyncRepository(db);
    if (command === 'status') {
      const [progress, checkpoint, pending] = await Promise.all([baseline.progress(), queue.checkpoint(), queue.pendingSummary()]);
      console.log(
        JSON.stringify({ workerPid: process.pid, progress, checkpoint, pending }, (_key, value) =>
          typeof value === 'bigint' ? value.toString() : value,
        ),
      );
      return;
    }
    if (command === 'reconcile') {
      const initial = await baseline.initialize(config.bookSearch.index);
      const progress = initial && (await baseline.startReconciliation());
      console.log(JSON.stringify({ workerPid: process.pid, phase: progress?.phase ?? 'busy', reconciliationRequested: Boolean(progress) }));
      return;
    }
    const generation = new ZincBookIndexGeneration(config);
    if (command === 'activate') {
      const active = await baseline.activateServing(config.bookSearch.index, process.argv[3], (tx, state) => generation.verifyComplete(tx, state));
      console.log(JSON.stringify({ workerPid: process.pid, phase: active ? 'active' : 'busy', sourceCoverageConfirmed: Boolean(active) }));
      return;
    }
    const sync = new BookSearchSyncService(config, queue, new BookSearchDocumentProjector(), new ZincBookIndexWriter(config), generation);
    const coordinator = new BookSearchCoordinatorService(config, baseline, sync, generation);
    const result = await coordinator.runOnce();
    console.log(JSON.stringify({ workerPid: process.pid, ...result }));
  } finally {
    await pool.end();
  }
}

void main().catch(() => {
  // Connection strings and provider credentials must not appear in CLI error output.
  console.error('Book search maintenance failed; check configuration, database migration and engine availability. Durable progress retained.');
  process.exitCode = 1;
});
