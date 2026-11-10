import { Test } from '@nestjs/testing';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import { DB } from '../src/db';
import * as schema from '../src/db/schema';
import { bookSearchPending, bookSearchState } from '../src/db/book-search-schema';
import { BookSearchSyncRepository, enqueueBookSearchChanges } from '../src/modules/book/search/book-search-sync.repository';
import { withDeferredBookSearchChanges } from '../src/db/book-search-changes';
import { BookSearchCaptureService } from '../src/common/services/book-search-capture.service';
import { appConfig } from '../src/config/config';

const connectionString = process.env.BOOK_SEARCH_TEST_DATABASE_URL;

function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe.runIf(Boolean(connectionString))('book search transactional queue on isolated PostgreSQL', () => {
  const pool = new Pool({ connectionString, max: 5, connectionTimeoutMillis: 3000, statement_timeout: 5000 });
  const db = drizzle(pool, { schema });
  let repository: BookSearchSyncRepository;

  beforeAll(async () => {
    const url = new URL(connectionString!);
    if (url.hostname !== '127.0.0.1' || url.port !== '55432' || url.pathname !== '/book_search_test') {
      throw new Error('This test requires the dedicated disposable search fixture');
    }
    const module = await Test.createTestingModule({
      providers: [BookSearchSyncRepository, { provide: DB, useValue: db }],
    }).compile();
    repository = module.get(BookSearchSyncRepository);
    await db.execute(sql`CREATE TABLE book_search.source_fixture (book_id integer PRIMARY KEY, title text NOT NULL)`);
  });

  beforeEach(async () => {
    await db.execute(sql`TRUNCATE book_search.pending, book_search.state, book_search.baseline, book_search.source_fixture`);
  });

  afterAll(async () => {
    await pool.end();
  });

  it('does not silently initialize or approve an empty unbuilt index', async () => {
    expect(await repository.checkpoint()).toBeNull();
    expect(await repository.withPendingBatch(10, vi.fn())).toEqual({ phase: 'idle', acknowledged: 0 });
    expect(await repository.checkpoint()).toBeNull();
  });

  it('rolls back the source change and pending record together', async () => {
    await expect(
      db.transaction(async (tx) => {
        await tx.execute(sql`INSERT INTO book_search.source_fixture VALUES (1, 'original')`);
        await enqueueBookSearchChanges(tx, [1]);
        throw new Error('interrupted source transaction');
      }),
    ).rejects.toThrow('interrupted source transaction');
    expect((await db.execute(sql`SELECT * FROM book_search.source_fixture`)).rows).toEqual([]);
    expect(await db.select().from(bookSearchPending)).toEqual([]);
    expect(await repository.checkpoint()).toBeNull();
  });

  it('coalesces duplicate book IDs and blocks readiness until bootstrap and write coverage are proven', async () => {
    expect(await db.transaction((tx) => enqueueBookSearchChanges(tx, [1, 1, 2]))).toBe(1n);
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId).sort()).toEqual([1, 2]);
    await repository.withPendingBatch(10, async () => {});
    expect(await repository.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 1n, snapshotComplete: false, writeCoverageComplete: false });
  });

  it('stages bounded pages in the database and flushes a large deduplicated fan-out once at transaction end', async () => {
    const config = appConfig();
    config.bookSearch.enabled = true;
    const capture = new BookSearchCaptureService(config);
    await db.transaction((tx) =>
      withDeferredBookSearchChanges(tx, async () => {
        await capture.enqueue(
          tx,
          Array.from({ length: 1500 }, (_, index) => index + 1),
        );
        await withDeferredBookSearchChanges(tx, async () => {
          await enqueueBookSearchChanges(
            tx,
            Array.from({ length: 1000 }, (_, index) => index + 501),
          );
        });
        expect(await tx.select().from(bookSearchPending)).toEqual([]);
        expect(await tx.select().from(bookSearchState)).toEqual([]);
      }),
    );
    expect(await db.select().from(bookSearchPending)).toHaveLength(1500);
    expect(await repository.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 0n });
    expect((await db.execute(sql`SELECT relname FROM pg_class WHERE relname LIKE 'book_search_capture_%'`)).rows).toEqual([]);
  });

  it('rolls deferred staging back with the source and does not create staging for an empty transaction', async () => {
    await db.transaction((tx) => withDeferredBookSearchChanges(tx, () => Promise.resolve()));
    expect(await repository.checkpoint()).toBeNull();
    await expect(
      db.transaction((tx) =>
        withDeferredBookSearchChanges(tx, async () => {
          await tx.execute(sql`INSERT INTO book_search.source_fixture VALUES (1, 'deferred rollback')`);
          await enqueueBookSearchChanges(tx, [1]);
          throw new Error('deferred source failed');
        }),
      ),
    ).rejects.toThrow('deferred source failed');
    expect((await db.execute(sql`SELECT * FROM book_search.source_fixture`)).rows).toEqual([]);
    expect(await repository.checkpoint()).toBeNull();
    expect((await db.execute(sql`SELECT relname FROM pg_class WHERE relname LIKE 'book_search_capture_%'`)).rows).toEqual([]);
  });

  it('retains pending changes after an external write succeeds but verification fails, then replays idempotently', async () => {
    await db.transaction((tx) => enqueueBookSearchChanges(tx, [1]));
    const index = new Map<number, string>();
    await expect(
      repository.withPendingBatch(10, (_tx, changes) => {
        for (const row of changes) index.set(row.bookId, row.revision.toString());
        return Promise.reject(new Error('visibility not verified'));
      }),
    ).rejects.toThrow('visibility not verified');
    expect(await repository.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 0n });
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    expect(
      await repository.withPendingBatch(10, (_tx, changes) => {
        for (const row of changes) index.set(row.bookId, row.revision.toString());
        return Promise.resolve();
      }),
    ).toEqual({ phase: 'applied', acknowledged: 1 });
    expect(index.size).toBe(1);
    expect(await repository.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 1n });
  });

  it('does not acknowledge a newer edit to the same book and excludes a concurrent worker', async () => {
    await db.transaction((tx) => enqueueBookSearchChanges(tx, [10]));
    const started = gate();
    const release = gate();
    const first = repository.withPendingBatch(10, async () => {
      started.resolve();
      await release.promise;
    });
    try {
      await started.promise;
      await db.transaction((tx) => enqueueBookSearchChanges(tx, [10]));
      const other = vi.fn();
      expect(await repository.withPendingBatch(10, other)).toEqual({ phase: 'busy', acknowledged: 0 });
      expect(other).not.toHaveBeenCalled();
    } finally {
      release.resolve();
    }
    expect(await first).toEqual({ phase: 'applied', acknowledged: 0 });
    expect(await repository.checkpoint()).toMatchObject({ revision: 2n, appliedRevision: 0n });
    expect(await db.select().from(bookSearchPending)).toMatchObject([{ bookId: 10, revision: 2n }]);
    await repository.withPendingBatch(10, async () => {});
    expect(await repository.checkpoint()).toMatchObject({ revision: 2n, appliedRevision: 2n });
  });

  it('keeps the source watermark dirty while another bounded page remains', async () => {
    await db.transaction((tx) => enqueueBookSearchChanges(tx, [1, 2, 3]));
    expect(
      await repository.withPendingBatch(2, (_tx, rows) => {
        expect(rows).toHaveLength(2);
        return Promise.resolve();
      }),
    ).toEqual({ phase: 'applied', acknowledged: 2 });
    expect(await repository.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 0n });
    expect(
      await repository.withPendingBatch(2, (_tx, rows) => {
        expect(rows).toHaveLength(1);
        return Promise.resolve();
      }),
    ).toEqual({ phase: 'applied', acknowledged: 1 });
    expect(await repository.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 1n });
  });

  it('keeps a deletion queued after its source row is gone', async () => {
    await db.execute(sql`INSERT INTO book_search.source_fixture VALUES (9, 'delete me')`);
    await db.transaction(async (tx) => {
      await tx.execute(sql`DELETE FROM book_search.source_fixture WHERE book_id=9`);
      await enqueueBookSearchChanges(tx, [9]);
    });
    const sink = new Map([[9, 'old document']]);
    await repository.withPendingBatch(10, async (tx, changes) => {
      expect(changes.map((change) => change.bookId)).toEqual([9]);
      expect((await tx.execute(sql`SELECT * FROM book_search.source_fixture WHERE book_id=9`)).rows).toEqual([]);
      sink.delete(9);
    });
    expect(sink.size).toBe(0);
    expect(await db.select().from(bookSearchPending)).toEqual([]);
  });

  it('refuses to publish a checkpoint after its generation is replaced during indexing', async () => {
    await db.transaction((tx) => enqueueBookSearchChanges(tx, [1]));
    await expect(
      repository.withPendingBatch(10, async () => {
        await db.update(bookSearchState).set({ epoch: sql`gen_random_uuid()` });
      }),
    ).rejects.toThrow('Book search generation changed');
    expect(await repository.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 0n });
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
  });

  it('bounds enqueue batches and does not advance a revision for an empty batch', async () => {
    expect(await db.transaction((tx) => enqueueBookSearchChanges(tx, []))).toBeNull();
    expect(await repository.checkpoint()).toBeNull();
    await expect(db.transaction((tx) => enqueueBookSearchChanges(tx, [0]))).rejects.toThrow('Invalid search change batch');
    await expect(
      db.transaction((tx) =>
        enqueueBookSearchChanges(
          tx,
          Array.from({ length: 1001 }, (_, i) => i + 1),
        ),
      ),
    ).rejects.toThrow('Invalid search change batch');
    await expect(repository.withPendingBatch(501, vi.fn())).rejects.toThrow('Invalid search worker batch size');
  });
});
