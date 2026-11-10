import { BadRequestException, Inject, Injectable, ServiceUnavailableException } from '@nestjs/common';
import { and, asc, desc, eq, gt, inArray, isNotNull, lt, ne, or, sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { DB } from '../../../db';
import { bookSearchBaseline, bookSearchPending, bookSearchState } from '../../../db/book-search-schema';
import type * as schema from '../../../db/schema';
import { bookMetadata, books } from '../../../db/schema';
import type { BookSearchTransaction } from '../../../db/book-search-changes';
import { normalizeMetadataText } from '../../../common/utils/metadata-text-normalize.utils';

export { enqueueBookSearchChanges } from '../../../db/book-search-changes';
export type { BookSearchTransaction } from '../../../db/book-search-changes';

type SearchDatabase = NodePgDatabase<typeof schema>;
export type PendingBookSearchChange = typeof bookSearchPending.$inferSelect;
export type BookSearchCheckpoint = typeof bookSearchState.$inferSelect;

@Injectable()
export class BookSearchSyncRepository {
  private binaryTitleSort?: boolean;
  private readonly nativeTitleIndexes = new Map<'asc' | 'desc', boolean>();
  constructor(@Inject(DB) private readonly db: SearchDatabase) {}

  async checkpoint(): Promise<BookSearchCheckpoint | null> {
    const [state] = await this.db.select().from(bookSearchState).where(eq(bookSearchState.id, 1));
    return state ?? null;
  }

  async pendingSummary() {
    const [summary] = await this.db
      .select({ count: sql<string>`count(*)::text`, oldestQueuedAt: sql<Date | null>`min(${bookSearchPending.queuedAt})` })
      .from(bookSearchPending);
    return summary;
  }

  async normalizeQuery(value: string): Promise<string> {
    const result = await this.db.execute<{ text: string }>(
      sql`SELECT lower(public.bookorbit_unaccent(${normalizeMetadataText(value) ?? ''})) AS text`,
    );
    if (typeof result.rows[0]?.text !== 'string') throw new ServiceUnavailableException('Book search normalization unavailable');
    return result.rows[0].text;
  }

  async supportsBinaryTitleSort(): Promise<boolean> {
    if (this.binaryTitleSort !== undefined) return this.binaryTitleSort;
    const result = await this.db.execute<{ provider: string; collation: string; encoding: string }>(sql`
      SELECT datlocprovider::text AS provider, datcollate AS collation, pg_encoding_to_char(encoding) AS encoding
      FROM pg_database WHERE datname=current_database()`);
    const row = result.rows[0];
    if (!row) throw new ServiceUnavailableException('Book search collation unavailable');
    // Keyword ordering agrees with native byte ordering only for these collations.
    // Keep linguistic collations on PostgreSQL rather than returning a different page.
    this.binaryTitleSort = row.provider === 'c' && ['C', 'POSIX'].includes(row.collation) && row.encoding === 'UTF8';
    return this.binaryTitleSort;
  }

  async sortNativeTitleIds(ids: number[], libraryIds: number[], direction: 'asc' | 'desc', offset: number, size: number): Promise<number[]> {
    if (!ids.length) return [];
    if (ids.length > 1000 || size < 1 || size > 100 || offset < 0 || offset + size > 1000)
      throw new BadRequestException('Invalid native title result window');
    const rows = await this.db
      .select({ id: books.id })
      .from(books)
      .leftJoin(bookMetadata, eq(bookMetadata.bookId, books.id))
      .where(and(inArray(books.id, ids), inArray(books.libraryId, libraryIds), ne(books.status, 'processing')))
      .orderBy(sql`${direction === 'asc' ? asc(bookMetadata.title) : desc(bookMetadata.title)} NULLS LAST`, asc(books.id))
      .offset(offset)
      .limit(size);
    return rows.map((row) => row.id);
  }

  async hasNativeTitleScanIndex(direction: 'asc' | 'desc'): Promise<boolean> {
    const cached = this.nativeTitleIndexes.get(direction);
    if (cached !== undefined) return cached;
    const result = await this.db.execute<{ indexed: boolean }>(sql`
      SELECT EXISTS (
        SELECT 1 FROM pg_index i
        JOIN pg_class ic ON ic.oid=i.indexrelid JOIN pg_am am ON am.oid=ic.relam
        JOIN pg_attribute title ON title.attrelid=i.indrelid AND title.attname='title'
        JOIN pg_attribute book ON book.attrelid=i.indrelid AND book.attname='book_id'
        WHERE i.indrelid='public.book_metadata'::regclass AND am.amname='btree'
          AND i.indisvalid AND i.indisready AND i.indpred IS NULL AND i.indexprs IS NULL
          AND i.indnkeyatts>=2 AND i.indkey[0]=title.attnum AND i.indkey[1]=book.attnum
          AND i.indcollation[0]=title.attcollation
          AND (i.indoption[0] & 1)=${direction === 'asc' ? 0 : 1} AND (i.indoption[1] & 1)=0
      ) AS indexed`);
    const indexed = result.rows[0]?.indexed === true;
    this.nativeTitleIndexes.set(direction, indexed);
    return indexed;
  }

  async nativeTitleCandidates(
    libraryIds: number[],
    direction: 'asc' | 'desc',
    size: number,
    cursor?: { title: string; id: number },
  ): Promise<{ title: string; id: number }[]> {
    if (size < 1 || size > 500) throw new BadRequestException('Invalid native title candidate window');
    const seek = cursor
      ? or(
          direction === 'asc' ? gt(bookMetadata.title, cursor.title) : lt(bookMetadata.title, cursor.title),
          and(eq(bookMetadata.title, cursor.title), gt(books.id, cursor.id)),
        )
      : undefined;
    // A non-null title has a metadata row, so the native title/book-id index can
    // drive this inner join. Null titles use the small-set or complete fallback.
    const rows = await this.db
      .select({ title: bookMetadata.title, id: books.id })
      .from(bookMetadata)
      .innerJoin(books, eq(books.id, bookMetadata.bookId))
      .where(and(inArray(books.libraryId, libraryIds), ne(books.status, 'processing'), isNotNull(bookMetadata.title), seek))
      .orderBy(direction === 'asc' ? asc(bookMetadata.title) : desc(bookMetadata.title), asc(books.id))
      .limit(size);
    return rows.map((row) => ({ id: row.id, title: row.title! }));
  }

  async withPendingBatch(
    size: number,
    applyAndVerifyVisible: (
      tx: BookSearchTransaction,
      changes: PendingBookSearchChange[],
      epoch: string,
      state: BookSearchCheckpoint,
    ) => Promise<void>,
    expectedBinding?: { indexName: string; schemaRevision: number },
    publishWhenDrained?: (tx: BookSearchTransaction, state: BookSearchCheckpoint) => Promise<void>,
  ): Promise<{ phase: 'busy' | 'idle' | 'applied' | 'seeding'; acknowledged: number }> {
    if (!Number.isSafeInteger(size) || size < 1 || size > 500) throw new BadRequestException('Invalid search worker batch size');
    return this.db.transaction(async (tx) => {
      const lock = await tx.execute<{ acquired: boolean }>(sql`SELECT pg_try_advisory_xact_lock(724073, 1) AS acquired`);
      if (!lock.rows[0]?.acquired) return { phase: 'busy', acknowledged: 0 };
      const [state] = await tx.select().from(bookSearchState).where(eq(bookSearchState.id, 1));
      if (!state) return { phase: 'idle', acknowledged: 0 };
      if (
        expectedBinding &&
        state.indexName &&
        (state.indexName !== expectedBinding.indexName || state.schemaRevision !== expectedBinding.schemaRevision)
      )
        throw new ServiceUnavailableException('Book search worker index binding changed');
      const [baseline] = await tx.select().from(bookSearchBaseline).where(eq(bookSearchBaseline.id, 1));
      if (baseline && (baseline.epoch !== state.epoch || baseline.indexName !== state.indexName || baseline.schemaRevision !== state.schemaRevision))
        throw new ServiceUnavailableException('Book search baseline binding changed');
      if (baseline?.phase === 'seeding') return { phase: 'seeding', acknowledged: 0 };
      if (expectedBinding && state.indexName && !state.engineIdentity) throw new ServiceUnavailableException('Book search engine is not initialized');
      const changes = await tx.select().from(bookSearchPending).orderBy(asc(bookSearchPending.revision), asc(bookSearchPending.bookId)).limit(size);
      if (!changes.length) return { phase: 'idle', acknowledged: 0 };
      // The callback must replace/remove documents idempotently and verify search visibility.
      // A crash after external writes leaves the DB queue intact for safe replay.
      await applyAndVerifyVisible(tx, changes, state.epoch, state);
      const [latest] = await tx.select().from(bookSearchState).where(eq(bookSearchState.id, 1)).for('update');
      if (
        !latest ||
        latest.epoch !== state.epoch ||
        latest.indexName !== state.indexName ||
        latest.schemaRevision !== state.schemaRevision ||
        latest.engineIdentity !== state.engineIdentity
      )
        throw new ServiceUnavailableException('Book search generation changed');
      let acknowledged = 0;
      const groups = new Map<bigint, number[]>();
      for (const change of changes) groups.set(change.revision, [...(groups.get(change.revision) ?? []), change.bookId]);
      for (const [revision, bookIds] of groups) {
        const deleted = await tx
          .delete(bookSearchPending)
          .where(and(eq(bookSearchPending.revision, revision), inArray(bookSearchPending.bookId, bookIds)))
          .returning({ bookId: bookSearchPending.bookId });
        acknowledged += deleted.length;
      }
      const remaining = await tx.select({ bookId: bookSearchPending.bookId }).from(bookSearchPending).limit(1);
      if (!remaining.length) {
        await publishWhenDrained?.(tx, latest);
        await tx
          .update(bookSearchState)
          .set({ appliedRevision: sql`${bookSearchState.revision}` })
          .where(eq(bookSearchState.id, 1));
      }
      return { phase: 'applied', acknowledged };
    });
  }
}
