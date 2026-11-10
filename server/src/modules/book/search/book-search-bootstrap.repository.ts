import { BadRequestException, Inject, Injectable, ServiceUnavailableException } from '@nestjs/common';
import { and, asc, eq, gt, inArray, lte, sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { DB } from '../../../db';
import type * as schema from '../../../db/schema';
import { books } from '../../../db/schema';
import { bookSearchBaseline, bookSearchPending, bookSearchState } from '../../../db/book-search-schema';
import type { BookSearchTransaction } from '../../../db/book-search-changes';
import { enqueueBookSearchChanges } from '../../../db/book-search-changes';
import { BOOK_SEARCH_SCHEMA_REVISION, bookSearchIndexDefinition } from './book-search-index-definition';
import { assertRestartProofFresh, type BookSearchRestartProof } from './book-search-restart-proof';

type SearchDatabase = NodePgDatabase<typeof schema>;
export type BookSearchBaselineProgress = typeof bookSearchBaseline.$inferSelect;

@Injectable()
export class BookSearchBootstrapRepository {
  constructor(@Inject(DB) private readonly db: SearchDatabase) {}

  async progress(): Promise<BookSearchBaselineProgress | null> {
    const [row] = await this.db.select().from(bookSearchBaseline).where(eq(bookSearchBaseline.id, 1));
    return row ?? null;
  }

  async disableServing(): Promise<void> {
    const found = await this.db.execute<{ present: boolean }>(sql`SELECT to_regclass('book_search.state') IS NOT NULL AS present`);
    if (found.rows[0]?.present)
      await this.db.execute(sql`UPDATE book_search.state SET snapshot_complete=false,write_coverage_complete=false WHERE id=1`);
  }

  async resumeVerifiedCheckpoint(
    sessionId: string,
    proof: BookSearchRestartProof,
    verifyIndex: (tx: BookSearchTransaction, state: typeof bookSearchState.$inferSelect) => Promise<void>,
  ): Promise<boolean | null> {
    if (proof.previousOwnerStopped !== true || proof.sourceCaptureVerified !== true)
      throw new BadRequestException('Source capture continuity and a stopped previous owner must be confirmed');
    if (!/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(sessionId) || sessionId === proof.checkpoint.captureSession)
      throw new BadRequestException('A new search capture session is required');
    assertRestartProofFresh(proof);
    return this.withWorkerLock(async (tx) => {
      const [baseline] = await tx.select().from(bookSearchBaseline).where(eq(bookSearchBaseline.id, 1)).for('update');
      if (!baseline) throw new ServiceUnavailableException('Search baseline has not been initialized');
      const state = await this.assertBinding(tx, baseline);
      if (
        baseline.phase !== 'indexed' ||
        state.schemaRevision !== BOOK_SEARCH_SCHEMA_REVISION ||
        !baseline.pruningComplete ||
        !state.snapshotComplete ||
        !state.writeCoverageComplete ||
        state.revision !== state.appliedRevision ||
        state.epoch !== proof.checkpoint.epoch ||
        state.revision !== proof.checkpoint.revision ||
        state.appliedRevision !== proof.checkpoint.appliedRevision ||
        state.indexName !== proof.checkpoint.indexName ||
        state.schemaRevision !== proof.checkpoint.schemaRevision ||
        state.engineIdentity !== proof.checkpoint.engineIdentity ||
        state.captureSession !== proof.checkpoint.captureSession
      )
        throw new ServiceUnavailableException('Verified search restart checkpoint changed');
      const pending = await tx.select({ id: bookSearchPending.bookId }).from(bookSearchPending).limit(1);
      if (pending.length) throw new ServiceUnavailableException('Book search still has pending changes');
      const source = await tx
        .select({ count: sql<string>`count(*)::text`, upper: sql<number>`coalesce(max(${books.id}), 0)::integer` })
        .from(books)
        .where(sql`${books.status} <> 'processing'`);
      if (source[0].count !== String(proof.sourceBooks) || source[0].upper !== proof.sourceUpperBookId)
        throw new ServiceUnavailableException('Verified search restart source census changed');
      await verifyIndex(tx, state);
      assertRestartProofFresh(proof);
      // The operator proves the old owner stopped and all external writes remained captured.
      // Consuming the old session under the worker/state locks makes this proof single use.
      await tx.update(bookSearchState).set({ captureSession: sessionId }).where(eq(bookSearchState.id, 1));
      return true;
    });
  }

  async startReconciliation(sessionId?: string): Promise<BookSearchBaselineProgress | null> {
    if (sessionId && !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(sessionId))
      throw new BadRequestException('Invalid search capture session');
    return this.withWorkerLock(async (tx) => {
      const [baseline] = await tx.select().from(bookSearchBaseline).where(eq(bookSearchBaseline.id, 1)).for('update');
      if (!baseline) throw new ServiceUnavailableException('Search baseline has not been initialized');
      const state = await this.assertBinding(tx, baseline);
      if (sessionId && state.captureSession === sessionId) return baseline;
      const [bound] = await tx
        .update(bookSearchState)
        .set({
          captureSession: sessionId ?? state.captureSession,
          snapshotComplete: false,
          writeCoverageComplete: false,
          revision: sql`${bookSearchState.revision} + 1`,
        })
        .where(eq(bookSearchState.id, 1))
        .returning();
      const [source] = await tx
        .select({ upper: sql<number>`coalesce(max(${books.id}), 0)::integer`, count: sql<string>`count(*)::text` })
        .from(books);
      const [started] = await tx
        .update(bookSearchBaseline)
        .set({
          phase: source.upper ? 'seeding' : 'catchup',
          upperBookId: source.upper,
          cursorBookId: 0,
          seededCount: 0n,
          estimatedBooks: BigInt(source.count),
          seedRevision: bound.revision,
          orphanCursorBookId: 0,
          pruningComplete: false,
          updatedAt: new Date(),
        })
        .where(eq(bookSearchBaseline.id, 1))
        .returning();
      return started;
    });
  }

  async initialize(indexName: string): Promise<BookSearchBaselineProgress | null> {
    bookSearchIndexDefinition(indexName);
    return this.withWorkerLock(async (tx) => {
      const [existing] = await tx.select().from(bookSearchBaseline).where(eq(bookSearchBaseline.id, 1));
      if (existing) {
        await this.assertBinding(tx, existing);
        if (existing.indexName !== indexName || existing.schemaRevision !== BOOK_SEARCH_SCHEMA_REVISION)
          throw new ServiceUnavailableException('Existing search baseline requires an explicit rebuild');
        return existing;
      }
      await tx.insert(bookSearchState).values({ id: 1 }).onConflictDoNothing();
      const [state] = await tx.select().from(bookSearchState).where(eq(bookSearchState.id, 1)).for('update');
      if (state.indexName) throw new ServiceUnavailableException('Existing search binding requires an explicit rebuild');
      const [bound] = await tx
        .update(bookSearchState)
        .set({
          indexName,
          schemaRevision: BOOK_SEARCH_SCHEMA_REVISION,
          snapshotComplete: false,
          writeCoverageComplete: false,
          appliedRevision: 0n,
          revision: sql`${bookSearchState.revision} + 1`,
        })
        .where(eq(bookSearchState.id, 1))
        .returning();
      const [source] = await tx
        .select({ upper: sql<number>`coalesce(max(${books.id}), 0)::integer`, count: sql<string>`count(*)::text` })
        .from(books);
      const [baseline] = await tx
        .insert(bookSearchBaseline)
        .values({
          id: 1,
          epoch: bound.epoch,
          indexName,
          schemaRevision: BOOK_SEARCH_SCHEMA_REVISION,
          phase: source.upper ? 'seeding' : 'catchup',
          upperBookId: source.upper,
          seedRevision: bound.revision,
          estimatedBooks: BigInt(source.count),
        })
        .returning();
      return baseline;
    });
  }

  async seedNextBatch(size = 1000): Promise<BookSearchBaselineProgress | null> {
    if (!Number.isSafeInteger(size) || size < 1 || size > 1000) throw new BadRequestException('Invalid baseline page size');
    return this.withWorkerLock(async (tx) => {
      const [baseline] = await tx.select().from(bookSearchBaseline).where(eq(bookSearchBaseline.id, 1)).for('update');
      if (!baseline) throw new ServiceUnavailableException('Search baseline has not been initialized');
      await this.assertBinding(tx, baseline);
      if (baseline.phase !== 'seeding') return baseline;
      const page = await tx
        .select({ id: books.id })
        .from(books)
        .where(and(gt(books.id, baseline.cursorBookId), lte(books.id, baseline.upperBookId)))
        .orderBy(asc(books.id))
        .limit(size);
      if (page.length) {
        // A source edit may already have queued a newer version while the baseline was paging.
        await tx
          .insert(bookSearchPending)
          .values(page.map((row) => ({ bookId: row.id, revision: baseline.seedRevision })))
          .onConflictDoNothing();
      }
      const complete = page.length < size || page[page.length - 1]?.id === baseline.upperBookId;
      const [updated] = await tx
        .update(bookSearchBaseline)
        .set({
          phase: complete ? 'catchup' : 'seeding',
          cursorBookId: complete ? baseline.upperBookId : page[page.length - 1].id,
          seededCount: baseline.seededCount + BigInt(page.length),
          updatedAt: new Date(),
        })
        .where(eq(bookSearchBaseline.id, 1))
        .returning();
      return updated;
    });
  }

  async prepareEngine(
    prepare: (baseline: BookSearchBaselineProgress, identity: string) => Promise<string>,
  ): Promise<BookSearchBaselineProgress | null> {
    return this.withWorkerLock(async (tx) => {
      const [baseline] = await tx.select().from(bookSearchBaseline).where(eq(bookSearchBaseline.id, 1)).for('update');
      if (!baseline) throw new ServiceUnavailableException('Search baseline has not been initialized');
      const state = await this.assertBinding(tx, baseline);
      const identity = await prepare(baseline, state.engineIdentity);
      if (!/^[a-f0-9]{64}$/.test(identity) || (state.engineIdentity && state.engineIdentity !== identity))
        throw new ServiceUnavailableException('Book search engine identity changed');
      if (!state.engineIdentity) await tx.update(bookSearchState).set({ engineIdentity: identity }).where(eq(bookSearchState.id, 1));
      return baseline;
    });
  }

  async pruneNextBatch(
    readLiveIds: (cursor: number, state: typeof bookSearchState.$inferSelect) => Promise<number[]>,
  ): Promise<BookSearchBaselineProgress | null> {
    return this.withWorkerLock(async (tx) => {
      const [baseline] = await tx.select().from(bookSearchBaseline).where(eq(bookSearchBaseline.id, 1)).for('update');
      if (!baseline) throw new ServiceUnavailableException('Search baseline has not been initialized');
      const state = await this.assertBinding(tx, baseline);
      if (baseline.phase === 'seeding' || baseline.pruningComplete) return baseline;
      const pending = await tx.select({ id: bookSearchPending.bookId }).from(bookSearchPending).limit(1);
      if (pending.length) return baseline;
      const ids = await readLiveIds(baseline.orphanCursorBookId, state);
      if (
        ids.length > 100 ||
        ids.some((id, index) => !Number.isSafeInteger(id) || id <= baseline.orphanCursorBookId || (index > 0 && id <= ids[index - 1]))
      )
        throw new ServiceUnavailableException('Invalid orphan scan page');
      if (ids.length) {
        const rows = await tx.select({ id: books.id, status: books.status }).from(books).where(inArray(books.id, ids));
        const live = new Set(rows.filter((row) => row.status !== 'processing').map((row) => row.id));
        await enqueueBookSearchChanges(
          tx,
          ids.filter((id) => !live.has(id)),
        );
      }
      const [updated] = await tx
        .update(bookSearchBaseline)
        .set({
          orphanCursorBookId: ids.at(-1) ?? baseline.orphanCursorBookId,
          pruningComplete: ids.length < 100,
          updatedAt: new Date(),
        })
        .where(eq(bookSearchBaseline.id, 1))
        .returning();
      return updated;
    });
  }

  async finishIfCaughtUp(
    verifyIndex: (tx: BookSearchTransaction, baseline: BookSearchBaselineProgress, state: typeof bookSearchState.$inferSelect) => Promise<void>,
    verification: 'enumerated' | 'complete-source-projection' = 'enumerated',
  ): Promise<BookSearchBaselineProgress | null> {
    return this.withWorkerLock(async (tx) => {
      const [baseline] = await tx.select().from(bookSearchBaseline).where(eq(bookSearchBaseline.id, 1)).for('update');
      if (!baseline) throw new ServiceUnavailableException('Search baseline has not been initialized');
      const state = await this.assertBinding(tx, baseline);
      if (baseline.phase === 'seeding') return baseline;
      if (!baseline.pruningComplete && verification !== 'complete-source-projection') return baseline;
      if (verification === 'complete-source-projection' && baseline.cursorBookId !== baseline.upperBookId) return baseline;
      const pending = await tx.select({ id: bookSearchPending.bookId }).from(bookSearchPending).limit(1);
      if (pending.length) return baseline;
      await verifyIndex(tx, baseline, state);
      await tx
        .update(bookSearchState)
        .set({ snapshotComplete: true, appliedRevision: sql`${bookSearchState.revision}` })
        .where(eq(bookSearchState.id, 1));
      const [completed] = await tx
        .update(bookSearchBaseline)
        .set({ phase: 'indexed', pruningComplete: true, updatedAt: new Date() })
        .where(eq(bookSearchBaseline.id, 1))
        .returning();
      // Source-capture continuity and engine-generation proof are separate serving gates.
      return completed;
    });
  }

  async activateServing(
    indexName: string,
    captureSession: string,
    verifyIndex: (tx: BookSearchTransaction, state: typeof bookSearchState.$inferSelect) => Promise<void>,
  ): Promise<boolean | null> {
    bookSearchIndexDefinition(indexName);
    if (!/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(captureSession))
      throw new BadRequestException('Invalid search capture session');
    return this.withWorkerLock(async (tx) => {
      const [baseline] = await tx.select().from(bookSearchBaseline).where(eq(bookSearchBaseline.id, 1)).for('update');
      if (!baseline) throw new ServiceUnavailableException('Search baseline has not been initialized');
      const state = await this.assertBinding(tx, baseline);
      if (state.captureSession !== captureSession) throw new ServiceUnavailableException('Book search capture session changed');
      if (
        state.indexName !== indexName ||
        state.schemaRevision !== BOOK_SEARCH_SCHEMA_REVISION ||
        !state.engineIdentity ||
        !state.snapshotComplete ||
        baseline.phase !== 'indexed' ||
        !baseline.pruningComplete ||
        state.revision !== state.appliedRevision
      )
        throw new ServiceUnavailableException('Book search is not caught up');
      const pending = await tx.select({ id: bookSearchPending.bookId }).from(bookSearchPending).limit(1);
      if (pending.length) throw new ServiceUnavailableException('Book search still has pending changes');
      await verifyIndex(tx, state);
      // The operator confirms all external producers. Keep that assertion separate
      // from baseline completion and bind it to this application capture session.
      await tx.update(bookSearchState).set({ writeCoverageComplete: true }).where(eq(bookSearchState.id, 1));
      return true;
    });
  }

  private async assertBinding(tx: BookSearchTransaction, baseline: BookSearchBaselineProgress) {
    const [state] = await tx.select().from(bookSearchState).where(eq(bookSearchState.id, 1)).for('update');
    if (!state || state.epoch !== baseline.epoch || state.indexName !== baseline.indexName || state.schemaRevision !== baseline.schemaRevision)
      throw new ServiceUnavailableException('Book search baseline binding changed');
    return state;
  }

  private async withWorkerLock<T>(action: (tx: BookSearchTransaction) => Promise<T>): Promise<T | null> {
    return this.db.transaction(async (tx) => {
      const lock = await tx.execute<{ acquired: boolean }>(sql`SELECT pg_try_advisory_xact_lock(724073, 1) AS acquired`);
      if (!lock.rows[0]?.acquired) return null;
      return action(tx);
    });
  }
}
