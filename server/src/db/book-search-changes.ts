import { BadRequestException } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { eq, sql, type SQL } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { bookSearchPending, bookSearchState } from './book-search-schema';
import type * as schema from './schema';

export type BookSearchTransaction = Parameters<Parameters<NodePgDatabase<typeof schema>['transaction']>[0]>[0];
export type BookSearchChangeExecutor = Pick<NodePgDatabase<typeof schema>, 'insert' | 'update'>;

const deferredEnqueues = new WeakMap<BookSearchChangeExecutor, (ids: number[]) => Promise<void>>();

// Editors call several mutation helpers in one transaction. Delay the global revision lock
// until all source writes finish, without collecting a large author fan-out in application memory.
export async function withDeferredBookSearchChanges<T>(tx: BookSearchTransaction, callback: () => Promise<T>): Promise<T> {
  if (deferredEnqueues.has(tx)) return callback();
  const table = sql.identifier(`book_search_capture_${randomUUID().replaceAll('-', '')}`);
  let ready: Promise<unknown> | undefined;
  deferredEnqueues.set(tx, async (ids) => {
    ready ??= Promise.resolve(tx.execute(sql`CREATE TEMP TABLE ${table} (book_id integer PRIMARY KEY) ON COMMIT DROP`));
    await ready;
    const values = sql.join(
      ids.map((id) => sql`(${id})`),
      sql`, `,
    );
    await tx.execute(sql`INSERT INTO ${table} (book_id) VALUES ${values} ON CONFLICT DO NOTHING`);
  });
  try {
    const result = await callback();
    if (ready) {
      await ready;
      await tx.execute(sql`WITH ${enqueueBookSearchSelection(sql`SELECT book_id FROM ${table}`, sql`SELECT 0`)}`);
    }
    return result;
  } finally {
    deferredEnqueues.delete(tx);
  }
}

// Both arguments are internal SQL selections. The dependency must finish all source writes
// before the global revision is locked, keeping mutation and worker lock ordering consistent.
export function enqueueBookSearchSelection(bookIds: SQL, sourceWrites: SQL): SQL {
  return sql`bumped_search AS (
    INSERT INTO ${bookSearchState} (id, revision)
    SELECT 1, 1 WHERE EXISTS (${bookIds}) AND (${sourceWrites}) >= 0
    ON CONFLICT (id) DO UPDATE SET revision = ${bookSearchState.revision} + 1
    RETURNING revision
  )
  INSERT INTO ${bookSearchPending} (book_id, revision)
  SELECT DISTINCT changed.book_id, bumped_search.revision FROM (${bookIds}) changed CROSS JOIN bumped_search
  ON CONFLICT (book_id) DO UPDATE SET revision = excluded.revision, queued_at = now()`;
}

// Call inside the same transaction as the source mutation, never after commit.
export async function enqueueBookSearchChanges(tx: BookSearchChangeExecutor, bookIds: number[]): Promise<bigint | null> {
  const ids = [...new Set(bookIds)];
  if (ids.length > 1000 || ids.some((id) => !Number.isSafeInteger(id) || id <= 0)) {
    throw new BadRequestException('Invalid search change batch');
  }
  if (!ids.length) return null;
  const defer = deferredEnqueues.get(tx);
  if (defer) {
    await defer(ids);
    return null;
  }
  await tx.insert(bookSearchState).values({ id: 1 }).onConflictDoNothing();
  const [state] = await tx
    .update(bookSearchState)
    .set({ revision: sql`${bookSearchState.revision} + 1` })
    .where(eq(bookSearchState.id, 1))
    .returning({ revision: bookSearchState.revision });
  await tx
    .insert(bookSearchPending)
    .values(ids.map((bookId) => ({ bookId, revision: state.revision })))
    .onConflictDoUpdate({ target: bookSearchPending.bookId, set: { revision: state.revision, queuedAt: sql`now()` } });
  return state.revision;
}
