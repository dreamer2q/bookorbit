import { sql } from 'drizzle-orm';
import { bigint, boolean, check, index, integer, pgSchema, text, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';

export const bookSearchSchema = pgSchema('book_search');

export const bookSearchState = bookSearchSchema.table(
  'state',
  {
    id: integer('id').primaryKey().default(1),
    epoch: uuid('epoch').notNull().defaultRandom(),
    revision: bigint('revision', { mode: 'bigint' })
      .notNull()
      .default(sql`0`),
    appliedRevision: bigint('applied_revision', { mode: 'bigint' })
      .notNull()
      .default(sql`0`),
    snapshotComplete: boolean('snapshot_complete').notNull().default(false),
    writeCoverageComplete: boolean('write_coverage_complete').notNull().default(false),
    indexName: varchar('index_name', { length: 128 }).notNull().default(''),
    schemaRevision: integer('schema_revision').notNull().default(0),
    engineIdentity: varchar('engine_identity', { length: 64 }).notNull().default(''),
    captureSession: uuid('capture_session'),
  },
  (t) => [
    check('state_singleton', sql`${t.id} = 1`),
    check('state_revisions', sql`${t.appliedRevision} >= 0 AND ${t.appliedRevision} <= ${t.revision}`),
  ],
);

export const bookSearchBaseline = bookSearchSchema.table(
  'baseline',
  {
    id: integer('id').primaryKey().default(1),
    epoch: uuid('epoch').notNull(),
    indexName: varchar('index_name', { length: 128 }).notNull(),
    schemaRevision: integer('schema_revision').notNull(),
    phase: text('phase', { enum: ['seeding', 'catchup', 'indexed'] }).notNull(),
    upperBookId: integer('upper_book_id').notNull(),
    cursorBookId: integer('cursor_book_id').notNull().default(0),
    seededCount: bigint('seeded_count', { mode: 'bigint' })
      .notNull()
      .default(sql`0`),
    estimatedBooks: bigint('estimated_books', { mode: 'bigint' })
      .notNull()
      .default(sql`0`),
    seedRevision: bigint('seed_revision', { mode: 'bigint' }).notNull(),
    orphanCursorBookId: integer('orphan_cursor_book_id').notNull().default(0),
    pruningComplete: boolean('pruning_complete').notNull().default(false),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    check('baseline_singleton', sql`${t.id} = 1`),
    check(
      'baseline_range',
      sql`${t.cursorBookId} >= 0 AND ${t.cursorBookId} <= ${t.upperBookId} AND ${t.seededCount} >= 0 AND ${t.seedRevision} > 0 AND ${t.orphanCursorBookId} >= 0`,
    ),
    check('baseline_phase', sql`${t.phase} IN ('seeding', 'catchup', 'indexed')`),
  ],
);

// Deliberately no FK: a deleted book must remain queued until its index document is removed.
export const bookSearchPending = bookSearchSchema.table(
  'pending',
  {
    bookId: integer('book_id').primaryKey(),
    revision: bigint('revision', { mode: 'bigint' }).notNull(),
    queuedAt: timestamp('queued_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [index('pending_revision_book_idx').on(t.revision, t.bookId), check('pending_positive_ids', sql`${t.bookId} > 0 AND ${t.revision} > 0`)],
);
