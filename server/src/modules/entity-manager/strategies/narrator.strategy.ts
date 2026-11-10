import { Inject, Injectable, Optional } from '@nestjs/common';
import { and, asc, eq, inArray, sql } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';

import { DB } from '../../../db';
import * as schema from '../../../db/schema';
import { bookMetadata, bookNarrators, books, narrators } from '../../../db/schema';
import type {
  BrowseParams,
  BrowseResult,
  EntityBookScope,
  MergeInput,
  RenameInput,
  DeleteInput,
  SplitInput,
  StrategyMergeResult,
  StrategyRenameResult,
  StrategyDeleteResult,
  StrategySplitResult,
} from './entity-strategy.interface';
import { buildEntityBookScopeClauses } from './entity-book-scope';
import { JunctionEntityStrategy } from './junction-entity.strategy';
import { BookSearchCaptureService } from '../../../common/services/book-search-capture.service';
import { withDeferredBookSearchChanges } from '../../../db/book-search-changes';

type Db = NodePgDatabase<typeof schema>;

@Injectable()
export class NarratorStrategy extends JunctionEntityStrategy {
  private inSearchTransaction = false;
  readonly entityType = 'narrator' as const;
  protected readonly entityTable = narrators;
  protected readonly junctionTable = bookNarrators;
  protected readonly entityIdCol = narrators.id;
  protected readonly junctionEntityIdCol = bookNarrators.narratorId;
  protected readonly junctionBookIdCol = bookNarrators.bookId;
  protected readonly nameCol = narrators.name;
  protected readonly rawTableName = 'narrators';
  protected readonly rawJunctionTable = 'book_narrators';
  protected readonly rawEntityIdCol = 'narrator_id';
  protected readonly hasCascade = false;

  constructor(
    @Inject(DB) db: Db,
    @Optional() private readonly searchCapture?: BookSearchCaptureService,
  ) {
    super(db);
  }

  protected buildJunctionRow(bookId: number, entityId: number) {
    return { bookId, narratorId: entityId, displayOrder: 0 };
  }

  override merge(input: MergeInput): Promise<StrategyMergeResult> {
    return this.captureMutation(
      [input.targetId, ...input.sourceIds],
      (repo) => repo.merge(input),
      () => super.merge(input),
    );
  }

  override rename(input: RenameInput): Promise<StrategyRenameResult> {
    return this.captureMutation(
      [input.entityId],
      (repo) => repo.rename(input),
      () => super.rename(input),
    );
  }

  override deleteEntity(input: DeleteInput): Promise<StrategyDeleteResult> {
    return this.captureMutation(
      [input.entityId],
      (repo) => repo.deleteEntity(input),
      () => super.deleteEntity(input),
    );
  }

  override split(input: SplitInput): Promise<StrategySplitResult> {
    return this.captureMutation(
      [input.entityId],
      (repo) => repo.split(input),
      () => super.split(input),
    );
  }

  private captureMutation<T extends { affectedBookIds: number[] }>(
    ids: (number | string)[],
    mutate: (repo: NarratorStrategy) => Promise<T>,
    original: () => Promise<T>,
  ): Promise<T> {
    if (!this.searchCapture?.enabled || this.inSearchTransaction) return original();
    return this.db.transaction((tx) =>
      withDeferredBookSearchChanges(tx, async () => {
        const numericIds = [...new Set(ids.map(Number).filter((id) => Number.isSafeInteger(id) && id > 0))].sort((a, b) => a - b);
        if (numericIds.length)
          await tx.select({ id: narrators.id }).from(narrators).where(inArray(narrators.id, numericIds)).orderBy(asc(narrators.id)).for('update');
        const scoped = new NarratorStrategy(tx as unknown as Db, this.searchCapture);
        scoped.inSearchTransaction = true;
        const result = await mutate(scoped);
        await this.searchCapture!.enqueue(tx, result.affectedBookIds);
        return result;
      }),
    );
  }

  override async browse(params: BrowseParams): Promise<BrowseResult> {
    const result = await super.browse(params);
    const ids = result.items.map((item) => item.id as number);
    if (ids.length === 0) return result;

    const sortNames = await this.db
      .select({ id: narrators.id, sortName: narrators.sortName })
      .from(narrators)
      .where(sql`${narrators.id} IN (${sql.raw(ids.join(','))})`);

    const sortNameMap = new Map(sortNames.map((r) => [r.id, r.sortName]));
    return {
      ...result,
      items: result.items.map((item) => ({ ...item, sortName: sortNameMap.get(item.id as number) ?? null })),
    };
  }

  override async getBookTitles(id: number | string, limit: number, scope?: EntityBookScope): Promise<string[]> {
    const rows = await this.db
      .select({ title: sql<string>`COALESCE(${bookMetadata.title}, 'Untitled')` })
      .from(bookNarrators)
      .innerJoin(books, eq(books.id, bookNarrators.bookId))
      .leftJoin(bookMetadata, eq(bookMetadata.bookId, books.id))
      .where(and(eq(bookNarrators.narratorId, id as number), ...(scope ? buildEntityBookScopeClauses(this.db, scope) : [])))
      .orderBy(asc(bookMetadata.title))
      .limit(limit);
    return rows.map((r) => r.title);
  }
}
