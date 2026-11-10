import { Inject, Injectable } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { appConfig } from '../../config/config';
import { enqueueBookSearchChanges, type BookSearchChangeExecutor } from '../../db/book-search-changes';
import type { bookMetadata } from '../../db/schema';
import { bookAuthors, bookNarrators, bookSeriesMemberships } from '../../db/schema';
import { and, asc, gt, inArray } from 'drizzle-orm';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';

const projectedMetadataKeys = new Set<keyof typeof bookMetadata.$inferInsert>([
  'title',
  'subtitle',
  'description',
  'publisher',
  'isbn10',
  'isbn13',
  'seriesName',
  'language',
  'publishedYear',
  'pageCount',
]);

@Injectable()
export class BookSearchCaptureService {
  constructor(@Inject(appConfig.KEY) private readonly config: ConfigType<typeof appConfig>) {}

  get enabled(): boolean {
    return this.config.bookSearch.enabled;
  }

  capturesMetadata(fields: Partial<typeof bookMetadata.$inferInsert>): boolean {
    return this.config.bookSearch.enabled && Object.keys(fields).some((key) => projectedMetadataKeys.has(key as keyof typeof fields));
  }

  async enqueue(tx: BookSearchChangeExecutor, bookIds: number[]): Promise<void> {
    if (!this.config.bookSearch.enabled) return;
    for (let offset = 0; offset < bookIds.length; offset += 1000) {
      const ids = [...new Set(bookIds.slice(offset, offset + 1000))].sort((a, b) => a - b);
      await enqueueBookSearchChanges(tx, ids);
    }
  }

  async enqueueAuthors(tx: BookSearchChangeExecutor & Pick<NodePgDatabase, 'select'>, authorIds: number[]): Promise<void> {
    if (!this.config.bookSearch.enabled || !authorIds.length) return;
    let cursor = 0;
    while (true) {
      const page = await tx
        .select({ id: bookAuthors.bookId })
        .from(bookAuthors)
        .where(and(inArray(bookAuthors.authorId, authorIds), gt(bookAuthors.bookId, cursor)))
        .groupBy(bookAuthors.bookId)
        .orderBy(asc(bookAuthors.bookId))
        .limit(1000);
      if (!page.length) return;
      await this.enqueue(
        tx,
        page.map((row) => row.id),
      );
      cursor = page[page.length - 1].id;
    }
  }

  async enqueueNarrators(tx: BookSearchChangeExecutor & Pick<NodePgDatabase, 'select'>, narratorIds: number[]): Promise<void> {
    if (!this.config.bookSearch.enabled || !narratorIds.length) return;
    let cursor = 0;
    while (true) {
      const page = await tx
        .select({ id: bookNarrators.bookId })
        .from(bookNarrators)
        .where(and(inArray(bookNarrators.narratorId, narratorIds), gt(bookNarrators.bookId, cursor)))
        .groupBy(bookNarrators.bookId)
        .orderBy(asc(bookNarrators.bookId))
        .limit(1000);
      if (!page.length) return;
      await this.enqueue(
        tx,
        page.map((row) => row.id),
      );
      cursor = page[page.length - 1].id;
    }
  }

  async enqueueSeries(tx: BookSearchChangeExecutor & Pick<NodePgDatabase, 'select'>, seriesIds: number[]): Promise<void> {
    if (!this.config.bookSearch.enabled || !seriesIds.length) return;
    let cursor = 0;
    while (true) {
      const page = await tx
        .select({ id: bookSeriesMemberships.bookId })
        .from(bookSeriesMemberships)
        .where(and(inArray(bookSeriesMemberships.seriesId, seriesIds), gt(bookSeriesMemberships.bookId, cursor)))
        .groupBy(bookSeriesMemberships.bookId)
        .orderBy(asc(bookSeriesMemberships.bookId))
        .limit(1000);
      if (!page.length) return;
      await this.enqueue(
        tx,
        page.map((row) => row.id),
      );
      cursor = page[page.length - 1].id;
    }
  }
}
