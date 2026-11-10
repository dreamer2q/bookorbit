import { BadRequestException, Injectable } from '@nestjs/common';
import { asc, eq, inArray, sql } from 'drizzle-orm';
import { authors, bookAuthors, bookMetadata, bookNarrators, bookSeries, bookSeriesMemberships, books, narrators } from '../../../db/schema';
import type { BookSearchTransaction, PendingBookSearchChange } from './book-search-sync.repository';
import type { BookIndexMutation } from './zinc-book-index-writer';
import { literalSearchFields } from './book-literal-search';

@Injectable()
export class BookSearchDocumentProjector {
  async project(tx: BookSearchTransaction, changes: PendingBookSearchChange[]): Promise<BookIndexMutation[]> {
    if (!changes.length) return [];
    if (changes.length > 500) throw new BadRequestException('Search projection batch exceeded its bound');
    const ids = changes.map((change) => change.bookId);
    const rows = await tx
      .select({
        id: books.id,
        libraryId: books.libraryId,
        status: books.status,
        title: bookMetadata.title,
        subtitle: bookMetadata.subtitle,
        publisher: bookMetadata.publisher,
        isbn10: bookMetadata.isbn10,
        isbn13: bookMetadata.isbn13,
        series: bookMetadata.seriesName,
        language: bookMetadata.language,
        publishedYear: bookMetadata.publishedYear,
        pageCount: bookMetadata.pageCount,
        description: sql<string | null>`CASE WHEN octet_length(${bookMetadata.description}) <= 65536 THEN ${bookMetadata.description} END`,
        descriptionTooLarge: sql<boolean>`coalesce(octet_length(${bookMetadata.description}) > 65536, false)`,
        nativeFields: sql<
          (string | null)[]
        >`ARRAY[lower(public.bookorbit_unaccent(${bookMetadata.title})), lower(public.bookorbit_unaccent(${bookMetadata.subtitle})), lower(public.bookorbit_unaccent(${bookMetadata.seriesName}))]`,
        foldedText: sql<string>`lower(public.bookorbit_unaccent(concat_ws(chr(10), ${bookMetadata.title}, ${bookMetadata.subtitle}, ${bookMetadata.publisher}, ${bookMetadata.seriesName}, ${bookMetadata.isbn10}, ${bookMetadata.isbn13}, CASE WHEN octet_length(${bookMetadata.description}) <= 65536 THEN ${bookMetadata.description} END)))`,
      })
      .from(books)
      .leftJoin(bookMetadata, eq(books.id, bookMetadata.bookId))
      .where(inArray(books.id, ids));
    const visibleIds = rows.filter((row) => row.status !== 'processing').map((row) => row.id);
    if (rows.some((row) => row.status !== 'processing' && row.descriptionTooLarge))
      throw new BadRequestException('Search metadata exceeded its document bound');
    if (!visibleIds.length) return changes.map((change) => ({ bookId: change.bookId, revision: change.revision, document: null }));
    const names = await tx
      .select({
        bookId: bookAuthors.bookId,
        name: authors.name,
        foldedName: sql<string>`lower(public.bookorbit_unaccent(${authors.name}))`,
        count: sql<number>`count(*) OVER (PARTITION BY ${bookAuthors.bookId})::integer`,
      })
      .from(bookAuthors)
      .innerJoin(authors, eq(authors.id, bookAuthors.authorId))
      .where(inArray(bookAuthors.bookId, visibleIds))
      .orderBy(asc(bookAuthors.bookId), asc(bookAuthors.displayOrder), asc(authors.id))
      .limit(visibleIds.length * 101);
    if (names.some((row) => row.count > 100)) throw new BadRequestException('Search author list exceeded its document bound');
    const seriesRows = await tx
      .select({
        bookId: bookSeriesMemberships.bookId,
        name: bookSeries.name,
        foldedName: sql<string>`lower(public.bookorbit_unaccent(${bookSeries.name}))`,
        count: sql<number>`count(*) OVER (PARTITION BY ${bookSeriesMemberships.bookId})::integer`,
      })
      .from(bookSeriesMemberships)
      .innerJoin(bookSeries, eq(bookSeries.id, bookSeriesMemberships.seriesId))
      .where(inArray(bookSeriesMemberships.bookId, visibleIds))
      .orderBy(asc(bookSeriesMemberships.bookId), asc(bookSeriesMemberships.displayOrder), asc(bookSeries.id))
      .limit(visibleIds.length * 101);
    if (seriesRows.some((row) => row.count > 100)) throw new BadRequestException('Search series list exceeded its document bound');
    const narratorRows = await tx
      .select({
        bookId: bookNarrators.bookId,
        name: narrators.name,
        foldedName: sql<string>`lower(public.bookorbit_unaccent(${narrators.name}))`,
        count: sql<number>`count(*) OVER (PARTITION BY ${bookNarrators.bookId})::integer`,
      })
      .from(bookNarrators)
      .innerJoin(narrators, eq(narrators.id, bookNarrators.narratorId))
      .where(inArray(bookNarrators.bookId, visibleIds))
      .orderBy(asc(bookNarrators.bookId), asc(bookNarrators.displayOrder), asc(narrators.id))
      .limit(visibleIds.length * 101);
    if (narratorRows.some((row) => row.count > 100)) throw new BadRequestException('Search narrator list exceeded its document bound');
    const byId = new Map(rows.map((row) => [row.id, row]));
    const byAuthor = this.groupNames(names);
    const bySeries = this.groupNames(seriesRows);
    const byNarrator = this.groupNames(narratorRows);
    const byFoldedAuthor = this.groupNames(names, 'foldedName');
    const byFoldedSeries = this.groupNames(seriesRows, 'foldedName');
    const byFoldedNarrator = this.groupNames(narratorRows, 'foldedName');
    return changes.map((change) => {
      const row = byId.get(change.bookId);
      if (!row || row.status === 'processing') return { bookId: change.bookId, revision: change.revision, document: null };
      const authorNames = byAuthor.get(row.id) ?? [];
      const seriesNames = [...new Set([...(row.series ? [row.series] : []), ...(bySeries.get(row.id) ?? [])])];
      const narratorNames = byNarrator.get(row.id) ?? [];
      const foldedAuthors = byFoldedAuthor.get(row.id) ?? [];
      const foldedSeries = byFoldedSeries.get(row.id) ?? [];
      const foldedNarrators = byFoldedNarrator.get(row.id) ?? [];
      return {
        bookId: row.id,
        revision: change.revision,
        document: {
          book_id: row.id,
          library_id: String(row.libraryId),
          status: row.status,
          title: row.title ?? '',
          subtitle: row.subtitle ?? '',
          // A nonempty common prefix keeps an empty title distinct from a missing
          // keyword value while preserving byte order, case and leading spaces.
          title_sort: `t${row.title ?? ''}`,
          title_null: row.title === null ? 1 : 0,
          publisher: row.publisher ?? '',
          publisher_exact: row.publisher ?? '',
          authors: authorNames.join('\n'),
          authors_exact: authorNames,
          isbn10: row.isbn10 ?? '',
          isbn13: row.isbn13 ?? '',
          series: row.series ?? '',
          series_exact: seriesNames,
          narrators: narratorNames.join('\n'),
          narrators_exact: narratorNames,
          language: row.language ?? '',
          ...(row.publishedYear !== null ? { published_year: row.publishedYear } : {}),
          ...(row.pageCount !== null ? { page_count: row.pageCount } : {}),
          description: row.description ?? '',
          ...literalSearchFields([
            ...row.nativeFields.filter((value): value is string => value !== null),
            ...foldedAuthors,
            ...foldedSeries,
            ...foldedNarrators,
          ]),
          search_text: [row.foldedText, ...foldedAuthors, ...foldedSeries, ...foldedNarrators].filter(Boolean).join('\n'),
        },
      };
    });
  }

  private groupNames(rows: { bookId: number; name: string; foldedName: string }[], field: 'name' | 'foldedName' = 'name'): Map<number, string[]> {
    const result = new Map<number, string[]>();
    for (const row of rows) {
      const names = result.get(row.bookId) ?? [];
      names.push(row[field]);
      result.set(row.bookId, names);
    }
    return result;
  }
}
