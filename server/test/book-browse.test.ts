import { execFile } from 'node:child_process';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { and, asc, eq, sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import * as schema from '../src/db/schema';
import { BookRepository } from '../src/modules/book/book.repository';

const connectionString = process.env.BOOK_SEARCH_TEST_DATABASE_URL;

describe.runIf(Boolean(connectionString))('native title browsing on disposable PostgreSQL', () => {
  let admin: Pool;
  let pool: Pool;
  let db: ReturnType<typeof drizzle<typeof schema>>;
  let repository: BookRepository;
  let libraryId: number;
  let userId: number;
  let firstBook: number;

  beforeAll(async () => {
    const url = new URL(connectionString!);
    if (url.hostname !== '127.0.0.1' || url.port !== '55432' || url.pathname !== '/book_search_test')
      throw new Error('Dedicated disposable PostgreSQL fixture required');
    admin = new Pool({ connectionString, max: 1 });
    await admin.query("CREATE DATABASE book_browse_test TEMPLATE template0 LC_COLLATE 'en_US.utf8' LC_CTYPE 'en_US.utf8'");
    url.pathname = '/book_browse_test';
    await promisify(execFile)(process.execPath, [join(process.cwd(), 'node_modules/tsx/dist/cli.mjs'), 'src/scripts/migrate.ts'], {
      env: { ...process.env, DATABASE_URL: url.toString(), BOOK_SEARCH_ENABLED: 'false' },
      timeout: 30000,
      maxBuffer: 256 * 1024,
    });
    pool = new Pool({ connectionString: url.toString(), max: 4, statement_timeout: 10000 });
    db = drizzle(pool, { schema });
    repository = new BookRepository(db);
    const [user] = await db.insert(schema.users).values({ username: 'browse-fixture', name: 'Browse fixture', passwordHash: 'unused' }).returning();
    userId = user.id;
    for (const hidden of [false, true]) {
      const [library] = await db
        .insert(schema.libraries)
        .values({ name: hidden ? 'Hidden browse fixture' : 'Visible browse fixture', watch: false })
        .returning();
      const [folder] = await db
        .insert(schema.libraryFolders)
        .values({ libraryId: library.id, path: hidden ? '/fixture/hidden' : '/fixture/visible' })
        .returning();
      if (!hidden) libraryId = library.id;
      for (const [index, title] of ['a', 'A', 'a', '', '中文', null, undefined, 'processing'].entries()) {
        const [book] = await db
          .insert(schema.books)
          .values({
            libraryId: library.id,
            libraryFolderId: folder.id,
            folderPath: `${folder.path}/${index}`,
            status: title === 'processing' ? 'processing' : 'present',
          })
          .returning();
        if (title !== undefined) await db.insert(schema.bookMetadata).values({ bookId: book.id, title, language: index % 2 === 0 ? 'en' : 'zh' });
        if (!hidden && index === 0) firstBook = book.id;
        for (const [fileIndex, format] of (index === 0 ? ['epub', 'epub', 'pdf'] : ['pdf']).entries()) {
          await db.insert(schema.bookFiles).values({
            bookId: book.id,
            libraryFolderId: folder.id,
            absolutePath: `${folder.path}/${index}/${fileIndex}.${format}`,
            relPath: `${index}/${fileIndex}.${format}`,
            ino: BigInt(book.id * 10 + fileIndex),
            format,
            mediaOverlayAvailable: index === 0 && format === 'epub',
          });
        }
      }
    }
  }, 60000);

  afterAll(async () => {
    await pool?.end();
    if (admin) {
      await admin.query('DROP DATABASE IF EXISTS book_browse_test');
      await admin.end();
    }
  });

  it.each(['asc', 'desc'] as const)('preserves exact %s ordering, null and absent metadata, offsets and totals', async (direction) => {
    for (const where of [eq(schema.books.libraryId, libraryId), and(eq(schema.books.libraryId, libraryId), eq(schema.bookMetadata.language, 'en'))]) {
      for (const offset of [0, 2, 4, 6, 8]) {
        const options = {
          where,
          orderBy: [sql`${schema.bookMetadata.title} ${direction === 'asc' ? sql`asc` : sql`desc`} nulls last`, asc(schema.books.id)],
          limit: 2,
          offset,
          userId,
        };
        const expected = await repository.findCards(options);
        const actual = await repository.findCards({ ...options, nativeTitleDirection: direction });
        expect(actual).toEqual(expected);
        expect(actual.rows.every((row) => row.libraryId === libraryId && row.status !== 'processing')).toBe(true);
      }
    }
  });

  it('counts each book once per physical and read-along format with correlated format filtering', async () => {
    const where = eq(schema.books.libraryId, libraryId);
    const facets = await repository.countFormatFacets(where, ['epub', 'pdf', 'epub:readalong']);
    expect(Object.fromEntries(facets.map((row) => [row.format, row.count]))).toEqual({ epub: 1, pdf: 7, 'epub:readalong': 1 });
    const filtered = await repository.countFormatFacets(
      and(where, sql`exists (select 1 from book_files where book_files.book_id = ${schema.books.id} and book_files.format = 'epub')`),
      ['pdf', 'epub:readalong'],
    );
    expect(Object.fromEntries(filtered.map((row) => [row.format, row.count]))).toEqual({ pdf: 1, 'epub:readalong': 1 });
    expect(await repository.countFormatFacets(where, ['pdf'])).toEqual([{ format: 'pdf', count: 7 }]);
  });

  it('keeps count, title selection and card hydration on one snapshot during a concurrent edit', async () => {
    const options = {
      where: eq(schema.books.libraryId, libraryId),
      orderBy: [sql`${schema.bookMetadata.title} asc nulls last`, asc(schema.books.id)],
      limit: 4,
      offset: 0,
      userId,
    };
    const expected = await repository.findCards(options);
    const count = BookRepository.prototype.countWhere;
    const spy = vi.spyOn(BookRepository.prototype, 'countWhere').mockImplementationOnce(async function (this: BookRepository, where) {
      const total = await count.call(this, where);
      await db.update(schema.bookMetadata).set({ title: 'ZZ concurrent edit' }).where(eq(schema.bookMetadata.bookId, firstBook));
      return total;
    });
    try {
      const actual = await repository.findCards({ ...options, nativeTitleDirection: 'asc' });
      expect(actual).toEqual(expected);
      expect(spy).toHaveBeenCalledTimes(1);
    } finally {
      spy.mockRestore();
      await db.update(schema.bookMetadata).set({ title: 'a' }).where(eq(schema.bookMetadata.bookId, firstBook));
    }
  });
});
