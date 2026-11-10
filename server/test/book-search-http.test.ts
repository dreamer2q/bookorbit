import { execFile } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rename, rm, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { Uint8ArrayReader, Uint8ArrayWriter, ZipWriter } from '@zip.js/zip.js';
import fastifyCookie from '@fastify/cookie';
import { ValidationPipe } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { Test } from '@nestjs/testing';
import type { BooksPage, BookQuery, GlobalSearchResponse } from '@bookorbit/types';
import { hash } from 'bcryptjs';
import { eq, sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import { appConfig } from '../src/config/config';
import { GlobalExceptionFilter } from '../src/common/filters/http-exception.filter';
import { DB } from '../src/db';
import { enqueueBookSearchChanges } from '../src/db/book-search-changes';
import * as schema from '../src/db/schema';
import { BookSearchProvider } from '../src/modules/book/search/book-search-provider';
import { BookSearchCoordinatorService } from '../src/modules/book/search/book-search-coordinator.service';
import { BookSearchSyncRepository } from '../src/modules/book/search/book-search-sync.repository';
import { BookIndexSearchService } from '../src/modules/book/search/book-index-search.service';
import { BookSearchBootstrapRepository } from '../src/modules/book/search/book-search-bootstrap.repository';

const connectionString = process.env.BOOK_SEARCH_TEST_DATABASE_URL;
const endpoint = process.env.BOOK_SEARCH_TEST_ZINC_ENDPOINT;

describe.runIf(Boolean(connectionString && endpoint))('native AppModule search over authenticated HTTP on disposable fixtures', () => {
  let admin: Pool;
  let pool: Pool;
  let db: ReturnType<typeof drizzle<typeof schema>>;
  let app: NestFastifyApplication;
  let config: ConfigType<typeof appConfig>;
  let provider: BookSearchProvider;
  let directory: string;
  let baseUrl: string;
  let adminToken: string;
  let viewerToken: string;
  let visibleLibrary: number;
  let hiddenLibrary: number;
  let visibleBook: number;
  let hiddenBook: number;
  let visibleAuthor: number;
  let hiddenAuthor: number;
  let visibleSeries: number;
  let hiddenSeries: number;
  let appPool: Pool | undefined;
  const savedEnvironment: Record<string, string | undefined> = {};
  const password = 'HttpFixturePassword123';
  const timings: { operation: string; durationMs: number; status: number }[] = [];
  const originalFetch = globalThis.fetch;

  async function request(path: string, token?: string, body?: unknown, method = body === undefined ? 'GET' : 'POST') {
    const started = performance.now();
    const response = await fetch(baseUrl + '/api/v1' + path, {
      method,
      headers: { ...(token ? { authorization: `Bearer ${token}` } : {}), ...(body === undefined ? {} : { 'content-type': 'application/json' }) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(10000),
    });
    timings.push({ operation: path.split('?')[0], durationMs: Math.round(performance.now() - started), status: response.status });
    return response;
  }

  async function search(token: string, libraryId?: number): Promise<GlobalSearchResponse> {
    const response = await request('/search?q=' + encodeURIComponent('英语') + '&limit=10' + (libraryId ? `&libraryId=${libraryId}` : ''), token);
    expect(response.status).toBe(200);
    return response.json() as Promise<GlobalSearchResponse>;
  }

  async function catchUp() {
    config.bookSearch.workerEnabled = true;
    try {
      const coordinator = app.get(BookSearchCoordinatorService);
      for (let step = 0; step < 30; step++) {
        const result = await coordinator.runOnce();
        if (result.phase === 'indexed' || result.phase === 'idle') break;
        if (step === 29) throw new Error('Fixture index did not catch up within its bounded work budget');
      }
    } finally {
      config.bookSearch.workerEnabled = false;
    }
    const checkpoint = await app.get(BookSearchSyncRepository).checkpoint();
    expect(checkpoint?.snapshotComplete).toBe(true);
    expect(checkpoint?.revision).toBe(checkpoint?.appliedRevision);
  }

  async function startApplication() {
    const { AppModule } = await import('../src/app.module');
    const fixture = await Test.createTestingModule({ imports: [AppModule] }).compile();
    app = fixture.createNestApplication<NestFastifyApplication>(new FastifyAdapter());
    app.setGlobalPrefix('api/v1');
    app.useGlobalPipes(new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true, transform: true }));
    app.useGlobalFilters(new GlobalExceptionFilter());
    await app.register(fastifyCookie as never);
    await app.listen(0, '127.0.0.1');
    baseUrl = await app.getUrl();
    config = app.get(appConfig.KEY);
    provider = app.get(BookSearchProvider);
    appPool = app.get<typeof db>(DB).$client;
  }

  beforeAll(async () => {
    const url = new URL(connectionString!);
    if (url.hostname !== '127.0.0.1' || url.port !== '55432' || url.pathname !== '/book_search_test')
      throw new Error('Dedicated disposable PostgreSQL fixture required');
    if (new URL(endpoint!).hostname !== '127.0.0.1' || new URL(endpoint!).port !== '4086')
      throw new Error('Dedicated disposable Zinc fixture required');
    admin = new Pool({ connectionString, max: 1 });
    await admin.query("CREATE DATABASE book_search_http_test TEMPLATE template0 LC_COLLATE 'en_US.utf8' LC_CTYPE 'en_US.utf8'");
    url.pathname = '/book_search_http_test';
    directory = await mkdtemp(join(tmpdir(), 'book-search-http-'));
    const environment = {
      DATABASE_URL: url.toString(),
      NODE_ENV: 'test',
      JWT_SECRET: 'http-fixture-jwt-secret-only',
      APP_DATA_PATH: join(directory, 'data'),
      BOOK_DOCK_PATH: join(directory, 'dock'),
      LIBRARY_BROWSE_ROOT: directory,
      BOOK_SEARCH_ENABLED: 'true',
      BOOK_SEARCH_INDEX_READY: 'true',
      BOOK_SEARCH_WORKER_ENABLED: 'false',
      BOOK_SEARCH_ENDPOINT: endpoint!,
      BOOK_SEARCH_INDEX: 'books_http_fixture_' + randomUUID().replaceAll('-', ''),
      BOOK_SEARCH_USERNAME: 'fixture-user',
      BOOK_SEARCH_PASSWORD: 'fixture-password',
      BOOK_SEARCH_TIMEOUT_MS: '5000',
      METADATA_PROVIDER_DIRECTORY_URL: 'off',
      COVER_SLOTS_BACKFILL_MODE: 'skip',
      SETUP_BOOTSTRAP_TOKEN: 'http-fixture-setup-token-only',
    };
    for (const [key, value] of Object.entries(environment)) {
      savedEnvironment[key] = process.env[key];
      process.env[key] = value;
    }
    // Create the native schema from generated migrations, never a production dump.
    await promisify(execFile)(process.execPath, [join(process.cwd(), 'node_modules/tsx/dist/cli.mjs'), 'src/scripts/migrate.ts'], {
      env: process.env,
      timeout: 30000,
      maxBuffer: 256 * 1024,
    });
    pool = new Pool({ connectionString: url.toString(), max: 2, statement_timeout: 10000 });
    db = drizzle(pool, { schema });
    const passwordHash = await hash(password, 4);
    const [administrator, viewer] = await db
      .insert(schema.users)
      .values([
        { username: 'http-admin', name: 'Fixture admin', passwordHash, isSuperuser: true, isDefaultPassword: false },
        { username: 'http-viewer', name: 'Fixture viewer', passwordHash, isSuperuser: false, isDefaultPassword: false },
      ])
      .returning();
    await db.insert(schema.appSettings).values({ key: 'update_check_enabled', value: 'false' }).onConflictDoNothing();
    for (const [scope, hidden] of [
      ['visible', false],
      ['hidden', true],
    ] as const) {
      const root = join(directory, scope);
      await mkdir(root, { recursive: true });
      const [library] = await db
        .insert(schema.libraries)
        .values({ name: `HTTP ${scope}`, watch: false, fileWriteEnabled: false })
        .returning();
      const [folder] = await db.insert(schema.libraryFolders).values({ libraryId: library.id, path: root }).returning();
      const [author] = await db
        .insert(schema.authors)
        .values({ name: hidden ? '秘密英语作者' : '英语作者' })
        .returning();
      const [series] = await db
        .insert(schema.bookSeries)
        .values({ name: hidden ? '秘密英语丛书' : '英语丛书', normalizedName: `http-${scope}` })
        .returning();
      const titles = hidden ? ['秘密英语'] : ['英语', '实用英语手册', 'AA programming', 'gram', 'a', 'A', '', null];
      for (const [position, title] of titles.entries()) {
        await db.transaction(async (tx) => {
          const folderPath = join(root, String(position));
          await mkdir(folderPath);
          const absolutePath = join(folderPath, 'fixture.txt');
          await writeFile(absolutePath, 'synthetic fixture only');
          const [book] = await tx.insert(schema.books).values({ libraryId: library.id, libraryFolderId: folder.id, folderPath }).returning();
          await tx.insert(schema.bookMetadata).values({ bookId: book.id, title, publisher: 'Fixture press', language: 'zh' });
          const [file] = await tx
            .insert(schema.bookFiles)
            .values({
              bookId: book.id,
              libraryFolderId: folder.id,
              absolutePath,
              relPath: `${position}/fixture.txt`,
              ino: BigInt(book.id),
              format: 'txt',
              sizeBytes: 22,
            })
            .returning();
          await tx.update(schema.books).set({ primaryFileId: file.id }).where(eq(schema.books.id, book.id));
          await tx.insert(schema.bookAuthors).values({ bookId: book.id, authorId: author.id });
          if (position === 0) {
            await tx.insert(schema.bookSeriesMemberships).values({ bookId: book.id, seriesId: series.id });
            if (hidden) hiddenBook = book.id;
            else visibleBook = book.id;
          }
          await enqueueBookSearchChanges(tx, [book.id]);
        });
      }
      if (hidden) {
        hiddenLibrary = library.id;
        hiddenAuthor = author.id;
        hiddenSeries = series.id;
      } else {
        visibleLibrary = library.id;
        visibleAuthor = author.id;
        visibleSeries = series.id;
        await db.insert(schema.userLibraryAccess).values({ userId: viewer.id, libraryId: library.id, accessLevel: 'viewer' });
      }
    }
    expect(administrator.isSuperuser).toBe(true);
    vi.spyOn(globalThis, 'fetch').mockImplementation((input, init) => {
      const target = new URL(input instanceof Request ? input.url : String(input));
      if (target.hostname !== '127.0.0.1' && target.hostname !== 'localhost')
        return Promise.reject(new Error('External HTTP is disabled in this disposable test'));
      return originalFetch(input, init);
    });
    await startApplication();
    for (const username of ['http-admin', 'http-viewer']) {
      const response = await request('/auth/login', undefined, { username, password });
      expect(response.status).toBe(200);
      const result = (await response.json()) as { accessToken: string };
      expect(result.accessToken).toBeTruthy();
      if (username === 'http-admin') adminToken = result.accessToken;
      else viewerToken = result.accessToken;
    }
    await catchUp();
    // This fixture has only the known captured seed writes and tested HTTP edits.
    // Confirm that controlled set through the actual maintenance entry point.
    const checkpoint = await app.get(BookSearchSyncRepository).checkpoint();
    const activated = await promisify(execFile)(
      process.execPath,
      [
        join(process.cwd(), 'node_modules/tsx/dist/cli.mjs'),
        'src/scripts/book-search-maintain.ts',
        'activate',
        checkpoint!.captureSession!,
        '--source-coverage-confirmed',
      ],
      {
        env: { ...process.env, BOOK_SEARCH_WORKER_ENABLED: 'true' },
        timeout: 15000,
        maxBuffer: 64 * 1024,
      },
    );
    expect(JSON.parse(activated.stdout.trim().split('\n').at(-1)!)).toMatchObject({ phase: 'active', sourceCoverageConfirmed: true });
  }, 60000);

  afterAll(async () => {
    if (app) await app.close();
    await appPool?.end();
    await pool?.end();
    if (admin) {
      await admin.query('DROP DATABASE IF EXISTS book_search_http_test');
      await admin.end();
    }
    if (directory) await rm(directory, { recursive: true, force: true });
    for (const [key, value] of Object.entries(savedEnvironment)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    vi.restoreAllMocks();
    const report = process.env.BOOK_SEARCH_TEST_HTTP_REPORT;
    if (report)
      await writeFile(
        report,
        JSON.stringify({ transport: 'TCP HTTP on loopback with real AppModule and JWT login', timings, production_readiness_claimed: false }) + '\n',
      );
  }, 30000);

  it('requires authentication on the actual top search route', async () => {
    expect((await request('/search?q=' + encodeURIComponent('英语'))).status).toBe(401);
  });

  it('uses the real provider for top relevance and preserves native hit envelopes and all three sections', async () => {
    const spy = vi.spyOn(provider, 'search');
    config.bookSearch.indexReady = false;
    const native = await search(adminToken);
    expect(spy).not.toHaveBeenCalled();
    config.bookSearch.indexReady = true;
    const indexed = await search(adminToken);
    expect(spy).toHaveBeenCalledWith(expect.objectContaining({ text: '英语', relevance: true, size: 10 }));
    expect(indexed.books.total).toBe(native.books.total);
    expect(new Set(indexed.books.items.map((hit) => hit.item.id))).toEqual(new Set(native.books.items.map((hit) => hit.item.id)));
    expect(indexed.authors).toEqual(native.authors);
    expect(indexed.series).toEqual(native.series);
    expect(indexed.books.items.find((hit) => hit.item.id === visibleBook)).toMatchObject({ matchedField: 'title', matchedText: '英语' });
    expect(indexed.books.items.some((hit) => hit.item.id === hiddenBook)).toBe(true);
    spy.mockRestore();
  });

  it('keeps hidden libraries, authors and series out of a viewer response', async () => {
    const result = await search(viewerToken);
    expect(result.books.items.some((hit) => hit.item.id === visibleBook)).toBe(true);
    expect(result.books.items.some((hit) => hit.item.id === hiddenBook)).toBe(false);
    expect(result.authors.items.map((hit) => hit.item.id)).toContain(visibleAuthor);
    expect(result.authors.items.map((hit) => hit.item.id)).not.toContain(hiddenAuthor);
    expect(result.series.items.map((hit) => hit.item.id)).toContain(visibleSeries);
    expect(result.series.items.map((hit) => hit.item.id)).not.toContain(hiddenSeries);
  });

  it('applies the requested library before provider lookup and rejects an inaccessible library', async () => {
    const spy = vi.spyOn(provider, 'search');
    const result = await search(viewerToken, visibleLibrary);
    expect(result.books.items.every((hit) => hit.item.libraryId === visibleLibrary)).toBe(true);
    expect(spy).toHaveBeenCalledWith(expect.objectContaining({ libraryIds: [visibleLibrary], relevance: true }));
    spy.mockClear();
    const denied = await request(`/search?q=${encodeURIComponent('英语')}&libraryId=${hiddenLibrary}`, viewerToken);
    expect(denied.status).toBe(403);
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it.each(['asc', 'desc'] as const)('preserves en_US title %s, case, empty/null titles and all native pages', async (dir) => {
    expect(await app.get(BookSearchSyncRepository).supportsBinaryTitleSort()).toBe(false);
    const spy = vi.spyOn(provider, 'search');
    for (let page = 0; page < 4; page++) {
      const query: BookQuery = { q: '英语', sort: [{ field: 'title', dir }], pagination: { page, size: 2 } };
      config.bookSearch.indexReady = false;
      const nativeResponse = await request('/books/query', viewerToken, query);
      expect(nativeResponse.status).toBe(201);
      const native = (await nativeResponse.json()) as BooksPage;
      config.bookSearch.indexReady = true;
      const indexedResponse = await request('/books/query', viewerToken, query);
      expect(indexedResponse.status).toBe(201);
      const indexed = (await indexedResponse.json()) as BooksPage;
      expect(indexed).toEqual(native);
    }
    expect(spy).toHaveBeenCalledWith(expect.objectContaining({ size: 1000, offset: 0, titleDirection: dir }));
    spy.mockRestore();
  });

  it('falls back to native HTTP results when Zinc is unavailable', async () => {
    const before = await search(viewerToken);
    const previous = config.bookSearch.endpoint;
    config.bookSearch.endpoint = 'http://127.0.0.1:9';
    try {
      const result = await search(viewerToken);
      expect(result.books.total).toBe(before.books.total);
      expect(new Set(result.books.items.map((hit) => hit.item.id))).toEqual(new Set(before.books.items.map((hit) => hit.item.id)));
    } finally {
      config.bookSearch.endpoint = previous;
    }
  });

  it('captures an authorized native metadata edit and refuses a viewer edit', async () => {
    expect((await request(`/books/${visibleBook}/metadata`, viewerToken, { title: 'Unauthorized' }, 'PATCH')).status).toBe(403);
    const response = await request(`/books/${visibleBook}/metadata`, adminToken, { title: '英语新版' }, 'PATCH');
    expect(response.status).toBe(200);
    expect((await app.get(BookSearchSyncRepository).pendingSummary()).count).not.toBe('0');
    await catchUp();
    const result = await search(viewerToken);
    expect(result.books.items.find((hit) => hit.item.id === visibleBook)?.item.title).toBe('英语新版');
    const source = await db.select().from(schema.bookMetadata).where(eq(schema.bookMetadata.bookId, visibleBook));
    expect(source[0].publisher).toBe('Fixture press');
  });

  it('keeps the actual title-shaped top request on the engine across native candidate pages', async () => {
    config.bookSearch.workerBatchSize = 500;
    const [folder] = await db.select().from(schema.libraryFolders).where(eq(schema.libraryFolders.libraryId, visibleLibrary)).limit(1);
    await db.execute(sql`CREATE INDEX http_native_title_scan_idx ON public.book_metadata(title,book_id)`);
    expect(await app.get(BookSearchSyncRepository).hasNativeTitleScanIndex('asc')).toBe(true);
    await db.transaction(async (tx) => {
      const books = await tx
        .insert(schema.books)
        .values(
          Array.from({ length: 1200 }, (_, i) => ({
            libraryId: visibleLibrary,
            libraryFolderId: folder.id,
            folderPath: join(directory, 'native-title-probe', String(i)),
          })),
        )
        .returning({ id: schema.books.id });
      await tx
        .insert(schema.bookMetadata)
        .values(books.map((book, i) => ({ bookId: book.id, title: `${i % 2 ? 'a' : 'A'} nativesortfixture ${String(i).padStart(4, '0')}` })));
      for (let offset = 0; offset < books.length; offset += 1000)
        await enqueueBookSearchChanges(
          tx,
          books.slice(offset, offset + 1000).map((book) => book.id),
        );
    });
    await catchUp();
    const spy = vi.spyOn(provider, 'search');
    try {
      for (const page of [0, 24, 49]) {
        const query: BookQuery = { q: 'nativesortfixture', sort: [{ field: 'title', dir: 'asc' }], pagination: { page, size: 20 } };
        config.bookSearch.indexReady = false;
        const native = (await (await request('/books/query', viewerToken, query)).json()) as BooksPage;
        config.bookSearch.indexReady = true;
        const response = await request('/books/query', viewerToken, query);
        expect(response.status).toBe(201);
        expect(await response.json()).toEqual(native);
      }
      expect(spy.mock.calls.some(([request]) => request.candidateIds?.length === 500)).toBe(true);
    } finally {
      spy.mockRestore();
    }
  }, 30_000);

  it('imports a watched multiformat directory once and preserves locked edits when its OPF is republished', async () => {
    const root = join(directory, 'watched');
    const stage = join(directory, 'staged-book');
    const title = 'watchedintakefixture';
    await mkdir(root);
    await mkdir(stage);
    const opf = (name: string, publisher: string) =>
      `<?xml version="1.0"?><package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id"><metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:identifier id="id">urn:uuid:http-intake-fixture</dc:identifier><dc:title>${name}</dc:title><dc:publisher>${publisher}</dc:publisher><dc:language>en</dc:language><meta property="dcterms:modified">2026-01-01T00:00:00Z</meta></metadata><manifest><item id="text" href="text.xhtml" media-type="application/xhtml+xml"/><item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/></manifest><spine><itemref idref="text"/></spine></package>`;
    const writer = new ZipWriter(new Uint8ArrayWriter(), { useWebWorkers: false });
    for (const [name, contents] of [
      ['mimetype', 'application/epub+zip'],
      [
        'META-INF/container.xml',
        '<container xmlns="urn:oasis:names:tc:opendocument:xmlns:container" version="1.0"><rootfiles><rootfile full-path="OPS/book.opf" media-type="application/oebps-package+xml"/></rootfiles></container>',
      ],
      ['OPS/book.opf', opf(title, 'Initial fixture press')],
      [
        'OPS/text.xhtml',
        '<html xmlns="http://www.w3.org/1999/xhtml"><head><title>Fixture</title></head><body><p id="start">Synthetic fixture only</p></body></html>',
      ],
      [
        'OPS/nav.xhtml',
        '<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops"><head><title>Contents</title></head><body><nav epub:type="toc"><ol><li><a href="text.xhtml#start">Fixture</a></li></ol></nav></body></html>',
      ],
    ])
      await writer.add(name, new Uint8ArrayReader(Buffer.from(contents)), { level: name === 'mimetype' ? 0 : 1 });
    const originals = {
      epub: Buffer.from(await writer.close()),
      fb2: Buffer.from(
        `<?xml version="1.0"?><FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0"><description><title-info><book-title>${title}</book-title><lang>en</lang></title-info></description><body><section><p>Synthetic fixture only</p></section></body></FictionBook>`,
      ),
    };
    for (const [format, contents] of Object.entries(originals)) await writeFile(join(stage, 'book.' + format), contents);
    await writeFile(join(stage, 'book.opf'), opf(title, 'Initial fixture press'));
    await writeFile(
      join(stage, 'book.png'),
      Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jk1sAAAAASUVORK5CYII=', 'base64'),
    );
    // The published batch is already settled; exercise the native stability shortcut.
    const settled = new Date(Date.now() - 120_000);
    for (const name of ['book.epub', 'book.fb2', 'book.opf', 'book.png']) await utimes(join(stage, name), settled, settled);
    const created = await request('/libraries', adminToken, {
      name: 'Watched fixture',
      icon: 'Library',
      folders: [root],
      watch: true,
      fileWriteEnabled: false,
      metadataPrecedence: ['opfFile', 'embedded'],
    });
    expect(created.status).toBe(201);
    const library = (await created.json()) as { id: number };
    const query: BookQuery = { q: title, sort: [{ field: 'title', dir: 'asc' }], pagination: { page: 0, size: 20 } };
    const load = async () => {
      const response = await request(`/libraries/${library.id}/books`, adminToken, query);
      expect(response.status).toBe(201);
      return response.json() as Promise<BooksPage>;
    };
    try {
      for (let step = 0; step < 30; step++) {
        const active = await db.select({ id: schema.scanJobs.id }).from(schema.scanJobs).where(eq(schema.scanJobs.status, 'running')).limit(1);
        if (!active.length) break;
        await delay(100);
      }
      await rename(stage, join(root, 'Book'));
      let bookId: number | undefined;
      for (let step = 0; step < 75; step++) {
        const page = await load();
        if (page.items.length === 1 && ['epub', 'fb2', 'opf'].every((format) => page.items[0].files.some((file) => file.format === format))) {
          bookId = page.items[0].id;
          break;
        }
        await delay(200);
      }
      expect(bookId).toBeDefined();
      await delay(1000);
      expect((await load()).items.map((book) => book.id)).toEqual([bookId]);
      expect(
        (
          await request(
            `/books/${bookId}/metadata-and-locks`,
            adminToken,
            { metadata: { title: title + ' manual' }, lockedFields: ['title'] },
            'PATCH',
          )
        ).status,
      ).toBe(200);
      expect((await request(`/books/${bookId}/personal-note`, adminToken, { note: 'Preserved fixture note' }, 'PATCH')).status).toBe(200);
      await writeFile(join(directory, 'updated.opf'), opf(title + ' republished', 'Republished fixture press'));
      await utimes(join(directory, 'updated.opf'), settled, new Date(settled.getTime() + 1000));
      await rename(join(directory, 'updated.opf'), join(root, 'Book/book.opf'));
      let republished = false;
      for (let step = 0; step < 75; step++) {
        const detail = (await (await request(`/books/${bookId}`, adminToken)).json()) as { title: string; publisher: string; personalNote: string };
        if (detail.publisher === 'Republished fixture press') {
          expect(detail.title).toBe(title + ' manual');
          expect(detail.personalNote).toBe('Preserved fixture note');
          republished = true;
          break;
        }
        await delay(200);
      }
      expect(republished).toBe(true);
      await catchUp();
      const indexed = vi.spyOn(provider, 'search');
      try {
        const page = await load();
        expect(page.items.map((book) => book.id)).toEqual([bookId]);
        expect(page.items[0].title).toBe(title + ' manual');
        expect(indexed).toHaveBeenCalled();
      } finally {
        indexed.mockRestore();
      }
      for (const [format, contents] of Object.entries(originals)) {
        expect(
          createHash('sha256')
            .update(await readFile(join(root, 'Book/book.' + format)))
            .digest('hex'),
        ).toBe(createHash('sha256').update(contents).digest('hex'));
      }
    } finally {
      expect((await request(`/libraries/${library.id}`, adminToken, { watch: false }, 'PATCH')).status).toBe(200);
    }
  }, 45_000);

  it('keeps a query on the verified index when a captured native edit commits during its provider lookup', async () => {
    const realSearch = provider.search.bind(provider);
    const index = vi.spyOn(app.get(BookIndexSearchService), 'tryQuery');
    const lookup = vi.spyOn(provider, 'search').mockImplementationOnce(async (query) => {
      const result = await realSearch(query);
      expect((await request(`/books/${visibleBook}/metadata`, adminToken, { title: '英语并发校验' }, 'PATCH')).status).toBe(200);
      await catchUp();
      return result;
    });
    const query: BookQuery = { q: '英语', sort: [{ field: 'title', dir: 'asc' }], pagination: { page: 0, size: 20 } };
    try {
      const response = await request('/books/query', viewerToken, query);
      expect(response.status).toBe(201);
      const indexed = (await response.json()) as BooksPage;
      expect(await index.mock.results.at(-1)!.value).not.toBeNull();
      expect(lookup.mock.calls.length).toBeGreaterThanOrEqual(2);
      expect(indexed.items.find((book) => book.id === visibleBook)?.title).toBe('英语并发校验');
      config.bookSearch.indexReady = false;
      expect(indexed).toEqual(await (await request('/books/query', viewerToken, query)).json());
    } finally {
      config.bookSearch.indexReady = true;
      index.mockRestore();
      lookup.mockRestore();
    }
  });

  it('waits for a pending captured edit over HTTP and serves the complete caught-up page', async () => {
    expect((await request(`/books/${visibleBook}/metadata`, adminToken, { title: '英语等待同步' }, 'PATCH')).status).toBe(200);
    const sync = app.get(BookSearchSyncRepository);
    const checkpoint = await sync.checkpoint();
    expect(checkpoint?.revision).not.toBe(checkpoint?.appliedRevision);
    const realCheckpoint = sync.checkpoint.bind(sync);
    let observedLag = false;
    const started = Date.now();
    const observations: unknown[] = [];
    const observation = vi.spyOn(sync, 'checkpoint').mockImplementation(async () => {
      const state = await realCheckpoint();
      observations.push({
        ms: Date.now() - started,
        revision: state?.revision.toString(),
        applied: state?.appliedRevision.toString(),
        snapshot: state?.snapshotComplete,
        coverage: state?.writeCoverageComplete,
        epoch: state?.epoch,
        session: state?.captureSession,
      });
      if (state && state.revision !== state.appliedRevision) observedLag = true;
      return state;
    });
    const index = vi.spyOn(app.get(BookIndexSearchService), 'tryQuery');
    const query: BookQuery = { q: '英语', sort: [{ field: 'title', dir: 'asc' }], pagination: { page: 0, size: 20 } };
    try {
      const response = request('/books/query', viewerToken, query);
      for (let step = 0; step < 20 && !observedLag; step++) await delay(20);
      expect(observedLag).toBe(true);
      await catchUp();
      const result = await response;
      expect(result.status).toBe(201);
      const indexed = (await result.json()) as BooksPage;
      expect(await index.mock.results.at(-1)!.value, JSON.stringify(observations)).not.toBeNull();
      expect(indexed.items.find((book) => book.id === visibleBook)?.title).toBe('英语等待同步');
      config.bookSearch.indexReady = false;
      expect(indexed).toEqual(await (await request('/books/query', viewerToken, query)).json());
    } finally {
      config.bookSearch.indexReady = true;
      index.mockRestore();
      observation.mockRestore();
    }
  });

  it('hands a stopped native HTTP application to a new verified restart session without reseeding or losing login and user data', async () => {
    expect((await request(`/books/${visibleBook}/personal-note`, adminToken, { note: 'Preserved restart fixture note' }, 'PATCH')).status).toBe(200);
    await catchUp();
    const query: BookQuery = { q: '英语', sort: [{ field: 'title', dir: 'asc' }], pagination: { page: 0, size: 20 } };
    const before = await (await request('/books/query', viewerToken, query)).json();
    const progress = await app.get(BookSearchBootstrapRepository).progress();
    const address = baseUrl;
    await app.close();
    await appPool!.end();
    appPool = undefined;
    await expect(originalFetch(address + '/api/v1/health', { signal: AbortSignal.timeout(1000) })).rejects.toThrow();
    const checkpoint = (await new BookSearchSyncRepository(db).checkpoint())!;
    const source = await db.execute<{ count: string; upper: number }>(
      sql`SELECT count(*)::text AS count,coalesce(max(id),0)::integer AS upper FROM public.books WHERE status <> 'processing'`,
    );
    const proofPath = join(directory, 'restart-proof.json');
    const issued = Date.now();
    await writeFile(
      proofPath,
      JSON.stringify({
        format: 'book-search-restart-proof-v1',
        previousOwnerStopped: true,
        sourceCaptureVerified: true,
        issuedAt: new Date(issued).toISOString(),
        expiresAt: new Date(issued + 120_000).toISOString(),
        sourceBooks: Number(source.rows[0].count),
        sourceUpperBookId: source.rows[0].upper,
        checkpoint: Object.fromEntries(
          ['epoch', 'revision', 'appliedRevision', 'indexName', 'schemaRevision', 'engineIdentity', 'captureSession'].map((key) => {
            const value = checkpoint[key as keyof typeof checkpoint];
            return [key, typeof value === 'bigint' ? value.toString() : value];
          }),
        ),
      }),
      { mode: 0o600 },
    );
    savedEnvironment.BOOK_SEARCH_RESTART_PROOF_PATH = process.env.BOOK_SEARCH_RESTART_PROOF_PATH;
    process.env.BOOK_SEARCH_RESTART_PROOF_PATH = proofPath;
    await startApplication();
    const current = (await app.get(BookSearchSyncRepository).checkpoint())!;
    expect(current.captureSession).not.toBe(checkpoint.captureSession);
    expect({ ...current, captureSession: checkpoint.captureSession }).toEqual(checkpoint);
    expect(await app.get(BookSearchBootstrapRepository).progress()).toEqual(progress);
    const index = vi.spyOn(app.get(BookIndexSearchService), 'tryQuery');
    try {
      const response = await request('/books/query', viewerToken, query);
      expect(response.status).toBe(201);
      expect(await response.json()).toEqual(before);
      expect(await index.mock.results.at(-1)!.value).not.toBeNull();
      const detail = await request(`/books/${visibleBook}`, adminToken);
      expect(detail.status).toBe(200);
      expect(await detail.json()).toMatchObject({ personalNote: 'Preserved restart fixture note' });
      expect((await request(`/books/${visibleBook}/metadata`, adminToken, { title: '英语新会话捕获' }, 'PATCH')).status).toBe(200);
      await catchUp();
      expect(await app.get(BookSearchSyncRepository).checkpoint()).toMatchObject({
        captureSession: current.captureSession,
        writeCoverageComplete: true,
      });
    } finally {
      index.mockRestore();
    }
  }, 30_000);
});
