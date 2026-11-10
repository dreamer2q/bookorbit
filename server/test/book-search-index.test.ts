import { Test } from '@nestjs/testing';
import { and, asc, eq, getTableColumns, ne, sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';
import { mkdtemp, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { appConfig } from '../src/config/config';
import { DB } from '../src/db';
import * as schema from '../src/db/schema';
import { bookSearchPending } from '../src/db/book-search-schema';
import { BookSearchSyncRepository, enqueueBookSearchChanges } from '../src/modules/book/search/book-search-sync.repository';
import { BookSearchDocumentProjector } from '../src/modules/book/search/book-search-document-projector';
import { BookSearchSyncService } from '../src/modules/book/search/book-search-sync.service';
import { ZincBookIndexWriter } from '../src/modules/book/search/zinc-book-index-writer';
import { ZincBookSearchProvider } from '../src/modules/book/search/zinc-book-search-provider';
import { BookRepository } from '../src/modules/book/book.repository';
import { MetadataService } from '../src/modules/metadata/metadata.service';
import { BookSearchCaptureService } from '../src/common/services/book-search-capture.service';
import { BookMoveRepository } from '../src/modules/book-move/book-move.repository';
import { FileRenameRepository } from '../src/modules/file-write/file-rename.repository';
import { AuthorsRepository } from '../src/modules/authors/authors.repository';
import { AuthorStrategy } from '../src/modules/entity-manager/strategies/author.strategy';
import { LibraryRepository } from '../src/modules/library/library.repository';
import { ScannerRepository } from '../src/modules/scanner/scanner.repository';
import { UploadProcessorService } from '../src/modules/upload/upload-processor.service';
import { UploadService } from '../src/modules/upload/upload.service';
import { SeriesMembershipService } from '../src/common/services/series-membership.service';
import { SeriesIdentityService } from '../src/common/services/series-identity.service';
import { SeriesStrategy } from '../src/modules/entity-manager/strategies/series.strategy';
import { BookDockFinalizeService } from '../src/modules/book-dock/book-dock-finalize.service';
import { MigrationImportRepository } from '../src/modules/migration/executor/migration-import.repository';
import { PublisherStrategy } from '../src/modules/entity-manager/strategies/publisher.strategy';
import { LanguageStrategy } from '../src/modules/entity-manager/strategies/language.strategy';
import { NarratorRepository } from '../src/modules/narrator/narrator.repository';
import { NarratorService } from '../src/modules/narrator/narrator.service';
import { NarratorStrategy } from '../src/modules/entity-manager/strategies/narrator.strategy';
import { BookQueryBuilder } from '../src/modules/book/book-query-builder.service';
import { installPostgresExtensions } from '../src/scripts/postgres-extensions';
import { BookSearchBootstrapRepository } from '../src/modules/book/search/book-search-bootstrap.repository';
import { bookSearchIndexDefinition } from '../src/modules/book/search/book-search-index-definition';
import { ZincBookIndexGeneration } from '../src/modules/book/search/zinc-book-index-generation';
import { BookSearchCoordinatorService } from '../src/modules/book/search/book-search-coordinator.service';
import { BookSearchLifecycleService } from '../src/modules/book/search/book-search-lifecycle.service';
import { readBookSearchRestartProof } from '../src/modules/book/search/book-search-restart-proof';
import type { BookSearchTransaction } from '../src/db/book-search-changes';

const connectionString = process.env.BOOK_SEARCH_TEST_DATABASE_URL;
const endpoint = process.env.BOOK_SEARCH_TEST_ZINC_ENDPOINT;

describe.runIf(Boolean(connectionString && endpoint))('metadata queue to real ZincSearch on disposable fixtures', () => {
  const pool = new Pool({ connectionString, max: 5, connectionTimeoutMillis: 3000, statement_timeout: 5000 });
  const db = drizzle(pool, { schema });
  const config = appConfig();
  let sync: BookSearchSyncService;
  let queue: BookSearchSyncRepository;
  let baseline: BookSearchBootstrapRepository;
  let generation: ZincBookIndexGeneration;
  let coordinator: BookSearchCoordinatorService;
  let provider: ZincBookSearchProvider;
  let bookRepository: BookRepository;
  let metadata: MetadataService;
  let moves: BookMoveRepository;
  let renames: FileRenameRepository;
  let authorsRepository: AuthorsRepository;
  let authorStrategy: AuthorStrategy;
  let libraries: LibraryRepository;
  let scanner: ScannerRepository;
  let capture: BookSearchCaptureService;
  let uploads: UploadProcessorService;
  let uploadService: UploadService;
  let series: SeriesMembershipService;
  let seriesStrategy: SeriesStrategy;
  let dock: BookDockFinalizeService;
  let migration: MigrationImportRepository;
  let publishers: PublisherStrategy;
  let languages: LanguageStrategy;
  let narratorService: NarratorService;
  let narratorStrategy: NarratorStrategy;
  let fixtureDirectory: string;
  let sequence = 0;

  async function engine(path: string, body: unknown) {
    const response = await fetch(endpoint! + path, {
      method: 'POST',
      headers: {
        Authorization: `Basic ${Buffer.from('fixture-user:fixture-password').toString('base64')}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(5000),
    });
    expect(response.status).toBe(200);
    return response.json();
  }
  const search = async (text: string, libraries = [3]) =>
    provider.search({ text: await queue.normalizeQuery(text), libraryIds: libraries, offset: 0, size: 20, titleDirection: 'asc' });
  const uploadFile = (folderPath: string, name = 'one.txt') => ({
    folderPath,
    absolutePath: join(fixtureDirectory, name),
    relPath: name,
    format: 'txt',
    sizeBytes: 20,
  });

  async function addBook() {
    await db.transaction(async (tx) => {
      await tx.execute(sql`INSERT INTO public.books (id,library_id,status) VALUES (11, 3, 'present')`);
      await tx.execute(
        sql`INSERT INTO public.book_metadata (book_id,title,publisher,description) VALUES (11,'BeforeRevision','Publisherword','Descriptionword')`,
      );
      await tx.execute(sql`INSERT INTO public.authors (id,name) VALUES (1,'Secondauthor'),(2,'Firstauthor')`);
      await tx.execute(sql`INSERT INTO public.book_authors VALUES (11,1,2),(11,2,1)`);
      await enqueueBookSearchChanges(tx, [11]);
    });
  }

  async function concurrentEdits(deferred: boolean) {
    let authorLocked!: () => void;
    let scalarWritten!: () => void;
    const locked = new Promise<void>((resolve) => {
      authorLocked = resolve;
    });
    const written = new Promise<void>((resolve) => {
      scalarWritten = resolve;
    });
    const transaction = (work: (tx: BookSearchTransaction) => Promise<void>) =>
      deferred ? bookRepository.withTransaction(work) : db.transaction(work);
    const authorOwner = transaction(async (tx) => {
      await tx.execute(sql`SELECT id FROM public.authors WHERE id=2 FOR UPDATE`);
      authorLocked();
      await written;
      await bookRepository.updateMetadataFields(22, { title: 'Otherconcurrentword' }, tx);
    });
    void authorOwner.catch(() => authorLocked());
    await locked;
    const editor = transaction(async (tx) => {
      try {
        await bookRepository.updateMetadataFields(11, { title: 'Concurrenteditorword' }, tx);
      } finally {
        scalarWritten();
      }
      await metadata.replaceAuthors(11, [{ name: 'Firstauthor', sortName: null }], { executor: tx });
    });
    return Promise.allSettled([authorOwner, editor]);
  }

  async function addSecondBook() {
    await db.transaction(async (tx) => {
      await tx.execute(sql`INSERT INTO public.books(id,library_id,library_folder_id,status,folder_path) VALUES(22,5,51,'present','/fixture22');
        INSERT INTO public.book_metadata(book_id,title,publisher) VALUES(22,'Othersourceword','Publisherword')`);
      await enqueueBookSearchChanges(tx, [22]);
    });
  }

  async function verifyBaselineIndex(tx: BookSearchTransaction, progress: Awaited<ReturnType<BookSearchBootstrapRepository['progress']>>) {
    expect(progress?.indexName).toBe(config.bookSearch.index);
    const expected = await tx
      .select({ id: schema.books.id })
      .from(schema.books)
      .where(ne(schema.books.status, 'processing'))
      .orderBy(asc(schema.books.id))
      .limit(100);
    const actual = await engine(`/es/${config.bookSearch.index}/_search`, {
      query: { bool: { should: [{ term: { library_id: '3' } }, { term: { library_id: '5' } }], minimum_should_match: 1 } },
      size: 100,
      _source: ['book_id', 'sync_epoch'],
    });
    expect(actual.hits.total.value).toBe(expected.length);
    expect(actual.hits.hits.map((row: { _source: { book_id: number } }) => row._source.book_id).sort((a: number, b: number) => a - b)).toEqual(
      expected.map((row) => row.id),
    );
    expect(actual.hits.hits.every((row: { _source: { sync_epoch: string } }) => row._source.sync_epoch === progress!.epoch)).toBe(true);
  }

  async function finishBaseline(repository: BookSearchBootstrapRepository, verify: Parameters<BookSearchBootstrapRepository['finishIfCaughtUp']>[0]) {
    const progress = await repository.progress();
    if (progress?.phase !== 'seeding' && !progress?.pruningComplete && (await queue.pendingSummary()).count === '0')
      await repository.pruneNextBatch((cursor, state) => generation.readLiveIds(cursor, state));
    return repository.finishIfCaughtUp(verify);
  }

  async function drainCoordinator(service: BookSearchCoordinatorService, maximum = 40) {
    for (let count = 0; count < maximum; count++) {
      const result = await service.runOnce();
      if (result.phase === 'indexed' || (result.phase === 'idle' && (await baseline.progress())?.phase === 'indexed')) return;
    }
    throw new Error('Bounded fixture coordinator did not finish');
  }

  beforeAll(async () => {
    const url = new URL(connectionString!);
    if (url.hostname !== '127.0.0.1' || url.port !== '55432' || url.pathname !== '/book_search_test' || endpoint !== 'http://127.0.0.1:4086') {
      throw new Error('Dedicated disposable search fixtures required');
    }
    await installPostgresExtensions(pool);
    fixtureDirectory = await mkdtemp(join(tmpdir(), 'book-search-upload-fixture-'));
    await writeFile(join(fixtureDirectory, 'one.txt'), 'Synthetic upload one');
    await writeFile(join(fixtureDirectory, 'two.txt'), 'Synthetic upload two');
    await db.execute(sql`CREATE TABLE public.libraries (id integer PRIMARY KEY,koreader_hash_revision bigint NOT NULL DEFAULT 0,format_priority jsonb NOT NULL DEFAULT '[]',allowed_formats jsonb NOT NULL DEFAULT '[]');
      CREATE TABLE public.library_folders (id integer PRIMARY KEY,library_id integer NOT NULL REFERENCES public.libraries(id) ON DELETE CASCADE,UNIQUE(id,library_id));
      CREATE TABLE public.books (id integer GENERATED BY DEFAULT AS IDENTITY (START WITH 1000) PRIMARY KEY, library_id integer NOT NULL REFERENCES public.libraries(id) ON DELETE CASCADE,status text NOT NULL DEFAULT 'present',
        library_folder_id integer NOT NULL DEFAULT 31 REFERENCES public.library_folders(id) ON DELETE CASCADE,folder_path text NOT NULL DEFAULT '/fixture11',primary_file_id integer,
        updated_at timestamptz NOT NULL DEFAULT now(),added_at timestamptz NOT NULL DEFAULT now(),primary_author_sort_name text,
        UNIQUE(library_id,folder_path),
        FOREIGN KEY(library_folder_id,library_id) REFERENCES public.library_folders(id,library_id));
      CREATE TABLE public.book_metadata (book_id integer PRIMARY KEY REFERENCES public.books(id) ON DELETE CASCADE, title text, subtitle text, publisher text, description text,
        isbn10 text, isbn13 text, series_name text, language text, published_year integer, page_count integer,updated_at timestamptz NOT NULL DEFAULT now());
      CREATE TABLE public.authors (id integer GENERATED BY DEFAULT AS IDENTITY (START WITH 100) PRIMARY KEY, name text NOT NULL UNIQUE,sort_name text,
        description text,has_photo boolean NOT NULL DEFAULT false,birth_date date,birth_year integer,death_date date,death_year integer,
        website text,genres text[],influences text[],metadata_provider text,metadata_provider_id text,
        metadata_provider_pinned boolean NOT NULL DEFAULT false,last_enriched_at timestamptz);
      CREATE TABLE public.book_authors (book_id integer REFERENCES public.books(id) ON DELETE CASCADE,author_id integer REFERENCES public.authors(id),display_order integer,PRIMARY KEY(book_id,author_id));
      CREATE TABLE public.narrators (id integer GENERATED BY DEFAULT AS IDENTITY (START WITH 100) PRIMARY KEY,name text NOT NULL UNIQUE,sort_name text,created_at timestamptz NOT NULL DEFAULT now());
      CREATE TABLE public.book_narrators (book_id integer NOT NULL REFERENCES public.books(id) ON DELETE CASCADE,narrator_id integer NOT NULL REFERENCES public.narrators(id),display_order integer NOT NULL DEFAULT 0,PRIMARY KEY(book_id,narrator_id));
      CREATE TABLE public.book_files (id integer GENERATED BY DEFAULT AS IDENTITY (START WITH 1000) PRIMARY KEY,book_id integer REFERENCES public.books(id) ON DELETE CASCADE,library_folder_id integer,
        absolute_path text UNIQUE,rel_path text,ino numeric,size_bytes bigint,mtime timestamptz,file_hash text,format text,role text,sort_order integer,
        media_overlay_available boolean NOT NULL DEFAULT false,created_at timestamptz DEFAULT now(),updated_at timestamptz DEFAULT now());
      ALTER TABLE public.books ADD FOREIGN KEY(primary_file_id) REFERENCES public.book_files(id) ON DELETE SET NULL;
      CREATE TABLE public.book_series (id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,name text NOT NULL,normalized_name text NOT NULL UNIQUE,
        expected_book_count integer,expected_book_count_source text,expected_book_count_updated_at timestamptz,created_at timestamptz DEFAULT now(),updated_at timestamptz DEFAULT now());
      CREATE TABLE public.book_series_memberships (book_id integer REFERENCES public.books(id) ON DELETE CASCADE,series_id integer REFERENCES public.book_series(id) ON DELETE CASCADE,
        series_index text,display_order integer NOT NULL DEFAULT 0,created_at timestamptz DEFAULT now(),updated_at timestamptz DEFAULT now(),
        PRIMARY KEY(book_id,series_id),UNIQUE(book_id,display_order));
      CREATE TABLE public.reading_attempts (id integer PRIMARY KEY,book_id integer REFERENCES public.books(id) ON DELETE SET NULL,outcome text,deleted_at timestamptz,
        book_title_snapshot text,book_authors_snapshot text,book_format_snapshot text,book_page_count_snapshot integer,book_library_id_snapshot integer,book_removed_at timestamptz);`);
    for (const column of Object.values(getTableColumns(schema.bookMetadata))) {
      await db.execute(sql`ALTER TABLE public.book_metadata ADD COLUMN IF NOT EXISTS ${sql.identifier(column.name)} ${sql.raw(column.getSQLType())}`);
    }
    for (const column of Object.values(getTableColumns(schema.bookFiles))) {
      if (column.name !== 'id')
        await db.execute(sql`ALTER TABLE public.book_files ADD COLUMN IF NOT EXISTS ${sql.identifier(column.name)} ${sql.raw(column.getSQLType())}`);
    }
    config.bookSearch.enabled = true;
    config.bookSearch.matchMode = 'fulltext';
    config.bookSearch.endpoint = endpoint!;
    config.bookSearch.username = 'fixture-user';
    config.bookSearch.password = 'fixture-password';
    config.bookSearch.timeoutMs = 5000;
    const module = await Test.createTestingModule({
      providers: [
        BookSearchSyncRepository,
        BookSearchBootstrapRepository,
        ZincBookIndexGeneration,
        BookSearchCoordinatorService,
        BookSearchDocumentProjector,
        BookSearchSyncService,
        ZincBookIndexWriter,
        ZincBookSearchProvider,
        BookRepository,
        MetadataService,
        BookSearchCaptureService,
        BookMoveRepository,
        FileRenameRepository,
        AuthorsRepository,
        AuthorStrategy,
        LibraryRepository,
        ScannerRepository,
        UploadProcessorService,
        UploadService,
        SeriesMembershipService,
        SeriesIdentityService,
        SeriesStrategy,
        BookDockFinalizeService,
        MigrationImportRepository,
        PublisherStrategy,
        LanguageStrategy,
        NarratorRepository,
        NarratorService,
        NarratorStrategy,
        { provide: DB, useValue: db },
        { provide: appConfig.KEY, useValue: config },
      ],
    })
      .useMocker((token) => {
        if (typeof token === 'function' && token.name === 'BookCoverStore') return { removeCoverDirectory: vi.fn().mockResolvedValue(undefined) };
        if (typeof token === 'function' && token.name === 'MetadataScoreService') return { calculateAndSave: vi.fn().mockResolvedValue(undefined) };
        if (typeof token === 'function' && token.name === 'BookMetadataLockService')
          return { filterAutomatedBookUpdate: vi.fn((_id, dto) => Promise.resolve({ dto, blockedFields: [] })) };
        if (typeof token === 'function' && token.name === 'AuthorImageStorageService')
          return { deleteAuthorDir: vi.fn().mockResolvedValue(undefined), promoteImage: vi.fn().mockResolvedValue(false) };
        if (typeof token === 'function' && token.name === 'AuthorEnrichmentOrchestratorService')
          return { schedule: vi.fn().mockResolvedValue(undefined) };
        const required = new Set([
          'MetadataExtractionService',
          'MetadataScoreService',
          'NarratorService',
          'ComicMetadataRepository',
          'BookMetadataLockService',
          'BookCoverStore',
          'AudiobookChaptersService',
          'AppSettingsService',
          'LibraryService',
          'UploadValidatorService',
          'UploadStorageService',
          'PathPolicyService',
          'BookDockRepository',
          'BookReadService',
          'BookDockEventsService',
          'BookDockGateway',
          'NotificationService',
          'BookDockProcessingStateService',
          'FileWriteService',
        ]);
        return typeof token === 'function' && required.has(token.name) ? {} : undefined;
      })
      .compile();
    sync = module.get(BookSearchSyncService);
    queue = module.get(BookSearchSyncRepository);
    baseline = module.get(BookSearchBootstrapRepository);
    generation = module.get(ZincBookIndexGeneration);
    coordinator = module.get(BookSearchCoordinatorService);
    provider = module.get(ZincBookSearchProvider);
    bookRepository = module.get(BookRepository);
    metadata = module.get(MetadataService);
    moves = module.get(BookMoveRepository);
    renames = module.get(FileRenameRepository);
    authorsRepository = module.get(AuthorsRepository);
    authorStrategy = module.get(AuthorStrategy);
    libraries = module.get(LibraryRepository);
    scanner = module.get(ScannerRepository);
    capture = module.get(BookSearchCaptureService);
    uploads = module.get(UploadProcessorService);
    uploadService = module.get(UploadService);
    series = module.get(SeriesMembershipService);
    seriesStrategy = module.get(SeriesStrategy);
    dock = module.get(BookDockFinalizeService);
    migration = module.get(MigrationImportRepository);
    publishers = module.get(PublisherStrategy);
    languages = module.get(LanguageStrategy);
    narratorService = module.get(NarratorService);
    narratorStrategy = module.get(NarratorStrategy);
  });
  beforeEach(async () => {
    await db.execute(sql`TRUNCATE public.books, public.book_metadata, public.authors, public.book_authors,public.narrators,public.book_narrators,public.book_files,public.reading_attempts,public.book_series_memberships,public.book_series,public.library_folders,public.libraries,book_search.pending,book_search.state,book_search.baseline;
      INSERT INTO public.libraries(id) VALUES(3),(5); INSERT INTO public.library_folders VALUES(31,3),(51,5);`);
    config.bookSearch.endpoint = endpoint!;
    config.bookSearch.enabled = true;
    config.bookSearch.workerEnabled = false;
    config.bookSearch.restartProofPath = undefined;
    config.bookSearch.matchMode = 'fulltext';
    config.bookSearch.index = `books_sync_fixture_${++sequence}`;
    await engine('/api/index', bookSearchIndexDefinition(config.bookSearch.index));
  });
  afterAll(async () => {
    await pool.end();
    if (fixtureDirectory) await rm(fixtureDirectory, { recursive: true, force: true });
  });

  it('projects real metadata and ordered authors, and advances the checkpoint only after searchable visibility', async () => {
    await addBook();
    expect(await sync.runOnce()).toEqual({ phase: 'applied', acknowledged: 1 });
    for (const term of ['BeforeRevision', 'Publisherword', 'Descriptionword', 'Firstauthor']) expect((await search(term)).ids).toEqual([11]);
    const raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { match_all: {} }, size: 20 });
    expect(raw.hits.hits[0]._source.authors_exact).toEqual(['Firstauthor', 'Secondauthor']);
    expect(await queue.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 1n, writeCoverageComplete: false, snapshotComplete: false });
  });

  it('fully replaces an edited document and removes old nullable metadata from recall without duplicating its ID', async () => {
    await addBook();
    await sync.runOnce();
    await db.transaction(async (tx) => {
      await tx.execute(sql`UPDATE public.book_metadata SET title='AfterRevision',publisher=NULL,description=NULL WHERE book_id=11`);
      await enqueueBookSearchChanges(tx, [11]);
    });
    await sync.runOnce();
    expect((await search('AfterRevision')).ids).toEqual([11]);
    expect((await search('BeforeRevision')).total).toBe(0);
    expect((await search('Publisherword')).total).toBe(0);
    expect((await engine(`/es/${config.bookSearch.index}/_search`, { query: { match_all: {} }, size: 0 })).hits.total.value).toBe(1);
  });

  it('ranks literal top-search results and retains zero-token fragments, exact totals, bounded pages and library scope', async () => {
    config.bookSearch.matchMode = 'contains';
    await db.transaction(async (tx) => {
      await tx.execute(
        sql`INSERT INTO public.books(id,library_id,status,folder_path,library_folder_id) VALUES(11,3,'present','/rank11',31),(12,3,'present','/rank12',31),(13,5,'present','/rank13',51),(14,3,'present','/rank14',31)`,
      );
      await tx.execute(sql`INSERT INTO public.book_metadata(book_id,title) VALUES(11,'AA programming'),(12,'gram'),(13,'gram'),(14,'英语')`);
      await enqueueBookSearchChanges(tx, [11, 12, 13, 14]);
    });
    await sync.runOnce();
    const request = { text: 'gram', libraryIds: [3], offset: 0, size: 20, titleDirection: 'asc' as const, relevance: true };
    const page = await provider.search(request);
    expect(page.total).toBe(2);
    expect(new Set(page.ids)).toEqual(new Set([11, 12]));
    expect(page.ids[0]).toBe(12);
    expect(await provider.search({ ...request, size: 1 })).toEqual({ ids: [page.ids[0]], total: 2 });
    expect(await provider.search({ ...request, size: 1, offset: 1 })).toEqual({ ids: [page.ids[1]], total: 2 });
    expect(await provider.search({ ...request, text: '英语' })).toEqual({ ids: [14], total: 1 });
    expect(await provider.search({ ...request, libraryIds: [5] })).toEqual({ ids: [13], total: 1 });
  });

  it('matches binary native title ordering across case, accents, empty/null titles and both page directions', async () => {
    config.bookSearch.matchMode = 'contains';
    expect(await queue.supportsBinaryTitleSort()).toBe(true);
    const titles = ['b', 'A', 'a', null, '', '英语', 'Á', 'B'];
    await db.transaction(async (tx) => {
      await tx.execute(sql`INSERT INTO public.authors(id,name) VALUES(1,'Sortfilterword')`);
      for (const [position, title] of titles.entries()) {
        const id = 101 + position;
        await tx.execute(
          sql`INSERT INTO public.books(id,library_id,library_folder_id,folder_path,status) VALUES(${id},3,31,${'/sort' + id},'present')`,
        );
        await tx.execute(sql`INSERT INTO public.book_metadata(book_id,title) VALUES(${id},${title})`);
        await tx.execute(sql`INSERT INTO public.book_authors(book_id,author_id,display_order) VALUES(${id},1,0)`);
      }
      await enqueueBookSearchChanges(
        tx,
        titles.map((_, position) => 101 + position),
      );
    });
    await sync.runOnce();
    for (const dir of ['asc', 'desc'] as const) {
      const native = await db.execute<{ id: number }>(
        sql`SELECT b.id FROM public.books b JOIN public.book_metadata m ON m.book_id=b.id ORDER BY m.title ${sql.raw(dir.toUpperCase())} NULLS LAST,b.id ASC`,
      );
      const expected = native.rows.map((row) => row.id);
      expect(expected.at(-1)).toBe(104);
      for (let offset = 0; offset < titles.length; offset += 3) {
        expect(await provider.search({ text: 'sortfilterword', libraryIds: [3], offset, size: 3, titleDirection: dir })).toEqual({
          ids: expected.slice(offset, offset + 3),
          total: titles.length,
        });
      }
    }
  });

  it('moves a book between authorized library scopes and suppresses processing books until they become visible again', async () => {
    await addBook();
    await sync.runOnce();
    await db.transaction(async (tx) => {
      await tx.execute(sql`UPDATE public.books SET library_id=5,library_folder_id=51 WHERE id=11`);
      await enqueueBookSearchChanges(tx, [11]);
    });
    await sync.runOnce();
    expect((await search('BeforeRevision')).total).toBe(0);
    expect((await search('BeforeRevision', [5])).ids).toEqual([11]);
    await db.transaction(async (tx) => {
      await tx.execute(sql`UPDATE public.books SET status='processing' WHERE id=11`);
      await enqueueBookSearchChanges(tx, [11]);
    });
    await sync.runOnce();
    expect((await search('BeforeRevision', [5])).total).toBe(0);
    await db.transaction(async (tx) => {
      await tx.execute(sql`UPDATE public.books SET status='present' WHERE id=11`);
      await enqueueBookSearchChanges(tx, [11]);
    });
    await sync.runOnce();
    expect((await search('BeforeRevision', [5])).ids).toEqual([11]);
  });

  it('replays tombstones safely for deleted and never-indexed books', async () => {
    await addBook();
    await sync.runOnce();
    await db.transaction(async (tx) => {
      await tx.execute(sql`DELETE FROM public.books WHERE id=11`);
      await enqueueBookSearchChanges(tx, [11, 99]);
    });
    await sync.runOnce();
    await db.transaction((tx) => enqueueBookSearchChanges(tx, [11, 99]));
    await sync.runOnce();
    expect((await search('BeforeRevision', [3, 5])).total).toBe(0);
    const raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { match_all: {} }, size: 20 });
    expect(raw.hits.total.value).toBe(2);
    expect(
      raw.hits.hits.every(
        (hit: { _source: { sync_deleted: boolean; library_id: string } }) => hit._source.sync_deleted && hit._source.library_id === '0',
      ),
    ).toBe(true);
  });

  it('retains an oversized source document for repair instead of acknowledging a truncated search copy', async () => {
    await addBook();
    await db.execute(sql`UPDATE public.book_metadata SET description=repeat('x',65537) WHERE book_id=11`);
    await expect(sync.runOnce()).rejects.toThrow('document bound');
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    expect(await queue.checkpoint()).toMatchObject({ appliedRevision: 0n });
    await db.transaction(async (tx) => {
      await tx.execute(sql`UPDATE public.book_metadata SET description='Repairedword' WHERE book_id=11`);
      await enqueueBookSearchChanges(tx, [11]);
    });
    await sync.runOnce();
    expect((await search('Repairedword')).ids).toEqual([11]);
  });

  it('keeps the queue durable through an engine outage and catches up after connectivity returns', async () => {
    await addBook();
    config.bookSearch.endpoint = 'http://127.0.0.1:1';
    await expect(sync.runOnce()).rejects.toThrow();
    expect(await queue.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 0n });
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    config.bookSearch.endpoint = endpoint!;
    await sync.runOnce();
    expect((await search('BeforeRevision')).ids).toEqual([11]);
    expect(await db.select().from(bookSearchPending)).toEqual([]);
  });

  it('captures an ordinary native metadata edit without a caller manually enqueueing it', async () => {
    await addBook();
    await sync.runOnce();
    await bookRepository.updateMetadataFields(11, { title: 'Nativeeditword' });
    expect(await queue.checkpoint()).toMatchObject({ revision: 2n, appliedRevision: 1n });
    await sync.runOnce();
    expect((await search('Nativeeditword')).ids).toEqual([11]);
    expect((await search('BeforeRevision')).total).toBe(0);
  });

  it('captures native bulk metadata writes inside their own transaction', async () => {
    await addBook();
    await sync.runOnce();
    await bookRepository.bulkUpdateMetadataFields([11], { publisher: 'Nativebulkword' });
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    await sync.runOnce();
    expect((await search('Nativebulkword')).ids).toEqual([11]);
  });

  it('rolls a native edit back when its search enqueue fails', async () => {
    await addBook();
    await sync.runOnce();
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(bookRepository.updateMetadataFields(11, { title: 'Mustrollbackword' })).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT title FROM public.book_metadata WHERE book_id=11`)).rows[0].title).toBe('BeforeRevision');
    expect(await queue.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 1n });
  });

  it('leaves irrelevant timestamps and disabled search writes out of the queue', async () => {
    await addBook();
    await sync.runOnce();
    await bookRepository.updateMetadataFields(11, { updatedAt: new Date() });
    expect(await db.select().from(bookSearchPending)).toEqual([]);
    config.bookSearch.enabled = false;
    await bookRepository.updateMetadataFields(11, { title: 'Disabledwriteword' });
    expect(await db.select().from(bookSearchPending)).toEqual([]);
    expect(await queue.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 1n });
  });

  it('captures native author replacement and clearing including an empty replacement list', async () => {
    await addBook();
    await sync.runOnce();
    await metadata.replaceAuthors(11, [{ name: 'Nativeauthorword', sortName: null }], { emitEvent: false });
    await sync.runOnce();
    expect((await search('Nativeauthorword')).ids).toEqual([11]);
    expect((await search('Firstauthor')).total).toBe(0);
    await metadata.replaceAuthors(11, [], { emitEvent: false });
    await sync.runOnce();
    expect((await search('Nativeauthorword')).total).toBe(0);
  });

  it('keeps metadata and author changes in the caller transaction when that transaction rolls back', async () => {
    await addBook();
    await sync.runOnce();
    await expect(
      bookRepository.withTransaction(async (tx) => {
        await bookRepository.updateMetadataFields(11, { title: 'Abortednativeword' }, tx);
        await metadata.replaceAuthors(11, [], { executor: tx, emitEvent: false });
        throw new Error('Caller cancelled');
      }),
    ).rejects.toThrow('Caller cancelled');
    expect(await db.select().from(bookSearchPending)).toEqual([]);
    expect(await queue.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 1n });
    expect((await search('Firstauthor')).ids).toEqual([11]);
    expect((await db.execute(sql`SELECT title FROM public.book_metadata WHERE book_id=11`)).rows[0].title).toBe('BeforeRevision');
  });

  it('captures an actual book deletion and preserves finished reading history', async () => {
    await addBook();
    await sync.runOnce();
    await db.execute(sql`INSERT INTO public.reading_attempts(id,book_id,outcome) VALUES(1,11,'finished'),(2,11,NULL)`);
    await bookRepository.deleteByIds([11]);
    expect(await db.select().from(bookSearchPending)).toMatchObject([{ bookId: 11 }]);
    const reads = (await db.execute(sql`SELECT book_id,book_title_snapshot FROM public.reading_attempts ORDER BY id`)).rows;
    expect(reads).toEqual([{ book_id: null, book_title_snapshot: 'BeforeRevision' }]);
    await sync.runOnce();
    expect((await search('BeforeRevision')).total).toBe(0);
  });

  it('rolls native deletion and reading history back if enqueueing its tombstone fails', async () => {
    await addBook();
    await sync.runOnce();
    await db.execute(
      sql`INSERT INTO public.reading_attempts(id,book_id,outcome) VALUES(1,11,NULL); ALTER TABLE book_search.pending RENAME TO held_pending`,
    );
    try {
      await expect(bookRepository.deleteByIds([11])).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT id FROM public.books`)).rows).toEqual([{ id: 11 }]);
    expect((await db.execute(sql`SELECT book_id FROM public.reading_attempts`)).rows).toEqual([{ book_id: 11 }]);
    expect(await queue.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 1n });
  });

  it.each(['folder', 'folders', 'library'] as const)('captures %s cascade deletion without deleting another library', async (kind) => {
    await addBook();
    await db.transaction(async (tx) => {
      await tx.execute(sql`INSERT INTO public.books(id,library_id,status,library_folder_id,folder_path) VALUES(22,5,'present',51,'/fixture22');
        INSERT INTO public.book_metadata(book_id,title) VALUES(22,'Untouchedword')`);
      await enqueueBookSearchChanges(tx, [22]);
    });
    await sync.runOnce();
    if (kind === 'folder') await libraries.deleteFolder(31);
    else if (kind === 'folders') await libraries.deleteFoldersByLibrary(3);
    else await libraries.delete(3);
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId)).toEqual([11]);
    await sync.runOnce();
    expect((await search('BeforeRevision')).total).toBe(0);
    expect((await search('Untouchedword', [5])).ids).toEqual([22]);
  });

  it('captures native moves between libraries and rolls back an unqueued move', async () => {
    await addBook();
    await sync.runOnce();
    const move = { bookId: 11, targetLibraryId: 5, targetFolderId: 51, targetFolderPathKey: '/moved11', fileUpdates: [] };
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(moves.applyBookMove(move)).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT library_id FROM public.books WHERE id=11`)).rows[0].library_id).toBe(3);
    expect(await moves.applyBookMove(move)).toEqual({ moved: true, mergedBookId: null });
    await sync.runOnce();
    expect((await search('BeforeRevision')).total).toBe(0);
    expect((await search('BeforeRevision', [5])).ids).toEqual([11]);
  });

  it('updates both identities when a folder merge removes the source book', async () => {
    await addBook();
    await db.transaction(async (tx) => {
      await tx.execute(sql`INSERT INTO public.books(id,library_id,status,folder_path) VALUES(12,3,'present','/fixture12');
        INSERT INTO public.book_metadata(book_id,title) VALUES(12,'Mergedsourceword')`);
      await enqueueBookSearchChanges(tx, [12]);
    });
    await sync.runOnce();
    await renames.applyExistingFolderMerge({ sourceBookId: 12, targetBookId: 11, updates: [], fallbackPrimaryFileId: null });
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId).sort()).toEqual([11, 12]);
    await sync.runOnce();
    expect((await search('Mergedsourceword')).total).toBe(0);
    expect((await search('BeforeRevision')).ids).toEqual([11]);
  });

  it('captures the moved and discarded IDs when a native move merges a duplicate', async () => {
    await addBook();
    await db.transaction(async (tx) => {
      await tx.execute(sql`INSERT INTO public.books(id,library_id,status,library_folder_id,folder_path) VALUES(12,5,'present',51,'/duplicate12');
        INSERT INTO public.book_metadata(book_id,title) VALUES(12,'Duplicatediscardword')`);
      await enqueueBookSearchChanges(tx, [12]);
    });
    await sync.runOnce();
    expect(
      await moves.applyBookMove({
        bookId: 11,
        targetLibraryId: 5,
        targetFolderId: 51,
        targetFolderPathKey: '/moved11',
        fileUpdates: [],
        mergeDuplicateBookId: 12,
      }),
    ).toEqual({ moved: true, mergedBookId: 12 });
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId).sort()).toEqual([11, 12]);
    await sync.runOnce();
    expect((await search('Duplicatediscardword', [5])).total).toBe(0);
    expect((await search('BeforeRevision', [5])).ids).toEqual([11]);
  });

  it('captures all followers when a native author is renamed', async () => {
    await addBook();
    await db.transaction(async (tx) => {
      await tx.execute(sql`INSERT INTO public.books(id,library_id,status,folder_path) VALUES(22,3,'present','/fixture22');
        INSERT INTO public.book_metadata(book_id,title) VALUES(22,'Followerword'); INSERT INTO public.book_authors VALUES(22,2,0)`);
      await enqueueBookSearchChanges(tx, [22]);
    });
    await sync.runOnce();
    await authorsRepository.updateAuthorById(2, { name: 'Renamedauthorword' });
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId).sort()).toEqual([11, 22]);
    await sync.runOnce();
    expect((await search('Firstauthor')).total).toBe(0);
    expect((await search('Renamedauthorword')).ids.sort()).toEqual([11, 22]);
  });

  it('captures native author merges and deletion', async () => {
    await addBook();
    await sync.runOnce();
    await authorsRepository.mergeAuthors(2, [1]);
    await sync.runOnce();
    expect((await search('Secondauthor')).total).toBe(0);
    expect((await search('Firstauthor')).ids).toEqual([11]);
    await authorsRepository.deleteAuthors([2]);
    await sync.runOnce();
    expect((await search('Firstauthor')).total).toBe(0);
  });

  it('captures entity-manager soft author unlinking', async () => {
    await addBook();
    await sync.runOnce();
    await authorStrategy.deleteEntity({ entityId: 1, mode: 'soft', libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Secondauthor')).total).toBe(0);
    expect((await search('Firstauthor')).ids).toEqual([11]);
  });

  it('captures entity-manager author splitting', async () => {
    await addBook();
    await sync.runOnce();
    await authorStrategy.split({ entityId: 1, newNames: ['Splitfirstword', 'Splitsecondword'], libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Secondauthor')).total).toBe(0);
    expect((await search('Splitfirstword')).ids).toEqual([11]);
    expect((await search('Splitsecondword')).ids).toEqual([11]);
  });

  it('keeps scanned processing books hidden through conflict, then publishes only after promotion', async () => {
    const input = { id: 22, libraryId: 3, libraryFolderId: 31, folderPath: '/scan22', status: 'processing' };
    expect((await scanner.createBook(input)).status).toBe('processing');
    await bookRepository.updateMetadataFields(22, { title: 'Scannedpublicationword' });
    await sync.runOnce();
    expect((await search('Scannedpublicationword')).total).toBe(0);
    expect((await scanner.createBook({ ...input, status: 'present' })).status).toBe('processing');
    await sync.runOnce();
    expect((await search('Scannedpublicationword')).total).toBe(0);
    expect(await scanner.promoteProcessingBookToPresent(22)).toBe(true);
    await sync.runOnce();
    expect((await search('Scannedpublicationword')).ids).toEqual([22]);
    expect(await scanner.promoteProcessingBookToPresent(22)).toBe(false);
    expect(await scanner.promoteProcessingBookToPresent(999)).toBe(false);
    expect(await db.select().from(bookSearchPending)).toEqual([]);
  });

  it('atomically restores a missing scanned book and retains its metadata', async () => {
    await addBook();
    await scanner.updateBookStatus(11, 'missing');
    expect((await scanner.createBook({ id: 99, libraryId: 3, libraryFolderId: 31, folderPath: '/fixture11' })).id).toBe(11);
    expect((await db.execute(sql`SELECT status FROM public.books WHERE id=11`)).rows[0].status).toBe('present');
    await sync.runOnce();
    expect((await search('BeforeRevision')).ids).toEqual([11]);
    expect(await db.select().from(bookSearchPending)).toEqual([]);
  });

  it('rolls scanned books, metadata and promotion back when their queue write fails', async () => {
    await scanner.createBook({ id: 22, libraryId: 3, libraryFolderId: 31, folderPath: '/scan22', status: 'processing' });
    await sync.runOnce();
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(scanner.createBook({ id: 33, libraryId: 3, libraryFolderId: 31, folderPath: '/scan33' })).rejects.toThrow();
      await expect(scanner.promoteProcessingBookToPresent(22)).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT status FROM public.books WHERE id=22`)).rows[0].status).toBe('processing');
    expect((await db.execute(sql`SELECT id FROM public.books WHERE id=33`)).rows).toEqual([]);
    expect((await db.execute(sql`SELECT book_id FROM public.book_metadata WHERE book_id=33`)).rows).toEqual([]);
  });

  it('captures bulk scanner statuses for actual rows and rolls failed changes back', async () => {
    await addBook();
    await sync.runOnce();
    await scanner.markBooksAsMissing([11, 11, 999]);
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId)).toEqual([11]);
    await sync.runOnce();
    const raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 11 } }, size: 1 });
    expect(raw.hits.hits[0]._source.status).toBe('missing');
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(scanner.markBooksAsPresent([11])).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT status FROM public.books WHERE id=11`)).rows[0].status).toBe('missing');
    await scanner.markBooksAsPresent([11]);
    await sync.runOnce();
    const checkpoint = await queue.checkpoint();
    await scanner.updateBookStatus(999, 'present');
    await scanner.markBooksAsMissing([]);
    expect(await queue.checkpoint()).toEqual(checkpoint);
    expect(await db.select().from(bookSearchPending)).toEqual([]);
  });

  it('captures scanner library reassignment without retaining the old library scope', async () => {
    await addBook();
    await sync.runOnce();
    expect(await scanner.moveBookToLibrary(11, 5, 51, '/scanner-moved11')).toMatchObject({ libraryChanged: true });
    await sync.runOnce();
    expect((await search('BeforeRevision')).total).toBe(0);
    expect((await search('BeforeRevision', [5])).ids).toEqual([11]);
    await scanner.moveBookToLibrary(11, 5, 51, '/scanner-moved11');
    expect(await db.select().from(bookSearchPending)).toEqual([]);
  });

  it('finishes canonical author links before capture and refreshes every existing follower', async () => {
    await addBook();
    await db.transaction(async (tx) => {
      await tx.execute(sql`INSERT INTO public.books(id,library_id,status,folder_path) VALUES(22,3,'present','/fixture22');
        INSERT INTO public.book_metadata(book_id,title) VALUES(22,'Canonicalfollowerword');
        INSERT INTO public.authors(id,name) VALUES(9,'  Canonicalauthorword  '); INSERT INTO public.book_authors VALUES(22,9,0)`);
      await enqueueBookSearchChanges(tx, [22]);
    });
    await sync.runOnce();
    const original = capture.enqueueAuthors.bind(capture);
    const spy = vi.spyOn(capture, 'enqueueAuthors').mockImplementation(async (tx, authorIds) => {
      expect((await tx.select().from(schema.bookAuthors)).filter((row) => row.bookId === 11).map((row) => row.authorId)).toEqual([9]);
      return original(tx, authorIds);
    });
    try {
      await metadata.replaceAuthors(11, [{ name: 'Canonicalauthorword', sortName: null }]);
    } finally {
      spy.mockRestore();
    }
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId).sort()).toEqual([11, 22]);
    await sync.runOnce();
    expect((await search('Canonicalauthorword')).ids.sort()).toEqual([11, 22]);
    const raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 22 } }, size: 1 });
    expect(raw.hits.hits[0]._source.authors_exact).toEqual(['Canonicalauthorword']);
  });

  it('creates uploaded records and rolls an unqueued upload back without ghost books or files', async () => {
    const file = uploadFile('/uploaded-single');
    const create = () => uploads.createBookRecord(3, 31, file.folderPath, file.absolutePath, file.relPath, file.format, file.sizeBytes);
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(create()).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT count(*)::integer AS total FROM public.books`)).rows[0].total).toBe(0);
    expect((await db.execute(sql`SELECT count(*)::integer AS total FROM public.book_metadata`)).rows[0].total).toBe(0);
    expect((await db.execute(sql`SELECT count(*)::integer AS total FROM public.book_files`)).rows[0].total).toBe(0);
    const uploaded = await create();
    expect(uploaded.created).toBe(true);
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId)).toEqual([uploaded.bookId]);
    await bookRepository.updateMetadataFields(uploaded.bookId, { title: 'Uploadsingleword' });
    await sync.runOnce();
    expect((await search('Uploadsingleword')).ids).toEqual([uploaded.bookId]);
  });

  it('captures multi-file upload compensation and rolls failed compensation back', async () => {
    const records = await uploads.createUnitBookRecords(3, 31, [uploadFile('/uploaded-unit'), uploadFile('/uploaded-unit', 'two.txt')]);
    expect(records.createdBookIds).toHaveLength(1);
    expect(records.bookIds).toEqual(records.createdBookIds);
    const [bookId] = records.bookIds;
    await bookRepository.updateMetadataFields(bookId, { title: 'Uploadunitword' });
    await sync.runOnce();
    expect((await search('Uploadunitword')).ids).toEqual([bookId]);
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(uploads.deleteUnitBookRecords(records)).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT count(*)::integer AS total FROM public.book_files WHERE book_id=${bookId}`)).rows[0].total).toBe(2);
    await uploads.deleteUnitBookRecords(records);
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId)).toEqual([bookId]);
    await sync.runOnce();
    expect((await search('Uploadunitword')).total).toBe(0);
  }, 15000);

  it('compensates an attachment without deleting its existing book or metadata', async () => {
    await addBook();
    await sync.runOnce();
    const records = await uploads.createUnitBookRecords(3, 31, [uploadFile('/fixture11')]);
    expect(records.createdBookIds).toEqual([]);
    expect(records.attachedFileIds).toHaveLength(1);
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId)).toEqual([11]);
    await uploads.deleteUnitBookRecords(records);
    await sync.runOnce();
    expect((await search('BeforeRevision')).ids).toEqual([11]);
    const book = (await db.execute(sql`SELECT primary_file_id FROM public.books WHERE id=11`)).rows[0];
    expect(book.primary_file_id).toBeNull();
    expect((await db.execute(sql`SELECT id FROM public.book_files WHERE book_id=11`)).rows).toEqual([]);
  }, 15000);

  it('captures restored visibility when a file is registered on an existing missing book', async () => {
    await addBook();
    await scanner.updateBookStatus(11, 'missing');
    await sync.runOnce();
    const file = uploadFile('/fixture11');
    const register = () =>
      uploadService.registerBookFile({
        bookId: 11,
        libraryFolderId: 31,
        libraryFolderPath: fixtureDirectory,
        destination: file.absolutePath,
        sizeBytes: 20,
        fileHash: '0123456789abcdef0123456789abcdef',
        format: 'txt',
      });
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(register()).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT status FROM public.books WHERE id=11`)).rows[0].status).toBe('missing');
    expect((await db.execute(sql`SELECT id FROM public.book_files WHERE book_id=11`)).rows).toEqual([]);
    expect((await register()).bookStatus).toBe('present');
    await sync.runOnce();
    const raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 11 } }, size: 1 });
    expect(raw.hits.hits[0]._source.status).toBe('present');
  }, 15000);

  it('captures series replacement and clearing and rolls failed replacement back as one transaction', async () => {
    await addBook();
    await series.replaceForBook(11, [{ seriesName: 'Originalseriesword', seriesIndex: '1' }]);
    await sync.runOnce();
    expect((await search('Originalseriesword')).ids).toEqual([11]);
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(series.replaceForBook(11, [{ seriesName: 'Rejectedseriesword' }])).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await series.findByBookId(11)).map((row) => row.seriesName)).toEqual(['Originalseriesword']);
    expect((await db.execute(sql`SELECT id FROM public.book_series WHERE name='Rejectedseriesword'`)).rows).toEqual([]);
    await series.replaceForBook(11, []);
    await sync.runOnce();
    expect((await search('Originalseriesword')).total).toBe(0);
  }, 15000);

  it('captures a scoped series rename while preserving the same series in another library', async () => {
    await addBook();
    await db.execute(sql`INSERT INTO public.books(id,library_id,library_folder_id,status,folder_path) VALUES(22,5,51,'present','/fixture22');
      INSERT INTO public.book_metadata(book_id,title) VALUES(22,'Otherserieslibraryword')`);
    const [original] = await series.replaceForBook(11, [{ seriesName: 'Scopedseriesword' }]);
    await series.replaceForBook(22, [{ seriesName: 'Scopedseriesword' }]);
    await sync.runOnce();
    await seriesStrategy.rename({ entityId: original.seriesId, newName: 'Renamedseriesword', libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Scopedseriesword')).total).toBe(0);
    expect((await search('Renamedseriesword')).ids).toEqual([11]);
    expect((await search('Scopedseriesword', [5])).ids).toEqual([22]);
  }, 15000);

  it('captures series merges and removal through the entity manager', async () => {
    await addBook();
    const [source] = await series.replaceForBook(11, [{ seriesName: 'Sourceseriesword' }]);
    const identity = new SeriesIdentityService(db);
    const targetId = await identity.resolveSeriesId('Targetseriesword');
    await sync.runOnce();
    await seriesStrategy.merge({ targetId: targetId!, sourceIds: [source.seriesId], libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Sourceseriesword')).total).toBe(0);
    expect((await search('Targetseriesword')).ids).toEqual([11]);
    await seriesStrategy.deleteEntity({ entityId: targetId!, mode: 'soft', libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Targetseriesword')).total).toBe(0);
  }, 20000);

  it('keeps shared series identity edits, all follower captures and failed renames in one transaction', async () => {
    await addBook();
    await addSecondBook();
    const [original] = await series.replaceForBook(11, [{ seriesName: 'Originalseriesword' }]);
    await series.replaceForBook(22, [{ seriesName: 'Originalseriesword' }]);
    await sync.runOnce();
    await seriesStrategy.rename({ entityId: original.seriesId, newName: 'ORIGINALSERIESWORD', libraryIds: [3] });
    expect(
      (await db.execute<{ book_id: number }>(sql`SELECT book_id FROM book_search.pending ORDER BY book_id`)).rows.map((row) => row.book_id),
    ).toEqual([11, 22]);
    await sync.runOnce();
    for (const id of [11, 22]) {
      const result = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: id } }, size: 1 });
      expect(result.hits.hits[0]._source.series_exact).toEqual(id === 11 ? ['ORIGINALSERIESWORD'] : ['Originalseriesword', 'ORIGINALSERIESWORD']);
    }
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(seriesStrategy.rename({ entityId: original.seriesId, newName: 'originalseriesword', libraryIds: [3] })).rejects.toThrow();
      await expect(seriesStrategy.rename({ entityId: original.seriesId, newName: 'Newrollbackseriesword', libraryIds: [3] })).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    const rows = await db.select({ name: schema.bookSeries.name }).from(schema.bookSeries);
    expect(rows).toEqual([{ name: 'ORIGINALSERIESWORD' }]);
    expect((await queue.pendingSummary()).count).toBe('0');
    expect((await db.select().from(schema.bookSeriesMemberships)).length).toBe(2);
  }, 20000);

  it('captures Book Dock scalar and series publication and rolls an unqueued publication back', async () => {
    await addBook();
    await sync.runOnce();
    const cover = vi
      .spyOn(dock as unknown as { applyUnpickedCover: (...args: unknown[]) => Promise<void> }, 'applyUnpickedCover')
      .mockResolvedValue(undefined);
    const row = {
      format: 'txt',
      selectedMetadata: { title: 'Dockscalarword', seriesName: 'Dockseriesword', isbn13: '9781234567897', authors: [], genres: [] },
    } as schema.BookDockFileRow;
    try {
      await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
      try {
        await expect(dock['applyMetadata'](11, row)).rejects.toThrow();
      } finally {
        await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
      }
      expect((await db.execute(sql`SELECT title FROM public.book_metadata WHERE book_id=11`)).rows[0].title).toBe('BeforeRevision');
      expect(await series.findByBookId(11)).toEqual([]);
      await dock['applyMetadata'](11, row);
      await sync.runOnce();
      for (const term of ['Dockscalarword', 'Dockseriesword', '9781234567897']) expect((await search(term)).ids).toEqual([11]);
    } finally {
      cover.mockRestore();
    }
  }, 15000);

  it('captures regex metadata through the same scalar transaction used by ordinary extraction', async () => {
    await addBook();
    await sync.runOnce();
    expect(await metadata.applyRegexMetadataRules(11, [], { title: 'Regexcapturedword' }, ['regex'])).toBe('updated');
    await sync.runOnce();
    expect((await search('BeforeRevision')).total).toBe(0);
    expect((await search('Regexcapturedword')).ids).toEqual([11]);
  }, 15000);

  it('reproduces the original scalar-enqueue and author-row lock inversion on disposable data', async () => {
    await addBook();
    await addSecondBook();
    const result = await concurrentEdits(false);
    const errors = result
      .filter((item) => item.status === 'rejected')
      .map((item) => {
        const error = item.reason as { code?: string; cause?: { code?: string } };
        return error.cause?.code ?? error.code;
      });
    expect(errors).toContain('40P01');
  }, 15000);

  it('finishes the same concurrent native editors when enqueue is deferred until source writes finish', async () => {
    await addBook();
    await addSecondBook();
    await sync.runOnce();
    expect((await concurrentEdits(true)).map((result) => result.status)).toEqual(['fulfilled', 'fulfilled']);
    await sync.runOnce();
    expect((await search('Concurrenteditorword')).ids).toEqual([11]);
    expect((await search('Otherconcurrentword', [5])).ids).toEqual([22]);
    expect((await search('Secondauthor')).total).toBe(0);
  }, 15000);

  it('defers all editor helper notifications, deduplicates the book, and shares source rollback', async () => {
    await addBook();
    await sync.runOnce();
    await expect(
      bookRepository.withTransaction(async (tx) => {
        await bookRepository.updateMetadataFields(11, { title: 'Rejectededitorword' }, tx);
        await metadata.replaceAuthors(11, [{ name: 'Rejectedauthorword', sortName: null }], { executor: tx });
        expect(await tx.select().from(bookSearchPending)).toEqual([]);
        throw new Error('editor cancelled');
      }),
    ).rejects.toThrow('editor cancelled');
    expect((await db.execute(sql`SELECT title FROM public.book_metadata WHERE book_id=11`)).rows[0].title).toBe('BeforeRevision');
    expect(await queue.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 1n });
    await bookRepository.withTransaction(async (tx) => {
      await bookRepository.updateMetadataFields(11, { title: 'Committededitorword' }, tx);
      await metadata.replaceAuthors(11, [{ name: 'Committedauthorword', sortName: null }], { executor: tx });
      expect(await tx.select().from(bookSearchPending)).toEqual([]);
    });
    expect(await queue.checkpoint()).toMatchObject({ revision: 2n, appliedRevision: 1n });
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    await sync.runOnce();
    expect((await search('Committededitorword')).ids).toEqual([11]);
    expect((await search('Committedauthorword')).ids).toEqual([11]);
  }, 15000);

  it('rolls the entire editor back when the deferred final enqueue fails', async () => {
    await addBook();
    await sync.runOnce();
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(
        bookRepository.withTransaction(async (tx) => {
          await bookRepository.updateMetadataFields(11, { title: 'Unqueuededitorword' }, tx);
          await metadata.replaceAuthors(11, [{ name: 'Unqueuedauthorword', sortName: null }], { executor: tx });
        }),
      ).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT title FROM public.book_metadata WHERE book_id=11`)).rows[0].title).toBe('BeforeRevision');
    expect((await db.execute(sql`SELECT name FROM public.authors WHERE name='Unqueuedauthorword'`)).rows).toEqual([]);
    expect(await queue.checkpoint()).toMatchObject({ revision: 1n, appliedRevision: 1n });
  });

  it('flushes native bulk metadata and nested series updates only once after all source writes', async () => {
    await addBook();
    await addSecondBook();
    await sync.runOnce();
    expect(await queue.checkpoint()).toMatchObject({ revision: 2n });
    await bookRepository.bulkUpdateMetadataFields([11, 22], { seriesName: 'Bulkseriesboundaryword' });
    expect(await queue.checkpoint()).toMatchObject({ revision: 3n, appliedRevision: 2n });
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId).sort()).toEqual([11, 22]);
    await sync.runOnce();
    expect((await search('Bulkseriesboundaryword')).ids).toEqual([11]);
    expect((await search('Bulkseriesboundaryword', [5])).ids).toEqual([22]);
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(bookRepository.bulkUpdateMetadataFields([11, 22], { seriesName: 'Rejectedbulkseriesword' })).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT DISTINCT series_name FROM public.book_metadata WHERE book_id IN (11,22)`)).rows).toEqual([
      { series_name: 'Bulkseriesboundaryword' },
    ]);
    expect(await queue.checkpoint()).toMatchObject({ revision: 3n, appliedRevision: 3n });
  }, 15000);

  it('captures standalone migration metadata and author insertion/removal', async () => {
    await addBook();
    await migration.upsertBookMetadata(11, { title: 'Migrationtitleword' });
    await migration.deleteBookAuthors(11);
    await migration.insertBookAuthor(11, 2, 0);
    await sync.runOnce();
    expect((await search('Migrationtitleword')).ids).toEqual([11]);
    expect((await search('Secondauthor')).total).toBe(0);
    expect((await search('Firstauthor')).ids).toEqual([11]);
    await migration.insertBookAuthor(11, 2, 0);
    await migration.deleteBookAuthors(999);
    expect(await db.select().from(bookSearchPending)).toEqual([]);
  });

  it('captures batch migration through the constructed transactional repository and rolls a failed import back', async () => {
    await addBook();
    await addSecondBook();
    await sync.runOnce();
    await migration.withTransaction(async (repo) => {
      await repo.batchUpsertBookMetadata([
        { bookId: 11, title: 'Batchmigrationword' },
        { bookId: 22, publisher: 'Batchpublisherword' },
      ]);
      await repo.batchDeleteBookAuthors([11]);
      await repo.batchInsertBookAuthors([{ bookId: 11, authorId: 2, displayOrder: 0 }]);
    });
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId).sort()).toEqual([11, 22]);
    await sync.runOnce();
    expect((await search('Batchmigrationword')).ids).toEqual([11]);
    expect((await search('Batchpublisherword', [5])).ids).toEqual([22]);
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(
        migration.withTransaction(async (repo) => {
          await repo.upsertBookMetadata(11, { title: 'Rejectedmigrationword' });
          await repo.deleteBookAuthors(11);
        }),
      ).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT title FROM public.book_metadata WHERE book_id=11`)).rows[0].title).toBe('Batchmigrationword');
    expect((await db.execute(sql`SELECT author_id FROM public.book_authors WHERE book_id=11`)).rows[0].author_id).toBe(2);
  }, 15000);

  it('captures scoped publisher rename, merge and removal without affecting another library', async () => {
    await addBook();
    await addSecondBook();
    await publishers.rename({ entityId: 'Publisherword', newName: 'Renamedpublisherword', libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Renamedpublisherword')).ids).toEqual([11]);
    expect((await search('Publisherword', [5])).ids).toEqual([22]);
    await publishers.merge({ targetId: 'Mergedpublisherword', sourceIds: ['Renamedpublisherword', 'Missingpublisherword'], libraryIds: [3] });
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId)).toEqual([11]);
    await sync.runOnce();
    expect((await search('Mergedpublisherword')).ids).toEqual([11]);
    await publishers.deleteEntity({ entityId: 'Mergedpublisherword', mode: 'soft', libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Mergedpublisherword')).total).toBe(0);
    expect((await search('Publisherword', [5])).ids).toEqual([22]);
  }, 20000);

  it('rolls publisher mutations back when their final enqueue fails and leaves empty scopes untouched', async () => {
    await addBook();
    await sync.runOnce();
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(publishers.rename({ entityId: 'Publisherword', newName: 'Rejectedpublisherword', libraryIds: [3] })).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT publisher FROM public.book_metadata WHERE book_id=11`)).rows[0].publisher).toBe('Publisherword');
    expect((await publishers.rename({ entityId: 'Publisherword', newName: 'Noaccessword', libraryIds: [] })).affectedBookIds).toEqual([]);
    expect(await db.select().from(bookSearchPending)).toEqual([]);
  });

  it('replaces and clears the stored language projection through inline entity operations', async () => {
    await addBook();
    await migration.upsertBookMetadata(11, { language: 'en' });
    await languages.rename({ entityId: 'en', newName: 'zh-CN', libraryIds: [3] });
    await sync.runOnce();
    let raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 11 } }, size: 1 });
    expect(raw.hits.hits[0]._source.language).toBe('zh-CN');
    await languages.deleteEntity({ entityId: 'zh-CN', mode: 'soft', libraryIds: [3] });
    await sync.runOnce();
    raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 11 } }, size: 1 });
    expect(raw.hits.hits[0]._source.language).toBe('');
  }, 15000);

  it('searches every series membership and removes an old secondary name after rename', async () => {
    await addBook();
    const memberships = await series.replaceForBook(11, [{ seriesName: 'Primaryseriesword' }, { seriesName: 'Secondaryseriesword' }]);
    await sync.runOnce();
    expect((await search('Secondaryseriesword')).ids).toEqual([11]);
    let raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 11 } }, size: 1 });
    expect(raw.hits.hits[0]._source.series_exact).toEqual(['Primaryseriesword', 'Secondaryseriesword']);
    await seriesStrategy.rename({ entityId: memberships[1].seriesId, newName: 'Renamedsecondaryword', libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Secondaryseriesword')).total).toBe(0);
    expect((await search('Renamedsecondaryword')).ids).toEqual([11]);
    raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 11 } }, size: 1 });
    expect(raw.hits.hits[0]._source.series).toBe('Primaryseriesword');
  }, 15000);

  it('captures narrator replacement and clearing and preserves display order in the document', async () => {
    await addBook();
    await narratorService.replaceForBook(11, ['Spokensecondword', 'Spokenfirstword']);
    await sync.runOnce();
    expect((await search('Spokenfirstword')).ids).toEqual([11]);
    const raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 11 } }, size: 1 });
    expect(raw.hits.hits[0]._source.narrators_exact).toEqual(['Spokensecondword', 'Spokenfirstword']);
    await narratorService.replaceForBook(11, []);
    await sync.runOnce();
    expect((await search('Spokenfirstword')).total).toBe(0);
  }, 15000);

  it('captures all followers after narrator normalization finishes the current links', async () => {
    await addBook();
    await addSecondBook();
    await db.execute(
      sql`INSERT INTO public.narrators(id,name) VALUES(9,'  Canonicalspokenword  '); INSERT INTO public.book_narrators VALUES(22,9,0)`,
    );
    await sync.runOnce();
    const original = capture.enqueueNarrators.bind(capture);
    const spy = vi.spyOn(capture, 'enqueueNarrators').mockImplementation(async (tx, ids) => {
      expect((await tx.select().from(schema.bookNarrators)).some((row) => row.bookId === 11 && row.narratorId === 9)).toBe(true);
      return original(tx, ids);
    });
    try {
      await narratorService.replaceForBook(11, ['Canonicalspokenword']);
    } finally {
      spy.mockRestore();
    }
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId).sort()).toEqual([11, 22]);
    await sync.runOnce();
    expect((await search('Canonicalspokenword')).ids).toEqual([11]);
    expect((await search('Canonicalspokenword', [5])).ids).toEqual([22]);
    const raw = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 22 } }, size: 1 });
    expect(raw.hits.hits[0]._source.narrators_exact).toEqual(['Canonicalspokenword']);
  }, 15000);

  it('captures narrator rename and implicit merge through the native strategy', async () => {
    await addBook();
    await narratorService.replaceForBook(11, ['Originalspokenword']);
    const id = Number((await db.execute(sql`SELECT id FROM public.narrators WHERE name='Originalspokenword'`)).rows[0].id);
    await narratorStrategy.rename({ entityId: id, newName: 'Renamedspokenword', libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Renamedspokenword')).ids).toEqual([11]);
    const target = await migration.upsertNarrator({ name: 'Targetspokenword', sortName: '' });
    expect((await narratorStrategy.rename({ entityId: id, newName: 'Targetspokenword', libraryIds: [3] })).wasImplicitMerge).toBe(true);
    await sync.runOnce();
    expect((await search('Renamedspokenword')).total).toBe(0);
    expect((await search('Targetspokenword')).ids).toEqual([11]);
    expect((await db.execute(sql`SELECT narrator_id FROM public.book_narrators WHERE book_id=11`)).rows[0].narrator_id).toBe(target!.id);
  }, 15000);

  it('captures narrator explicit merge, split, soft unlink and hard removal', async () => {
    await addBook();
    await narratorService.replaceForBook(11, ['Mergesourcespokenword', 'Mergetargetspokenword']);
    const ids = (await db.execute(sql`SELECT id,name FROM public.narrators ORDER BY id`)).rows;
    await narratorStrategy.merge({ sourceIds: [Number(ids[0].id)], targetId: Number(ids[1].id), libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Mergesourcespokenword')).total).toBe(0);
    expect((await search('Mergetargetspokenword')).ids).toEqual([11]);
    const split = await narratorStrategy.split({
      entityId: Number(ids[1].id),
      newNames: ['Splitspokenfirstword', 'Splitspokensecondword'],
      libraryIds: [3],
    });
    await sync.runOnce();
    expect((await search('Mergetargetspokenword')).total).toBe(0);
    expect((await search('Splitspokenfirstword')).ids).toEqual([11]);
    expect((await search('Splitspokensecondword')).ids).toEqual([11]);
    await narratorStrategy.deleteEntity({ entityId: split.newEntities[0].id, mode: 'soft', libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Splitspokenfirstword')).total).toBe(0);
    expect((await search('Splitspokensecondword')).ids).toEqual([11]);
    await narratorStrategy.deleteEntity({ entityId: split.newEntities[1].id, mode: 'hard', libraryIds: [3] });
    await sync.runOnce();
    expect((await search('Splitspokensecondword')).total).toBe(0);
  }, 25000);

  it('preserves native rejection when a narrator is shared with a library outside the edit scope', async () => {
    await addBook();
    await addSecondBook();
    await narratorService.replaceForBook(11, ['Sharedspokenword']);
    await narratorService.replaceForBook(22, ['Sharedspokenword']);
    await sync.runOnce();
    const id = Number((await db.execute(sql`SELECT id FROM public.narrators WHERE name='Sharedspokenword'`)).rows[0].id);
    await expect(narratorStrategy.rename({ entityId: id, newName: 'Forbiddenspokenword', libraryIds: [3] })).rejects.toThrow('cannot edit');
    expect(await db.select().from(bookSearchPending)).toEqual([]);
    await narratorStrategy.rename({ entityId: id, newName: 'Allowedspokenword', libraryIds: [3, 5] });
    await sync.runOnce();
    expect((await search('Allowedspokenword')).ids).toEqual([11]);
    expect((await search('Allowedspokenword', [5])).ids).toEqual([22]);
  }, 15000);

  it('captures single and batch narrator import and rolls a failed deletion back', async () => {
    await addBook();
    await addSecondBook();
    const narrator = await migration.upsertNarrator({ name: 'Importedspokenword', sortName: '' });
    await migration.withTransaction(async (repo) => {
      await repo.insertBookNarrator(11, narrator!.id, 0);
      await repo.batchInsertBookNarrators([{ bookId: 22, narratorId: narrator!.id, displayOrder: 0 }]);
    });
    await sync.runOnce();
    expect((await search('Importedspokenword')).ids).toEqual([11]);
    expect((await search('Importedspokenword', [5])).ids).toEqual([22]);
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(migration.deleteBookNarrators(11)).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect((await db.execute(sql`SELECT narrator_id FROM public.book_narrators WHERE book_id=11`)).rows).toHaveLength(1);
    await migration.batchDeleteBookNarrators([11, 999]);
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId)).toEqual([11]);
    await sync.runOnce();
    expect((await search('Importedspokenword')).total).toBe(0);
    expect((await search('Importedspokenword', [5])).ids).toEqual([22]);
    await migration.deleteBookNarrators(22);
    await sync.runOnce();
    expect((await search('Importedspokenword', [5])).total).toBe(0);
  }, 20000);

  it('rolls narrator replacement and strategy rename back when final enqueue fails', async () => {
    await addBook();
    await narratorService.replaceForBook(11, ['Keptspokenword']);
    await sync.runOnce();
    const id = Number((await db.execute(sql`SELECT id FROM public.narrators WHERE name='Keptspokenword'`)).rows[0].id);
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(narratorService.replaceForBook(11, ['Rejectedspokenword'])).rejects.toThrow();
      await expect(narratorStrategy.rename({ entityId: id, newName: 'Rejectedrenameword', libraryIds: [3] })).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    await expect(
      bookRepository.withTransaction(async (tx) => {
        await narratorService.replaceForBook(11, ['Cancelledspokenword'], { executor: tx });
        throw new Error('cancel narrator edit');
      }),
    ).rejects.toThrow('cancel narrator edit');
    expect((await db.execute(sql`SELECT name FROM public.narrators WHERE id=${id}`)).rows[0].name).toBe('Keptspokenword');
    expect((await db.execute(sql`SELECT name FROM public.narrators WHERE name IN ('Rejectedspokenword','Cancelledspokenword')`)).rows).toEqual([]);
    expect(await db.select().from(bookSearchPending)).toEqual([]);
  });

  it.each(['series', 'narrator'] as const)(
    'retains pending work instead of truncating an oversized visible %s list',
    async (kind) => {
      await addBook();
      await sync.runOnce();
      if (kind === 'series')
        await series.replaceForBook(
          11,
          Array.from({ length: 101 }, (_, index) => ({ seriesName: `Overflowseriesword${index}` })),
        );
      else
        await narratorService.replaceForBook(
          11,
          Array.from({ length: 101 }, (_, index) => `Overflowspokenword${index}`),
        );
      await expect(sync.runOnce()).rejects.toThrow('exceeded');
      expect((await db.select().from(bookSearchPending)).map((row) => row.bookId)).toEqual([11]);
      const checkpoint = await queue.checkpoint();
      expect(checkpoint!.revision > checkpoint!.appliedRevision).toBe(true);
    },
    15000,
  );

  it('removes a hidden processing book without reading oversized related-name lists', async () => {
    await scanner.createBook({ id: 33, libraryId: 3, libraryFolderId: 31, folderPath: '/hidden33', status: 'processing' });
    await bookRepository.updateMetadataFields(33, { title: 'Hiddenoverflowword', description: 'x'.repeat(65537) });
    await narratorService.replaceForBook(
      33,
      Array.from({ length: 101 }, (_, index) => `Hiddenspokenword${index}`),
    );
    expect(await sync.runOnce()).toEqual({ phase: 'applied', acknowledged: 1 });
    expect((await search('Hiddenoverflowword')).total).toBe(0);
    expect(await db.select().from(bookSearchPending)).toEqual([]);
  });

  it('compares actual native SQL and engine recall on the same literal and multilingual corpus', async () => {
    const corpus = [
      { id: 101, title: '英语学习指南' },
      { id: 102, title: 'Café Español' },
      { id: 103, title: 'Programming Guide' },
      { id: 104, title: 'Alpha   Beta' },
      { id: 105, title: 'Alpha Beta' },
      { id: 106, title: 'Literature', publisher: 'Onlypublisherword' },
      { id: 107, title: 'SQL 100%_safe' },
      { id: 108, title: '相邻', subtitle: '字段' },
      { id: 109, title: '𠮷野家' },
      { id: 110, title: 'Symbols *:* ?? [x]' },
      { id: 111, title: 'literal z_boundary marker' },
      { id: 112, title: 'Audio collection' },
      { id: 113, title: 'Straße Æther Œuvre' },
      { id: 114, title: 'ab', subtitle: 'bc' },
      { id: 115, title: 'a', subtitle: 'b' },
    ];
    await db.transaction(async (tx) => {
      for (const row of corpus) {
        await tx.insert(schema.books).values({ id: row.id, libraryId: 3, libraryFolderId: 31, folderPath: `/recall${row.id}`, status: 'present' });
        await tx.insert(schema.bookMetadata).values({ ...row, bookId: row.id });
      }
      await enqueueBookSearchChanges(
        tx,
        corpus.map((row) => row.id),
      );
    });
    await narratorService.replaceForBook(112, ['René']);
    await series.replaceForBook(112, [{ seriesName: 'Léçons' }, { seriesName: 'Second série' }]);
    await metadata.replaceAuthors(112, [{ name: 'José', sortName: null }]);
    await sync.runOnce();
    const builder = new BookQueryBuilder(db, {} as never);
    const comparison: { term: string; native_ids: number[]; fulltext_ids: number[]; contains_ids: number[]; equal: boolean }[] = [];
    for (const term of [
      '英语',
      '英',
      '语学',
      'cafe',
      'CAFÉ',
      'gram',
      'Alpha\tBeta',
      'Alpha   Beta',
      '100%_',
      '相邻字段',
      'Onlypublisherword',
      '指南 英语',
      '𠮷',
      '*:*',
      '?',
      '[x]',
      'z_boundary',
      'jose',
      'rene',
      'lecons',
      'serie',
      'strasse',
      'ß',
      'aether',
      'oeuvre',
      'Cafe\u0301',
      'abc',
      'ab',
      'a',
    ]) {
      const rows = await db
        .select({ id: schema.books.id })
        .from(schema.books)
        .leftJoin(schema.bookMetadata, eq(schema.bookMetadata.bookId, schema.books.id))
        .where(and(eq(schema.books.libraryId, 3), ne(schema.books.status, 'processing'), builder.buildQuickSearch(term)))
        .orderBy(asc(schema.books.id));
      const native = rows.map((row) => row.id);
      config.bookSearch.matchMode = 'fulltext';
      const indexed = (await search(term)).ids.sort((a, b) => a - b);
      config.bookSearch.matchMode = 'contains';
      const contains = (await search(term)).ids.sort((a, b) => a - b);
      comparison.push({
        term,
        native_ids: native,
        fulltext_ids: indexed,
        contains_ids: contains,
        equal: JSON.stringify(native) === JSON.stringify(contains),
      });
    }
    const report = process.env.BOOK_SEARCH_TEST_RECALL_REPORT;
    if (report)
      await writeFile(
        report,
        JSON.stringify({ scope: 'Disposable synthetic corpus; native SQL versus both real Zinc contains and fulltext modes', comparison }, null, 2),
      );
    expect(comparison.filter((row) => !row.equal)).toEqual([]);
    await bookRepository.updateMetadataFields(103, { title: 'Rewritten Reference' });
    await sync.runOnce();
    expect((await search('gram')).ids).toEqual([]);
    expect((await search('reference')).ids).toEqual([103]);
  }, 15000);

  it('resumes baseline paging and preserves edits, deletion and creation beyond the initial ID bound', async () => {
    config.bookSearch.matchMode = 'contains';
    await db.execute(sql`INSERT INTO public.books(id,library_id,library_folder_id,status,folder_path) VALUES(11,3,31,'present','/baseline11'),(22,5,51,'present','/baseline22');
      INSERT INTO public.book_metadata(book_id,title) VALUES(11,'Initialbaselineword'),(22,'Deletedbaselineword')`);
    const initial = await baseline.initialize(config.bookSearch.index);
    await baseline.prepareEngine((row, identity) => generation.prepare(row.epoch, identity));
    expect(initial).toMatchObject({ phase: 'seeding', upperBookId: 22, estimatedBooks: 2n, seededCount: 0n });
    expect(await baseline.seedNextBatch(1)).toMatchObject({ cursorBookId: 11, seededCount: 1n });
    expect(await sync.runOnce()).toEqual({ phase: 'seeding', acknowledged: 0 });
    await bookRepository.updateMetadataFields(11, { title: 'Changedbaselineword' });
    const edited = (await db.select().from(bookSearchPending)).find((row) => row.bookId === 11)!;
    expect(edited.revision > initial!.seedRevision).toBe(true);
    const restarted = new BookSearchBootstrapRepository(db);
    expect(await restarted.initialize(config.bookSearch.index)).toMatchObject({ cursorBookId: 11, seededCount: 1n });
    expect(await restarted.seedNextBatch(1)).toMatchObject({ phase: 'catchup', cursorBookId: 22, seededCount: 2n });
    expect((await db.select().from(bookSearchPending)).find((row) => row.bookId === 11)!.revision).toBe(edited.revision);
    await scanner.createBook({ id: 33, libraryId: 3, libraryFolderId: 31, folderPath: '/late33' });
    await bookRepository.updateMetadataFields(33, { title: 'Latebaselineword' });
    await bookRepository.deleteByIds([22]);
    expect((await sync.runOnce()).acknowledged).toBe(3);
    expect(await finishBaseline(restarted, verifyBaselineIndex)).toMatchObject({ phase: 'indexed', estimatedBooks: 2n, seededCount: 2n });
    expect((await search('Changedbaselineword')).ids).toEqual([11]);
    expect((await search('Latebaselineword')).ids).toEqual([33]);
    expect((await search('Deletedbaselineword', [5])).total).toBe(0);
    expect(await queue.checkpoint()).toMatchObject({
      snapshotComplete: true,
      writeCoverageComplete: false,
      indexName: config.bookSearch.index,
      schemaRevision: 4,
    });
  }, 15000);

  it('commits baseline cursor and queue together and does not duplicate progress after a failed page', async () => {
    await db.execute(
      sql`INSERT INTO public.books(id,library_id,status,folder_path) VALUES(1,3,'present','/seed1'),(10,3,'present','/seed10'),(100,3,'present','/seed100')`,
    );
    await baseline.initialize(config.bookSearch.index);
    await expect(baseline.seedNextBatch(1001)).rejects.toThrow('page size');
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    try {
      await expect(baseline.seedNextBatch(2)).rejects.toThrow();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    expect(await baseline.progress()).toMatchObject({ cursorBookId: 0, seededCount: 0n, phase: 'seeding' });
    const restarted = new BookSearchBootstrapRepository(db);
    expect(await restarted.seedNextBatch(2)).toMatchObject({ cursorBookId: 10, seededCount: 2n, phase: 'seeding' });
    const verify = vi.fn();
    expect(await restarted.finishIfCaughtUp(verify)).toMatchObject({ phase: 'seeding' });
    expect(verify).not.toHaveBeenCalled();
    expect(await restarted.seedNextBatch(2)).toMatchObject({ cursorBookId: 100, seededCount: 3n, phase: 'catchup' });
    expect(await restarted.seedNextBatch(2)).toMatchObject({ seededCount: 3n, phase: 'catchup' });
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId).sort((a, b) => a - b)).toEqual([1, 10, 100]);
    expect(await restarted.finishIfCaughtUp(verify)).toMatchObject({ phase: 'catchup' });
    expect(verify).not.toHaveBeenCalled();
  });

  it('handles an empty source only after verifying the empty engine and keeps serving coverage false', async () => {
    expect(await baseline.initialize(config.bookSearch.index)).toMatchObject({ phase: 'catchup', upperBookId: 0, estimatedBooks: 0n });
    await baseline.prepareEngine((row, identity) => generation.prepare(row.epoch, identity));
    expect(await finishBaseline(baseline, verifyBaselineIndex)).toMatchObject({ phase: 'indexed' });
    const checkpoint = await queue.checkpoint();
    expect(checkpoint).toMatchObject({ snapshotComplete: true, writeCoverageComplete: false });
    expect(checkpoint!.appliedRevision).toBe(checkpoint!.revision);
  });

  it('preserves pre-existing deletion events when initializing an empty baseline', async () => {
    await db.transaction((tx) => enqueueBookSearchChanges(tx, [999]));
    await baseline.initialize(config.bookSearch.index);
    const verify = vi.fn();
    await baseline.prepareEngine((row, identity) => generation.prepare(row.epoch, identity));
    expect(await baseline.finishIfCaughtUp(verify)).toMatchObject({ phase: 'catchup' });
    expect(verify).not.toHaveBeenCalled();
    expect((await sync.runOnce()).acknowledged).toBe(1);
    expect(await finishBaseline(baseline, verifyBaselineIndex)).toMatchObject({ phase: 'indexed' });
  });

  it('rejects another index or changed schema without acknowledging queued work', async () => {
    await addBook();
    const progress = await baseline.initialize(config.bookSearch.index);
    await expect(baseline.initialize('another_index')).rejects.toThrow('explicit rebuild');
    const original = config.bookSearch.index;
    config.bookSearch.index = 'another_index';
    await expect(sync.runOnce()).rejects.toThrow('binding changed');
    config.bookSearch.index = original;
    expect(await baseline.progress()).toEqual(progress);
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    await db.execute(sql`UPDATE book_search.state SET schema_revision=1 WHERE id=1`);
    await expect(baseline.seedNextBatch()).rejects.toThrow('binding changed');
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    await db.execute(sql`UPDATE book_search.state SET schema_revision=4,epoch=gen_random_uuid() WHERE id=1`);
    await expect(sync.runOnce()).rejects.toThrow('binding changed');
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
  });

  it('shares the worker advisory lock and leaves initialization untouched when another worker owns it', async () => {
    let locked!: () => void;
    let release!: () => void;
    const acquired = new Promise<void>((resolve) => {
      locked = resolve;
    });
    const done = new Promise<void>((resolve) => {
      release = resolve;
    });
    const owner = db.transaction(async (tx) => {
      await tx.execute(sql`SELECT pg_advisory_xact_lock(724073,1)`);
      locked();
      await done;
    });
    await acquired;
    try {
      expect(await baseline.initialize(config.bookSearch.index)).toBeNull();
      expect(await baseline.progress()).toBeNull();
      expect(await queue.checkpoint()).toBeNull();
    } finally {
      release();
      await owner;
    }
    expect(await baseline.initialize(config.bookSearch.index)).toMatchObject({ phase: 'catchup' });
  });

  it('leaves the baseline unfinished when engine verification fails and resumes after recovery', async () => {
    await addBook();
    await baseline.initialize(config.bookSearch.index);
    await baseline.prepareEngine((row, identity) => generation.prepare(row.epoch, identity));
    await baseline.seedNextBatch();
    await sync.runOnce();
    await expect(finishBaseline(baseline, () => Promise.reject(new Error('engine proof failed')))).rejects.toThrow('engine proof failed');
    expect(await baseline.progress()).toMatchObject({ phase: 'catchup' });
    expect(await queue.checkpoint()).toMatchObject({ snapshotComplete: false, writeCoverageComplete: false });
    expect(await finishBaseline(baseline, verifyBaselineIndex)).toMatchObject({ phase: 'indexed' });
  });

  it('coordinates initial seeding, verified publication, later edits and repository reconstruction', async () => {
    config.bookSearch.matchMode = 'contains';
    config.bookSearch.index = `books_created_by_coordinator_${sequence}`;
    await addBook();
    expect(await coordinator.runOnce()).toEqual({ phase: 'disabled' });
    expect(await baseline.progress()).toBeNull();
    config.bookSearch.workerEnabled = true;
    expect(await coordinator.runOnce()).toMatchObject({ phase: 'catchup', seeded: '1', estimate: '1' });
    expect(await coordinator.runOnce()).toEqual({ phase: 'applied', acknowledged: 1 });
    expect(await coordinator.runOnce()).toMatchObject({ phase: 'indexed' });
    const state = (await queue.checkpoint())!;
    expect(state).toMatchObject({ snapshotComplete: true, writeCoverageComplete: false });
    expect(state.engineIdentity).toMatch(/^[a-f0-9]{64}$/);
    await generation.verifyCheckpoint(state);
    await bookRepository.updateMetadataFields(11, { title: 'Coordinatedreplacementword' });
    const restarted = new BookSearchCoordinatorService(config, new BookSearchBootstrapRepository(db), sync, generation);
    expect(await restarted.runOnce()).toEqual({ phase: 'applied', acknowledged: 1 });
    expect(await restarted.runOnce()).toEqual({ phase: 'idle' });
    expect((await search('Coordinatedreplacementword')).ids).toEqual([11]);
    expect((await search('BeforeRevision')).ids).toEqual([]);
    await generation.verifyCheckpoint((await queue.checkpoint())!);
    expect((await baseline.progress())!.seededCount).toBe(1n);
  }, 20_000);

  it('resumes an engine claim after the database binding transaction failed', async () => {
    await baseline.initialize(config.bookSearch.index);
    await expect(
      baseline.prepareEngine(async (row, identity) => {
        await generation.prepare(row.epoch, identity);
        throw new Error('simulated DB binding failure');
      }),
    ).rejects.toThrow('DB binding failure');
    expect((await queue.checkpoint())!.engineIdentity).toBe('');
    await baseline.prepareEngine((row, identity) => generation.prepare(row.epoch, identity));
    const state = (await queue.checkpoint())!;
    expect(state.engineIdentity).toMatch(/^[a-f0-9]{64}$/);
    await generation.verifyCheckpoint(state);
  }, 15_000);

  it('refuses to adopt pre-existing documents and does not erase the original index', async () => {
    await addBook();
    await sync.runOnce();
    config.bookSearch.workerEnabled = true;
    await expect(coordinator.runOnce()).rejects.toThrow();
    expect((await search('BeforeRevision')).ids).toEqual([11]);
    expect((await queue.checkpoint())!.engineIdentity).toBe('');
    expect((await baseline.progress())!.seededCount).toBe(0n);
  }, 15_000);

  it('rejects a replaced physical index with the same name and keeps pending changes intact', async () => {
    await addBook();
    config.bookSearch.workerEnabled = true;
    await coordinator.runOnce();
    const identity = (await queue.checkpoint())!.engineIdentity;
    const response = await fetch(`${endpoint}/api/index/${config.bookSearch.index}`, {
      method: 'DELETE',
      headers: { Authorization: `Basic ${Buffer.from('fixture-user:fixture-password').toString('base64')}` },
      signal: AbortSignal.timeout(5000),
    });
    expect(response.status).toBe(200);
    await expect(coordinator.runOnce()).rejects.toThrow('missing; rebuild required');
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    await engine('/api/index', bookSearchIndexDefinition(config.bookSearch.index));
    await expect(coordinator.runOnce()).rejects.toThrow('identity changed');
    expect((await queue.checkpoint())!.engineIdentity).toBe(identity);
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    expect((await baseline.progress())!.phase).toBe('catchup');
  }, 15_000);

  it('refuses an older engine watermark even when later source edits are queued', async () => {
    await addBook();
    config.bookSearch.workerEnabled = true;
    await coordinator.runOnce();
    await coordinator.runOnce();
    await coordinator.runOnce();
    const state = (await queue.checkpoint())!;
    await generation.publishCheckpoint(state.epoch, 0n, state.engineIdentity);
    await bookRepository.updateMetadataFields(11, { title: 'Afterrestoredengineword' });
    await expect(generation.verifyCheckpoint(state)).rejects.toThrow('watermark changed');
    await expect(coordinator.runOnce()).rejects.toThrow('watermark changed');
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    expect((await queue.checkpoint())!.appliedRevision).toBe(state.appliedRevision);
  }, 20_000);

  it('replays safely when an external checkpoint became visible before a failed queue commit', async () => {
    await addBook();
    config.bookSearch.workerEnabled = true;
    await coordinator.runOnce();
    const publish = generation.publishCheckpoint.bind(generation);
    const spy = vi.spyOn(generation, 'publishCheckpoint').mockImplementationOnce(async (...args) => {
      await publish(...args);
      throw new Error('simulated DB acknowledgement failure');
    });
    try {
      await expect(coordinator.runOnce()).rejects.toThrow('acknowledgement failure');
    } finally {
      spy.mockRestore();
    }
    expect((await queue.checkpoint())!.appliedRevision).toBe(0n);
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    expect(await coordinator.runOnce()).toEqual({ phase: 'applied', acknowledged: 1 });
    expect(await coordinator.runOnce()).toMatchObject({ phase: 'indexed' });
    expect((await search('BeforeRevision')).ids).toEqual([11]);
    await generation.verifyCheckpoint((await queue.checkpoint())!);
  }, 20_000);

  it.each([false, true])('accepts a verified complete source without sorting engine IDs (empty=%s)', async (empty) => {
    if (!empty) await addBook();
    config.bookSearch.workerEnabled = true;
    const read = vi.spyOn(generation, 'readLiveIds').mockRejectedValue(new Error('Unnecessary full index enumeration'));
    try {
      await drainCoordinator(coordinator);
      expect(read).not.toHaveBeenCalled();
      expect(await baseline.progress()).toMatchObject({ phase: 'indexed', pruningComplete: true, orphanCursorBookId: 0 });
      expect(await queue.checkpoint()).toMatchObject({ snapshotComplete: true, writeCoverageComplete: false });
      const state = (await queue.checkpoint())!;
      expect(state.appliedRevision).toBe(state.revision);
      await generation.verifyCheckpoint(state);
      expect((await search('BeforeRevision')).ids).toEqual(empty ? [] : [11]);
    } finally {
      read.mockRestore();
    }
  });

  it('retains residual removal when the verified engine has an extra live source ID', async () => {
    await addBook();
    await addSecondBook();
    config.bookSearch.workerEnabled = true;
    await coordinator.runOnce();
    await coordinator.runOnce();
    await db.execute(sql`DELETE FROM public.books WHERE id=22`);
    const read = vi.spyOn(generation, 'readLiveIds');
    try {
      await drainCoordinator(coordinator);
      expect(read).toHaveBeenCalledTimes(1);
      expect(await baseline.progress()).toMatchObject({ phase: 'indexed', pruningComplete: true });
      expect((await search('Othersourceword', [3, 5])).ids).toEqual([]);
      expect((await search('BeforeRevision')).ids).toEqual([11]);
      const state = (await queue.checkpoint())!;
      await db.transaction((tx) => generation.verifyComplete(tx, state));
    } finally {
      read.mockRestore();
    }
  });

  it('does not use complete source projection while its keyset sweep or queue is unfinished', async () => {
    await addBook();
    await addSecondBook();
    await baseline.initialize(config.bookSearch.index);
    await baseline.prepareEngine((row, identity) => generation.prepare(row.epoch, identity));
    await baseline.seedNextBatch(1);
    const verify = vi.fn();
    expect(await baseline.finishIfCaughtUp(verify, 'complete-source-projection')).toMatchObject({ phase: 'seeding' });
    await baseline.seedNextBatch(1);
    expect(await baseline.finishIfCaughtUp(verify, 'complete-source-projection')).toMatchObject({ phase: 'catchup' });
    expect(verify).not.toHaveBeenCalled();
    expect((await queue.checkpoint())!.snapshotComplete).toBe(false);
  });

  it('does not finalize a baseline with missing visible books or foreign generation tombstones', async () => {
    await addBook();
    config.bookSearch.workerEnabled = true;
    await coordinator.runOnce();
    await coordinator.runOnce();
    const state = (await queue.checkpoint())!;
    const writer = new ZincBookIndexWriter(config);
    await writer.applyAndVerify([{ bookId: 11, revision: state.revision, document: null }], state.epoch);
    await expect(coordinator.runOnce()).rejects.toThrow('visible count');
    expect((await queue.checkpoint())!.snapshotComplete).toBe(false);
    await db.transaction((tx) => enqueueBookSearchChanges(tx, [11]));
    await coordinator.runOnce();
    await writer.applyAndVerify([{ bookId: 999, revision: 1n, document: null }], 'another-generation');
    await expect(coordinator.runOnce()).rejects.toThrow('another generation');
    expect((await queue.checkpoint())!.snapshotComplete).toBe(false);
  }, 20_000);

  it('rejects an incompatible analyzer instead of rewriting the existing mapping', async () => {
    config.bookSearch.index = `books_bad_mapping_${sequence}`;
    const definition = bookSearchIndexDefinition(config.bookSearch.index);
    definition.mappings.properties.native_bigrams.analyzer = 'standard';
    await engine('/api/index', definition);
    config.bookSearch.workerEnabled = true;
    await expect(coordinator.runOnce()).rejects.toThrow('mapping');
    expect((await queue.checkpoint())!.engineIdentity).toBe('');
    expect((await baseline.progress())!.seededCount).toBe(0n);
  });

  it('activates serving only for the current completed session after queued changes and engine verification', async () => {
    await addBook();
    const owner = new BookSearchLifecycleService(config, baseline);
    await owner.onModuleInit();
    const session = (await queue.checkpoint())!.captureSession!;
    const verify = (tx: BookSearchTransaction, state: NonNullable<Awaited<ReturnType<BookSearchSyncRepository['checkpoint']>>>) =>
      generation.verifyComplete(tx, state);
    await expect(baseline.activateServing(config.bookSearch.index, session, verify)).rejects.toThrow('not caught up');
    config.bookSearch.workerEnabled = true;
    await drainCoordinator(coordinator);
    await expect(baseline.activateServing(config.bookSearch.index, '00000000-0000-4000-8000-000000000000', verify)).rejects.toThrow(
      'session changed',
    );
    await bookRepository.updateMetadataFields(11, { title: 'Activationcaptureword' });
    await expect(baseline.activateServing(config.bookSearch.index, session, verify)).rejects.toThrow('not caught up');
    expect((await queue.checkpoint())!.writeCoverageComplete).toBe(false);
    await sync.runOnce();
    expect(await baseline.activateServing(config.bookSearch.index, session, verify)).toBe(true);
    expect((await queue.checkpoint())!.writeCoverageComplete).toBe(true);
    expect(owner.isCurrent((await queue.checkpoint())!.captureSession)).toBe(true);
  });

  it('retains disabled serving after failed activation verification or a replaced application session', async () => {
    await addBook();
    await new BookSearchLifecycleService(config, baseline).onModuleInit();
    const session = (await queue.checkpoint())!.captureSession!;
    config.bookSearch.workerEnabled = true;
    await drainCoordinator(coordinator);
    await expect(
      baseline.activateServing(config.bookSearch.index, session, () => Promise.reject(new Error('Fixture engine unavailable'))),
    ).rejects.toThrow('Fixture engine unavailable');
    expect((await queue.checkpoint())!.writeCoverageComplete).toBe(false);
    await new BookSearchLifecycleService(config, baseline).onModuleInit();
    await expect(baseline.activateServing(config.bookSearch.index, session, (tx, state) => generation.verifyComplete(tx, state))).rejects.toThrow(
      'session changed',
    );
    expect((await queue.checkpoint())!.writeCoverageComplete).toBe(false);
  });

  it('maintains real source edits through its timer loop and stops scheduling on shutdown', async () => {
    await addBook();
    config.bookSearch.workerEnabled = true;
    const background = new BookSearchCoordinatorService(config, baseline, sync, generation);
    const waitFor = async (check: () => Promise<boolean>) => {
      const deadline = Date.now() + 15_000;
      while (Date.now() < deadline) {
        if (await check()) return;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      throw new Error('Scheduled index maintenance did not reach its durable checkpoint');
    };
    background.onApplicationBootstrap();
    try {
      await waitFor(async () => (await baseline.progress())?.phase === 'indexed');
      await bookRepository.updateMetadataFields(11, { title: 'Backgroundtimerword' });
      await waitFor(async () => {
        const state = await queue.checkpoint();
        return Boolean(state && state.revision === state.appliedRevision);
      });
      expect((await search('Backgroundtimerword')).ids).toEqual([11]);
      await generation.verifyCheckpoint((await queue.checkpoint())!);
    } finally {
      await background.onModuleDestroy();
    }
    await bookRepository.updateMetadataFields(11, { title: 'Afterworkershutdownword' });
    await new Promise((resolve) => setTimeout(resolve, 1200));
    expect(await db.select().from(bookSearchPending)).toHaveLength(1);
    expect(await background.runOnce()).toEqual({ phase: 'disabled' });
  }, 35_000);

  it('continues durable maintenance across separate CLI processes and reports status without writes', async () => {
    await addBook();
    const owner = new BookSearchLifecycleService(config, baseline);
    await owner.onModuleInit();
    const invoke = async (command = 'once') => {
      const result = await promisify(execFile)(
        process.execPath,
        [join(process.cwd(), 'node_modules/tsx/dist/cli.mjs'), 'src/scripts/book-search-maintain.ts', command],
        {
          cwd: process.cwd(),
          timeout: 20_000,
          maxBuffer: 64 * 1024,
          env: {
            ...process.env,
            DATABASE_URL: connectionString!,
            BOOK_SEARCH_ENABLED: 'true',
            BOOK_SEARCH_WORKER_ENABLED: 'true',
            BOOK_SEARCH_ENDPOINT: endpoint!,
            BOOK_SEARCH_INDEX: config.bookSearch.index,
            BOOK_SEARCH_USERNAME: 'fixture-user',
            BOOK_SEARCH_PASSWORD: 'fixture-password',
            BOOK_SEARCH_TIMEOUT_MS: '5000',
          },
        },
      );
      return JSON.parse(result.stdout.trim().split('\n').at(-1)!);
    };
    const seed = await invoke();
    expect(seed).toMatchObject({ phase: 'catchup', seeded: '1', estimate: '1' });
    const applied = await invoke();
    expect(applied).toMatchObject({ phase: 'applied', acknowledged: 1 });
    const complete = await invoke();
    expect(complete.phase).toBe('indexed');
    await bookRepository.updateMetadataFields(11, { title: 'Afterprocessrestartword' });
    const edited = await invoke();
    expect(edited).toMatchObject({ phase: 'applied', acknowledged: 1 });
    const status = await invoke('status');
    expect(status.progress.phase).toBe('indexed');
    expect(status.pending.count).toBe('0');
    expect(status.checkpoint.writeCoverageComplete).toBe(false);
    expect(owner.isCurrent(status.checkpoint.captureSession)).toBe(true);
    const processIds = [seed, applied, complete, edited, status].map((row) => row.workerPid);
    expect(new Set(processIds).size).toBe(5);
    expect((await search('Afterprocessrestartword')).ids).toEqual([11]);
    await generation.verifyCheckpoint((await queue.checkpoint())!);
    const requested = await invoke('reconcile');
    expect(requested).toMatchObject({ phase: 'seeding', reconciliationRequested: true });
    expect((await queue.checkpoint())!.captureSession).toBe(status.checkpoint.captureSession);
    expect((await queue.checkpoint())!.snapshotComplete).toBe(false);
    const reseeded = await invoke();
    const reapplied = await invoke();
    const reverified = await invoke();
    expect([reseeded.phase, reapplied.phase, reverified.phase]).toEqual(['catchup', 'applied', 'indexed']);
    expect(owner.isCurrent((await queue.checkpoint())!.captureSession)).toBe(true);
    expect((await db.select().from(schema.bookMetadata).where(eq(schema.bookMetadata.bookId, 11)))[0].title).toBe('Afterprocessrestartword');
    if (process.env.BOOK_SEARCH_TEST_PROCESS_REPORT)
      await writeFile(
        process.env.BOOK_SEARCH_TEST_PROCESS_REPORT,
        JSON.stringify(
          {
            scope:
              'Separate real CLI processes on disposable PG and Zinc; clean exits between phases, not a container restart or forced mid-batch kill',
            processIds,
            phases: [seed.phase, applied.phase, complete.phase, edited.phase],
            pending: status.pending.count,
            snapshotComplete: status.checkpoint.snapshotComplete,
            writeCoverageComplete: status.checkpoint.writeCoverageComplete,
            reconciliationProcessIds: [requested, reseeded, reapplied, reverified].map((row) => row.workerPid),
            explicit_reconcile_preserves_application_session: true,
          },
          null,
          2,
        ),
      );
  }, 90_000);

  it('captures startup series memberships and makes their canonical names searchable without changing legacy metadata', async () => {
    config.bookSearch.matchMode = 'contains';
    await addBook();
    await db.execute(sql`UPDATE public.book_metadata SET series_name='Canonical   Seriesword',series_id=NULL WHERE book_id=11;
      INSERT INTO public.book_series(name,normalized_name) VALUES('Canonical Seriesword','canonical seriesword')`);
    await sync.runOnce();
    expect((await search('canonical seriesword')).ids).toEqual([]);
    const prior = (await queue.checkpoint())!.revision;
    const identity = new SeriesIdentityService(db, capture);
    await identity.onModuleInit();
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId)).toEqual([11]);
    expect((await queue.checkpoint())!.revision).toBe(prior + 1n);
    const [source] = await db.select().from(schema.bookMetadata).where(eq(schema.bookMetadata.bookId, 11));
    expect(source.seriesName).toBe('Canonical   Seriesword');
    expect(source.seriesId).not.toBeNull();
    expect((await search('canonical seriesword')).ids).toEqual([]);
    await sync.runOnce();
    expect((await search('canonical seriesword')).ids).toEqual([11]);
    const builder = new BookQueryBuilder(db, {} as never);
    const native = await db
      .select({ id: schema.books.id })
      .from(schema.books)
      .leftJoin(schema.bookMetadata, eq(schema.bookMetadata.bookId, schema.books.id))
      .where(builder.buildQuickSearch('canonical seriesword'));
    expect(native.map((row) => row.id)).toEqual([11]);
    await identity.onModuleInit();
    expect(await db.select().from(bookSearchPending)).toEqual([]);
    expect((await queue.checkpoint())!.revision).toBe(prior + 1n);
  }, 15_000);

  it('queues a large startup membership insert in the database once and leaves existing memberships untouched', async () => {
    await db.execute(sql`INSERT INTO public.books(id,library_id,library_folder_id,status,folder_path)
      SELECT id,3,31,'present','/startup'||id FROM generate_series(2000,3500) id;
      INSERT INTO public.book_metadata(book_id,title,series_name) SELECT id,'Startup'||id,'Boundedseriesword' FROM public.books`);
    const identity = new SeriesIdentityService(db, capture);
    await identity.onModuleInit();
    const pending = await queue.pendingSummary();
    expect(pending.count).toBe('1501');
    expect((await queue.checkpoint())!.revision).toBe(1n);
    const memberships = await db.execute<{ count: string }>(sql`SELECT count(*)::text AS count FROM public.book_series_memberships`);
    expect(memberships.rows[0].count).toBe('1501');
    await identity.onModuleInit();
    expect((await queue.pendingSummary()).count).toBe('1501');
    expect((await queue.checkpoint())!.revision).toBe(1n);
  });

  it('rolls back startup series identities and memberships together when enqueue fails, then retries cleanly', async () => {
    await db.execute(sql`INSERT INTO public.books(id,library_id,library_folder_id,status,folder_path) VALUES(11,3,31,'present','/rollbackstartup');
      INSERT INTO public.book_metadata(book_id,title,series_name) VALUES(11,'Startuprollbackword','Newstartupseriesword')`);
    await db.execute(sql`ALTER TABLE book_search.pending RENAME TO held_pending`);
    const identity = new SeriesIdentityService(db, capture);
    try {
      await identity.onModuleInit();
      expect(await db.select().from(schema.bookSeriesMemberships)).toEqual([]);
      expect(await db.select().from(schema.bookSeries)).toEqual([]);
      expect((await db.select().from(schema.bookMetadata))[0].seriesId).toBeNull();
      expect(await queue.checkpoint()).toBeNull();
    } finally {
      await db.execute(sql`ALTER TABLE book_search.held_pending RENAME TO pending`);
    }
    await identity.onModuleInit();
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId)).toEqual([11]);
    expect(await db.select().from(schema.bookSeriesMemberships)).toHaveLength(1);
  });

  it('keeps native startup behavior without search tables when capture is disabled', async () => {
    config.bookSearch.enabled = false;
    await db.execute(sql`INSERT INTO public.books(id,library_id,library_folder_id,status,folder_path) VALUES(11,3,31,'present','/disabledstartup');
      INSERT INTO public.book_metadata(book_id,title,series_name) VALUES(11,'Disabledstartupword','Disabledseriesword')`);
    await db.execute(sql`ALTER SCHEMA book_search RENAME TO held_book_search`);
    try {
      await new SeriesIdentityService(db, capture).onModuleInit();
      expect(await db.select().from(schema.bookSeriesMemberships)).toHaveLength(1);
    } finally {
      await db.execute(sql`ALTER SCHEMA held_book_search RENAME TO book_search`);
    }
    expect(await queue.checkpoint()).toBeNull();
  });

  it('verifies real content fingerprints without rewriting unchanged books at a newer source revision', async () => {
    await addBook();
    await sync.runOnce();
    const state = (await queue.checkpoint())!;
    const projected = await db.transaction((tx) =>
      new BookSearchDocumentProjector().project(tx, [{ bookId: 11, revision: state.revision + 1n, queuedAt: new Date() }]),
    );
    const writer = new ZincBookIndexWriter(config);
    expect(await writer.applyAndVerify(projected, state.epoch)).toEqual({ written: 0, unchanged: 1 });
    const prior = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 11 } }, size: 1 });
    expect(prior.hits.hits[0]._source.sync_revision).toBe(state.revision.toString());
    await bookRepository.updateMetadataFields(11, { publisher: null });
    await sync.runOnce();
    const after = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 11 } }, size: 1 });
    expect(after.hits.hits[0]._source.sync_hash).not.toBe(prior.hits.hits[0]._source.sync_hash);
    expect((await search('Publisherword')).ids).toEqual([]);
  }, 15_000);

  it('reconciles untracked edits, additions and deletions after disabled or older application writes', async () => {
    config.bookSearch.workerEnabled = true;
    config.bookSearch.matchMode = 'contains';
    await db.execute(sql`INSERT INTO public.books(id,library_id,library_folder_id,status,folder_path)
      VALUES(11,3,31,'present','/activation11'),(22,3,31,'present','/activation22'),(33,3,31,'present','/activation33');
      INSERT INTO public.book_metadata(book_id,title,description) VALUES(11,'Oldactivationword','Preservedmanualnote'),(22,'Deletedactivationword',NULL),(33,'Unchangedactivationword',NULL)`);
    const first = new BookSearchLifecycleService(config, baseline);
    await first.onModuleInit();
    await drainCoordinator(new BookSearchCoordinatorService(config, baseline, sync, generation, first));
    const initial = (await queue.checkpoint())!;
    const oldUnchanged = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 33 } }, size: 1 });
    expect(first.isCurrent(initial.captureSession)).toBe(true);
    config.bookSearch.enabled = false;
    await new BookSearchLifecycleService(config, baseline).onModuleInit();
    expect((await queue.checkpoint())!.snapshotComplete).toBe(false);
    await db.execute(sql`UPDATE public.book_metadata SET title='Newactivationword' WHERE book_id=11;
      DELETE FROM public.books WHERE id=22;
      INSERT INTO public.books(id,library_id,library_folder_id,status,folder_path) VALUES(44,3,31,'present','/activation44');
      INSERT INTO public.book_metadata(book_id,title) VALUES(44,'Addedactivationword')`);
    expect((await queue.pendingSummary()).count).toBe('0');
    config.bookSearch.enabled = true;
    const restarted = new BookSearchLifecycleService(config, new BookSearchBootstrapRepository(db));
    expect(restarted.isCurrent(initial.captureSession)).toBe(false);
    await restarted.onModuleInit();
    const activating = (await queue.checkpoint())!;
    expect(activating.captureSession).not.toBe(initial.captureSession);
    expect(first.isCurrent(activating.captureSession)).toBe(false);
    expect(activating.snapshotComplete).toBe(false);
    const resumed = new BookSearchCoordinatorService(config, baseline, sync, generation, restarted);
    await drainCoordinator(resumed);
    expect((await search('Newactivationword')).ids).toEqual([11]);
    expect((await search('Oldactivationword')).ids).toEqual([]);
    expect((await search('Deletedactivationword')).ids).toEqual([]);
    expect((await search('Addedactivationword')).ids).toEqual([44]);
    const current = (await queue.checkpoint())!;
    expect(current).toMatchObject({ snapshotComplete: true, writeCoverageComplete: false });
    await generation.verifyCheckpoint(current);
    const stillUnchanged = await engine(`/es/${config.bookSearch.index}/_search`, { query: { term: { book_id: 33 } }, size: 1 });
    expect(stillUnchanged.hits.hits[0]._source.sync_revision).toBe(oldUnchanged.hits.hits[0]._source.sync_revision);
    expect((await db.select().from(schema.bookMetadata).where(eq(schema.bookMetadata.bookId, 11)))[0].description).toBe('Preservedmanualnote');
    const completed = await baseline.progress();
    await restarted.ensureStarted();
    expect(await baseline.progress()).toEqual(completed);
  }, 30_000);

  async function verifiedRestartFixture() {
    config.bookSearch.workerEnabled = true;
    await addBook();
    const owner = new BookSearchLifecycleService(config, baseline, generation);
    await owner.onModuleInit();
    await drainCoordinator(new BookSearchCoordinatorService(config, baseline, sync, generation, owner));
    const initial = (await queue.checkpoint())!;
    await baseline.activateServing(config.bookSearch.index, initial.captureSession!, (tx, state) => generation.verifyComplete(tx, state));
    const checkpoint = (await queue.checkpoint())!;
    const progress = await baseline.progress();
    const issued = Date.now();
    const proof = {
      format: 'book-search-restart-proof-v1',
      previousOwnerStopped: true,
      sourceCaptureVerified: true,
      issuedAt: new Date(issued).toISOString(),
      expiresAt: new Date(issued + 120_000).toISOString(),
      sourceBooks: 1,
      sourceUpperBookId: 11,
      checkpoint: {
        epoch: checkpoint.epoch,
        revision: checkpoint.revision.toString(),
        appliedRevision: checkpoint.appliedRevision.toString(),
        indexName: checkpoint.indexName,
        schemaRevision: checkpoint.schemaRevision,
        engineIdentity: checkpoint.engineIdentity,
        captureSession: checkpoint.captureSession,
      },
    };
    const path = join(fixtureDirectory, 'restart-proof.json');
    await writeFile(path, JSON.stringify(proof), { mode: 0o600 });
    config.bookSearch.restartProofPath = path;
    return { owner, checkpoint, progress, proof, path };
  }

  it('consumes a verified restart proof once without reseeding and captures edits in the new owner session', async () => {
    const before = await verifiedRestartFixture();
    const source = await db.select().from(schema.bookMetadata);
    const restarted = new BookSearchLifecycleService(config, baseline, generation);
    const seed = vi.spyOn(baseline, 'seedNextBatch');
    try {
      expect(await restarted.ensureStarted()).toBe(true);
      const after = (await queue.checkpoint())!;
      expect(after.captureSession).not.toBe(before.checkpoint.captureSession);
      expect({ ...after, captureSession: before.checkpoint.captureSession }).toEqual(before.checkpoint);
      expect(await baseline.progress()).toEqual(before.progress);
      expect(await db.select().from(schema.bookMetadata)).toEqual(source);
      expect(before.owner.isCurrent(after.captureSession)).toBe(false);
      expect(restarted.isCurrent(after.captureSession)).toBe(true);
      expect(await new BookSearchCoordinatorService(config, baseline, sync, generation, restarted).runOnce()).toEqual({ phase: 'idle' });
      expect(seed).not.toHaveBeenCalled();
      await expect(new BookSearchLifecycleService(config, baseline, generation).ensureStarted()).rejects.toThrow('checkpoint changed');
      expect(await queue.checkpoint()).toEqual(after);
      await bookRepository.updateMetadataFields(11, { title: 'Verifiedrestartedit' });
      expect(await sync.runOnce()).toEqual({ phase: 'applied', acknowledged: 1 });
      expect((await search('Verifiedrestartedit')).ids).toEqual([11]);
      expect(await queue.checkpoint()).toMatchObject({ captureSession: after.captureSession, snapshotComplete: true, writeCoverageComplete: true });
    } finally {
      seed.mockRestore();
    }
  }, 20_000);

  it('rejects a verified restart proof after a captured source edit and preserves its pending work', async () => {
    const before = await verifiedRestartFixture();
    await bookRepository.updateMetadataFields(11, { title: 'Pendingrestartedit' });
    const checkpoint = await queue.checkpoint();
    const pending = await queue.pendingSummary();
    const restarted = new BookSearchLifecycleService(config, baseline, generation);
    await expect(restarted.ensureStarted()).rejects.toThrow('checkpoint changed');
    expect(await queue.checkpoint()).toEqual(checkpoint);
    expect(await queue.pendingSummary()).toEqual(pending);
    expect(await baseline.progress()).toEqual(before.progress);
    expect(restarted.isCurrent(checkpoint!.captureSession)).toBe(false);
  }, 15_000);

  it.each(['epoch', 'captureSession', 'engineIdentity', 'indexName', 'schemaRevision', 'revision'] as const)(
    'rejects a verified restart proof with a different %s binding',
    async (field) => {
      const before = await verifiedRestartFixture();
      const checkpoint = { ...before.proof.checkpoint };
      if (field === 'epoch' || field === 'captureSession') checkpoint[field] = '33333333-3333-4333-8333-333333333333';
      else if (field === 'engineIdentity') checkpoint[field] = 'b'.repeat(64);
      else if (field === 'indexName') checkpoint[field] = 'other_restart_index';
      else if (field === 'schemaRevision') checkpoint[field] += 1;
      else checkpoint.revision = checkpoint.appliedRevision = (BigInt(checkpoint.revision) + 1n).toString();
      await writeFile(before.path, JSON.stringify({ ...before.proof, checkpoint }));
      await expect(new BookSearchLifecycleService(config, baseline, generation).ensureStarted()).rejects.toThrow();
      expect(await queue.checkpoint()).toEqual(before.checkpoint);
      expect(await baseline.progress()).toEqual(before.progress);
    },
    15_000,
  );

  it.each(['sourceBooks', 'sourceUpperBookId'] as const)(
    'rejects a verified restart proof with a different %s census',
    async (field) => {
      const before = await verifiedRestartFixture();
      await writeFile(before.path, JSON.stringify({ ...before.proof, [field]: before.proof[field] + 1 }));
      await expect(new BookSearchLifecycleService(config, baseline, generation).ensureStarted()).rejects.toThrow('source census changed');
      expect(await queue.checkpoint()).toEqual(before.checkpoint);
    },
    15_000,
  );

  it('rolls back a verified restart handoff when engine verification fails', async () => {
    const before = await verifiedRestartFixture();
    const verify = vi.spyOn(generation, 'verifyComplete').mockRejectedValueOnce(new Error('Fixture engine unavailable'));
    try {
      await expect(new BookSearchLifecycleService(config, baseline, generation).ensureStarted()).rejects.toThrow('Fixture engine unavailable');
      expect(await queue.checkpoint()).toEqual(before.checkpoint);
      expect(await baseline.progress()).toEqual(before.progress);
    } finally {
      verify.mockRestore();
    }
  }, 15_000);

  it('rechecks expiry inside the verified restart transaction after engine verification', async () => {
    const before = await verifiedRestartFixture();
    const proof = await readBookSearchRestartProof(before.path);
    const realClock = Date.now.bind(Date);
    const clock = vi.spyOn(Date, 'now').mockImplementation(realClock);
    try {
      await expect(
        baseline.resumeVerifiedCheckpoint('44444444-4444-4444-8444-444444444444', proof, async (tx, state) => {
          await generation.verifyComplete(tx, state);
          clock.mockReturnValue(Date.parse(proof.expiresAt));
        }),
      ).rejects.toThrow('proof expired');
      expect(await queue.checkpoint()).toEqual(before.checkpoint);
    } finally {
      clock.mockRestore();
    }
  }, 15_000);

  it('refuses a verified restart when source coverage has been invalidated', async () => {
    const before = await verifiedRestartFixture();
    await db.execute(sql`UPDATE book_search.state SET write_coverage_complete=false WHERE id=1`);
    const checkpoint = await queue.checkpoint();
    await expect(new BookSearchLifecycleService(config, baseline, generation).ensureStarted()).rejects.toThrow('checkpoint changed');
    expect(await queue.checkpoint()).toEqual(checkpoint);
    expect(await baseline.progress()).toEqual(before.progress);
  }, 15_000);

  it('waits for the worker lock before consuming a verified restart and permits only one new owner', async () => {
    const before = await verifiedRestartFixture();
    const lock = await pool.connect();
    const first = new BookSearchLifecycleService(config, baseline, generation);
    const second = new BookSearchLifecycleService(config, baseline, generation);
    try {
      await lock.query('BEGIN');
      await lock.query('SELECT pg_advisory_xact_lock(724073,1)');
      expect(await first.ensureStarted()).toBe(false);
      expect(await queue.checkpoint()).toEqual(before.checkpoint);
      await lock.query('COMMIT');
      const results = await Promise.allSettled([first.ensureStarted(), second.ensureStarted()]);
      expect(results.filter((result) => result.status === 'fulfilled' && result.value === true)).toHaveLength(1);
      const current = (await queue.checkpoint())!;
      expect([first.isCurrent(current.captureSession), second.isCurrent(current.captureSession)].filter(Boolean)).toHaveLength(1);
      expect(current.revision).toBe(before.checkpoint.revision);
      expect(await baseline.progress()).toEqual(before.progress);
    } finally {
      await lock.query('ROLLBACK');
      lock.release();
    }
  }, 15_000);

  it('commits orphan scan pages with queued removals and resumes a failed page without skipping IDs', async () => {
    await db.execute(sql`INSERT INTO public.books(id,library_id,library_folder_id,status,folder_path)
      SELECT id,3,31,'present','/prune'||id FROM generate_series(1100,1204) id;
      INSERT INTO public.book_metadata(book_id,title) SELECT id,'Pruneword'||id FROM public.books`);
    await baseline.initialize(config.bookSearch.index);
    await baseline.prepareEngine((row, identity) => generation.prepare(row.epoch, identity));
    await baseline.seedNextBatch();
    await sync.runOnce();
    await sync.runOnce();
    const state = (await queue.checkpoint())!;
    await db.execute(sql`DELETE FROM public.books WHERE id IN (1100,1101,1102)`);
    await db.execute(sql`ALTER TABLE book_search.state ADD CONSTRAINT fixture_revision_cap CHECK(revision <= ${sql.raw(state.revision.toString())})`);
    const read = vi.fn((cursor: number, current: typeof state) => generation.readLiveIds(cursor, current));
    try {
      await expect(baseline.pruneNextBatch(read)).rejects.toThrow();
      expect(read).toHaveBeenCalledTimes(1);
      expect(await baseline.progress()).toMatchObject({ orphanCursorBookId: 0, pruningComplete: false });
      expect((await queue.pendingSummary()).count).toBe('0');
    } finally {
      await db.execute(sql`ALTER TABLE book_search.state DROP CONSTRAINT fixture_revision_cap`);
    }
    expect(await baseline.pruneNextBatch(read)).toMatchObject({ orphanCursorBookId: 1199, pruningComplete: false });
    expect((await db.select().from(bookSearchPending)).map((row) => row.bookId).sort()).toEqual([1100, 1101, 1102]);
    await sync.runOnce();
    expect(await baseline.pruneNextBatch(read)).toMatchObject({ orphanCursorBookId: 1204, pruningComplete: true });
    expect(await baseline.finishIfCaughtUp((tx, _row, current) => generation.verifyComplete(tx, current))).toMatchObject({ phase: 'indexed' });
    expect((await search('Pruneword1100')).ids).toEqual([]);
  }, 30_000);

  it('starts the disabled lifecycle without requiring an installed search schema', async () => {
    config.bookSearch.enabled = false;
    await db.execute(sql`ALTER SCHEMA book_search RENAME TO held_book_search`);
    const lifecycle = new BookSearchLifecycleService(config, baseline);
    try {
      await expect(lifecycle.onModuleInit()).resolves.toBeUndefined();
      expect(lifecycle.isCurrent(null)).toBe(false);
    } finally {
      await db.execute(sql`ALTER SCHEMA held_book_search RENAME TO book_search`);
    }
  });
});
