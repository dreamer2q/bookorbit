import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import type { BookQuery, ContentFilterRules } from '@bookorbit/types';
import { appConfig } from '../../../config/config';
import { BookSearchProvider, type BookIndexPage, type BookIndexRequest } from './book-search-provider';
import { BookSearchSyncRepository, type BookSearchCheckpoint } from './book-search-sync.repository';
import { BOOK_SEARCH_SCHEMA_REVISION } from './book-search-index-definition';
import { ZincBookIndexGeneration } from './zinc-book-index-generation';
import { BookSearchLifecycleService } from './book-search-lifecycle.service';

interface QueryContext<T> {
  query: BookQuery;
  libraryIds: number[];
  contentFilters?: ContentFilterRules;
  loadCards: (ids: number[]) => Promise<T[]>;
}

type QueryPage<T> = { items: T[]; total: number; page: number; size: number };
type QueryAttempt<T> = QueryPage<T> | { retry: BookSearchCheckpoint } | null;
const TITLE_SCAN_EXPIRED = Symbol('title scan expired');

@Injectable()
export class BookIndexSearchService {
  private readonly logger = new Logger(BookIndexSearchService.name);

  constructor(
    @Inject(appConfig.KEY) private readonly config: ConfigType<typeof appConfig>,
    private readonly provider: BookSearchProvider,
    @Optional() private readonly sync?: BookSearchSyncRepository,
    @Optional() private readonly generation?: ZincBookIndexGeneration,
    @Optional() private readonly lifecycle?: BookSearchLifecycleService,
  ) {}

  async tryQuery<T extends { id: number }>(context: QueryContext<T>): Promise<QueryPage<T> | null> {
    const first = await this.tryCurrentQuery(context);
    if (!first || !('retry' in first)) return first;
    try {
      if (!(await this.waitForCaughtUp(first.retry))) return null;
      const second = await this.tryCurrentQuery(context, first.retry);
      return second && !('retry' in second) ? second : null;
    } catch {
      this.logger.warn('[book.index_search] [fail] errorClass=ProviderFailure - indexed query unavailable; using native query');
      return null;
    }
  }

  private sameGeneration(first: BookSearchCheckpoint, second: BookSearchCheckpoint): boolean {
    return (
      first.epoch === second.epoch &&
      first.indexName === second.indexName &&
      first.schemaRevision === second.schemaRevision &&
      first.engineIdentity === second.engineIdentity &&
      first.captureSession === second.captureSession
    );
  }

  private async waitForCaughtUp(expected: BookSearchCheckpoint): Promise<boolean> {
    const deadline = Date.now() + 3000;
    while (true) {
      const current = await this.sync?.checkpoint();
      if (
        !current?.snapshotComplete ||
        !current.writeCoverageComplete ||
        !this.sameGeneration(expected, current) ||
        current.revision < expected.revision ||
        !this.lifecycle?.isCurrent(current.captureSession)
      )
        return false;
      if (current.revision === current.appliedRevision) return true;
      if (Date.now() >= deadline) return false;
      await new Promise((resolve) => setTimeout(resolve, Math.min(100, deadline - Date.now())));
    }
  }

  private async tryCurrentQuery<T extends { id: number }>(context: QueryContext<T>, expected?: BookSearchCheckpoint): Promise<QueryAttempt<T>> {
    const { query, libraryIds, contentFilters } = context;
    const settings = this.config.bookSearch;
    if (!settings.enabled || !settings.indexReady) return null;
    const { page, size } = query.pagination;
    if (!query.q?.trim() || query.q.length > 2000 || query.collapseSeries || query.filter?.rules.length || query.randomSeed !== undefined)
      return null;
    if (query.sort.length !== 1) return null;
    const sort = query.sort[0];
    const relevance = sort.field === 'relevance' && sort.dir === 'desc';
    if (sort.field !== 'title' && !relevance) return null;
    if (
      !Number.isSafeInteger(page) ||
      page < 0 ||
      !Number.isSafeInteger(size) ||
      size < 1 ||
      size > 100 ||
      (page + 1) * size > settings.maxResultWindow
    )
      return null;
    if (libraryIds.length > 500 || libraryIds.some((id) => !Number.isSafeInteger(id) || id <= 0)) return null;
    if (
      contentFilters &&
      (contentFilters.includeTagIds.length ||
        contentFilters.excludeTagIds.length ||
        contentFilters.includeGenreIds.length ||
        contentFilters.excludeGenreIds.length ||
        contentFilters.exemptRequestsFromUserId !== undefined)
    )
      return null;
    if (libraryIds.length === 0) return { items: [], total: 0, page, size };
    let queryCheckpoint: BookSearchCheckpoint | undefined;
    try {
      const nativeTitle = !relevance && !(await this.sync?.supportsBinaryTitleSort());
      const before = await this.sync?.checkpoint();
      if (!before?.snapshotComplete || !before.writeCoverageComplete) return null;
      if (expected && (!this.sameGeneration(expected, before) || before.revision < expected.revision)) return null;
      if (!this.lifecycle?.isCurrent(before.captureSession)) return null;
      if (before.indexName !== settings.index || before.schemaRevision !== BOOK_SEARCH_SCHEMA_REVISION) return null;
      if (!this.generation || !before.engineIdentity) return null;
      if (before.revision !== before.appliedRevision) return { retry: before };
      queryCheckpoint = before;
      await this.generation.verifyCheckpoint(before);
      const text = await this.sync!.normalizeQuery(query.q);
      if (!text) return null;
      const request: BookIndexRequest = {
        text,
        libraryIds: [...new Set(libraryIds)],
        offset: page * size,
        size,
        titleDirection: relevance ? 'asc' : sort.dir,
        ...(relevance ? { relevance: true } : {}),
        epoch: before.epoch,
      };
      const result = nativeTitle ? await this.nativeTitleSearch(request) : await this.provider.search(request);
      if (result === TITLE_SCAN_EXPIRED) return { retry: before };
      if (!result) return null;
      if (
        !Number.isSafeInteger(result.total) ||
        result.total < 0 ||
        result.ids.length > size ||
        new Set(result.ids).size !== result.ids.length ||
        result.ids.some((id) => !Number.isSafeInteger(id) || id <= 0)
      )
        return null;
      const cards = result.ids.length ? await context.loadCards(result.ids) : [];
      const byId = new Map(cards.map((card) => [card.id, card]));
      if (cards.length !== result.ids.length || byId.size !== result.ids.length || result.ids.some((id) => !byId.has(id))) return null;
      await this.generation.verifyCheckpoint(before);
      const after = await this.sync!.checkpoint();
      if (
        !after?.snapshotComplete ||
        !after.writeCoverageComplete ||
        after.epoch !== before.epoch ||
        after.indexName !== before.indexName ||
        after.schemaRevision !== before.schemaRevision ||
        after.engineIdentity !== before.engineIdentity ||
        after.captureSession !== before.captureSession
      )
        return null;
      if (after.revision < before.revision || after.appliedRevision < before.appliedRevision) return null;
      if (after.revision !== before.revision || after.appliedRevision !== before.appliedRevision) return { retry: after };
      return { items: result.ids.map((id) => byId.get(id)!), total: result.total, page, size };
    } catch {
      const current = await this.sync?.checkpoint().catch(() => null);
      if (
        queryCheckpoint &&
        current?.snapshotComplete &&
        current.writeCoverageComplete &&
        this.sameGeneration(queryCheckpoint, current) &&
        current.revision > queryCheckpoint.revision
      )
        return { retry: current };
      this.logger.warn('[book.index_search] [fail] errorClass=ProviderFailure - indexed query unavailable; using native query');
      return null;
    }
  }

  private async nativeTitleSearch(request: BookIndexRequest): Promise<BookIndexPage | typeof TITLE_SCAN_EXPIRED | null> {
    if (!this.sync) return null;
    const window = Math.min(1000, this.config.bookSearch.maxResultWindow);
    const first = await this.provider.search({ ...request, offset: 0, size: window, unordered: true });
    if (first.total <= window) {
      if (first.ids.length !== first.total) return null;
      return {
        ids: await this.sync.sortNativeTitleIds(first.ids, request.libraryIds, request.titleDirection, request.offset, request.size),
        total: first.total,
      };
    }
    if (!(await this.sync.hasNativeTitleScanIndex(request.titleDirection))) return null;
    const needed = Math.min(first.total, request.offset + request.size);
    const orderedMatches: number[] = [];
    const batchSize = Math.min(500, window);
    let cursor: { title: string; id: number } | undefined;
    const deadline = Date.now() + 1000;
    let scanned = 0;
    for (; scanned < 5000 && Date.now() < deadline; scanned += batchSize) {
      const candidates = await this.sync.nativeTitleCandidates(request.libraryIds, request.titleDirection, batchSize, cursor);
      if (!candidates.length) return null;
      const matched = await this.provider.search({
        ...request,
        offset: 0,
        size: candidates.length,
        unordered: true,
        candidateIds: candidates.map((row) => row.id),
      });
      if (matched.ids.length !== matched.total) return null;
      const ids = new Set(matched.ids);
      orderedMatches.push(...candidates.filter((row) => ids.has(row.id)).map((row) => row.id));
      if (orderedMatches.length >= needed) return { ids: orderedMatches.slice(request.offset, request.offset + request.size), total: first.total };
      cursor = candidates[candidates.length - 1];
    }
    return scanned < 5000 ? TITLE_SCAN_EXPIRED : null;
  }
}
