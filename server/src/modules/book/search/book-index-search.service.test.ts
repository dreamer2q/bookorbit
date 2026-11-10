import { Test } from '@nestjs/testing';
import { Logger } from '@nestjs/common';
import type { BookQuery } from '@bookorbit/types';
import { EMPTY_CONTENT_FILTER_RULES } from '@bookorbit/types';
import { appConfig } from '../../../config/config';
import { BookIndexSearchService } from './book-index-search.service';
import { BookSearchProvider } from './book-search-provider';
import { BookSearchSyncRepository } from './book-search-sync.repository';
import { ZincBookIndexGeneration } from './zinc-book-index-generation';
import { BookSearchLifecycleService } from './book-search-lifecycle.service';

describe('BookIndexSearchService', () => {
  let service: BookIndexSearchService;
  let config: ReturnType<typeof appConfig>;
  const provider = { search: vi.fn() };
  const query: BookQuery = { q: '英语', sort: [{ field: 'title', dir: 'asc' }], pagination: { page: 2, size: 2 } };
  const loadCards = vi.fn();
  const checkpoint = {
    id: 1,
    epoch: 'verified-generation',
    revision: 8n,
    appliedRevision: 8n,
    snapshotComplete: true,
    writeCoverageComplete: true,
    indexName: 'books_metadata_gse_v1',
    schemaRevision: 4,
    engineIdentity: 'a'.repeat(64),
    captureSession: 'current-session',
  };
  const sync = {
    checkpoint: vi.fn(),
    normalizeQuery: vi.fn((value: string) => Promise.resolve(value.trim().toLowerCase())),
    supportsBinaryTitleSort: vi.fn(),
    sortNativeTitleIds: vi.fn(),
    nativeTitleCandidates: vi.fn(),
    hasNativeTitleScanIndex: vi.fn(),
  };
  const generation = { verifyCheckpoint: vi.fn() };
  const lifecycle = { isCurrent: vi.fn() };

  beforeEach(async () => {
    vi.clearAllMocks();
    config = appConfig();
    config.bookSearch.enabled = true;
    config.bookSearch.indexReady = true;
    sync.checkpoint.mockReset().mockResolvedValue(checkpoint);
    sync.supportsBinaryTitleSort.mockReset().mockResolvedValue(true);
    sync.sortNativeTitleIds.mockReset();
    sync.nativeTitleCandidates.mockReset();
    sync.hasNativeTitleScanIndex.mockReset().mockResolvedValue(true);
    generation.verifyCheckpoint.mockReset().mockResolvedValue(undefined);
    lifecycle.isCurrent.mockReset().mockReturnValue(true);
    sync.normalizeQuery.mockReset().mockImplementation((value: string) => Promise.resolve(value.trim().toLowerCase()));
    const module = await Test.createTestingModule({
      providers: [
        BookIndexSearchService,
        { provide: appConfig.KEY, useValue: config },
        { provide: BookSearchProvider, useValue: provider },
        { provide: BookSearchSyncRepository, useValue: sync },
        { provide: ZincBookIndexGeneration, useValue: generation },
        { provide: BookSearchLifecycleService, useValue: lifecycle },
      ],
    }).compile();
    service = module.get(BookIndexSearchService);
    provider.search.mockResolvedValue({ ids: [6, 5], total: 10 });
    loadCards.mockResolvedValue([{ id: 5 }, { id: 6 }]);
  });

  it('uses the external page once and preserves its order and complete total', async () => {
    const result = await service.tryQuery({ query, libraryIds: [3, 3], loadCards });
    expect(provider.search).toHaveBeenCalledWith({
      text: '英语',
      libraryIds: [3],
      offset: 4,
      size: 2,
      titleDirection: 'asc',
      epoch: checkpoint.epoch,
    });
    expect(loadCards).toHaveBeenCalledWith([6, 5]);
    expect(result).toEqual({ items: [{ id: 6 }, { id: 5 }], total: 10, page: 2, size: 2 });
  });

  it('falls back when the engine generation or visible watermark cannot be proven', async () => {
    generation.verifyCheckpoint.mockRejectedValueOnce(new Error('old restored index'));
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(provider.search).not.toHaveBeenCalled();
    generation.verifyCheckpoint.mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error('engine replaced during query'));
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
  });

  it('orders a complete small linguistic result set in PostgreSQL before hydrating only the requested page', async () => {
    sync.supportsBinaryTitleSort.mockResolvedValue(false);
    provider.search.mockResolvedValueOnce({ ids: [10, 9, 8, 7, 6, 5, 4, 3, 2, 1], total: 10 });
    sync.sortNativeTitleIds.mockResolvedValueOnce([5, 6]);
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toEqual({ items: [{ id: 5 }, { id: 6 }], total: 10, page: 2, size: 2 });
    expect(sync.sortNativeTitleIds).toHaveBeenCalledWith([10, 9, 8, 7, 6, 5, 4, 3, 2, 1], [3], 'asc', 4, 2);
    expect(loadCards).toHaveBeenCalledWith([5, 6]);
    expect(sync.nativeTitleCandidates).not.toHaveBeenCalled();
    const relevance: BookQuery = { ...query, sort: [{ field: 'relevance', dir: 'desc' }] };
    expect(await service.tryQuery({ query: relevance, libraryIds: [3], loadCards })).not.toBeNull();
    expect(provider.search).toHaveBeenCalledWith(expect.objectContaining({ relevance: true }));
  });

  it('intersects bounded native title pages and preserves pagination across a candidate boundary', async () => {
    sync.supportsBinaryTitleSort.mockResolvedValue(false);
    provider.search
      .mockResolvedValueOnce({ ids: Array.from({ length: 1000 }, (_, i) => i + 1), total: 2000 })
      .mockResolvedValueOnce({ ids: [499, 2], total: 2 })
      .mockResolvedValueOnce({ ids: [999, 888, 777, 501], total: 4 });
    const candidates = (start: number) => Array.from({ length: 500 }, (_, i) => ({ id: start + i, title: `title ${start + i}` }));
    sync.nativeTitleCandidates.mockResolvedValueOnce(candidates(1)).mockResolvedValueOnce(candidates(501));
    loadCards.mockResolvedValueOnce([{ id: 999 }, { id: 888 }]);
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toEqual({
      items: [{ id: 888 }, { id: 999 }],
      total: 2000,
      page: 2,
      size: 2,
    });
    expect(sync.nativeTitleCandidates).toHaveBeenNthCalledWith(2, [3], 'asc', 500, { id: 500, title: 'title 500' });
    expect(provider.search.mock.calls[0][0]).toMatchObject({ unordered: true, offset: 0 });
    expect(provider.search.mock.calls[1][0]).toMatchObject({ unordered: true, offset: 0 });
    expect(provider.search.mock.calls[1][0].candidateIds).toHaveLength(500);
    expect(sync.sortNativeTitleIds).not.toHaveBeenCalled();
  });

  it('does not start a source title scan without an equivalent native index', async () => {
    sync.supportsBinaryTitleSort.mockResolvedValue(false);
    sync.hasNativeTitleScanIndex.mockResolvedValue(false);
    provider.search.mockResolvedValueOnce({ ids: Array.from({ length: 1000 }, (_, i) => i + 1), total: 2000 });
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(sync.nativeTitleCandidates).not.toHaveBeenCalled();
    expect(loadCards).not.toHaveBeenCalled();
  });

  it('falls back instead of returning a truncated result when its bounded native prefix has too few matches', async () => {
    sync.supportsBinaryTitleSort.mockResolvedValue(false);
    provider.search.mockResolvedValue({ ids: [], total: 0 }).mockResolvedValueOnce({ ids: [], total: 2000 });
    sync.nativeTitleCandidates.mockImplementation((_libraries, _direction, size, cursor) =>
      Promise.resolve(Array.from({ length: size }, (_, i) => ({ id: (cursor?.id ?? 0) + i + 1, title: 'same title' }))),
    );
    const clock = vi.spyOn(Date, 'now').mockReturnValue(1000);
    try {
      expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
      expect(sync.nativeTitleCandidates).toHaveBeenCalledTimes(10);
      expect(loadCards).not.toHaveBeenCalled();
    } finally {
      clock.mockRestore();
    }
  });

  it('keeps the source revision fence around native linguistic sorting', async () => {
    sync.supportsBinaryTitleSort.mockResolvedValue(false);
    provider.search.mockResolvedValueOnce({ ids: [6, 5], total: 2 });
    sync.sortNativeTitleIds.mockResolvedValueOnce([5, 6]);
    sync.checkpoint.mockResolvedValueOnce(checkpoint).mockResolvedValueOnce({ ...checkpoint, revision: 9n });
    expect(await service.tryQuery({ query: { ...query, pagination: { page: 0, size: 2 } }, libraryIds: [3], loadCards })).toBeNull();
    expect(loadCards).toHaveBeenCalledWith([5, 6]);
  });

  it('routes the top search relevance request without changing its scope or page', async () => {
    const request: BookQuery = { ...query, sort: [{ field: 'relevance', dir: 'desc' }], pagination: { page: 0, size: 5 } };
    const result = await service.tryQuery({ query: request, libraryIds: [3], loadCards });
    expect(provider.search).toHaveBeenCalledWith(expect.objectContaining({ relevance: true, titleDirection: 'asc', offset: 0, size: 5 }));
    expect(result?.items.map((item) => item.id)).toEqual([6, 5]);
    expect(result?.total).toBe(10);
  });

  it('does not trust persisted readiness from another application capture session', async () => {
    lifecycle.isCurrent.mockReturnValue(false);
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(provider.search).not.toHaveBeenCalled();
    expect(generation.verifyCheckpoint).not.toHaveBeenCalled();
  });

  it('uses source normalization and leaves an empty normalized query on the native path', async () => {
    sync.normalizeQuery.mockResolvedValueOnce('cafe guide');
    await service.tryQuery({ query: { ...query, q: 'CAFÉ\tGUIDE' }, libraryIds: [3], loadCards });
    expect(sync.normalizeQuery).toHaveBeenCalledWith('CAFÉ\tGUIDE');
    expect(provider.search).toHaveBeenCalledWith(expect.objectContaining({ text: 'cafe guide' }));
    sync.normalizeQuery.mockResolvedValueOnce('');
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(provider.search).toHaveBeenCalledTimes(1);
  });

  it('does not use a disabled or unverified index', async () => {
    config.bookSearch.enabled = false;
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    config.bookSearch.enabled = true;
    config.bookSearch.indexReady = false;
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(provider.search).not.toHaveBeenCalled();
  });

  it('does not route a complete checkpoint to a different index or schema', async () => {
    sync.checkpoint.mockResolvedValueOnce({ ...checkpoint, indexName: 'another-index' });
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    for (const schemaRevision of [1, 3]) {
      sync.checkpoint.mockResolvedValueOnce({ ...checkpoint, schemaRevision });
      expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    }
    expect(provider.search).not.toHaveBeenCalled();
  });

  it('never queries the engine for a user with no accessible libraries', async () => {
    expect(await service.tryQuery({ query, libraryIds: [], loadCards })).toEqual({ items: [], total: 0, page: 2, size: 2 });
    expect(provider.search).not.toHaveBeenCalled();
    expect(loadCards).not.toHaveBeenCalled();
  });

  it('keeps unmapped content restrictions on the original query path', async () => {
    for (const rules of [
      { ...EMPTY_CONTENT_FILTER_RULES, includeTagIds: [9] },
      { ...EMPTY_CONTENT_FILTER_RULES, exemptRequestsFromUserId: 2 },
    ]) {
      expect(await service.tryQuery({ query, libraryIds: [3], contentFilters: rules, loadCards })).toBeNull();
    }
    expect(provider.search).not.toHaveBeenCalled();
  });

  it('does not silently discard advanced filters, collapse, sorting, or deep pagination', async () => {
    const unsupported: BookQuery[] = [
      { ...query, collapseSeries: true },
      { ...query, sort: [{ field: 'author', dir: 'asc' }] },
      { ...query, pagination: { page: 500, size: 20 } },
      { ...query, filter: { type: 'group', join: 'AND', rules: [{ type: 'rule', field: 'publisher', operator: 'eq', value: '人民文学' }] } },
    ];
    for (const request of unsupported) expect(await service.tryQuery({ query: request, libraryIds: [3], loadCards })).toBeNull();
    expect(provider.search).not.toHaveBeenCalled();
  });

  it('does not return an indexed total when native permission checks remove a result', async () => {
    loadCards.mockResolvedValue([{ id: 6 }]);
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
  });

  it('keeps a lagging, unbuilt or incompletely captured index off the query path', async () => {
    for (const state of [
      null,
      { ...checkpoint, appliedRevision: 7n },
      { ...checkpoint, snapshotComplete: false },
      { ...checkpoint, writeCoverageComplete: false },
    ]) {
      sync.checkpoint.mockResolvedValue(state);
      expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    }
    expect(provider.search).not.toHaveBeenCalled();
  });

  it('discards the indexed total if a source change commits while querying', async () => {
    sync.checkpoint.mockResolvedValueOnce(checkpoint).mockResolvedValueOnce({ ...checkpoint, revision: 9n });
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(loadCards).toHaveBeenCalled();
  });

  it('discards results from a replaced generation even when revisions happen to match', async () => {
    sync.checkpoint.mockResolvedValueOnce(checkpoint).mockResolvedValueOnce({ ...checkpoint, epoch: 'replacement-generation' });
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
  });

  it('rejects a backwards native watermark without retrying an unproven restored checkpoint', async () => {
    sync.checkpoint.mockResolvedValueOnce(checkpoint).mockResolvedValueOnce({ ...checkpoint, revision: 7n, appliedRevision: 7n });
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(provider.search).toHaveBeenCalledTimes(1);
  });

  it('waits briefly for a captured batch and queries only its caught-up checkpoint', async () => {
    vi.useFakeTimers();
    try {
      sync.checkpoint
        .mockResolvedValueOnce({ ...checkpoint, appliedRevision: 7n })
        .mockResolvedValueOnce({ ...checkpoint, appliedRevision: 7n })
        .mockResolvedValue(checkpoint);
      const result = service.tryQuery({ query, libraryIds: [3], loadCards });
      await vi.advanceTimersByTimeAsync(100);
      expect(await result).toEqual({ items: [{ id: 6 }, { id: 5 }], total: 10, page: 2, size: 2 });
      expect(provider.search).toHaveBeenCalledTimes(1);
      expect(generation.verifyCheckpoint).toHaveBeenCalledWith(checkpoint);
    } finally {
      vi.useRealTimers();
    }
  });

  it('discards the old page and retries a source revision race with fresh IDs and total', async () => {
    const next = { ...checkpoint, revision: 9n, appliedRevision: 9n };
    sync.checkpoint.mockResolvedValueOnce(checkpoint).mockResolvedValueOnce(next).mockResolvedValue(next);
    provider.search.mockResolvedValueOnce({ ids: [6, 5], total: 10 }).mockResolvedValueOnce({ ids: [10, 9], total: 11 });
    loadCards.mockResolvedValueOnce([{ id: 5 }, { id: 6 }]).mockResolvedValueOnce([{ id: 9 }, { id: 10 }]);
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toEqual({
      items: [{ id: 10 }, { id: 9 }],
      total: 11,
      page: 2,
      size: 2,
    });
    expect(provider.search).toHaveBeenCalledTimes(2);
  });

  it('retries a moved engine watermark only when native state proves a newer revision in the same generation', async () => {
    const next = { ...checkpoint, revision: 9n, appliedRevision: 9n };
    sync.checkpoint.mockResolvedValueOnce(checkpoint).mockResolvedValue(next);
    generation.verifyCheckpoint.mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error('watermark advanced'));
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).not.toBeNull();
    expect(provider.search).toHaveBeenCalledTimes(2);
  });

  it('bounds lag waiting and never serves a page from a stalled index', async () => {
    vi.useFakeTimers();
    try {
      sync.checkpoint.mockResolvedValue({ ...checkpoint, appliedRevision: 7n });
      const result = service.tryQuery({ query, libraryIds: [3], loadCards });
      await vi.advanceTimersByTimeAsync(3000);
      expect(await result).toBeNull();
      expect(provider.search).not.toHaveBeenCalled();
      expect(sync.checkpoint.mock.calls.length).toBeLessThanOrEqual(33);
    } finally {
      vi.useRealTimers();
    }
  });

  it.each(['epoch', 'engineIdentity', 'captureSession'] as const)('rejects a changed %s during catch-up without querying it', async (field) => {
    sync.checkpoint.mockResolvedValueOnce({ ...checkpoint, appliedRevision: 7n }).mockResolvedValue({ ...checkpoint, [field]: 'replacement' });
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(provider.search).not.toHaveBeenCalled();
  });

  it('does not retry indefinitely when another source batch overtakes the second page', async () => {
    const next = { ...checkpoint, revision: 9n, appliedRevision: 9n };
    sync.checkpoint
      .mockResolvedValueOnce(checkpoint)
      .mockResolvedValueOnce(next)
      .mockResolvedValueOnce(next)
      .mockResolvedValueOnce(next)
      .mockResolvedValue({ ...checkpoint, revision: 10n, appliedRevision: 10n });
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(provider.search).toHaveBeenCalledTimes(2);
  });

  it('falls back if checkpoint observation fails while waiting instead of rejecting the HTTP request', async () => {
    const warning = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    sync.checkpoint.mockResolvedValueOnce({ ...checkpoint, appliedRevision: 7n }).mockRejectedValue(new Error('private database error'));
    try {
      expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
      expect(provider.search).not.toHaveBeenCalled();
      expect(JSON.stringify(warning.mock.calls)).not.toContain('private database error');
    } finally {
      warning.mockRestore();
    }
  });

  it('retries an expired linguistic prefix scan once and returns only the complete new page', async () => {
    vi.useFakeTimers();
    try {
      sync.supportsBinaryTitleSort.mockResolvedValue(false);
      provider.search
        .mockResolvedValueOnce({ ids: [], total: 2000 })
        .mockResolvedValueOnce({ ids: [5], total: 1 })
        .mockResolvedValueOnce({ ids: [], total: 2000 })
        .mockResolvedValueOnce({ ids: [6, 5], total: 2 });
      sync.nativeTitleCandidates
        .mockImplementationOnce(async () => {
          await vi.advanceTimersByTimeAsync(1100);
          return [{ id: 5, title: 'first' }];
        })
        .mockResolvedValue([
          { id: 5, title: 'first' },
          { id: 6, title: 'second' },
        ]);
      loadCards.mockResolvedValue([{ id: 6 }, { id: 5 }]);
      expect(await service.tryQuery({ query: { ...query, pagination: { page: 0, size: 2 } }, libraryIds: [3], loadCards })).toEqual({
        items: [{ id: 5 }, { id: 6 }],
        total: 2000,
        page: 0,
        size: 2,
      });
      expect(sync.nativeTitleCandidates).toHaveBeenCalledTimes(2);
      expect(loadCards).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('rejects inconsistent provider IDs before hydrating a page', async () => {
    provider.search.mockResolvedValue({ ids: [6, 6], total: 10 });
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(loadCards).not.toHaveBeenCalled();
  });

  it('falls back without logging provider credentials or raw errors', async () => {
    const warning = vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    provider.search.mockRejectedValue(new Error('private-password=never-log-this'));
    expect(await service.tryQuery({ query, libraryIds: [3], loadCards })).toBeNull();
    expect(JSON.stringify(warning.mock.calls)).not.toContain('never-log-this');
    warning.mockRestore();
  });
});
