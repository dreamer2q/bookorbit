import { Test } from '@nestjs/testing';
import { ServiceUnavailableException } from '@nestjs/common';
import { appConfig } from '../../../config/config';
import { ZincBookSearchProvider } from './zinc-book-search-provider';
import type { BookIndexRequest } from './book-search-provider';

describe('ZincBookSearchProvider', () => {
  let service: ZincBookSearchProvider;
  let config: ReturnType<typeof appConfig>;
  const request: BookIndexRequest = { text: '英语 OR *:*', libraryIds: [3, 5], offset: 0, size: 2, titleDirection: 'asc' };
  const fetchMock = vi.fn();
  const value = (hits = [{ _source: { book_id: 11, library_id: '3' } }, { _source: { book_id: 12, library_id: '5' } }]) => ({
    hits: { total: { value: 3, relation: 'eq' }, hits },
  });

  beforeEach(async () => {
    vi.clearAllMocks();
    vi.stubGlobal('fetch', fetchMock);
    config = appConfig();
    config.bookSearch.matchMode = 'fulltext';
    config.bookSearch.username = 'fixture-user';
    config.bookSearch.password = 'fixture-password';
    const module = await Test.createTestingModule({ providers: [ZincBookSearchProvider, { provide: appConfig.KEY, useValue: config }] }).compile();
    service = module.get(ZincBookSearchProvider);
    fetchMock.mockResolvedValue(new Response(JSON.stringify(value()), { status: 200 }));
  });

  afterEach(() => vi.unstubAllGlobals());

  it('sends literal match text, authorized library filters, and a stable bounded page', async () => {
    expect(await service.search(request)).toEqual({ ids: [11, 12], total: 3 });
    const [url, options] = fetchMock.mock.calls[0] as [URL, RequestInit];
    expect(url.pathname).toBe('/es/books_metadata_gse_v1/_search');
    const payload = JSON.parse(options.body as string);
    expect(payload.query.bool.must).toEqual([{ match: { search_text: { query: '英语 OR *:*', operator: 'and' } } }]);
    expect(payload.query.bool.filter[0].bool.should).toEqual([{ term: { library_id: '3' } }, { term: { library_id: '5' } }]);
    expect(payload).toMatchObject({
      size: 2,
      from: 0,
      sort: [{ title_null: 'asc' }, { title_sort: 'asc' }, { book_id: 'asc' }],
      _source: ['book_id', 'library_id'],
    });
  });

  it('keeps native title candidates inside both the request filter and the returned result set', async () => {
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({ hits: { total: { value: 2, relation: 'eq' }, hits: value().hits.hits } })));
    expect(await service.search({ ...request, candidateIds: [11, 12] })).toEqual({ ids: [11, 12], total: 2 });
    const payload = JSON.parse(fetchMock.mock.calls[0][1].body as string);
    expect(payload.query.bool.filter.at(-1)).toEqual({
      bool: { should: [{ term: { book_id: 11 } }, { term: { book_id: 12 } }], minimum_should_match: 1 },
    });
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ hits: { total: { value: 1, relation: 'eq' }, hits: [{ _source: { book_id: 99, library_id: '3' } }] } })),
    );
    await expect(service.search({ ...request, size: 1, candidateIds: [11, 12] })).rejects.toBeInstanceOf(ServiceUnavailableException);
  });

  it('collects native-order candidates without engine field sorting and rejects unsafe unordered pages', async () => {
    expect(await service.search({ ...request, unordered: true })).toEqual({ ids: [11, 12], total: 3 });
    const payload = JSON.parse(fetchMock.mock.calls[0][1].body as string);
    expect(payload.sort).toBeUndefined();
    expect(payload.query.bool.filter[0].bool.should).toHaveLength(2);
    await expect(service.search({ ...request, unordered: true, offset: 2 })).rejects.toThrow('Unordered collection');
    await expect(service.search({ ...request, unordered: true, relevance: true })).rejects.toThrow('Unordered collection');
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('rejects hits from a library outside the server authorized set', async () => {
    fetchMock.mockResolvedValue(new Response(JSON.stringify(value([{ _source: { book_id: 11, library_id: '9' } }]))));
    await expect(service.search(request)).rejects.toBeInstanceOf(ServiceUnavailableException);
  });

  it('ranks the complete literal result set without making token matches mandatory', async () => {
    config.bookSearch.matchMode = 'contains';
    await service.search({ ...request, text: '英语', relevance: true });
    const payload = JSON.parse(fetchMock.mock.calls[0][1].body as string);
    expect(payload.sort).toEqual(['_score', { title_null: 'asc' }, { title_sort: 'asc' }, { book_id: 'asc' }]);
    expect(payload.query.bool.must).toBeUndefined();
    expect(payload.query.bool.minimum_should_match).toBe(1);
    expect(payload.query.bool.should[0]).toEqual({ match: { search_text: { query: '英语', operator: 'or' } } });
    expect(payload.query.bool.should[1]).toEqual({ bool: { filter: [payload.query.bool.filter[0]] } });
    expect(payload.query.bool.filter[0]).toHaveProperty('match_phrase.native_bigrams');
    expect(payload.query.bool.filter[1].bool.should).toEqual([{ term: { library_id: '3' } }, { term: { library_id: '5' } }]);
  });

  it('supports literal fragments without exposing query syntax or bridging separate fields', async () => {
    config.bookSearch.matchMode = 'contains';
    await service.search({ ...request, text: 'gram' });
    let payload = JSON.parse(fetchMock.mock.calls[0][1].body as string);
    expect(payload.query.bool.must).toEqual([{ match_phrase: { native_bigrams: { query: 'b67_72 b72_61 b61_6d', analyzer: 'whitespace' } } }]);
    fetchMock.mockResolvedValue(new Response(JSON.stringify(value()), { status: 200 }));
    await service.search({ ...request, text: '*' });
    payload = JSON.parse(fetchMock.mock.calls[1][1].body as string);
    expect(payload.query.bool.must).toEqual([{ term: { native_characters: 'u2a' } }]);
  });

  it('rejects duplicate IDs and incomplete pages instead of silently shrinking them', async () => {
    for (const hits of [
      [{ _source: { book_id: 11, library_id: '3' } }],
      [{ _source: { book_id: 11, library_id: '3' } }, { _source: { book_id: 11, library_id: '3' } }],
    ]) {
      fetchMock.mockResolvedValue(new Response(JSON.stringify(value(hits))));
      await expect(service.search(request)).rejects.toBeInstanceOf(ServiceUnavailableException);
    }
  });

  it('does not expose an approximate total as the native exact page count', async () => {
    const response = value();
    response.hits.total.relation = 'gte';
    fetchMock.mockResolvedValue(new Response(JSON.stringify(response)));
    await expect(service.search(request)).rejects.toBeInstanceOf(ServiceUnavailableException);
  });

  it('bounds response bytes even when content length is absent', async () => {
    fetchMock.mockResolvedValue(new Response('x'.repeat(1024 * 1024 + 1)));
    await expect(service.search(request)).rejects.toThrow('exceeded its bound');
  });

  it('does not include a private provider error body in the thrown error', async () => {
    fetchMock.mockResolvedValue(new Response('private-credential-value', { status: 500 }));
    await expect(service.search(request)).rejects.toThrow('Book search provider unavailable');
  });
});
