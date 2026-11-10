import { Inject, Injectable, ServiceUnavailableException } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { appConfig } from '../../../config/config';
import { BookSearchProvider, type BookIndexPage, type BookIndexRequest } from './book-search-provider';
import { requestZinc, zincIndexPath } from './zinc-book-search.transport';
import { literalSearchQuery } from './book-literal-search';

@Injectable()
export class ZincBookSearchProvider extends BookSearchProvider {
  constructor(@Inject(appConfig.KEY) private readonly config: ConfigType<typeof appConfig>) {
    super();
  }

  async search(request: BookIndexRequest): Promise<BookIndexPage> {
    const settings = this.config.bookSearch;
    if (request.unordered && (request.relevance || request.offset !== 0))
      throw new ServiceUnavailableException('Unordered collection cannot serve a ranked or offset page');
    if (
      request.candidateIds &&
      (request.candidateIds.length < 1 ||
        request.candidateIds.length > 500 ||
        new Set(request.candidateIds).size !== request.candidateIds.length ||
        request.candidateIds.some((id) => !Number.isSafeInteger(id) || id <= 0))
    )
      throw new ServiceUnavailableException('Invalid native title candidate page');
    const match =
      settings.matchMode === 'contains' ? literalSearchQuery(request.text) : { match: { search_text: { query: request.text, operator: 'and' } } };
    const value = await requestZinc<{
      error?: unknown;
      hits?: { total?: { value?: number; relation?: string }; hits?: { _source?: { book_id?: number; library_id?: string } }[] };
    }>(
      settings,
      zincIndexPath(settings, '_search'),
      JSON.stringify({
        query: {
          bool: {
            // Zinc may require a should hit even with minimum_should_match=0.
            // A zero-score filtered branch retains fragments without GSE tokens.
            ...(request.relevance
              ? {
                  should: [{ match: { search_text: { query: request.text, operator: 'or' } } }, { bool: { filter: [match] } }],
                  minimum_should_match: 1,
                }
              : { must: [match] }),
            filter: [
              ...(request.relevance ? [match] : []),
              { bool: { should: request.libraryIds.map((id) => ({ term: { library_id: String(id) } })), minimum_should_match: 1 } },
              ...(request.epoch ? [{ term: { sync_epoch: request.epoch } }, { term: { sync_deleted: false } }] : []),
              ...(request.candidateIds
                ? [{ bool: { should: request.candidateIds.map((id) => ({ term: { book_id: id } })), minimum_should_match: 1 } }]
                : []),
            ],
          },
        },
        size: request.size,
        from: request.offset,
        // Zinc recognizes the string form as score; an object sorts a stored field.
        ...(request.unordered
          ? {}
          : { sort: [...(request.relevance ? ['_score'] : []), { title_null: 'asc' }, { title_sort: request.titleDirection }, { book_id: 'asc' }] }),
        _source: ['book_id', 'library_id'],
      }),
    );
    const hits = value.hits?.hits;
    const total = value.hits?.total?.value;
    const relation = value.hits?.total?.relation;
    if (
      value.error ||
      !Array.isArray(hits) ||
      hits.length > request.size ||
      !Number.isSafeInteger(total) ||
      total! < 0 ||
      (relation && relation !== 'eq')
    ) {
      throw new ServiceUnavailableException('Invalid book search response');
    }
    const ids: number[] = [];
    const libraries = new Set(request.libraryIds);
    for (const hit of hits) {
      const id = hit._source?.book_id;
      const libraryId = Number(hit._source?.library_id);
      if (!Number.isSafeInteger(id) || id! <= 0 || !libraries.has(libraryId) || ids.includes(id!)) {
        throw new ServiceUnavailableException('Invalid book search result scope');
      }
      ids.push(id!);
      if (request.candidateIds && !request.candidateIds.includes(id!)) throw new ServiceUnavailableException('Invalid native title intersection');
    }
    if (ids.length !== Math.min(request.size, Math.max(0, total! - request.offset))) {
      throw new ServiceUnavailableException('Incomplete book search page');
    }
    return { ids, total: total! };
  }
}
