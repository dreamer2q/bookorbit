import { Inject, Injectable, ServiceUnavailableException } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { sql } from 'drizzle-orm';
import { appConfig } from '../../../config/config';
import type { BookSearchTransaction } from '../../../db/book-search-changes';
import type { BookSearchCheckpoint } from './book-search-sync.repository';
import { BOOK_SEARCH_SCHEMA_REVISION, bookSearchIndexDefinition } from './book-search-index-definition';
import { requestZinc, zincIndexPath } from './zinc-book-search.transport';

interface IndexMetadata {
  name?: string;
  storage_type?: string;
  shard_num?: number;
  shards?: Record<string, { id?: string }>;
  settings?: { analysis?: Record<string, unknown> };
  mappings?: { properties?: Record<string, Record<string, unknown>> };
}

interface Marker {
  book_id: number;
  library_id: string;
  sync_epoch: string;
  sync_revision: string;
  sync_deleted: boolean;
  schema_revision: number;
  index_identity: string;
}

interface SearchResult {
  error?: unknown;
  hits?: { total?: { value?: number; relation?: string }; hits?: { _id?: string; _source?: Marker }[] };
}

const MARKER_ID = '__bookorbit_checkpoint__';

export class BookSearchSourceCountMismatch extends ServiceUnavailableException {}

@Injectable()
export class ZincBookIndexGeneration {
  constructor(@Inject(appConfig.KEY) private readonly config: ConfigType<typeof appConfig>) {}

  async prepare(epoch: string, expectedIdentity: string): Promise<string> {
    const settings = this.config.bookSearch;
    bookSearchIndexDefinition(settings.index);
    let metadata = await this.metadata(true);
    if (!metadata) {
      if (expectedIdentity) throw new ServiceUnavailableException('Bound book search index is missing; rebuild required');
      await requestZinc(settings, '/api/index', JSON.stringify(bookSearchIndexDefinition(settings.index)));
      metadata = await this.metadata();
    }
    const identity = this.identity(metadata!);
    if (expectedIdentity && identity !== expectedIdentity) throw new ServiceUnavailableException('Book search engine identity changed');
    const marker = await this.marker();
    if (expectedIdentity) {
      this.assertMarker(marker, epoch, identity);
      return identity;
    }
    const count = await this.count({ match_all: {} });
    // A crash after claiming an empty index but before committing the DB binding is resumable.
    if (count !== 0) {
      this.assertMarker(marker, epoch, identity);
      if (count !== 1 || marker!.sync_revision !== '0') throw new ServiceUnavailableException('Refusing to adopt a nonempty book search index');
    }
    await this.publishCheckpoint(epoch, 0n, identity);
    return identity;
  }

  async assertWorkerCheckpoint(state: BookSearchCheckpoint): Promise<Marker> {
    await this.verifyIdentity(state.engineIdentity);
    const marker = await this.marker();
    this.assertMarker(marker, state.epoch, state.engineIdentity);
    const revision = BigInt(marker!.sync_revision);
    // An engine write can precede a failed DB commit; replay that queue, but never heal an older restored index in place.
    if (revision < state.appliedRevision || revision > state.revision) throw new ServiceUnavailableException('Book search engine watermark changed');
    return marker!;
  }

  async verifyCheckpoint(state: BookSearchCheckpoint): Promise<void> {
    const marker = await this.assertWorkerCheckpoint(state);
    if (marker.sync_revision !== state.appliedRevision.toString()) throw new ServiceUnavailableException('Book search engine is not caught up');
  }

  async readLiveIds(cursor: number, state: BookSearchCheckpoint): Promise<number[]> {
    if (!Number.isSafeInteger(cursor) || cursor < 0) throw new ServiceUnavailableException('Invalid orphan scan cursor');
    await this.assertWorkerCheckpoint(state);
    const result = await requestZinc<SearchResult>(
      this.config.bookSearch,
      zincIndexPath(this.config.bookSearch, '_search'),
      JSON.stringify({
        query: {
          bool: { filter: [{ term: { sync_epoch: state.epoch } }, { term: { sync_deleted: false } }, { range: { book_id: { gt: cursor } } }] },
        },
        size: 100,
        sort: [{ book_id: 'asc' }],
        _source: ['book_id', 'sync_epoch', 'sync_deleted'],
      }),
    );
    const hits = result.hits?.hits;
    const total = result.hits?.total?.value;
    if (
      result.error ||
      !Array.isArray(hits) ||
      !Number.isSafeInteger(total) ||
      total! < 0 ||
      hits.length !== Math.min(100, total!) ||
      (result.hits?.total?.relation && result.hits.total.relation !== 'eq')
    )
      throw new ServiceUnavailableException('Invalid orphan search response');
    const ids: number[] = [];
    for (const hit of hits) {
      const source = hit._source;
      const id = source?.book_id;
      if (
        !Number.isSafeInteger(id) ||
        id! <= (ids.at(-1) ?? cursor) ||
        hit._id !== String(id) ||
        source?.sync_epoch !== state.epoch ||
        source.sync_deleted !== false
      )
        throw new ServiceUnavailableException('Invalid orphan search identity');
      ids.push(id!);
    }
    await this.verifyIdentity(state.engineIdentity);
    return ids;
  }

  async verifyComplete(tx: BookSearchTransaction, state: BookSearchCheckpoint): Promise<void> {
    await this.assertWorkerCheckpoint(state);
    const source = await tx.execute<{ count: string }>(sql`SELECT count(*)::text AS count FROM public.books WHERE status <> 'processing'`);
    const count = await this.count({ bool: { must: [{ term: { sync_epoch: state.epoch } }], must_not: [{ term: { sync_deleted: true } }] } });
    if (BigInt(count) !== BigInt(source.rows[0].count)) throw new BookSearchSourceCountMismatch('Book search visible count does not match source');
    const foreign = await this.count({ bool: { must_not: [{ term: { sync_epoch: state.epoch } }] } });
    if (foreign !== 0) throw new ServiceUnavailableException('Book search contains documents from another generation');
    await this.publishCheckpoint(state.epoch, state.revision, state.engineIdentity);
  }

  async publishCheckpoint(epoch: string, revision: bigint, identity: string): Promise<void> {
    await this.verifyIdentity(identity);
    const settings = this.config.bookSearch;
    const marker = {
      book_id: 0,
      library_id: '0',
      sync_epoch: epoch,
      sync_revision: revision.toString(),
      sync_deleted: true,
      schema_revision: BOOK_SEARCH_SCHEMA_REVISION,
      index_identity: identity,
      search_text: '',
      title_sort: '',
    };
    const signal = AbortSignal.timeout(10_000);
    const result = await requestZinc<{
      error?: unknown;
      errors?: boolean;
      items?: { index?: { _id?: string; _index?: string; status?: number; error?: unknown } }[];
    }>(
      settings,
      zincIndexPath(settings, '_bulk'),
      `${JSON.stringify({ index: { _index: settings.index, _id: MARKER_ID } })}\n${JSON.stringify(marker)}\n`,
      'application/x-ndjson',
      signal,
    );
    const item = result.items?.[0]?.index;
    if (
      result.error ||
      result.errors ||
      result.items?.length !== 1 ||
      item?.error ||
      item?._id !== MARKER_ID ||
      item?._index !== settings.index ||
      ![200, 201].includes(item?.status ?? 0)
    )
      throw new ServiceUnavailableException('Book search checkpoint write was not acknowledged');
    while (!signal.aborted) {
      const visible = await this.marker(signal);
      if (
        visible &&
        Object.entries(marker)
          .filter(([key]) => key !== 'search_text' && key !== 'title_sort')
          .every(([key, value]) => visible[key as keyof Marker] === value)
      ) {
        await this.verifyIdentity(identity);
        return;
      }
      await delay(100, undefined, { signal });
    }
    throw new ServiceUnavailableException('Book search checkpoint is not visible');
  }

  private async metadata(allowNotFound = false): Promise<IndexMetadata | null> {
    return requestZinc(this.config.bookSearch, `/api/index/${this.config.bookSearch.index}`, '', 'application/json', undefined, {
      method: 'GET',
      allowNotFound,
    });
  }

  private identity(metadata: IndexMetadata): string {
    const expected = bookSearchIndexDefinition(this.config.bookSearch.index);
    const properties = metadata.mappings?.properties;
    if (
      metadata.name !== expected.name ||
      metadata.storage_type !== 'disk' ||
      metadata.shard_num !== 1 ||
      !properties ||
      Object.keys(metadata.settings?.analysis ?? {}).length
    )
      throw new ServiceUnavailableException('Incompatible book search index structure');
    for (const [name, definition] of Object.entries(expected.mappings.properties)) {
      const actual = properties[name];
      if (
        !actual ||
        Object.entries(definition).some(([key, value]) => actual[key] !== (key === 'type' && value === 'boolean' ? 'bool' : value)) ||
        actual.search_analyzer
      )
        throw new ServiceUnavailableException('Incompatible book search index mapping');
    }
    const ids = Object.keys(metadata.shards ?? {}).sort();
    if (ids.length !== 1 || ids.some((id) => !/^[a-zA-Z0-9_-]{1,128}$/.test(id) || metadata.shards![id].id !== id))
      throw new ServiceUnavailableException('Invalid book search index identity');
    return createHash('sha256')
      .update(JSON.stringify([expected.name, BOOK_SEARCH_SCHEMA_REVISION, ids]))
      .digest('hex');
  }

  private async verifyIdentity(expected: string): Promise<void> {
    if (!/^[a-f0-9]{64}$/.test(expected) || this.identity((await this.metadata())!) !== expected)
      throw new ServiceUnavailableException('Book search engine identity changed');
  }

  private assertMarker(marker: Marker | null, epoch: string, identity: string): void {
    if (
      !marker ||
      marker.book_id !== 0 ||
      marker.library_id !== '0' ||
      marker.sync_epoch !== epoch ||
      marker.sync_deleted !== true ||
      marker.schema_revision !== BOOK_SEARCH_SCHEMA_REVISION ||
      marker.index_identity !== identity ||
      !/^(0|[1-9][0-9]{0,18})$/.test(marker.sync_revision)
    )
      throw new ServiceUnavailableException('Book search generation marker changed');
  }

  private async search(query: unknown, size = 0, signal?: AbortSignal): Promise<SearchResult> {
    const result = await requestZinc<SearchResult>(
      this.config.bookSearch,
      zincIndexPath(this.config.bookSearch, '_search'),
      JSON.stringify({ query, size }),
      'application/json',
      signal,
    );
    if (
      result.error ||
      !Number.isSafeInteger(result.hits?.total?.value) ||
      result.hits!.total!.value! < 0 ||
      (result.hits?.total?.relation && result.hits.total.relation !== 'eq') ||
      !Array.isArray(result.hits?.hits) ||
      result.hits.hits.length > size
    )
      throw new ServiceUnavailableException('Invalid book search generation response');
    return result;
  }

  private async count(query: unknown): Promise<number> {
    return (await this.search(query)).hits!.total!.value!;
  }

  private async marker(signal?: AbortSignal): Promise<Marker | null> {
    const result = await this.search({ term: { book_id: 0 } }, 1, signal);
    if (result.hits!.total!.value === 0) return null;
    const hit = result.hits!.hits![0];
    if (result.hits!.total!.value !== 1 || hit?._id !== MARKER_ID || !hit._source)
      throw new ServiceUnavailableException('Invalid book search generation marker');
    return hit._source;
  }
}
