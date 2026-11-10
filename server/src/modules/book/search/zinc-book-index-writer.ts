import { BadRequestException, Inject, Injectable, ServiceUnavailableException } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { setTimeout as delay } from 'node:timers/promises';
import { createHash } from 'node:crypto';
import { appConfig } from '../../../config/config';
import { requestZinc, zincIndexPath } from './zinc-book-search.transport';

export interface BookIndexMutation {
  bookId: number;
  revision: bigint;
  document: { library_id: string; [key: string]: unknown } | null;
}

interface BulkResult {
  error?: unknown;
  errors?: boolean;
  items?: { index?: { _index?: string; _id?: string; status?: number; error?: unknown } }[];
}

interface VisibleResult {
  error?: unknown;
  hits?: {
    total?: { value?: number; relation?: string };
    hits?: {
      _id?: string;
      _source?: { book_id?: number; library_id?: string; sync_epoch?: string; sync_revision?: string; sync_deleted?: boolean; sync_hash?: string };
    }[];
  };
}

export function bookIndexContent(mutation: BookIndexMutation): Record<string, unknown> {
  const content: Record<string, unknown> = {
    ...(mutation.document ?? { library_id: '0', search_text: '', title_sort: '', status: 'deleted' }),
    book_id: mutation.bookId,
    sync_deleted: mutation.document === null,
  };
  for (const key of ['sync_epoch', 'sync_revision', 'sync_hash']) delete content[key];
  return content;
}

export function bookIndexContentHash(mutation: BookIndexMutation): string {
  const content = bookIndexContent(mutation);
  return createHash('sha256')
    .update(
      JSON.stringify(
        Object.fromEntries(
          Object.keys(content)
            .sort()
            .map((key) => [key, content[key]]),
        ),
      ),
    )
    .digest('hex');
}

@Injectable()
export class ZincBookIndexWriter {
  constructor(@Inject(appConfig.KEY) private readonly config: ConfigType<typeof appConfig>) {}

  async applyAndVerify(
    mutations: BookIndexMutation[],
    epoch: string,
    signal = AbortSignal.timeout(10_000),
  ): Promise<{ written: number; unchanged: number }> {
    if (!mutations.length) return { written: 0, unchanged: 0 };
    if (
      mutations.length > 500 ||
      !/^[a-zA-Z0-9_-]{1,128}$/.test(epoch) ||
      new Set(mutations.map((row) => row.bookId)).size !== mutations.length ||
      mutations.some(
        (row) =>
          !Number.isSafeInteger(row.bookId) ||
          row.bookId <= 0 ||
          row.revision <= 0n ||
          (row.document !== null && (!/^[1-9][0-9]*$/.test(row.document.library_id) || !Number.isSafeInteger(Number(row.document.library_id)))),
      )
    )
      throw new BadRequestException('Invalid search mutation batch');
    const settings = this.config.bookSearch;
    const prepared = mutations.map((row) => {
      const hash = bookIndexContentHash(row);
      return {
        row,
        hash,
        revision: row.revision.toString(),
        lines: [
          JSON.stringify({ index: { _index: settings.index, _id: String(row.bookId) } }),
          JSON.stringify({ ...bookIndexContent(row), sync_epoch: epoch, sync_revision: row.revision.toString(), sync_hash: hash }),
        ],
      };
    });
    const lines = prepared.flatMap((item) => item.lines);
    const payload = lines.join('\n') + '\n';
    if (Buffer.byteLength(payload) > 4 * 1024 * 1024 || lines.some((line) => Buffer.byteLength(line) > 128 * 1024)) {
      throw new BadRequestException('Search mutation payload exceeded its bound');
    }
    const query = JSON.stringify({
      query: { bool: { should: mutations.map((row) => ({ term: { book_id: row.bookId } })), minimum_should_match: 1 } },
      size: mutations.length,
      _source: ['book_id', 'library_id', 'sync_epoch', 'sync_revision', 'sync_deleted', 'sync_hash'],
    });
    const current = await requestZinc<VisibleResult>(settings, zincIndexPath(settings, '_search'), query, 'application/json', signal);
    if (
      current.error ||
      !Array.isArray(current.hits?.hits) ||
      current.hits.total?.value !== current.hits.hits.length ||
      current.hits.hits.length > mutations.length ||
      (current.hits.total.relation && current.hits.total.relation !== 'eq')
    )
      throw new ServiceUnavailableException('Invalid current search documents');
    const byId = new Map(prepared.map((item) => [item.row.bookId, item]));
    const seenCurrent = new Set<number>();
    const unchanged = new Set<number>();
    for (const hit of current.hits.hits) {
      const source = hit._source;
      const item = source && byId.get(source.book_id!);
      if (!source || !item || hit._id !== String(item.row.bookId) || seenCurrent.has(item.row.bookId))
        throw new ServiceUnavailableException('Invalid current search document identity');
      seenCurrent.add(item.row.bookId);
      if (source.sync_epoch !== epoch) continue;
      if (
        typeof source.sync_revision !== 'string' ||
        !/^[1-9][0-9]{0,18}$/.test(source.sync_revision) ||
        BigInt(source.sync_revision) > item.row.revision
      )
        throw new ServiceUnavailableException('Search document revision is ahead of queued work');
      if (
        source.sync_hash === item.hash &&
        source.library_id === (item.row.document?.library_id ?? '0') &&
        source.sync_deleted === (item.row.document === null)
      ) {
        unchanged.add(item.row.bookId);
        item.revision = source.sync_revision;
      }
    }
    const writes = prepared.filter((item) => !unchanged.has(item.row.bookId));
    if (writes.length) {
      const result = await requestZinc<BulkResult>(
        settings,
        zincIndexPath(settings, '_bulk'),
        writes.flatMap((item) => item.lines).join('\n') + '\n',
        'application/x-ndjson',
        signal,
      );
      if (
        result.error ||
        result.errors ||
        !Array.isArray(result.items) ||
        result.items.length !== writes.length ||
        result.items.some(
          (item, index) =>
            !item.index ||
            item.index._id !== String(writes[index].row.bookId) ||
            item.index._index !== settings.index ||
            ![200, 201].includes(item.index.status ?? 0) ||
            item.index.error,
        )
      )
        throw new ServiceUnavailableException('Book search bulk write was not fully acknowledged');
    }
    while (!signal.aborted) {
      const visible = await requestZinc<VisibleResult>(settings, zincIndexPath(settings, '_search'), query, 'application/json', signal);
      const hits = visible.hits?.hits;
      if (visible.error || !Array.isArray(hits)) throw new ServiceUnavailableException('Invalid search visibility response');
      const seen = new Set<number>();
      const complete =
        visible.hits?.total?.value === mutations.length &&
        (!visible.hits.total.relation || visible.hits.total.relation === 'eq') &&
        hits.length === mutations.length &&
        hits.every((hit) => {
          const source = hit._source;
          const item = source && byId.get(source.book_id!);
          const row = item?.row;
          if (!source || !item || !row || seen.has(row.bookId)) return false;
          seen.add(row.bookId);
          return (
            hit._id === String(row.bookId) &&
            source.sync_epoch === epoch &&
            source.sync_revision === item.revision &&
            source.sync_hash === item.hash &&
            source.sync_deleted === (row.document === null) &&
            source.library_id === (row.document?.library_id ?? '0')
          );
        });
      if (complete) return { written: writes.length, unchanged: unchanged.size };
      await delay(100, undefined, { signal });
    }
    throw new ServiceUnavailableException('Search changes are not yet visible');
  }
}
