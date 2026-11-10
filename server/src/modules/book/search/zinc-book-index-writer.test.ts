import { Test } from '@nestjs/testing';
import { appConfig } from '../../../config/config';
import { ZincBookIndexWriter, bookIndexContentHash, type BookIndexMutation } from './zinc-book-index-writer';

describe('ZincBookIndexWriter', () => {
  let writer: ZincBookIndexWriter;
  const fetchMock = vi.fn();
  const mutation: BookIndexMutation = { bookId: 12, revision: 7n, document: { library_id: '3', title: 'original' } };
  const bulk = () => ({ items: [{ index: { _id: '12', _index: 'books_metadata_gse_v1', status: 200 } }] });
  const empty = () => ({ hits: { total: { value: 0, relation: 'eq' }, hits: [] } });
  const visible = (
    revision = '7',
    libraryId = '3',
    deleted = false,
    hash = bookIndexContentHash(deleted ? { ...mutation, document: null } : mutation),
  ) => ({
    hits: {
      total: { value: 1, relation: 'eq' },
      hits: [
        {
          _id: '12',
          _source: { book_id: 12, library_id: libraryId, sync_epoch: 'epoch-a', sync_revision: revision, sync_deleted: deleted, sync_hash: hash },
        },
      ],
    },
  });
  const respond = (value: unknown) => new Response(JSON.stringify(value));

  beforeEach(async () => {
    vi.stubGlobal('fetch', fetchMock);
    fetchMock.mockReset();
    const module = await Test.createTestingModule({ providers: [ZincBookIndexWriter, { provide: appConfig.KEY, useValue: appConfig() }] }).compile();
    writer = module.get(ZincBookIndexWriter);
    fetchMock
      .mockResolvedValueOnce(respond(empty()))
      .mockResolvedValueOnce(respond(bulk()))
      .mockImplementation(() => Promise.resolve(respond(visible())));
  });
  afterEach(() => vi.unstubAllGlobals());

  it('replaces a stable document ID and verifies its visible revision before returning', async () => {
    await writer.applyAndVerify([mutation], 'epoch-a');
    const options = fetchMock.mock.calls[1][1] as RequestInit;
    const lines = (options.body as string)
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line));
    expect(lines[0].index._id).toBe('12');
    expect(lines[1]).toMatchObject({ book_id: 12, library_id: '3', sync_revision: '7', sync_epoch: 'epoch-a', sync_deleted: false });
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it('does not acknowledge a partial bulk failure hidden inside HTTP200', async () => {
    const failed = bulk();
    Object.assign(failed.items[0].index, { error: 'not accepted' });
    fetchMock.mockReset().mockResolvedValueOnce(respond(empty())).mockResolvedValue(respond(failed));
    await expect(writer.applyAndVerify([mutation], 'epoch-a')).rejects.toThrow('not fully acknowledged');
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('waits through a stale searchable version instead of treating write acceptance as visibility', async () => {
    fetchMock
      .mockReset()
      .mockResolvedValueOnce(respond(empty()))
      .mockResolvedValueOnce(respond(bulk()))
      .mockResolvedValueOnce(respond(visible('6')))
      .mockResolvedValueOnce(respond(visible()));
    await writer.applyAndVerify([mutation], 'epoch-a');
    expect(fetchMock).toHaveBeenCalledTimes(4);
  });

  it('does not accept a matching revision assigned to the wrong library', async () => {
    fetchMock
      .mockReset()
      .mockResolvedValueOnce(respond(empty()))
      .mockResolvedValueOnce(respond(bulk()))
      .mockImplementation(() => Promise.resolve(respond(visible('7', '9'))));
    await expect(writer.applyAndVerify([mutation], 'epoch-a', AbortSignal.timeout(30))).rejects.toThrow();
  });

  it('writes an idempotent tombstone outside all valid library scopes', async () => {
    fetchMock
      .mockReset()
      .mockResolvedValueOnce(respond(empty()))
      .mockResolvedValueOnce(respond(bulk()))
      .mockResolvedValueOnce(respond(visible('7', '0', true)));
    await writer.applyAndVerify([{ ...mutation, document: null }], 'epoch-a');
    const payload = JSON.parse((fetchMock.mock.calls[1][1].body as string).trim().split('\n')[1]);
    expect(payload).toMatchObject({ book_id: 12, library_id: '0', search_text: '', sync_deleted: true });
  });

  it('rejects duplicate IDs, invalid scope and oversized UTF8 documents before issuing writes', async () => {
    for (const changes of [
      [mutation, mutation],
      [{ ...mutation, document: { library_id: '0' } }],
      [{ ...mutation, document: { library_id: '3', description: 'é'.repeat(70000) } }],
    ]) {
      await expect(writer.applyAndVerify(changes, 'epoch-a')).rejects.toThrow();
    }
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('verifies unchanged content without rewriting it just to advance a source revision', async () => {
    fetchMock.mockReset().mockImplementation(() => Promise.resolve(respond(visible('6'))));
    expect(await writer.applyAndVerify([mutation], 'epoch-a')).toEqual({ written: 0, unchanged: 1 });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls.every(([url]) => String(url).endsWith('/_search'))).toBe(true);
  });

  it('rewrites a nullable metadata edit instead of reusing the previous content hash', async () => {
    const changed = { ...mutation, document: { ...mutation.document!, publisher: null } };
    fetchMock
      .mockReset()
      .mockResolvedValueOnce(respond(visible('6')))
      .mockResolvedValueOnce(respond(bulk()))
      .mockResolvedValueOnce(respond(visible('7', '3', false, bookIndexContentHash(changed))));
    expect(await writer.applyAndVerify([changed], 'epoch-a')).toEqual({ written: 1, unchanged: 0 });
  });

  it('never accepts a matching revision whose content fingerprint is wrong', async () => {
    fetchMock
      .mockReset()
      .mockResolvedValueOnce(respond(empty()))
      .mockResolvedValueOnce(respond(bulk()))
      .mockImplementation(() => Promise.resolve(respond(visible('7', '3', false, 'wrong-hash'))));
    await expect(writer.applyAndVerify([mutation], 'epoch-a', AbortSignal.timeout(30))).rejects.toThrow();
  });

  it('rejects ambiguous preflight identities without issuing a bulk request', async () => {
    const duplicate = visible();
    duplicate.hits.total.value = 2;
    duplicate.hits.hits.push(duplicate.hits.hits[0]);
    fetchMock.mockReset().mockResolvedValue(respond(duplicate));
    await expect(writer.applyAndVerify([mutation], 'epoch-a')).rejects.toThrow('current search documents');
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('does not reuse matching content from another epoch', async () => {
    const foreign = visible('6');
    foreign.hits.hits[0]._source.sync_epoch = 'old-epoch';
    fetchMock.mockReset().mockResolvedValueOnce(respond(foreign)).mockResolvedValueOnce(respond(bulk())).mockResolvedValueOnce(respond(visible()));
    expect(await writer.applyAndVerify([mutation], 'epoch-a')).toEqual({ written: 1, unchanged: 0 });
  });

  it('rejects an engine document ahead of the queued source revision', async () => {
    fetchMock.mockReset().mockResolvedValue(respond(visible('8')));
    await expect(writer.applyAndVerify([mutation], 'epoch-a')).rejects.toThrow('ahead of queued work');
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
