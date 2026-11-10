import { chmod, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { assertRestartProofFresh, parseBookSearchRestartProof, readBookSearchRestartProof } from './book-search-restart-proof';

function proof(now = Date.now()) {
  return {
    format: 'book-search-restart-proof-v1',
    previousOwnerStopped: true,
    sourceCaptureVerified: true,
    issuedAt: new Date(now).toISOString(),
    expiresAt: new Date(now + 60_000).toISOString(),
    sourceBooks: 22,
    sourceUpperBookId: 100,
    checkpoint: {
      epoch: '11111111-1111-4111-8111-111111111111',
      captureSession: '22222222-2222-4222-8222-222222222222',
      engineIdentity: 'a'.repeat(64),
      indexName: 'books_metadata_v4',
      schemaRevision: 4,
      revision: '50',
      appliedRevision: '50',
    },
  };
}

describe('verified search restart proof', () => {
  let directory: string;
  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'book-search-proof-'));
  });
  afterEach(async () => rm(directory, { recursive: true, force: true }));

  it('parses an explicit, fresh stopped-owner and source-capture assertion with exact bigint watermarks', () => {
    expect(parseBookSearchRestartProof(proof())).toMatchObject({ checkpoint: { revision: 50n, appliedRevision: 50n }, sourceBooks: 22 });
  });

  it.each([
    { previousOwnerStopped: false },
    { sourceCaptureVerified: false },
    { format: 'unknown' },
    { sourceBooks: -1 },
    { sourceUpperBookId: 2147483648 },
    { issuedAt: 'invalid' },
  ])('rejects missing or invalid operator evidence %j', (change) => {
    expect(() => parseBookSearchRestartProof({ ...proof(), ...change })).toThrow();
  });

  it.each([
    { epoch: 'other' },
    { captureSession: null },
    { engineIdentity: '' },
    { revision: '9223372036854775808', appliedRevision: '9223372036854775808' },
    { revision: '050', appliedRevision: '050' },
    { appliedRevision: '49' },
    { indexName: '../other' },
    { schemaRevision: 0 },
  ])('rejects invalid or lagging checkpoint fields %j', (change) => {
    const value = proof();
    expect(() => parseBookSearchRestartProof({ ...value, checkpoint: { ...value.checkpoint, ...change } })).toThrow();
  });

  it('rejects expired, future, overlong or noncanonical proof validity windows', () => {
    const now = Date.now();
    expect(() => parseBookSearchRestartProof(proof(now - 61_000), now)).toThrow();
    expect(() => parseBookSearchRestartProof(proof(now + 6000), now)).toThrow();
    expect(() => parseBookSearchRestartProof({ ...proof(now), expiresAt: new Date(now + 120_001).toISOString() }, now)).toThrow();
    expect(() => parseBookSearchRestartProof({ ...proof(now), issuedAt: new Date(now).toISOString().replace('Z', '+00:00') }, now)).toThrow();
    expect(() => assertRestartProofFresh(proof(now), now + 60_000)).toThrow();
  });

  it('reads a bounded regular proof file', async () => {
    const path = join(directory, 'proof.json');
    await writeFile(path, JSON.stringify(proof()), { mode: 0o600 });
    expect(await readBookSearchRestartProof(path)).toMatchObject({ checkpoint: { revision: 50n } });
  });

  it('rejects directories, oversized files and malformed JSON without leaking their content', async () => {
    await expect(readBookSearchRestartProof(directory)).rejects.toThrow('proof unavailable');
    const path = join(directory, 'proof.json');
    await writeFile(path, 'private-content'.repeat(1000), { mode: 0o600 });
    await expect(readBookSearchRestartProof(path)).rejects.toThrow('proof unavailable');
    await writeFile(path, 'private-content');
    const error = await readBookSearchRestartProof(path).catch((value: unknown) => value);
    expect(String(error)).not.toContain('private-content');
  });

  it.skipIf(process.platform === 'win32')('rejects writable group files and symbolic links on the production POSIX filesystem', async () => {
    const path = join(directory, 'proof.json');
    await writeFile(path, JSON.stringify(proof()), { mode: 0o600 });
    await chmod(path, 0o660);
    await expect(readBookSearchRestartProof(path)).rejects.toThrow('proof unavailable');
    await chmod(path, 0o600);
    const link = join(directory, 'link.json');
    await symlink(path, link);
    await expect(readBookSearchRestartProof(link)).rejects.toThrow('proof unavailable');
  });
});
