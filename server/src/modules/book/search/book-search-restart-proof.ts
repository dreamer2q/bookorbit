import { BadRequestException, ServiceUnavailableException } from '@nestjs/common';
import { constants } from 'node:fs';
import { open } from 'node:fs/promises';
import type { BookSearchCheckpoint } from './book-search-sync.repository';

export interface BookSearchRestartProof {
  format: 'book-search-restart-proof-v1';
  previousOwnerStopped: true;
  sourceCaptureVerified: true;
  issuedAt: string;
  expiresAt: string;
  sourceBooks: number;
  sourceUpperBookId: number;
  checkpoint: Pick<
    BookSearchCheckpoint,
    'epoch' | 'revision' | 'appliedRevision' | 'indexName' | 'schemaRevision' | 'engineIdentity' | 'captureSession'
  >;
}

const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;

function record(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

export function assertRestartProofFresh(proof: Pick<BookSearchRestartProof, 'issuedAt' | 'expiresAt'>, now = Date.now()): void {
  const issued = Date.parse(proof.issuedAt);
  const expires = Date.parse(proof.expiresAt);
  if (
    !Number.isFinite(issued) ||
    !Number.isFinite(expires) ||
    new Date(issued).toISOString() !== proof.issuedAt ||
    new Date(expires).toISOString() !== proof.expiresAt ||
    issued > now + 5000 ||
    expires <= now ||
    expires <= issued ||
    expires - issued > 120_000
  )
    throw new BadRequestException('Verified search restart proof expired or invalid');
}

export function parseBookSearchRestartProof(value: unknown, now = Date.now()): BookSearchRestartProof {
  if (
    !record(value) ||
    value.format !== 'book-search-restart-proof-v1' ||
    value.previousOwnerStopped !== true ||
    value.sourceCaptureVerified !== true ||
    typeof value.issuedAt !== 'string' ||
    typeof value.expiresAt !== 'string' ||
    !Number.isSafeInteger(value.sourceBooks) ||
    (value.sourceBooks as number) < 0 ||
    !Number.isSafeInteger(value.sourceUpperBookId) ||
    (value.sourceUpperBookId as number) < 0 ||
    (value.sourceUpperBookId as number) > 2147483647 ||
    !record(value.checkpoint)
  )
    throw new BadRequestException('Invalid verified search restart proof');
  assertRestartProofFresh({ issuedAt: value.issuedAt, expiresAt: value.expiresAt }, now);
  const checkpoint = value.checkpoint;
  if (
    typeof checkpoint.epoch !== 'string' ||
    !uuid.test(checkpoint.epoch) ||
    typeof checkpoint.captureSession !== 'string' ||
    !uuid.test(checkpoint.captureSession) ||
    typeof checkpoint.engineIdentity !== 'string' ||
    !/^[a-f0-9]{64}$/.test(checkpoint.engineIdentity) ||
    typeof checkpoint.indexName !== 'string' ||
    !/^[a-z][a-z0-9_-]{0,127}$/.test(checkpoint.indexName) ||
    !Number.isSafeInteger(checkpoint.schemaRevision) ||
    (checkpoint.schemaRevision as number) < 1 ||
    typeof checkpoint.revision !== 'string' ||
    !/^[1-9][0-9]{0,18}$/.test(checkpoint.revision) ||
    BigInt(checkpoint.revision) > 9223372036854775807n ||
    checkpoint.appliedRevision !== checkpoint.revision
  )
    throw new BadRequestException('Invalid verified search restart checkpoint');
  return {
    format: 'book-search-restart-proof-v1',
    previousOwnerStopped: true,
    sourceCaptureVerified: true,
    issuedAt: value.issuedAt,
    expiresAt: value.expiresAt,
    sourceBooks: value.sourceBooks as number,
    sourceUpperBookId: value.sourceUpperBookId as number,
    checkpoint: {
      epoch: checkpoint.epoch,
      captureSession: checkpoint.captureSession,
      engineIdentity: checkpoint.engineIdentity,
      indexName: checkpoint.indexName,
      schemaRevision: checkpoint.schemaRevision as number,
      revision: BigInt(checkpoint.revision),
      appliedRevision: BigInt(checkpoint.revision),
    },
  };
}

export async function readBookSearchRestartProof(path: string): Promise<BookSearchRestartProof> {
  try {
    const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0));
    try {
      const info = await file.stat();
      if (!info.isFile() || info.size > 8192 || (process.platform !== 'win32' && (info.mode & 0o022) !== 0))
        throw new BadRequestException('Invalid verified search restart proof file');
      const data = Buffer.alloc(8193);
      let length = 0;
      while (length < data.length) {
        const { bytesRead } = await file.read(data, length, data.length - length, length);
        if (!bytesRead) break;
        length += bytesRead;
      }
      if (length > 8192) throw new BadRequestException('Invalid verified search restart proof file');
      return parseBookSearchRestartProof(JSON.parse(data.subarray(0, length).toString('utf8')));
    } finally {
      await file.close();
    }
  } catch {
    throw new ServiceUnavailableException('Verified search restart proof unavailable');
  }
}
