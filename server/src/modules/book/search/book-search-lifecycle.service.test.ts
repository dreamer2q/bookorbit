import { Test } from '@nestjs/testing';
import { Logger } from '@nestjs/common';
import { appConfig } from '../../../config/config';
import { BookSearchLifecycleService } from './book-search-lifecycle.service';
import { BookSearchBootstrapRepository } from './book-search-bootstrap.repository';
import { readBookSearchRestartProof, type BookSearchRestartProof } from './book-search-restart-proof';
import { ZincBookIndexGeneration } from './zinc-book-index-generation';

vi.mock('./book-search-restart-proof', async (original) => ({
  ...(await original<typeof import('./book-search-restart-proof')>()),
  readBookSearchRestartProof: vi.fn(),
}));

describe('BookSearchLifecycleService', () => {
  let service: BookSearchLifecycleService;
  let config: ReturnType<typeof appConfig>;
  const baseline = { disableServing: vi.fn(), initialize: vi.fn(), startReconciliation: vi.fn(), resumeVerifiedCheckpoint: vi.fn() };
  const generation = { verifyComplete: vi.fn() };
  beforeEach(async () => {
    vi.spyOn(Logger.prototype, 'log').mockImplementation(() => {});
    vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    config = appConfig();
    config.bookSearch.enabled = true;
    baseline.disableServing.mockReset().mockResolvedValue(undefined);
    baseline.initialize.mockReset().mockResolvedValue({});
    baseline.startReconciliation.mockReset().mockResolvedValue({});
    baseline.resumeVerifiedCheckpoint.mockReset().mockResolvedValue(true);
    generation.verifyComplete.mockReset().mockResolvedValue(undefined);
    vi.mocked(readBookSearchRestartProof).mockReset();
    const module = await Test.createTestingModule({
      providers: [
        BookSearchLifecycleService,
        { provide: appConfig.KEY, useValue: config },
        { provide: BookSearchBootstrapRepository, useValue: baseline },
        { provide: ZincBookIndexGeneration, useValue: generation },
      ],
    }).compile();
    service = module.get(BookSearchLifecycleService);
  });
  afterEach(() => vi.restoreAllMocks());

  it('invalidates stored readiness when starting with capture disabled', async () => {
    config.bookSearch.enabled = false;
    await service.onModuleInit();
    expect(baseline.disableServing).toHaveBeenCalledTimes(1);
    expect(baseline.initialize).not.toHaveBeenCalled();
    expect(service.isCurrent('old-session')).toBe(false);
  });

  it('accepts its capture session only after the database activation committed', async () => {
    expect(service.isCurrent('old-session')).toBe(false);
    baseline.startReconciliation.mockResolvedValueOnce(null);
    expect(await service.ensureStarted()).toBe(false);
    const session = baseline.startReconciliation.mock.calls[0][0];
    expect(service.isCurrent(session)).toBe(false);
    expect(await service.ensureStarted()).toBe(true);
    expect(service.isCurrent(session)).toBe(true);
    expect(service.isCurrent('other-session')).toBe(false);
    await service.ensureStarted();
    expect(baseline.startReconciliation).toHaveBeenCalledTimes(2);
  });

  it('keeps application startup available after a failed activation and allows a later retry', async () => {
    baseline.initialize.mockRejectedValueOnce(new Error('database unavailable'));
    await expect(service.onModuleInit()).resolves.toBeUndefined();
    expect(service.isCurrent('old-session')).toBe(false);
    expect(await service.ensureStarted()).toBe(true);
  });

  it('hands over only a committed explicit proof and never disables or reseeds its verified baseline', async () => {
    config.bookSearch.restartProofPath = '/private/restart-proof.json';
    const proof = { checkpoint: { indexName: config.bookSearch.index } } as BookSearchRestartProof;
    vi.mocked(readBookSearchRestartProof).mockResolvedValue(proof);
    baseline.resumeVerifiedCheckpoint.mockResolvedValueOnce(null).mockResolvedValueOnce(true);
    expect(await service.ensureStarted()).toBe(false);
    const session = baseline.resumeVerifiedCheckpoint.mock.calls[0][0];
    expect(service.isCurrent(session)).toBe(false);
    expect(await service.ensureStarted()).toBe(true);
    expect(service.isCurrent(session)).toBe(true);
    expect(baseline.disableServing).not.toHaveBeenCalled();
    expect(baseline.startReconciliation).not.toHaveBeenCalled();
    const verify = baseline.resumeVerifiedCheckpoint.mock.calls[1][2];
    const transaction = {},
      checkpoint = {};
    await verify(transaction, checkpoint);
    expect(generation.verifyComplete).toHaveBeenCalledWith(transaction, checkpoint);
    await service.ensureStarted();
    expect(readBookSearchRestartProof).toHaveBeenCalledTimes(2);
  });

  it('leaves an invalid explicit handoff uninitialized and does not silently start a full reconciliation', async () => {
    config.bookSearch.restartProofPath = '/private/restart-proof.json';
    vi.mocked(readBookSearchRestartProof).mockRejectedValue(new Error('private proof rejected'));
    await expect(service.onModuleInit()).resolves.toBeUndefined();
    expect(service.isCurrent('old-session')).toBe(false);
    expect(baseline.disableServing).not.toHaveBeenCalled();
    expect(baseline.startReconciliation).not.toHaveBeenCalled();
    expect(JSON.stringify(vi.mocked(Logger.prototype.warn).mock.calls)).not.toContain('private proof rejected');
  });

  it('rejects a proof for another configured index before consuming it', async () => {
    config.bookSearch.restartProofPath = '/private/restart-proof.json';
    vi.mocked(readBookSearchRestartProof).mockResolvedValue({ checkpoint: { indexName: 'other' } } as BookSearchRestartProof);
    await expect(service.ensureStarted()).rejects.toThrow('index changed');
    expect(baseline.resumeVerifiedCheckpoint).not.toHaveBeenCalled();
    expect(baseline.disableServing).not.toHaveBeenCalled();
  });
});
