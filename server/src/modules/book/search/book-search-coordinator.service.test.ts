import { Test } from '@nestjs/testing';
import { Logger, ServiceUnavailableException } from '@nestjs/common';
import { appConfig } from '../../../config/config';
import { BookSearchCoordinatorService } from './book-search-coordinator.service';
import { BookSearchBootstrapRepository } from './book-search-bootstrap.repository';
import { BookSearchSyncService } from './book-search-sync.service';
import { BookSearchSourceCountMismatch, ZincBookIndexGeneration } from './zinc-book-index-generation';

describe('BookSearchCoordinatorService', () => {
  let service: BookSearchCoordinatorService;
  let config: ReturnType<typeof appConfig>;
  const progress = { phase: 'seeding', epoch: 'epoch', seededCount: 1000n, estimatedBooks: 2000n, pruningComplete: true, orphanCursorBookId: 0 };
  const baseline = { initialize: vi.fn(), prepareEngine: vi.fn(), seedNextBatch: vi.fn(), finishIfCaughtUp: vi.fn(), pruneNextBatch: vi.fn() };
  const sync = { runOnce: vi.fn() };
  const generation = { prepare: vi.fn(), verifyComplete: vi.fn() };

  beforeEach(async () => {
    vi.useFakeTimers();
    vi.spyOn(Logger.prototype, 'log').mockImplementation(() => {});
    vi.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    config = appConfig();
    config.bookSearch.enabled = true;
    config.bookSearch.workerEnabled = true;
    baseline.initialize.mockReset().mockResolvedValue(progress);
    baseline.prepareEngine.mockReset().mockResolvedValue(progress);
    baseline.seedNextBatch.mockReset().mockResolvedValue(progress);
    baseline.finishIfCaughtUp.mockReset().mockResolvedValue({ ...progress, phase: 'indexed' });
    baseline.pruneNextBatch.mockReset();
    generation.verifyComplete.mockReset().mockResolvedValue(undefined);
    sync.runOnce.mockReset().mockResolvedValue({ phase: 'idle', acknowledged: 0 });
    const module = await Test.createTestingModule({
      providers: [
        BookSearchCoordinatorService,
        { provide: appConfig.KEY, useValue: config },
        { provide: BookSearchBootstrapRepository, useValue: baseline },
        { provide: BookSearchSyncService, useValue: sync },
        { provide: ZincBookIndexGeneration, useValue: generation },
      ],
    }).compile();
    service = module.get(BookSearchCoordinatorService);
  });
  afterEach(async () => {
    await service.onModuleDestroy();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('does no work unless both search and its worker are enabled', async () => {
    config.bookSearch.workerEnabled = false;
    service.onApplicationBootstrap();
    expect(await service.runOnce()).toEqual({ phase: 'disabled' });
    await vi.advanceTimersByTimeAsync(1000);
    expect(baseline.initialize).not.toHaveBeenCalled();
    config.bookSearch.workerEnabled = true;
    config.bookSearch.enabled = false;
    expect(await service.runOnce()).toEqual({ phase: 'disabled' });
  });

  it('seeds one bounded page and reports actual count separately from the estimate', async () => {
    expect(await service.runOnce()).toEqual({ phase: 'seeding', seeded: '1000', estimate: '2000' });
    expect(baseline.seedNextBatch).toHaveBeenCalledTimes(1);
    expect(sync.runOnce).not.toHaveBeenCalled();
    expect(baseline.finishIfCaughtUp).not.toHaveBeenCalled();
  });

  it('runs catch-up before final verification and does not repeat full counts while idle', async () => {
    baseline.prepareEngine.mockResolvedValue({ ...progress, phase: 'catchup' });
    sync.runOnce.mockResolvedValueOnce({ phase: 'applied', acknowledged: 100 });
    expect(await service.runOnce()).toEqual({ phase: 'applied', acknowledged: 100 });
    expect(baseline.finishIfCaughtUp).not.toHaveBeenCalled();
    expect(await service.runOnce()).toMatchObject({ phase: 'indexed' });
    baseline.prepareEngine.mockResolvedValue({ ...progress, phase: 'indexed' });
    expect(await service.runOnce()).toEqual({ phase: 'idle' });
    expect(baseline.finishIfCaughtUp).toHaveBeenCalledTimes(1);
  });

  it('verifies a completely projected source before skipping residual enumeration', async () => {
    baseline.prepareEngine.mockResolvedValue({ ...progress, phase: 'catchup', pruningComplete: false });
    baseline.finishIfCaughtUp.mockImplementationOnce(async (verify) => {
      await verify({}, progress, { epoch: 'epoch' });
      return { ...progress, phase: 'indexed' };
    });
    expect(await service.runOnce()).toMatchObject({ phase: 'indexed', residualVerification: 'complete-source-projection' });
    expect(baseline.finishIfCaughtUp).toHaveBeenCalledWith(expect.any(Function), 'complete-source-projection');
    expect(generation.verifyComplete).toHaveBeenCalledTimes(1);
    expect(baseline.pruneNextBatch).not.toHaveBeenCalled();
  });

  it('waits for the source projection gate instead of enumerating while work remains', async () => {
    baseline.prepareEngine.mockResolvedValue({ ...progress, phase: 'catchup', pruningComplete: false });
    baseline.finishIfCaughtUp.mockResolvedValueOnce({ ...progress, phase: 'catchup' }).mockResolvedValueOnce(null);
    expect(await service.runOnce()).toEqual({ phase: 'busy' });
    expect(await service.runOnce()).toEqual({ phase: 'busy' });
    expect(baseline.pruneNextBatch).not.toHaveBeenCalled();
  });

  it('enumerates real count discrepancies and resumes without repeating full counts for each page', async () => {
    baseline.prepareEngine.mockResolvedValue({ ...progress, phase: 'catchup', pruningComplete: false });
    baseline.finishIfCaughtUp.mockRejectedValueOnce(new BookSearchSourceCountMismatch('count mismatch'));
    baseline.pruneNextBatch.mockResolvedValueOnce({ ...progress, pruningComplete: false, orphanCursorBookId: 100 });
    expect(await service.runOnce()).toEqual({ phase: 'catchup', orphanCursor: 100 });
    baseline.prepareEngine.mockResolvedValue({ ...progress, phase: 'catchup', pruningComplete: false, orphanCursorBookId: 100 });
    baseline.pruneNextBatch.mockResolvedValueOnce({ ...progress, pruningComplete: true, orphanCursorBookId: 120 });
    expect(await service.runOnce()).toMatchObject({ phase: 'indexed' });
    expect(baseline.finishIfCaughtUp.mock.calls.map((args) => args[1])).toEqual(['complete-source-projection', undefined]);
    expect(baseline.pruneNextBatch).toHaveBeenCalledTimes(2);
  });

  it('propagates engine proof failures without attempting to prune a mismatched generation', async () => {
    baseline.prepareEngine.mockResolvedValue({ ...progress, phase: 'catchup', pruningComplete: false });
    baseline.finishIfCaughtUp.mockRejectedValueOnce(new ServiceUnavailableException('identity changed'));
    await expect(service.runOnce()).rejects.toThrow('identity changed');
    expect(baseline.pruneNextBatch).not.toHaveBeenCalled();
  });

  it('does not overlap a slow local tick or consume while another DB worker owns initialization', async () => {
    let release!: (value: typeof progress) => void;
    baseline.initialize.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = resolve;
        }),
    );
    const first = service.runOnce();
    expect(await service.runOnce()).toEqual({ phase: 'busy' });
    release(progress);
    await first;
    baseline.initialize.mockResolvedValueOnce(null);
    expect(await service.runOnce()).toEqual({ phase: 'busy' });
    expect(baseline.seedNextBatch).toHaveBeenCalledTimes(1);
  });

  it('backs off failures and resets the delay after a successful tick', async () => {
    baseline.initialize.mockRejectedValueOnce(new Error('temporary')).mockRejectedValueOnce(new Error('temporary'));
    service.onApplicationBootstrap();
    await vi.advanceTimersByTimeAsync(0);
    expect(baseline.initialize).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(999);
    expect(baseline.initialize).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(baseline.initialize).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(2000);
    expect(baseline.initialize).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(100);
    expect(baseline.initialize).toHaveBeenCalledTimes(4);
  });

  it('waits for an active batch during shutdown and schedules no further ticks', async () => {
    let release!: (value: typeof progress) => void;
    baseline.initialize.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = resolve;
        }),
    );
    service.onApplicationBootstrap();
    await vi.advanceTimersByTimeAsync(0);
    let ended = false;
    const stop = service.onModuleDestroy().then(() => {
      ended = true;
    });
    await Promise.resolve();
    expect(ended).toBe(false);
    release(progress);
    await stop;
    await vi.advanceTimersByTimeAsync(60_000);
    expect(baseline.initialize).toHaveBeenCalledTimes(1);
    expect(await service.runOnce()).toEqual({ phase: 'disabled' });
  });
});
