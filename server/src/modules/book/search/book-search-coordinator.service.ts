import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy, Optional } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { appConfig } from '../../../config/config';
import { BookSearchBootstrapRepository } from './book-search-bootstrap.repository';
import { BookSearchSyncService } from './book-search-sync.service';
import { BookSearchSourceCountMismatch, ZincBookIndexGeneration } from './zinc-book-index-generation';
import { BookSearchLifecycleService } from './book-search-lifecycle.service';

@Injectable()
export class BookSearchCoordinatorService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(BookSearchCoordinatorService.name);
  private timer?: ReturnType<typeof setTimeout>;
  private inFlight?: Promise<void>;
  private stopped = false;
  private executing = false;
  private failures = 0;
  private lastPhase = '';
  private lastLogAt = 0;

  constructor(
    @Inject(appConfig.KEY) private readonly config: ConfigType<typeof appConfig>,
    private readonly baseline: BookSearchBootstrapRepository,
    private readonly sync: BookSearchSyncService,
    private readonly generation: ZincBookIndexGeneration,
    @Optional() private readonly lifecycle?: BookSearchLifecycleService,
  ) {}

  onApplicationBootstrap(): void {
    if (!this.config.bookSearch.enabled || !this.config.bookSearch.workerEnabled) return;
    this.stopped = false;
    this.schedule(0);
  }

  async onModuleDestroy(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    await this.inFlight;
  }

  async runOnce() {
    if (this.stopped || !this.config.bookSearch.enabled || !this.config.bookSearch.workerEnabled) return { phase: 'disabled' as const };
    if (this.executing) return { phase: 'busy' as const };
    this.executing = true;
    try {
      if (this.lifecycle && !(await this.lifecycle.ensureStarted())) return { phase: 'busy' as const };
      if (!(await this.baseline.initialize(this.config.bookSearch.index))) return { phase: 'busy' as const };
      const progress = await this.baseline.prepareEngine((row, identity) => this.generation.prepare(row.epoch, identity));
      if (!progress) return { phase: 'busy' as const };
      if (progress.phase === 'seeding') {
        const page = await this.baseline.seedNextBatch();
        return page
          ? { phase: page.phase, seeded: page.seededCount.toString(), estimate: page.estimatedBooks.toString() }
          : { phase: 'busy' as const };
      }
      const result = await this.sync.runOnce();
      if (result.phase !== 'idle') return result;
      // Full counts are a one-time baseline acceptance gate, not a query on every idle tick.
      if (progress.phase === 'indexed') return { phase: 'idle' as const };
      if (!progress.pruningComplete) {
        if (progress.orphanCursorBookId === 0) {
          try {
            // Every source ID has been projected and verified before queue ACK.
            // With this worker lock and an empty queue, equality of live/source
            // counts proves there is no extra document. Avoid repeatedly sorting
            // the full engine index when there is nothing to remove.
            const complete = await this.baseline.finishIfCaughtUp(
              (tx, _row, state) => this.generation.verifyComplete(tx, state),
              'complete-source-projection',
            );
            if (!complete || complete.phase !== 'indexed') return { phase: 'busy' as const };
            return {
              phase: complete.phase,
              seeded: complete.seededCount.toString(),
              estimate: complete.estimatedBooks.toString(),
              residualVerification: 'complete-source-projection' as const,
            };
          } catch (error) {
            if (!(error instanceof BookSearchSourceCountMismatch)) throw error;
          }
        }
        const page = await this.baseline.pruneNextBatch((cursor, state) => this.generation.readLiveIds(cursor, state));
        if (!page) return { phase: 'busy' as const };
        if (!page.pruningComplete) return { phase: 'catchup' as const, orphanCursor: page.orphanCursorBookId };
      }
      const complete = await this.baseline.finishIfCaughtUp((tx, _row, state) => this.generation.verifyComplete(tx, state));
      return complete
        ? { phase: complete.phase, seeded: complete.seededCount.toString(), estimate: complete.estimatedBooks.toString() }
        : { phase: 'busy' as const };
    } finally {
      this.executing = false;
    }
  }

  private schedule(ms: number): void {
    if (this.stopped) return;
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.inFlight = this.tick();
    }, ms);
    this.timer.unref();
  }

  private async tick(): Promise<void> {
    const started = Date.now();
    const report = !this.lastPhase || started - this.lastLogAt >= 30_000;
    if (report) this.logger.log('[book.search_sync] [start] - bounded index maintenance tick started');
    let wait = this.config.bookSearch.workerIntervalMs;
    try {
      const result = await this.runOnce();
      this.failures = 0;
      if (result.phase === 'disabled') return;
      if (result.phase === 'idle' || result.phase === 'busy') wait = Math.max(wait, 1000);
      if (report) {
        const details =
          'seeded' in result
            ? ` seeded=${result.seeded} estimate=${result.estimate}`
            : 'acknowledged' in result
              ? ` acknowledged=${result.acknowledged}`
              : '';
        this.logger.log(
          `[book.search_sync] [end] durationMs=${Date.now() - started} phase=${result.phase}${details} - bounded index maintenance tick completed`,
        );
        this.lastLogAt = Date.now();
      }
      this.lastPhase = result.phase;
    } catch (error) {
      wait = Math.min(60_000, 1000 * 2 ** Math.min(this.failures++, 6));
      const errorClass = error instanceof Error ? error.name.replace(/[^a-zA-Z0-9_]/g, '') : 'Error';
      this.logger.warn(
        `[book.search_sync] [fail] durationMs=${Date.now() - started} retryMs=${wait} errorClass=${errorClass} error="Index maintenance failed; durable progress retained" - retry scheduled`,
      );
    } finally {
      if (!this.stopped && this.config.bookSearch.enabled && this.config.bookSearch.workerEnabled) this.schedule(wait);
    }
  }
}
