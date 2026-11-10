import { Inject, Injectable, Logger, OnModuleInit, Optional, ServiceUnavailableException } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { randomUUID } from 'node:crypto';
import { appConfig } from '../../../config/config';
import { BookSearchBootstrapRepository } from './book-search-bootstrap.repository';
import { readBookSearchRestartProof } from './book-search-restart-proof';
import { ZincBookIndexGeneration } from './zinc-book-index-generation';

@Injectable()
export class BookSearchLifecycleService implements OnModuleInit {
  private readonly logger = new Logger(BookSearchLifecycleService.name);
  private readonly sessionId = randomUUID();
  private initialized = false;

  constructor(
    @Inject(appConfig.KEY) private readonly config: ConfigType<typeof appConfig>,
    private readonly baseline: BookSearchBootstrapRepository,
    @Optional() private readonly generation?: ZincBookIndexGeneration,
  ) {}

  async onModuleInit(): Promise<void> {
    try {
      if (!this.config.bookSearch.enabled) {
        await this.baseline.disableServing();
        return;
      }
      await this.ensureStarted();
    } catch {
      this.logger.warn(
        '[book.search_activation] [fail] errorClass=ActivationUnavailable error="Index readiness not established" - native search remains active',
      );
    }
  }

  isCurrent(sessionId: string | null): boolean {
    return this.config.bookSearch.enabled && this.initialized && sessionId === this.sessionId;
  }

  async ensureStarted(): Promise<boolean> {
    if (!this.config.bookSearch.enabled) return false;
    if (this.initialized) return true;
    const started = Date.now();
    this.logger.log('[book.search_activation] [start] - source reconciliation activation started');
    if (this.config.bookSearch.restartProofPath) {
      if (!this.generation) throw new ServiceUnavailableException('Verified search restart provider unavailable');
      const proof = await readBookSearchRestartProof(this.config.bookSearch.restartProofPath);
      if (proof.checkpoint.indexName !== this.config.bookSearch.index) throw new ServiceUnavailableException('Verified search restart index changed');
      if (!(await this.baseline.resumeVerifiedCheckpoint(this.sessionId, proof, (tx, state) => this.generation!.verifyComplete(tx, state))))
        return false;
      this.initialized = true;
      this.logger.log(`[book.search_activation] [end] durationMs=${Date.now() - started} mode=verified-restart - source capture session handed over`);
      return true;
    }
    await this.baseline.disableServing();
    if (!(await this.baseline.initialize(this.config.bookSearch.index))) return false;
    if (!(await this.baseline.startReconciliation(this.sessionId))) return false;
    this.initialized = true;
    this.logger.log(`[book.search_activation] [end] durationMs=${Date.now() - started} - source reconciliation activation committed`);
    return true;
  }
}
