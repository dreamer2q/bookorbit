import { Inject, Injectable } from '@nestjs/common';
import type { ConfigType } from '@nestjs/config';
import { appConfig } from '../../../config/config';
import { BookSearchSyncRepository } from './book-search-sync.repository';
import { BookSearchDocumentProjector } from './book-search-document-projector';
import { ZincBookIndexWriter } from './zinc-book-index-writer';
import { BOOK_SEARCH_SCHEMA_REVISION } from './book-search-index-definition';
import { ZincBookIndexGeneration } from './zinc-book-index-generation';

@Injectable()
export class BookSearchSyncService {
  constructor(
    @Inject(appConfig.KEY) private readonly config: ConfigType<typeof appConfig>,
    private readonly queue: BookSearchSyncRepository,
    private readonly projector: BookSearchDocumentProjector,
    private readonly writer: ZincBookIndexWriter,
    private readonly generation: ZincBookIndexGeneration,
  ) {}

  async runOnce() {
    if (!this.config.bookSearch.enabled) return { phase: 'disabled' as const, acknowledged: 0 };
    return this.queue.withPendingBatch(
      this.config.bookSearch.workerBatchSize,
      async (tx, changes, epoch, state) => {
        if (state.engineIdentity) await this.generation.assertWorkerCheckpoint(state);
        await this.writer.applyAndVerify(await this.projector.project(tx, changes), epoch);
        if (state.engineIdentity) await this.generation.assertWorkerCheckpoint(state);
      },
      { indexName: this.config.bookSearch.index, schemaRevision: BOOK_SEARCH_SCHEMA_REVISION },
      async (_tx, state) => {
        if (state.engineIdentity) await this.generation.publishCheckpoint(state.epoch, state.revision, state.engineIdentity);
      },
    );
  }
}
