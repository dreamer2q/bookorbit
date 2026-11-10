import { Inject, Injectable, Optional } from '@nestjs/common';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';

import { DB } from '../../../db';
import * as schema from '../../../db/schema';
import { BookSearchCaptureService } from '../../../common/services/book-search-capture.service';
import { InlineEntityStrategy } from './inline-entity.strategy';

type Db = NodePgDatabase<typeof schema>;

@Injectable()
export class LanguageStrategy extends InlineEntityStrategy {
  readonly entityType = 'language' as const;
  protected readonly fieldName = 'language';
  protected readonly rawFieldName = 'language';

  constructor(@Inject(DB) db: Db, @Optional() searchCapture?: BookSearchCaptureService) {
    super(db, searchCapture);
  }
}
