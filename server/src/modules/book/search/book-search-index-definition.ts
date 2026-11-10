import { BadRequestException } from '@nestjs/common';

export const BOOK_SEARCH_SCHEMA_REVISION = 4;

export function bookSearchIndexDefinition(name: string) {
  if (!/^[a-zA-Z0-9_-]{1,128}$/.test(name)) throw new BadRequestException('Invalid book search index name');
  return {
    name,
    storage_type: 'disk',
    shard_num: 1,
    mappings: {
      properties: {
        book_id: { type: 'numeric', index: true, store: true, sortable: true },
        library_id: { type: 'keyword', index: true, store: true },
        title_sort: { type: 'keyword', index: true, store: true, sortable: true },
        title_null: { type: 'numeric', index: true, store: true, sortable: true },
        search_text: { type: 'text', analyzer: 'gse_search', index: true, store: true },
        native_characters: { type: 'keyword', index: true, store: true },
        native_bigrams: { type: 'text', analyzer: 'whitespace', index: true, store: true },
        sync_epoch: { type: 'keyword', index: true, store: true },
        sync_revision: { type: 'keyword', index: true, store: true },
        sync_deleted: { type: 'boolean', index: true, store: true },
        sync_hash: { type: 'keyword', index: false, store: true },
      },
    },
  };
}
