import type { Config } from 'drizzle-kit';

export default {
  schema: './src/db/book-search-schema.ts',
  out: './src/db/search-migrations',
  dialect: 'postgresql',
} satisfies Config;
