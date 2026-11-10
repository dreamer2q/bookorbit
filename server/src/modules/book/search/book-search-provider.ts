export interface BookIndexRequest {
  // Whitespace, case and accents are normalized by the source database.
  text: string;
  libraryIds: number[];
  offset: number;
  size: number;
  titleDirection: 'asc' | 'desc';
  // Engine relevance is opt-in and does not reproduce PostgreSQL similarity scores.
  relevance?: boolean;
  epoch?: string;
  // Internal bounded intersection with a page in the source database's title order.
  candidateIds?: number[];
  // Internal collection for a caller that restores the native database order.
  unordered?: boolean;
}

export interface BookIndexPage {
  ids: number[];
  total: number;
}

export abstract class BookSearchProvider {
  abstract search(request: BookIndexRequest): Promise<BookIndexPage>;
}
