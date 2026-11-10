import { BadRequestException } from '@nestjs/common';

const characterToken = (value: string) => `u${value.codePointAt(0)!.toString(16)}`;
const pairToken = (left: string, right: string) => `b${left.codePointAt(0)!.toString(16)}_${right.codePointAt(0)!.toString(16)}`;

export function literalSearchFields(values: readonly string[]): { native_characters: string[]; native_bigrams: string } {
  const characters = new Set<string>();
  const sequences: string[] = [];
  for (const value of new Set(values)) {
    const points = Array.from(value);
    for (const point of points) characters.add(characterToken(point));
    const pairs: string[] = [];
    for (let index = 1; index < points.length; index++) pairs.push(pairToken(points[index - 1], points[index]));
    if (pairs.length) sequences.push(pairs.join(' '));
  }
  // A phrase must not bridge two metadata fields or two different people/series names.
  return { native_characters: [...characters], native_bigrams: sequences.join(' z_boundary ') };
}

export function literalSearchQuery(value: string): Record<string, unknown> {
  const points = Array.from(value);
  if (!points.length) throw new BadRequestException('Empty literal search query');
  if (points.length === 1) return { term: { native_characters: characterToken(points[0]) } };
  const pairs: string[] = [];
  for (let index = 1; index < points.length; index++) pairs.push(pairToken(points[index - 1], points[index]));
  return { match_phrase: { native_bigrams: { query: pairs.join(' '), analyzer: 'whitespace' } } };
}
