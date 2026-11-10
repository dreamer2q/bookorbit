import { ServiceUnavailableException } from '@nestjs/common';

export interface ZincConnectionSettings {
  endpoint: string;
  index: string;
  username: string;
  password: string;
  timeoutMs: number;
}

export function zincIndexPath(settings: ZincConnectionSettings, suffix: string): string {
  if (!/^[a-zA-Z0-9_-]{1,128}$/.test(settings.index)) throw new ServiceUnavailableException('Invalid book search configuration');
  return `/es/${settings.index}/${suffix}`;
}

export async function requestZinc<T>(
  settings: ZincConnectionSettings,
  path: string,
  body: string,
  contentType = 'application/json',
  outerSignal?: AbortSignal,
  options: { method?: 'GET' | 'POST'; allowNotFound?: boolean } = {},
): Promise<T> {
  const endpoint = new URL(settings.endpoint);
  if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
    throw new ServiceUnavailableException('Invalid book search configuration');
  }
  endpoint.pathname = `${endpoint.pathname.replace(/\/$/, '')}${path}`;
  const timeout = AbortSignal.timeout(Math.min(settings.timeoutMs, 5000));
  const response = await fetch(endpoint, {
    method: options.method ?? 'POST',
    headers: { 'Content-Type': contentType, Authorization: `Basic ${Buffer.from(`${settings.username}:${settings.password}`).toString('base64')}` },
    signal: outerSignal ? AbortSignal.any([outerSignal, timeout]) : timeout,
    body: options.method === 'GET' ? undefined : body,
  });
  if (response.status === 404 && options.allowNotFound) {
    await response.body?.cancel();
    return null as T;
  }
  if (!response.ok || !response.body) throw new ServiceUnavailableException('Book search provider unavailable');
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      bytes += next.value.byteLength;
      if (bytes > 1024 * 1024) throw new ServiceUnavailableException('Book search response exceeded its bound');
      chunks.push(next.value);
    }
  } finally {
    await reader.cancel();
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8')) as T;
}
