import { describe, expect, test } from 'vitest';
import { MemoryCache, cacheKey } from '../src/gateway/cache.js';

const body = { model: 'm', max_tokens: 10, messages: [{ role: 'user', content: 'oi' }] };

describe('chave do cache', () => {
  test('a ordem das chaves do JSON não muda a chave', () => {
    const reordered = { messages: [{ content: 'oi', role: 'user' }], max_tokens: 10, model: 'm' };
    expect(cacheKey('acme', reordered)).toBe(cacheKey('acme', body));
  });

  test('stream true e false dão a mesma chave', () => {
    expect(cacheKey('acme', { ...body, stream: true })).toBe(cacheKey('acme', { ...body, stream: false }));
    expect(cacheKey('acme', { ...body, stream: true })).toBe(cacheKey('acme', body));
  });

  test('tenants diferentes dão chaves diferentes', () => {
    expect(cacheKey('acme', body)).not.toBe(cacheKey('beta', body));
  });

  test('uma letra diferente na mensagem muda a chave', () => {
    expect(cacheKey('acme', { ...body, messages: [{ role: 'user', content: 'oI' }] })).not.toBe(cacheKey('acme', body));
  });

  test('cache em memória respeita o prazo', async () => {
    let now = 0;
    const cache = new MemoryCache(() => now);
    await cache.set('k', 'v', 1);
    expect(await cache.get('k')).toBe('v');
    now = 1_001;
    expect(await cache.get('k')).toBeNull();
  });
});
