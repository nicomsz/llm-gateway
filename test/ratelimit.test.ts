import { afterEach, describe, expect, test } from 'vitest';
import { MemoryTokenBucket } from '../src/gateway/tokenBucket.js';
import { ACME, BETA, BODY, readStream, send, setup, waitFor } from './helpers.js';

let env: Awaited<ReturnType<typeof setup>>;
afterEach(() => env?.close());

const tenants = (acme: number) => ({ [ACME]: { name: 'acme', tokensPerMinute: acme }, [BETA]: { name: 'beta', tokensPerMinute: 1_000_000 } });

describe('rate limit por tokens', () => {
  test('cota esgotada: 429 com retry-after, e o provedor nem é chamado', async () => {
    let now = 0;
    env = await setup([{}], { tenants: tenants(2_000) }, { rateLimit: new MemoryTokenBucket(() => now) });
    const big = { ...BODY, max_tokens: 900 };
    // cada request reserva ~925 e usa poucos tokens, então a devolução permite vários; forçamos com max_tokens alto e sem devolução relevante
    const first = await send(env.gateway.url, { ...big, max_tokens: 1_900 });
    expect(first.status).toBe(200);
    await waitFor(() => env.requests().length === 1);
    // depois da devolução o saldo voltou para perto do teto; dois requests grandes simultâneos disputam o saldo
    const [a, b] = await Promise.all([send(env.gateway.url, { ...big, max_tokens: 1_500 }), send(env.gateway.url, { ...big, max_tokens: 1_500 })]);
    const statuses = [a.status, b.status].sort();
    expect(statuses).toEqual([200, 429]);
    const denied = a.status === 429 ? a : b;
    expect(Number(denied.headers.get('retry-after'))).toBeGreaterThan(0);
    expect((await denied.json()).error.type).toBe('rate_limit_error');
    expect(env.mocks[0]!.stats().requests).toBe(2);
  });

  test('a diferença entre a reserva e o uso real volta ao saldo', async () => {
    const bucket = new MemoryTokenBucket(() => 0); // relógio parado: sem recarga
    env = await setup([{}], { tenants: tenants(2_000) }, { rateLimit: bucket });
    for (let i = 0; i < 5; i++) {
      expect((await send(env.gateway.url, { ...BODY, max_tokens: 1_500 })).status).toBe(200);
      await waitFor(() => env.requests().length === i + 1);
    }
    // sem a devolução, o segundo request já seria negado (2 x ~1.525 > 2.000)
    const used = env.requests().reduce((sum, log) => sum + log.inputTokens + log.outputTokens, 0);
    expect((await bucket.take('rl:acme', 2_000, 0, 0)).remaining).toBe(2_000 - used);
  });

  test('um tenant estourar não afeta o outro', async () => {
    env = await setup([{ ttftMs: 100 }], { tenants: tenants(1_200) }, { rateLimit: new MemoryTokenBucket(() => 0) });
    const acme = await Promise.all(Array.from({ length: 5 }, () => send(env.gateway.url, { ...BODY, max_tokens: 1_000 })));
    expect(acme.filter((r) => r.status === 429)).toHaveLength(4);
    const beta = await Promise.all(Array.from({ length: 5 }, () => send(env.gateway.url, BODY, { 'x-api-key': BETA })));
    expect(beta.map((r) => r.status)).toEqual([200, 200, 200, 200, 200]);
  });

  test('request maior que a cota inteira: 400, porque esperar não resolve', async () => {
    env = await setup([{}], { tenants: tenants(500) });
    const response = await send(env.gateway.url, { ...BODY, max_tokens: 5_000 });
    expect(response.status).toBe(400);
    expect(response.headers.get('retry-after')).toBeNull();
  });

  test('streaming cancelado devolve o que não foi usado', async () => {
    const bucket = new MemoryTokenBucket(() => 0);
    env = await setup([{ ttftMs: 5, chunkMs: 50, chunks: 50 }], { tenants: tenants(2_000) }, { rateLimit: bucket });
    const controller = new AbortController();
    const response = await send(env.gateway.url, { ...BODY, max_tokens: 1_500, stream: true }, undefined, { signal: controller.signal });
    setTimeout(() => controller.abort(), 120);
    await readStream(response).catch(() => {});
    await waitFor(() => env.requests().length === 1);
    const log = env.requests()[0]!;
    expect(log.canceled).toBe(true);
    expect((await bucket.take('rl:acme', 2_000, 0, 0)).remaining).toBe(2_000 - log.inputTokens - log.outputTokens);
  });

  test('store do rate limit fora do ar: o request passa (fail open)', async () => {
    const broken = { take: async () => Promise.reject(new Error('redis fora do ar')), refund: async () => {} };
    env = await setup([{}], {}, { rateLimit: broken });
    expect((await send(env.gateway.url)).status).toBe(200);
  });
});
