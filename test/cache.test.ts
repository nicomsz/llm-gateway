import { afterEach, describe, expect, test } from 'vitest';
import { MemoryCache } from '../src/gateway/cache.js';
import { MemoryTokenBucket } from '../src/gateway/tokenBucket.js';
import { ACME, BETA, BODY, readStream, send, setup, waitFor } from './helpers.js';

let env: Awaited<ReturnType<typeof setup>>;
afterEach(() => env?.close());
const CACHE = { 'x-api-key': ACME, 'x-gateway-cache': '60' };

describe('cache', () => {
  test('o mesmo request duas vezes: o provedor é chamado uma vez só', async () => {
    env = await setup([{ ttftMs: 80 }]);
    const first = await send(env.gateway.url, BODY, CACHE);
    expect(first.headers.get('x-gateway-cache')).toBe('miss');
    const firstBody = await first.text();
    await waitFor(() => env.requests().length === 1);

    const second = await send(env.gateway.url, BODY, CACHE);
    expect(second.headers.get('x-gateway-cache')).toBe('hit');
    expect(await second.text()).toBe(firstBody);
    expect(env.mocks[0]!.stats().requests).toBe(1);

    await waitFor(() => env.requests().length === 2);
    const [miss, hit] = env.requests();
    expect(miss).toMatchObject({ cache: 'miss' });
    expect(hit).toMatchObject({ cache: 'hit', costUsd: 0, inputTokens: 0 });
    expect(hit!.savedUsd).toBe(miss!.costUsd);
    expect(hit!.ms).toBeLessThan(miss!.ms);
  });

  test('sem o header, nunca usa o cache', async () => {
    env = await setup([{}]);
    await send(env.gateway.url);
    await send(env.gateway.url);
    expect(env.mocks[0]!.stats().requests).toBe(2);
  });

  test('tenants diferentes não compartilham resposta', async () => {
    env = await setup([{}]);
    await send(env.gateway.url, BODY, CACHE);
    await waitFor(() => env.requests().length === 1);
    const other = await send(env.gateway.url, BODY, { 'x-api-key': BETA, 'x-gateway-cache': '60' });
    expect(other.headers.get('x-gateway-cache')).toBe('miss');
    expect(env.mocks[0]!.stats().requests).toBe(2);
  });

  test('depois do prazo, vai ao provedor de novo', async () => {
    let now = 0;
    env = await setup([{}], {}, { cache: new MemoryCache(() => now) });
    await send(env.gateway.url, BODY, { ...CACHE, 'x-gateway-cache': '1' });
    await waitFor(() => env.requests().length === 1);
    now = 2_000;
    await send(env.gateway.url, BODY, { ...CACHE, 'x-gateway-cache': '1' });
    expect(env.mocks[0]!.stats().requests).toBe(2);
  });

  test('erro do provedor não é guardado', async () => {
    env = await setup([{}], { retry: { maxAttempts: 1 }, breaker: { enabled: false } });
    env.mocks[0]!.setMode('down');
    expect((await send(env.gateway.url, BODY, CACHE)).status).toBe(529);
    env.mocks[0]!.setMode('ok');
    const response = await send(env.gateway.url, BODY, CACHE);
    expect(response.status).toBe(200);
    expect(response.headers.get('x-gateway-cache')).toBe('miss');
  });

  test('resposta guardada, pedida com streaming, volta como SSE válido', async () => {
    env = await setup([{}]);
    const original = await (await send(env.gateway.url, BODY, CACHE)).json();
    await waitFor(() => env.requests().length === 1);
    const response = await send(env.gateway.url, { ...BODY, stream: true }, CACHE);
    expect(response.headers.get('x-gateway-cache')).toBe('hit');
    expect(response.headers.get('content-type')).toBe('text/event-stream');
    const { events, text } = await readStream(response);
    expect(events[0]!.event).toBe('message_start');
    expect(events.at(-1)!.event).toBe('message_stop');
    expect(text).toBe(original.content[0].text);
  });

  test('streaming completo é guardado; o seguinte, sem streaming, vem do cache', async () => {
    env = await setup([{}]);
    const { text } = await readStream(await send(env.gateway.url, { ...BODY, stream: true }, CACHE));
    await waitFor(() => env.requests().length === 1);
    const response = await send(env.gateway.url, BODY, CACHE);
    expect(response.headers.get('x-gateway-cache')).toBe('hit');
    const message = await response.json();
    expect(message.content[0].text).toBe(text);
    expect(message.usage).toEqual({ input_tokens: env.requests()[0]!.inputTokens, output_tokens: 3 });
  });

  test('streaming cancelado no meio não é guardado', async () => {
    env = await setup([{ ttftMs: 5, chunkMs: 50, chunks: 30 }]);
    const controller = new AbortController();
    const response = await send(env.gateway.url, { ...BODY, stream: true }, CACHE, { signal: controller.signal });
    setTimeout(() => controller.abort(), 120);
    await readStream(response).catch(() => {});
    await waitFor(() => env.requests().length === 1);
    const again = await send(env.gateway.url, BODY, CACHE);
    expect(again.headers.get('x-gateway-cache')).toBe('miss');
  });

  test('10 requests idênticos ao mesmo tempo: o provedor recebe 1 chamada', async () => {
    env = await setup([{ ttftMs: 100 }]);
    const responses = await Promise.all(Array.from({ length: 10 }, () => send(env.gateway.url, BODY, CACHE)));
    expect(responses.map((r) => r.status)).toEqual(Array(10).fill(200));
    const bodies = await Promise.all(responses.map((r) => r.text()));
    expect(new Set(bodies).size).toBe(1);
    expect(env.mocks[0]!.stats().requests).toBe(1);
    expect(responses.filter((r) => r.headers.get('x-gateway-cache') === 'hit')).toHaveLength(9);
  });

  test('resposta do cache não desconta tokens do tenant', async () => {
    const bucket = new MemoryTokenBucket(() => 0);
    env = await setup([{}], {}, { rateLimit: bucket });
    await send(env.gateway.url, BODY, CACHE);
    await waitFor(() => env.requests().length === 1);
    const before = (await bucket.take('rl:acme', 1_000_000, 0, 0)).remaining;
    for (let i = 0; i < 5; i++) await send(env.gateway.url, BODY, CACHE);
    expect((await bucket.take('rl:acme', 1_000_000, 0, 0)).remaining).toBe(before);
  });
});
