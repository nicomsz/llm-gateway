import { afterEach, describe, expect, test } from 'vitest';
import { BODY, readStream, send, setup, sleep, waitFor } from './helpers.js';

let env: Awaited<ReturnType<typeof setup>>;
afterEach(() => env?.close());
const STREAM = { ...BODY, stream: true };

describe('failover', () => {
  test('um provedor fora do ar: 20 requests, 20 respostas 200', async () => {
    env = await setup([{}, {}, {}], { breaker: { enabled: false }, strategy: 'round-robin' });
    env.mocks[2]!.setMode('down');
    const responses = await Promise.all(Array.from({ length: 20 }, () => send(env.gateway.url)));
    expect(responses.map((r) => r.status)).toEqual(Array(20).fill(200));

    await waitFor(() => env.requests().length === 20);
    const withFailover = env.requests().filter((log) => log.attempts.length > 1);
    expect(withFailover.length).toBeGreaterThan(0);
    expect(withFailover[0]!.attempts[0]).toMatchObject({ upstream: 'mock-c', status: 529 });
    expect(withFailover.every((log) => log.upstream !== 'mock-c')).toBe(true);
  });

  test('com o failover desligado, o mesmo cenário devolve erros', async () => {
    env = await setup([{}, {}, {}], { breaker: { enabled: false }, strategy: 'round-robin', retry: { maxAttempts: 1 } });
    env.mocks[2]!.setMode('down');
    const responses = await Promise.all(Array.from({ length: 21 }, () => send(env.gateway.url)));
    expect(responses.filter((r) => r.status === 529)).toHaveLength(7); // um terço foi para o provedor caído
  });

  test('todos fora do ar: devolve o erro do provedor depois de 3 tentativas', async () => {
    env = await setup([{}, {}, {}], { breaker: { enabled: false } });
    for (const mock of env.mocks) mock.setMode('down');
    const response = await send(env.gateway.url);
    expect(response.status).toBe(529);
    expect((await response.json()).error.type).toBe('overloaded_error');
    await waitFor(() => env.requests().length === 1);
    expect(env.requests()[0]!.attempts.map((a: any) => a.upstream).sort()).toEqual(['mock-a', 'mock-b', 'mock-c']);
  });

  test('streaming: falha antes do primeiro byte troca de provedor sem o cliente perceber', async () => {
    env = await setup([{}, {}], { breaker: { enabled: false }, strategy: 'round-robin' });
    env.mocks[0]!.setMode('down');
    const response = await send(env.gateway.url, STREAM);
    const { events, text } = await readStream(response);
    expect(response.status).toBe(200);
    expect(text).toContain('mock-b');
    expect(events.at(-1)!.event).toBe('message_stop');
  });

  test('streaming: falha depois de enviar pedaços não gera nova tentativa', async () => {
    env = await setup([{ ttftMs: 10, chunkMs: 60, chunks: 10 }, {}], { breaker: { enabled: false }, strategy: 'round-robin' });
    const response = await send(env.gateway.url, STREAM);
    setTimeout(() => env.mocks[0]!.setMode('down'), 150);
    const { events, text } = await readStream(response);
    expect(events.at(-1)!.event).toBe('error');
    expect(text).not.toContain('mock-b');
    await waitFor(() => env.requests().length === 1);
    expect(env.requests()[0]!.attempts).toHaveLength(1);
    expect(env.mocks[1]!.stats().requests).toBe(0);
  });

  test('menos ocupado: o provedor lento recebe bem menos que um terço', async () => {
    env = await setup([{ ttftMs: 5 }, { ttftMs: 300 }, { ttftMs: 5 }], { strategy: 'least-in-flight' });
    let next = 0;
    const worker = async () => {
      while (next++ < 60) await send(env.gateway.url);
    };
    await Promise.all(Array.from({ length: 10 }, worker));
    await waitFor(() => env.requests().length === 60);
    const toSlow = env.requests().filter((log) => log.upstream === 'mock-b').length;
    expect(toSlow).toBeLessThan(10); // um terço seria 20
  });
});

describe('circuit breaker no gateway', () => {
  test('depois de N falhas, o provedor para de receber tentativas', async () => {
    env = await setup([{}, {}], { strategy: 'round-robin', breaker: { enabled: true, failureThreshold: 3, openMs: 60_000 } });
    env.mocks[1]!.setMode('down');
    for (let i = 0; i < 20; i++) expect((await send(env.gateway.url)).status).toBe(200);
    expect(env.mocks[1]!.stats().requests).toBe(3);
    expect(env.gateway.status().upstreams[1]).toMatchObject({ name: 'mock-b', breaker: 'open' });
    expect(env.logger.entries.filter((e) => e.event === 'breaker')).toMatchObject([{ upstream: 'mock-b', from: 'closed', to: 'open' }]);
  });

  test('todos os circuitos abertos: 503 na hora, com retry-after', async () => {
    env = await setup([{}], { breaker: { enabled: true, failureThreshold: 2, openMs: 5_000 }, retry: { maxAttempts: 1 } });
    env.mocks[0]!.setMode('down');
    await send(env.gateway.url);
    await send(env.gateway.url);
    const start = Date.now();
    const response = await send(env.gateway.url);
    expect(response.status).toBe(503);
    expect(Number(response.headers.get('retry-after'))).toBeGreaterThanOrEqual(1);
    expect(Date.now() - start).toBeLessThan(100);
    expect(env.mocks[0]!.stats().requests).toBe(2);
  });

  test('depois da espera, um request de teste fecha o circuito de novo', async () => {
    env = await setup([{}], { breaker: { enabled: true, failureThreshold: 2, openMs: 150 }, retry: { maxAttempts: 1 } });
    env.mocks[0]!.setMode('down');
    await send(env.gateway.url);
    await send(env.gateway.url);
    expect(env.gateway.status().upstreams[0]!.breaker).toBe('open');
    env.mocks[0]!.setMode('ok');
    await sleep(200);
    expect(env.gateway.status().upstreams[0]!.breaker).toBe('half-open');
    expect((await send(env.gateway.url)).status).toBe(200);
    expect(env.gateway.status().upstreams[0]!.breaker).toBe('closed');
  });

  test('erro 400 do cliente não conta como falha do provedor', async () => {
    env = await setup([{}], { breaker: { enabled: true, failureThreshold: 2, openMs: 60_000 } });
    for (let i = 0; i < 5; i++) await send(env.gateway.url, { model: 'mock-1' });
    expect(env.gateway.status().upstreams[0]!.breaker).toBe('closed');
  });
});

describe('fila', () => {
  test('sem vaga e com a fila cheia: 503 rápido, e as vagas voltam a zero', async () => {
    env = await setup([{ ttftMs: 150 }], { limiter: { maxInFlight: 2, maxQueue: 2, maxWaitMs: 2_000 }, retry: { maxAttempts: 1 } });
    const responses = await Promise.all(Array.from({ length: 10 }, () => send(env.gateway.url)));
    const statuses = responses.map((r) => r.status);
    expect(statuses.filter((s) => s === 200)).toHaveLength(4);
    expect(statuses.filter((s) => s === 503)).toHaveLength(6);
    await waitFor(() => env.requests().length === 10);
    const rejected = env.requests().filter((log) => log.status === 503);
    expect(Math.max(...rejected.map((log) => log.ms))).toBeLessThan(100);
    expect(env.gateway.status().upstreams[0]).toMatchObject({ inFlight: 0, queued: 0 });
    expect(env.requests().some((log) => log.status === 200 && log.queueMs > 100)).toBe(true);
  });
});
