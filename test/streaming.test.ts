import { afterEach, describe, expect, test } from 'vitest';
import { BODY, readStream, send, setup, sleep, waitFor } from './helpers.js';

let env: Awaited<ReturnType<typeof setup>>;
afterEach(() => env?.close());
const STREAM = { ...BODY, stream: true };

describe('streaming', () => {
  test('os pedaços chegam separados, e o primeiro chega perto do TTFT do provedor', async () => {
    env = await setup([{ ttftMs: 50, chunkMs: 100, chunks: 5 }]);
    const start = performance.now();
    const response = await send(env.gateway.url, STREAM);
    expect(response.headers.get('content-type')).toBe('text/event-stream');
    const { events, arrivals, text } = await readStream(response);
    const total = performance.now() - start;

    expect(events.at(-1)!.event).toBe('message_stop');
    expect(text).toContain('resposta falsa do mock-a');
    expect(total).toBeGreaterThan(450);        // 5 pedaços a cada 100 ms
    expect(arrivals[0]!).toBeLessThan(250);     // o primeiro não esperou o resto
    expect(arrivals.length).toBeGreaterThanOrEqual(5);

    await waitFor(() => env.requests().length === 1);
    const log = env.requests()[0]!;
    expect(log).toMatchObject({ stream: true, status: 200, outputTokens: 5 });
    expect(log.inputTokens).toBeGreaterThan(0);
    expect(log.ttftMs).toBeLessThan(250);
    expect(log.ms).toBeGreaterThan(450);
  });

  test('cliente desconecta no meio: a chamada ao provedor é cancelada', async () => {
    env = await setup([{ ttftMs: 10, chunkMs: 100, chunks: 20 }]);
    const controller = new AbortController();
    const response = await send(env.gateway.url, STREAM, undefined, { signal: controller.signal });
    let chunks = 0;
    try {
      for await (const _chunk of response.body!) if (++chunks === 2) controller.abort();
    } catch {
      // esperado: o fetch do teste foi cancelado
    }
    await waitFor(() => env.mocks[0]!.stats().disconnects === 1);
    await waitFor(() => env.requests().length === 1);
    const log = env.requests()[0]!;
    expect(log.canceled).toBe(true);
    expect(log.outputTokens).toBeGreaterThan(0);
    expect(log.outputTokens).toBeLessThan(20);
    expect(env.gateway.status().upstreams[0]!.inFlight).toBe(0); // a vaga voltou
  });

  test('provedor cai no meio do streaming: o cliente recebe event: error', async () => {
    env = await setup([{ ttftMs: 10, chunkMs: 60, chunks: 10 }]);
    const response = await send(env.gateway.url, STREAM);
    setTimeout(() => env.mocks[0]!.setMode('down'), 150);
    const { events } = await readStream(response);
    expect(response.status).toBe(200); // o status já tinha sido enviado
    expect(events.at(-1)!.event).toBe('error');
    expect(JSON.parse(events.at(-1)!.data).error.type).toBe('api_error');
    expect(events.some((e) => e.event === 'message_stop')).toBe(false);
  });

  test('sem primeiro byte dentro do limite: 504, sem nenhum byte de streaming', async () => {
    env = await setup([{ ttftMs: 3_000 }], { timeouts: { firstByteMs: 150 }, retry: { maxAttempts: 1 } });
    const response = await send(env.gateway.url, STREAM);
    expect(response.status).toBe(504);
    expect(response.headers.get('content-type')).toBe('application/json');
  });

  test('provedor para de enviar no meio: limite de inatividade encerra com event: error', async () => {
    env = await setup([{ ttftMs: 10, chunkMs: 2_000, chunks: 5 }], { timeouts: { idleMs: 150 } });
    const start = Date.now();
    const { events } = await readStream(await send(env.gateway.url, STREAM));
    expect(Date.now() - start).toBeLessThan(1_500);
    expect(JSON.parse(events.at(-1)!.data).error.type).toBe('timeout_error');
  });

  test('cliente lento não faz o gateway perder dados (backpressure)', async () => {
    env = await setup([{ ttftMs: 5, chunkMs: 0, chunks: 400 }]);
    const response = await send(env.gateway.url, STREAM);
    const events = [];
    const { SseParser } = await import('../src/gateway/sse.js');
    const parser = new SseParser();
    for await (const chunk of response.body!) {
      events.push(...parser.push(chunk));
      await sleep(1); // lê devagar
    }
    expect(events.filter((e) => e.event === 'content_block_delta')).toHaveLength(400);
    expect(events.at(-1)!.event).toBe('message_stop');
  });
});
