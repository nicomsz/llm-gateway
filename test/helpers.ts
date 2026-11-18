import { makeConfig, type Config } from '../src/gateway/config.js';
import { memoryLogger } from '../src/gateway/log.js';
import { startGateway, type GatewayDeps } from '../src/gateway/server.js';
import { SseParser, type SseEvent } from '../src/gateway/sse.js';
import { startMock, type MockOptions } from '../src/mock/server.js';

export const ACME = 'gw_acme_test';
export const BETA = 'gw_beta_test';
export const UPSTREAM_KEY = 'chave-real-do-provedor';
export const BODY = { model: 'mock-1', max_tokens: 100, messages: [{ role: 'user', content: 'oi' }] };

type Overrides = Parameters<typeof makeConfig>[0];

/** Sobe N provedores falsos e um gateway apontando para eles, tudo em portas livres. */
export async function setup(mockOptions: MockOptions[], overrides: Overrides = {}, deps: GatewayDeps = {}) {
  const mocks = await Promise.all(
    mockOptions.map((options, i) => startMock({ name: `mock-${'abc'[i]}`, ttftMs: 5, chunkMs: 2, chunks: 3, ...options })),
  );
  const logger = memoryLogger();
  const config: Config = makeConfig({
    port: 0,
    tenants: {
      [ACME]: { name: 'acme', tokensPerMinute: 1_000_000 },
      [BETA]: { name: 'beta', tokensPerMinute: 1_000_000 },
    },
    upstreams: mocks.map((mock) => ({ name: mock.stats().name, url: mock.url, apiKey: UPSTREAM_KEY })),
    prices: { 'mock-1': { inputPerMTok: 3, outputPerMTok: 15 } },
    logFile: undefined,
    ...overrides,
    retry: { maxAttempts: 3, baseMs: 1, capMs: 5, ...overrides.retry },
  });
  const gateway = await startGateway(config, { logger, ...deps });
  return {
    mocks,
    gateway,
    logger,
    config,
    /** linhas de log de requests (sem as de mudança de estado do breaker) */
    requests: () => logger.entries.filter((entry) => entry.event === undefined),
    close: async () => {
      await gateway.close();
      await Promise.all(mocks.map((mock) => mock.close()));
    },
  };
}

export function send(url: string, body: unknown = BODY, headers: Record<string, string> = { 'x-api-key': ACME }, init: RequestInit = {}) {
  return fetch(`${url}/v1/messages`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: typeof body === 'string' ? body : JSON.stringify(body),
    ...init,
  });
}

/** Lê um streaming até o fim e devolve os eventos e o momento em que cada pedaço chegou. */
export async function readStream(response: Response) {
  const parser = new SseParser();
  const events: SseEvent[] = [];
  const arrivals: number[] = [];
  const start = performance.now();
  for await (const chunk of response.body!) {
    arrivals.push(performance.now() - start);
    events.push(...parser.push(chunk));
  }
  return { events, arrivals, text: events.filter((e) => e.event === 'content_block_delta').map((e) => JSON.parse(e.data).delta.text).join('') };
}

/** O log é gravado logo depois de a resposta sair: espera a condição ficar verdadeira. */
export async function waitFor(condition: () => boolean, timeoutMs = 2000) {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor: a condição não ficou verdadeira a tempo');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
