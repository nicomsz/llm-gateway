// Experimentos de carga. Sobe os provedores falsos e o gateway como processos separados,
// envia requests e compara cada mecanismo ligado e desligado.
//
//   pnpm bench               roda todos
//   pnpm bench A C           roda só os experimentos A e C
//
// Os resultados vão para docs/resultados.md e docs/bench-results.json.
import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { cpus, tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

import type { Config } from '../gateway/config.js';
import type { RequestLog } from '../gateway/server.js';
import type { MockStats } from '../mock/server.js';
import { percentile, readLog } from '../report.js';

const ROOT = fileURLToPath(new URL('../..', import.meta.url));
const TMP = mkdtempSync(join(tmpdir(), 'llm-gateway-bench-'));
const ACME = 'gw_acme_bench';
const BETA = 'gw_beta_bench';

// ---------------------------------------------------------------- processos

const children = new Set<ChildProcess>();
process.on('exit', () => {
  for (const child of children) child.kill('SIGKILL');
  rmSync(TMP, { recursive: true, force: true });
});

function run(file: string, env: Record<string, string>): ChildProcess {
  const child = spawn(process.execPath, ['--import', 'tsx', join(ROOT, file)], {
    cwd: ROOT,
    env: { ...process.env, ...env },
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  children.add(child);
  child.once('exit', () => children.delete(child));
  return child;
}

async function stop(child: ChildProcess) {
  if (child.exitCode !== null) return;
  child.kill('SIGTERM');
  await once(child, 'exit');
}

function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const probe = createServer().listen(0, () => {
      const { port } = probe.address() as { port: number };
      probe.close(() => resolve(port));
    });
  });
}

async function waitReady(url: string) {
  for (let attempt = 0; attempt < 200; attempt++) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {
      // ainda subindo
    }
    await sleep(50);
  }
  throw new Error(`${url} não respondeu`);
}

interface MockProc {
  name: string;
  url: string;
  mode(mode: 'ok' | 'down' | 'slow'): Promise<void>;
  stats(): Promise<MockStats>;
  stop(): Promise<void>;
}

async function startMock(name: string, options: { ttftMs: number; chunkMs: number; chunks: number }): Promise<MockProc> {
  const port = await freePort();
  const child = run('src/mock/main.ts', {
    PORT: String(port),
    MOCK_NAME: name,
    MOCK_TTFT_MS: String(options.ttftMs),
    MOCK_CHUNK_MS: String(options.chunkMs),
    MOCK_CHUNKS: String(options.chunks),
  });
  const url = `http://localhost:${port}`;
  await waitReady(`${url}/_control`);
  return {
    name,
    url,
    mode: async (mode) => void (await fetch(`${url}/_control`, { method: 'POST', body: JSON.stringify({ mode }) })),
    stats: async () => (await fetch(`${url}/_control`)).json() as Promise<MockStats>,
    stop: () => stop(child),
  };
}

type Overrides = { [K in keyof Config]?: Partial<Config[K]> | Config[K] };
let gatewaySeq = 0;

async function startGateway(mocks: MockProc[], overrides: Overrides = {}) {
  const port = await freePort();
  const logFile = join(TMP, `gateway-${++gatewaySeq}.jsonl`);
  const base: Record<string, unknown> = {
    port,
    tenants: { [ACME]: { name: 'acme', tokensPerMinute: 1e9 }, [BETA]: { name: 'beta', tokensPerMinute: 1e9 } },
    upstreams: mocks.map((mock) => ({ name: mock.name, url: mock.url, apiKey: 'chave-falsa' })),
    strategy: 'least-in-flight',
    timeouts: { firstByteMs: 1_000, idleMs: 5_000, totalMs: 10_000 },
    retry: { maxAttempts: 3, baseMs: 200, capMs: 2_000 },
    breaker: { enabled: true, failureThreshold: 5, openMs: 2_000 },
    limiter: { maxInFlight: 50, maxQueue: 100, maxWaitMs: 3_000 },
    prices: { 'mock-1': { inputPerMTok: 3, outputPerMTok: 15 } },
    logFile,
  };
  // cada experimento muda só os campos que interessam; o resto fica como acima
  const config = { ...base };
  for (const [key, value] of Object.entries(overrides)) {
    const nested = key !== 'tenants' && typeof value === 'object' && !Array.isArray(value);
    config[key] = nested ? { ...(base[key] as object), ...value } : value;
  }
  const configFile = join(TMP, `config-${gatewaySeq}.json`);
  writeFileSync(configFile, JSON.stringify(config));
  const child = run('src/gateway/main.ts', { GATEWAY_CONFIG: configFile, PORT: String(port), REDIS_URL: '' });
  const url = `http://localhost:${port}`;
  await waitReady(`${url}/_status`);
  return {
    url,
    /** encerra o processo e devolve o log de requests que ele gravou */
    stop: async (): Promise<RequestLog[]> => {
      await stop(child);
      return readLog(logFile);
    },
  };
}

// ---------------------------------------------------------------- gerador de carga

interface Req {
  url: string;
  tenant?: string;
  prompt: string;
  stream?: boolean;
  cacheTtl?: number;
  /** o cliente desiste depois desse tempo */
  abortAfterMs?: number;
}

interface Sample {
  status: number; // 0 = conexão falhou ou o cliente desistiu
  ms: number;
  ttftMs?: number;
}

async function send(req: Req): Promise<Sample> {
  const start = performance.now();
  const controller = new AbortController();
  const timer = req.abortAfterMs ? setTimeout(() => controller.abort(), req.abortAfterMs) : undefined;
  try {
    const response = await fetch(`${req.url}/v1/messages`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': req.tenant ?? ACME,
        ...(req.cacheTtl ? { 'x-gateway-cache': String(req.cacheTtl) } : {}),
      },
      body: JSON.stringify({ model: 'mock-1', max_tokens: 100, stream: req.stream === true, messages: [{ role: 'user', content: req.prompt }] }),
      signal: controller.signal,
    });
    let ttftMs: number | undefined;
    for await (const _chunk of response.body!) ttftMs ??= performance.now() - start;
    return { status: response.status, ms: performance.now() - start, ttftMs };
  } catch {
    return { status: 0, ms: performance.now() - start };
  } finally {
    clearTimeout(timer);
  }
}

/** Concorrência fixa: N workers, cada um envia o próximo request assim que o anterior termina. */
async function closedLoop(total: number, concurrency: number, make: (i: number) => Req) {
  const samples: Sample[] = [];
  let next = 0;
  const start = performance.now();
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (next < total) samples.push(await send(make(next++)));
    }),
  );
  return { samples, seconds: (performance.now() - start) / 1000 };
}

/**
 * Taxa fixa: os requests saem no horário marcado, mesmo que os anteriores não tenham terminado.
 * Sem `random`, sai um a cada 1/taxa segundos. Com `random`, os intervalos são aleatórios
 * (distribuição exponencial) e a taxa vale na média, como chegadas de usuários independentes.
 */
async function openLoop(ratePerSec: number, seconds: number, make: (i: number) => Req, random?: () => number) {
  const start = performance.now();
  const pending: Array<Promise<Sample>> = [];
  let due = 0;
  for (let i = 0; due < seconds * 1000; i++) {
    const wait = start + due - performance.now();
    if (wait > 0) await sleep(wait);
    pending.push(send(make(i)));
    due = random ? due + (-Math.log(1 - random()) * 1000) / ratePerSec : ((i + 1) * 1000) / ratePerSec;
  }
  return Promise.all(pending);
}

/** Gerador de números aleatórios com semente, para repetir a mesma sequência em toda rodada. */
function seeded(seed: number): () => number {
  return () => (seed = (seed * 1_103_515_245 + 12_345) % 2 ** 31) / 2 ** 31;
}

const round = (value: number, digits = 0) => Math.round(value * 10 ** digits) / 10 ** digits;
const pct = (part: number, total: number) => (total === 0 ? 0 : round((part / total) * 100, 1));

function latency(samples: Sample[]) {
  const ok = samples.filter((sample) => sample.status === 200);
  return {
    requests: samples.length,
    errorPct: pct(samples.filter((sample) => sample.status === 0 || sample.status >= 500).length, samples.length),
    p50Ms: round(percentile(ok.map((sample) => sample.ms), 50)),
    p99Ms: round(percentile(ok.map((sample) => sample.ms), 99)),
  };
}

// ---------------------------------------------------------------- saída

type Row = Record<string, string | number>;
const results: Record<string, { title: string; setup: string; rows: Row[] }> = {};

function table(rows: Row[]): string {
  const columns = Object.keys(rows[0]!);
  const line = (cells: Array<string | number>) => `| ${cells.join(' | ')} |`;
  return [line(columns), line(columns.map(() => '---')), ...rows.map((row) => line(columns.map((column) => row[column]!)))].join('\n');
}

function report(id: string, title: string, setup: string, rows: Row[]) {
  results[id] = { title, setup, rows };
  console.log(`\n${id}. ${title}\n${setup}\n\n${table(rows)}`);
}

// ---------------------------------------------------------------- experimentos

const FAST = { ttftMs: 100, chunkMs: 0, chunks: 5 }; // resposta inteira em ~100 ms

const experiments: Record<string, () => Promise<void>> = {
  /** Quanto o gateway acrescenta de latência? */
  async A() {
    const mock = await startMock('mock-a', FAST);
    const gateway = await startGateway([mock]);
    const row = async (name: string, url: string) => {
      await closedLoop(200, 20, (i) => ({ url, prompt: `aquecimento ${i}` }));
      const { samples, seconds } = await closedLoop(2_000, 20, (i) => ({ url, prompt: `pergunta ${i}` }));
      const stats = latency(samples);
      const ms = samples.map((sample) => sample.ms);
      return { caminho: name, requests: stats.requests, 'erros %': stats.errorPct, 'p50 ms': round(percentile(ms, 50), 1), 'p99 ms': round(percentile(ms, 99), 1), 'req/s': round(samples.length / seconds) };
    };
    const rows = [await row('direto no provedor', mock.url), await row('pelo gateway', gateway.url)];
    await gateway.stop();
    await mock.stop();
    report('A', 'Custo de passar pelo gateway', '1 provedor falso que responde em 100 ms; 2.000 requests, 20 ao mesmo tempo, depois de 200 de aquecimento.', rows);
  },

  /** Um dos três provedores devolve 529 para tudo. */
  async B() {
    const variants: Array<{ name: string; overrides: Overrides }> = [
      { name: 'round-robin, sem failover', overrides: { strategy: 'round-robin', retry: { maxAttempts: 1 }, breaker: { enabled: false } } },
      { name: 'least-in-flight, sem failover', overrides: { retry: { maxAttempts: 1 }, breaker: { enabled: false } } },
      { name: 'least-in-flight + failover', overrides: { breaker: { enabled: false } } },
      { name: 'least-in-flight + failover + circuit breaker', overrides: {} },
    ];
    const rows: Row[] = [];
    for (const variant of variants) {
      const mocks = await Promise.all(['mock-a', 'mock-b', 'mock-c'].map((name) => startMock(name, FAST)));
      const gateway = await startGateway(mocks, variant.overrides);
      await mocks[2]!.mode('down');
      const stats = latency(await openLoop(150, 8, (i) => ({ url: gateway.url, prompt: `pergunta ${i}` })));
      const down = await mocks[2]!.stats();
      await gateway.stop();
      await Promise.all(mocks.map((mock) => mock.stop()));
      rows.push({ configuração: variant.name, requests: stats.requests, 'erros %': stats.errorPct, 'p50 ms': stats.p50Ms, 'p99 ms': stats.p99Ms, 'chamadas ao provedor caído': down.requests });
    }
    report('B', 'Um provedor fora do ar', '3 provedores de 100 ms, 1 deles devolvendo 529 em tudo; 150 requests por segundo durante 8 s. Latência só dos requests com sucesso.', rows);
  },

  /** Um dos três provedores aceita a conexão e não responde. */
  async C() {
    const rows: Row[] = [];
    for (const breaker of [false, true]) {
      const mocks = await Promise.all(['mock-a', 'mock-b', 'mock-c'].map((name) => startMock(name, FAST)));
      const gateway = await startGateway(mocks, { breaker: { enabled: breaker } });
      await mocks[2]!.mode('slow');
      const samples = await openLoop(150, 15, (i) => ({ url: gateway.url, prompt: `pergunta ${i}` }));
      const stats = latency(samples);
      await gateway.stop();
      await Promise.all(mocks.map((mock) => mock.stop()));
      rows.push({
        configuração: breaker ? 'com circuit breaker' : 'sem circuit breaker',
        requests: stats.requests,
        'erros %': stats.errorPct,
        'p50 ms': stats.p50Ms,
        'p99 ms': stats.p99Ms,
        'requests acima de 1 s': samples.filter((sample) => sample.ms > 1_000).length,
      });
    }
    report('C', 'Um provedor travado', '3 provedores de 100 ms, 1 deles sem responder; tempo limite de 1 s por tentativa; 150 requests por segundo durante 15 s.', rows);
  },

  /** Um tenant envia muito mais do que o provedor aguenta. O outro tenant sente? */
  async D() {
    const rows: Row[] = [];
    for (const limited of [false, true]) {
      const mock = await startMock('mock-a', FAST);
      const gateway = await startGateway([mock], {
        tenants: { [ACME]: { name: 'acme', tokensPerMinute: 6_000 }, [BETA]: { name: 'beta', tokensPerMinute: 1e9 } },
        limiter: { maxInFlight: 10, maxQueue: 100, maxWaitMs: 3_000 }, // o provedor aguenta ~100 requests por segundo
        rateLimit: { enabled: limited },
        retry: { maxAttempts: 1 },
      });
      const noisy = openLoop(300, 17, (i) => ({ url: gateway.url, tenant: ACME, prompt: `barulho ${i}` }));
      await sleep(2_000); // deixa o tenant barulhento gastar o saldo inicial do balde
      const quiet = latency(await openLoop(20, 15, (i) => ({ url: gateway.url, tenant: BETA, prompt: `pergunta ${i}` }), seeded(7)));
      const acme = await noisy;
      await gateway.stop();
      await mock.stop();
      rows.push({
        configuração: limited ? 'com rate limit' : 'sem rate limit',
        'beta: requests': quiet.requests,
        'beta: erros %': quiet.errorPct,
        'beta: p50 ms': quiet.p50Ms,
        'beta: p99 ms': quiet.p99Ms,
        'acme: atendidos': acme.filter((sample) => sample.status === 200).length,
        'acme: 429': acme.filter((sample) => sample.status === 429).length,
        'acme: 503': acme.filter((sample) => sample.status === 503).length,
      });
    }
    report(
      'D',
      'Um tenant barulhento',
      'Provedor com capacidade para ~100 requests por segundo. O tenant acme envia 300 por segundo (cota: 6.000 tokens por minuto); o tenant beta envia 20 por segundo em média, em intervalos aleatórios, durante 15 s.',
      rows,
    );
  },

  /** 30% dos requests repetem uma de 10 perguntas frequentes. */
  async E() {
    const rows: Row[] = [];
    // a mesma sequência de perguntas nas duas rodadas
    const random = seeded(42);
    const prompts = Array.from({ length: 1_000 }, (_, i) => (random() < 0.3 ? `pergunta frequente ${Math.floor(random() * 10)}` : `pergunta única ${i}`));
    for (const cached of [false, true]) {
      const mock = await startMock('mock-a', FAST);
      const gateway = await startGateway([mock]);
      const { samples } = await closedLoop(prompts.length, 10, (i) => ({ url: gateway.url, prompt: prompts[i]!, cacheTtl: cached ? 300 : undefined }));
      const calls = (await mock.stats()).requests;
      const log = await gateway.stop();
      await mock.stop();
      rows.push({
        configuração: cached ? 'com cache' : 'sem cache',
        requests: samples.length,
        'chamadas ao provedor': calls,
        'respostas do cache %': pct(log.filter((entry) => entry.cache === 'hit').length, log.length),
        'custo US$': round(log.reduce((sum, entry) => sum + entry.costUsd, 0), 4),
        'média ms': round(samples.reduce((sum, sample) => sum + sample.ms, 0) / samples.length),
      });
    }
    report('E', 'Cache de respostas', '1.000 requests, 10 ao mesmo tempo; 30% deles repetem uma de 10 perguntas frequentes (premissa do teste). Custo calculado com um preço de exemplo.', rows);
  },

  /** O primeiro pedaço chega logo, ou o gateway segura a resposta até o fim? */
  async F() {
    const mock = await startMock('mock-a', { ttftMs: 200, chunkMs: 50, chunks: 20 }); // primeiro pedaço em 200 ms, resposta completa em ~1,2 s
    const gateway = await startGateway([mock]);
    const row = async (name: string, url: string) => {
      await closedLoop(40, 20, (i) => ({ url, prompt: `aquecimento ${i}`, stream: true }));
      const { samples } = await closedLoop(300, 20, (i) => ({ url, prompt: `pergunta ${i}`, stream: true }));
      const ttft = samples.map((sample) => sample.ttftMs ?? 0);
      const stats = latency(samples);
      return { caminho: name, requests: samples.length, 'primeiro pedaço p50 ms': round(percentile(ttft, 50)), 'primeiro pedaço p99 ms': round(percentile(ttft, 99)), 'resposta completa p50 ms': stats.p50Ms };
    };
    const rows = [await row('direto no provedor', mock.url), await row('pelo gateway', gateway.url)];

    // cancelamento: o cliente desiste 400 ms depois de enviar
    const before = await mock.stats();
    await closedLoop(100, 20, (i) => ({ url: gateway.url, prompt: `cancelado ${i}`, stream: true, abortAfterMs: 400 }));
    await sleep(300);
    const after = await mock.stats();
    const canceled = (await gateway.stop()).filter((entry) => entry.canceled).length;
    await mock.stop();
    report('F', 'Streaming', 'Provedor que envia o primeiro pedaço em 200 ms e depois 20 pedaços de texto, um a cada 50 ms; 300 requests, 20 ao mesmo tempo, depois de 40 de aquecimento.', rows);
    report('G', 'Cancelamento', '100 requests com streaming; o cliente fecha a conexão 400 ms depois de enviar. Cada resposta completa teria 20 pedaços de texto.', [
      {
        'requests cancelados': canceled,
        'cancelamentos que chegaram ao provedor': after.disconnects - before.disconnects,
        'pedaços que o provedor gerou': after.chunksSent - before.chunksSent,
        'pedaços sem cancelamento': 100 * 20,
      },
    ]);
  },
};

// ---------------------------------------------------------------- execução

const selected = process.argv.slice(2).map((arg) => arg.toUpperCase());
for (const [id, experiment] of Object.entries(experiments)) {
  if (selected.length === 0 || selected.includes(id)) await experiment();
}

if (selected.length === 0) {
  const environment = `${cpus()[0]?.model ?? 'CPU desconhecida'}, ${cpus().length} núcleos, Node ${process.version}, ${process.platform}`;
  mkdirSync(join(ROOT, 'docs'), { recursive: true });
  writeFileSync(join(ROOT, 'docs/bench-results.json'), `${JSON.stringify({ date: new Date().toISOString(), environment, results }, null, 2)}\n`);
  const sections = Object.entries(results).map(([id, result]) => `## ${id}. ${result.title}\n\n${result.setup}\n\n${table(result.rows)}`);
  writeFileSync(
    join(ROOT, 'docs/resultados.md'),
    `# Resultados dos experimentos\n\nGerado por \`pnpm bench\` em ${new Date().toISOString().slice(0, 10)}.\nAmbiente: ${environment}.\nTudo roda na mesma máquina, com provedores falsos de latência fixa.\n\n${sections.join('\n\n')}\n`,
  );
  console.log('\nResultados gravados em docs/resultados.md e docs/bench-results.json');
}
process.exit(0);
