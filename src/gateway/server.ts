// O gateway: recebe POST /v1/messages e faz o request passar por estas paradas, nesta ordem:
//
//   autenticação -> cache -> rate limit -> [ fila -> provedor ] com failover e circuit breaker
//
// A resposta volta em streaming quando o cliente pede, e todo request gera uma linha de log.
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { setTimeout as sleepFor } from 'node:timers/promises';

import { Balancer, backoffMs, isRetryable, isUpstreamFailure, type AttemptResult, type UpstreamState } from './balancer.js';
import { CircuitBreaker, type BreakerState } from './breaker.js';
import { MemoryCache, cacheKey, type CacheStore } from './cache.js';
import type { Config, Tenant } from './config.js';
import { Limiter, QueueFullError, QueueTimeoutError } from './limiter.js';
import { fileLogger, memoryLogger, type Logger } from './log.js';
import { RedisCache, RedisTokenBucket, connectRedis, type Redis } from './redis.js';
import { SseParser, StreamTracker, errorEvent, messageToSse, type Message } from './sse.js';
import { MemoryTokenBucket, estimateTokens, type RateLimitStore } from './tokenBucket.js';

const MAX_BODY_BYTES = 1024 * 1024;

export interface RequestLog {
  ts: string;
  id: string;
  tenant: string | null;
  model?: string;
  upstream: string | null;
  /** 0 quando o cliente saiu antes de qualquer resposta */
  status: number;
  ms: number;
  stream: boolean;
  ttftMs?: number;
  queueMs?: number;
  inputTokens: number;
  outputTokens: number;
  costUsd: number;
  /** quanto a resposta guardada teria custado no provedor */
  savedUsd?: number;
  cache?: 'hit' | 'miss';
  canceled?: boolean;
  attempts: Array<{ upstream: string; status: number | null; ms: number; error?: string }>;
}

export interface GatewayDeps {
  rateLimit?: RateLimitStore;
  cache?: CacheStore;
  logger?: Logger;
  random?: () => number;
  sleep?: (ms: number) => Promise<unknown>;
}

export interface GatewayStatus {
  upstreams: Array<{ name: string; breaker: BreakerState; inFlight: number; queued: number }>;
}

export interface Gateway {
  server: Server;
  port: number;
  url: string;
  status(): GatewayStatus;
  close(): Promise<void>;
}

type Outcome =
  | { kind: 'sent'; cacheable?: string }
  | {
      kind: 'failed';
      retryable: boolean;
      reason: 'status' | 'timeout' | 'network' | 'queue' | 'breaker' | 'canceled';
      status?: number;
      body?: string;
      retryAfter?: string;
    };

class BodyTooLargeError extends Error {}

/** Lê o corpo do request, descartando tudo se ele passar do limite. */
function readBody(req: IncomingMessage, limit: number): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let tooLarge = false;
    req.on('data', (chunk: Buffer) => {
      if (tooLarge) return;
      size += chunk.length;
      if (size > limit) {
        tooLarge = true;
        chunks.length = 0;
      } else {
        chunks.push(chunk);
      }
    });
    req.on('end', () => (tooLarge ? reject(new BodyTooLargeError()) : resolve(Buffer.concat(chunks).toString('utf8'))));
    req.on('error', reject);
  });
}

/** Todo erro do gateway sai no mesmo formato de corpo do provedor: o app tem um tratamento só. */
function sendError(res: ServerResponse, status: number, type: string, message: string, headers: Record<string, string> = {}) {
  if (res.headersSent) return;
  res.writeHead(status, { 'content-type': 'application/json', ...headers });
  res.end(JSON.stringify({ type: 'error', error: { type, message } }));
}

const round6 = (value: number) => Math.round(value * 1e6) / 1e6;

export async function startGateway(config: Config, deps: GatewayDeps = {}): Promise<Gateway> {
  const logger = deps.logger ?? (config.logFile ? fileLogger(config.logFile) : memoryLogger());
  const random = deps.random ?? Math.random;
  const sleep = deps.sleep ?? ((ms: number) => sleepFor(ms));

  let redis: Redis | undefined;
  let rateLimit = deps.rateLimit;
  let cache = deps.cache;
  if (config.redisUrl && (!rateLimit || !cache)) {
    redis = await connectRedis(config.redisUrl);
    rateLimit ??= new RedisTokenBucket(redis);
    cache ??= new RedisCache(redis);
  }
  const limits: RateLimitStore = rateLimit ?? new MemoryTokenBucket();
  const responses: CacheStore = cache ?? new MemoryCache();

  const states: UpstreamState[] = config.upstreams.map((upstream) => ({
    upstream,
    breaker: new CircuitBreaker(config.breaker, Date.now, (from, to) =>
      logger.write({ ts: new Date().toISOString(), event: 'breaker', upstream: upstream.name, from, to }),
    ),
    limiter: new Limiter(config.limiter),
  }));
  const balancer = new Balancer(states, config.strategy);

  // requests idênticos que chegam enquanto o primeiro ainda está no provedor esperam por ele
  const inProgress = new Map<string, Promise<string | null>>();

  const price = (model: string | undefined, inputTokens: number, outputTokens: number) => {
    const p = model ? config.prices[model] : undefined;
    return p ? round6((inputTokens * p.inputPerMTok + outputTokens * p.outputPerMTok) / 1e6) : 0;
  };

  async function handleMessages(req: IncomingMessage, res: ServerResponse) {
    const startedAt = performance.now();
    const log: RequestLog = {
      ts: new Date().toISOString(),
      id: `gw_${randomUUID().slice(0, 8)}`,
      tenant: null,
      upstream: null,
      status: 0,
      ms: 0,
      stream: false,
      inputTokens: 0,
      outputTokens: 0,
      costUsd: 0,
      attempts: [],
    };

    // o cliente fechou a conexão antes do fim: cancela tudo o que estiver em andamento por ele
    const client = new AbortController();
    res.on('close', () => {
      if (!res.writableEnded) {
        log.canceled = true;
        client.abort();
      }
    });

    let tenant: Tenant | undefined;
    let reserved = 0;
    let ttlSec = 0;
    let flightKey: string | undefined;
    let settleFlight: ((value: string | null) => void) | undefined;
    let toCache: string | null = null;

    try {
      // ---- corpo ----
      if (Number(req.headers['content-length']) > MAX_BODY_BYTES) {
        return sendError(res, 413, 'request_too_large', 'o corpo do request passa de 1 MB', { connection: 'close' });
      }
      let raw: string;
      try {
        raw = await readBody(req, MAX_BODY_BYTES);
      } catch (error) {
        if (error instanceof BodyTooLargeError) return sendError(res, 413, 'request_too_large', 'o corpo do request passa de 1 MB');
        throw error;
      }
      let body: Record<string, unknown>;
      try {
        const parsed: unknown = JSON.parse(raw);
        if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('not an object');
        body = parsed as Record<string, unknown>;
      } catch {
        return sendError(res, 400, 'invalid_request_error', 'o corpo precisa ser um objeto JSON');
      }

      // ---- autenticação ----
      const key = req.headers['x-api-key'];
      tenant = typeof key === 'string' && Object.hasOwn(config.tenants, key) ? config.tenants[key] : undefined;
      if (!tenant) return sendError(res, 401, 'authentication_error', 'chave de tenant inválida');
      log.tenant = tenant.name;
      log.stream = body.stream === true;
      if (typeof body.model === 'string') log.model = body.model;

      // ---- cache (só quando o cliente pede, pelo header) ----
      ttlSec = Number(req.headers['x-gateway-cache']);
      const useCache = config.cache.enabled && Number.isFinite(ttlSec) && ttlSec > 0;
      if (useCache) {
        flightKey = cacheKey(tenant.name, body);
        let cached = await responses.get(flightKey).catch(() => null);
        const pending = cached === null ? inProgress.get(flightKey) : undefined;
        if (pending) cached = await pending;
        if (cached !== null) {
          const message = JSON.parse(cached) as Message;
          log.cache = 'hit';
          log.savedUsd = price(log.model, message.usage.input_tokens, message.usage.output_tokens);
          const headers = { 'x-gateway-request-id': log.id, 'x-gateway-cache': 'hit' };
          if (log.stream) {
            res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', ...headers });
            res.end(messageToSse(message));
          } else {
            res.writeHead(200, { 'content-type': 'application/json', ...headers });
            res.end(cached);
          }
          return;
        }
        log.cache = 'miss';
        if (!inProgress.has(flightKey)) inProgress.set(flightKey, new Promise((resolve) => void (settleFlight = resolve)));
      }

      // ---- rate limit por tokens ----
      // Só depois da resposta sabemos quantos tokens o request usou. Então reservamos uma
      // estimativa agora e devolvemos a diferença no fim.
      if (config.rateLimit.enabled) {
        const capacity = tenant.tokensPerMinute;
        const estimate = estimateTokens(raw, body.max_tokens);
        if (estimate > capacity) {
          return sendError(res, 400, 'invalid_request_error', `o request pede cerca de ${estimate} tokens, e a cota do tenant é de ${capacity} por minuto`);
        }
        // Redis fora do ar: deixamos passar (fail open). Preferimos ficar sem limite por um
        // tempo a derrubar todos os tenants junto com o Redis.
        const taken = await limits.take(`rl:${tenant.name}`, capacity, capacity / 60, estimate).catch(() => null);
        if (taken && !taken.allowed) {
          return sendError(res, 429, 'rate_limit_error', `cota de tokens esgotada; tente de novo em ${taken.retryAfterSec}s`, {
            'retry-after': String(taken.retryAfterSec),
          });
        }
        if (taken) reserved = estimate;
      }

      // ---- tentativas: escolhe um provedor, tenta, e troca se ele falhar ----
      const deadline = Date.now() + config.timeouts.totalMs;
      const tried = new Set<UpstreamState>();
      let last: Extract<Outcome, { kind: 'failed' }> | undefined;

      for (let attempt = 0; attempt < config.retry.maxAttempts && !client.signal.aborted; attempt++) {
        if (attempt > 0) {
          const wait = backoffMs(attempt - 1, random(), config.retry.baseMs, config.retry.capMs);
          if (Date.now() + wait >= deadline) break;
          await sleep(wait);
        }
        const candidates = states.filter((state) => !tried.has(state) && (!config.breaker.enabled || state.breaker.available()));
        const state = balancer.pick(candidates);
        if (!state) break;
        tried.add(state);

        const outcome = await attemptOnce(state, { raw, req, res, log, client, deadline, startedAt, useCache });
        if (outcome.kind === 'sent') {
          toCache = outcome.cacheable ?? null;
          break;
        }
        last = outcome;
        if (!outcome.retryable) break;
      }

      if (res.headersSent || client.signal.aborted) return;

      // ---- nenhuma tentativa deu certo ----
      if (!last) {
        const waitMs = Math.min(...states.map((state) => state.breaker.retryAfterMs()), config.breaker.openMs);
        return sendError(res, 503, 'overloaded_error', 'nenhum provedor disponível no momento', {
          'retry-after': String(Math.max(1, Math.ceil(waitMs / 1000))),
        });
      }
      if (last.body !== undefined && last.status !== undefined) {
        // devolve ao app o erro que o provedor deu, sem alterar
        res.writeHead(last.status, { 'content-type': 'application/json', ...(last.retryAfter ? { 'retry-after': last.retryAfter } : {}) });
        return void res.end(last.body);
      }
      if (last.reason === 'timeout') return sendError(res, 504, 'timeout_error', 'o provedor não respondeu a tempo');
      if (last.reason === 'queue' || last.reason === 'breaker') {
        return sendError(res, 503, 'overloaded_error', 'sem vaga para atender o request agora', { 'retry-after': '1' });
      }
      return sendError(res, 502, 'api_error', 'falha de conexão com o provedor');
    } finally {
      log.status = res.headersSent ? res.statusCode : 0;
      log.ms = Math.round(performance.now() - startedAt);
      if (log.cache !== 'hit') log.costUsd = price(log.model, log.inputTokens, log.outputTokens);

      if (tenant && reserved > 0) {
        const unused = reserved - (log.inputTokens + log.outputTokens);
        if (unused > 0) await limits.refund(`rl:${tenant.name}`, tenant.tokensPerMinute, unused).catch(() => {});
      }
      if (flightKey && toCache) await responses.set(flightKey, toCache, ttlSec).catch(() => {});
      if (flightKey && settleFlight) {
        inProgress.delete(flightKey);
        settleFlight(toCache);
      }
      logger.write(log);
    }
  }

  interface AttemptContext {
    raw: string;
    req: IncomingMessage;
    res: ServerResponse;
    log: RequestLog;
    client: AbortController;
    deadline: number;
    startedAt: number;
    useCache: boolean;
  }

  /** Uma tentativa em um provedor. Ou a resposta foi enviada ao cliente, ou nada foi enviado. */
  async function attemptOnce(state: UpstreamState, ctx: AttemptContext): Promise<Outcome> {
    const { upstream, breaker, limiter } = state;
    const { raw, req, res, log, client, deadline, startedAt, useCache } = ctx;
    const attemptStart = performance.now();
    const record = (status: number | null, error?: string) =>
      log.attempts.push({ upstream: upstream.name, status, ms: Math.round(performance.now() - attemptStart), ...(error ? { error } : {}) });

    // o circuit breaker conta como falha só o que indica problema no provedor
    const verdict = (result: AttemptResult) => {
      if (!config.breaker.enabled) return;
      if (isUpstreamFailure(result)) breaker.failure();
      else if (result.status === 429) breaker.release();
      else breaker.success();
    };

    if (config.breaker.enabled && !breaker.acquire()) {
      record(null, 'breaker_open');
      return { kind: 'failed', retryable: true, reason: 'breaker' };
    }

    // ---- fila: espera uma vaga neste provedor ----
    let release: () => void;
    try {
      release = await limiter.acquire(client.signal);
    } catch (error) {
      breaker.release();
      const full = error instanceof QueueFullError;
      const waited = error instanceof QueueTimeoutError;
      record(null, full ? 'queue_full' : waited ? 'queue_timeout' : 'canceled');
      return full || waited ? { kind: 'failed', retryable: true, reason: 'queue' } : { kind: 'failed', retryable: false, reason: 'canceled' };
    }
    log.queueMs = (log.queueMs ?? 0) + Math.round(performance.now() - attemptStart);

    // um sinal só para cancelar a chamada: dispara se o cliente sair ou se um tempo limite estourar
    const abort = new AbortController();
    const onClientGone = () => abort.abort();
    client.signal.addEventListener('abort', onClientGone, { once: true });
    if (client.signal.aborted) abort.abort();
    let timedOut = false;
    const expire = () => {
      timedOut = true;
      abort.abort();
    };
    let timer = setTimeout(expire, Math.max(1, Math.min(config.timeouts.firstByteMs, deadline - Date.now())));

    try {
      const response = await fetch(`${upstream.url}/v1/messages`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          // a chave do tenant fica aqui; o provedor só vê a chave real
          'x-api-key': upstream.apiKey,
          'anthropic-version': String(req.headers['anthropic-version'] ?? '2023-06-01'),
          ...(req.headers['anthropic-beta'] ? { 'anthropic-beta': String(req.headers['anthropic-beta']) } : {}),
        },
        body: raw,
        signal: abort.signal,
      });
      clearTimeout(timer);
      log.upstream = upstream.name;

      if (response.status !== 200) {
        const text = await response.text();
        verdict({ status: response.status });
        record(response.status);
        return {
          kind: 'failed',
          retryable: isRetryable({ status: response.status }),
          reason: 'status',
          status: response.status,
          body: text,
          retryAfter: response.headers.get('retry-after') ?? undefined,
        };
      }

      const headers = {
        'x-gateway-request-id': log.id,
        'x-gateway-upstream': upstream.name,
        ...(useCache ? { 'x-gateway-cache': 'miss' } : {}),
      };

      // ---- resposta comum: um JSON só ----
      if (!log.stream) {
        timer = setTimeout(expire, Math.max(1, deadline - Date.now()));
        const text = await response.text();
        clearTimeout(timer);
        verdict({ status: 200 });
        record(200);
        let message: Message | undefined;
        try {
          message = JSON.parse(text) as Message;
        } catch {
          // resposta que não é JSON: repassa mesmo assim, sem contar tokens
        }
        log.inputTokens = message?.usage?.input_tokens ?? 0;
        log.outputTokens = message?.usage?.output_tokens ?? 0;
        res.writeHead(200, { 'content-type': 'application/json', ...headers });
        res.end(text);
        return { kind: 'sent', cacheable: message?.stop_reason ? text : undefined };
      }

      // ---- streaming ----
      // A partir do writeHead o cliente já recebeu bytes. Daqui em diante não existe mais
      // troca de provedor: se algo falhar, o cliente recebe um evento de erro.
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', ...headers });
      const parser = new SseParser();
      const tracker = new StreamTracker();
      try {
        timer = setTimeout(expire, config.timeouts.idleMs);
        for await (const chunk of response.body!) {
          clearTimeout(timer);
          timer = setTimeout(expire, config.timeouts.idleMs);
          log.ttftMs ??= Math.round(performance.now() - startedAt);
          tracker.feed(parser.push(chunk));
          // backpressure: se o cliente não está conseguindo receber, paramos de ler do provedor
          if (!res.write(chunk)) await once(res, 'drain', { signal: abort.signal });
        }
        clearTimeout(timer);
        if (!tracker.complete && !tracker.sawError) throw new Error('o streaming terminou antes do message_stop');
        res.end();
        verdict({ status: tracker.sawError ? 529 : 200 });
        record(200);
      } catch {
        clearTimeout(timer);
        if (client.signal.aborted) {
          breaker.release();
          record(200, 'canceled');
        } else {
          verdict(timedOut ? { timeout: true } : { networkError: true });
          record(200, timedOut ? 'idle_timeout' : 'stream_error');
          res.end(errorEvent(timedOut ? 'timeout_error' : 'api_error', 'o provedor parou de responder no meio do streaming'));
        }
      }
      log.inputTokens = tracker.inputTokens;
      log.outputTokens = tracker.outputTokens;
      return { kind: 'sent', cacheable: tracker.complete ? JSON.stringify(tracker.toMessage()) : undefined };
    } catch {
      clearTimeout(timer);
      if (client.signal.aborted) {
        breaker.release();
        record(null, 'canceled');
        return { kind: 'failed', retryable: false, reason: 'canceled' };
      }
      verdict(timedOut ? { timeout: true } : { networkError: true });
      record(null, timedOut ? 'timeout' : 'network');
      return { kind: 'failed', retryable: true, reason: timedOut ? 'timeout' : 'network' };
    } finally {
      clearTimeout(timer);
      client.signal.removeEventListener('abort', onClientGone);
      // a vaga volta sempre: com sucesso, com erro ou com cancelamento
      release();
    }
  }

  const status = (): GatewayStatus => ({
    upstreams: states.map((state) => ({
      name: state.upstream.name,
      breaker: state.breaker.state,
      inFlight: state.limiter.inFlight,
      queued: state.limiter.queued,
    })),
  });

  const server = createServer((req, res) => {
    const route = `${req.method} ${req.url}`;
    if (route === 'POST /v1/messages') {
      handleMessages(req, res).catch(() => {
        if (res.headersSent) res.destroy();
        else sendError(res, 500, 'api_error', 'erro interno do gateway');
      });
    } else if (route === 'GET /_status') {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify(status()));
    } else {
      sendError(res, 404, 'not_found_error', 'rota desconhecida');
    }
  });

  await new Promise<void>((resolve) => server.listen(config.port, resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    server,
    port,
    url: `http://localhost:${port}`,
    status,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await logger.close();
      if (redis) await redis.close().catch(() => {});
    },
  };
}
