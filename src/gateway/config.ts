import { readFileSync } from 'node:fs';

export interface Tenant {
  name: string;
  /** cota do tenant: teto do balde e também a recarga por minuto */
  tokensPerMinute: number;
}

export interface Upstream {
  name: string;
  url: string;
  apiKey: string;
}

export interface Config {
  port: number;
  /** chave de API do tenant -> tenant */
  tenants: Record<string, Tenant>;
  upstreams: Upstream[];
  strategy: 'round-robin' | 'least-in-flight';
  timeouts: {
    /** sem cabeçalho de resposta do provedor nesse tempo: a tentativa falha */
    firstByteMs: number;
    /** streaming sem nenhum pedaço novo nesse tempo: encerra */
    idleMs: number;
    /** prazo total do request, somando todas as tentativas e esperas */
    totalMs: number;
  };
  retry: {
    /** 1 desliga o failover */
    maxAttempts: number;
    baseMs: number;
    capMs: number;
  };
  breaker: { enabled: boolean; failureThreshold: number; openMs: number };
  /** limite de requests em andamento, por provedor */
  limiter: { maxInFlight: number; maxQueue: number; maxWaitMs: number };
  rateLimit: { enabled: boolean };
  cache: { enabled: boolean };
  /** sem redisUrl, o rate limit e o cache ficam na memória do processo */
  redisUrl?: string;
  logFile?: string;
  /** preço em dólares por milhão de tokens, por modelo */
  prices: Record<string, { inputPerMTok: number; outputPerMTok: number }>;
}

export const defaults: Config = {
  port: 3000,
  tenants: {},
  upstreams: [],
  strategy: 'least-in-flight',
  timeouts: { firstByteMs: 10_000, idleMs: 15_000, totalMs: 60_000 },
  retry: { maxAttempts: 3, baseMs: 200, capMs: 2_000 },
  breaker: { enabled: true, failureThreshold: 5, openMs: 10_000 },
  limiter: { maxInFlight: 50, maxQueue: 100, maxWaitMs: 3_000 },
  rateLimit: { enabled: true },
  cache: { enabled: true },
  logFile: 'logs/requests.jsonl',
  prices: {},
};

type DeepPartial<T> = { [K in keyof T]?: T[K] extends Record<string, unknown> ? Partial<T[K]> : T[K] };

/** Junta uma configuração parcial com os valores padrão. */
export function makeConfig(partial: DeepPartial<Config>): Config {
  return {
    ...defaults,
    ...partial,
    timeouts: { ...defaults.timeouts, ...partial.timeouts },
    retry: { ...defaults.retry, ...partial.retry },
    breaker: { ...defaults.breaker, ...partial.breaker },
    limiter: { ...defaults.limiter, ...partial.limiter },
    rateLimit: { ...defaults.rateLimit, ...partial.rateLimit },
    cache: { ...defaults.cache, ...partial.cache },
  } as Config;
}

export function loadConfig(path = process.env.GATEWAY_CONFIG ?? 'config.json'): Config {
  const config = makeConfig(JSON.parse(readFileSync(path, 'utf8')));
  if (process.env.PORT) config.port = Number(process.env.PORT);
  if (process.env.REDIS_URL) config.redisUrl = process.env.REDIS_URL;
  // a chave real do provedor fica no .env, nunca no arquivo de configuração
  for (const upstream of config.upstreams) {
    if (upstream.apiKey.startsWith('env:')) upstream.apiKey = process.env[upstream.apiKey.slice(4)] ?? '';
  }
  return config;
}
