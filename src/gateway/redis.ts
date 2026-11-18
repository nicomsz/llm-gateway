// Rate limit e cache no Redis: o mesmo saldo e as mesmas respostas para todas as cópias do gateway.
import { createClient } from 'redis';
import type { CacheStore } from './cache.js';
import type { RateLimitStore, TakeResult } from './tokenBucket.js';

/** A parte do cliente do Redis que o gateway usa. */
export interface Redis {
  eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown>;
  get(key: string): Promise<unknown>;
  setEx(key: string, seconds: number, value: string): Promise<unknown>;
  close(): Promise<unknown>;
}

export async function connectRedis(url: string): Promise<Redis> {
  const client = createClient({
    url,
    // sem conexão, os comandos falham na hora em vez de esperar numa fila
    disableOfflineQueue: true,
    socket: { reconnectStrategy: (retries) => Math.min(retries * 100, 2000) },
  });
  // os erros de conexão já aparecem em cada comando; este listener só impede o processo de cair
  client.on('error', () => {});
  await client.connect();
  return client as unknown as Redis;
}

// Lê o saldo, calcula a recarga, decide e grava, tudo numa execução só.
// O Redis roda o script inteiro sem atender outro comando no meio. Com "ler" e "gravar" em
// dois comandos, dois requests leriam o mesmo saldo e os dois passariam.
//   KEYS[1] = balde   ARGV = teto, taxa por segundo, pedido, agora (ms)
const TAKE = `
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local amount = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

tokens = math.min(capacity, tokens + math.max(0, now - ts) / 1000 * rate)

local allowed = 0
local retry = 0
if amount <= tokens then
  tokens = tokens - amount
  allowed = 1
else
  retry = math.ceil((amount - tokens) / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('EXPIRE', KEYS[1], math.ceil(capacity / rate) * 2)
return { allowed, math.floor(tokens), retry }
`;

//   KEYS[1] = balde   ARGV = teto, quantidade a devolver
const REFUND = `
local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
if tokens == nil then return 0 end
tokens = math.min(tonumber(ARGV[1]), tokens + tonumber(ARGV[2]))
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens))
return math.floor(tokens)
`;

export class RedisTokenBucket implements RateLimitStore {
  constructor(
    private redis: Redis,
    private now: () => number = Date.now,
  ) {}

  async take(key: string, capacity: number, ratePerSec: number, amount: number): Promise<TakeResult> {
    const [allowed, remaining, retryAfterSec] = (await this.redis.eval(TAKE, {
      keys: [key],
      arguments: [String(capacity), String(ratePerSec), String(amount), String(this.now())],
    })) as [number, number, number];
    return { allowed: allowed === 1, remaining, retryAfterSec };
  }

  async refund(key: string, capacity: number, amount: number): Promise<void> {
    await this.redis.eval(REFUND, { keys: [key], arguments: [String(capacity), String(amount)] });
  }
}

export class RedisCache implements CacheStore {
  constructor(private redis: Redis) {}

  get(key: string): Promise<string | null> {
    return this.redis.get(key) as Promise<string | null>;
  }

  async set(key: string, value: string, ttlSec: number): Promise<void> {
    await this.redis.setEx(key, Math.ceil(ttlSec), value);
  }
}
