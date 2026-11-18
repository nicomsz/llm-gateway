// Testes do caminho com Redis. Sobem um redis-server próprio numa porta livre.
// Se o binário não existir na máquina, os testes são pulados.
import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { afterAll, afterEach, beforeAll, describe, expect, test } from 'vitest';
import { RedisCache, RedisTokenBucket, connectRedis, type Redis } from '../src/gateway/redis.js';
import { ACME, BODY, send, setup, sleep, waitFor } from './helpers.js';

const hasRedis = spawnSync('redis-server', ['--version']).status === 0;

async function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const probe = createServer().listen(0, () => {
      const { port } = probe.address() as { port: number };
      probe.close(() => resolve(port));
    });
  });
}

describe.skipIf(!hasRedis)('com Redis', () => {
  let server: ChildProcess;
  let url: string;
  let redis: Redis;
  let env: Awaited<ReturnType<typeof setup>> | undefined;

  beforeAll(async () => {
    const port = await freePort();
    server = spawn('redis-server', ['--port', String(port), '--save', '', '--appendonly', 'no'], { stdio: 'ignore' });
    url = `redis://localhost:${port}`;
    for (let attempt = 0; ; attempt++) {
      try {
        redis = await connectRedis(url);
        break;
      } catch (error) {
        if (attempt > 50) throw error;
        await sleep(50);
      }
    }
  });
  afterEach(async () => {
    await env?.close();
    env = undefined;
  });
  afterAll(async () => {
    await redis?.close().catch(() => {});
    server?.kill();
  });

  test('o script Lua faz a mesma conta do balde em memória', async () => {
    let now = 1_000_000;
    const bucket = new RedisTokenBucket(redis, () => now);
    const key = `rl:t1:${Math.random()}`;
    expect(await bucket.take(key, 1000, 1000 / 60, 400)).toEqual({ allowed: true, remaining: 600, retryAfterSec: 0 });
    expect((await bucket.take(key, 1000, 1000 / 60, 400)).allowed).toBe(true);
    expect(await bucket.take(key, 1000, 1000 / 60, 400)).toEqual({ allowed: false, remaining: 200, retryAfterSec: 12 });
    now += 30_000;
    expect(await bucket.take(key, 1000, 1000 / 60, 400)).toEqual({ allowed: true, remaining: 300, retryAfterSec: 0 });
    await bucket.refund(key, 1000, 5000);
    expect((await bucket.take(key, 1000, 1000 / 60, 0)).remaining).toBe(1000); // a devolução não passa do teto
  });

  test('50 pedidos ao mesmo tempo, por duas conexões: o total permitido nunca passa do teto', async () => {
    const second = await connectRedis(url);
    const now = () => 5_000_000; // relógio parado: sem recarga
    const buckets = [new RedisTokenBucket(redis, now), new RedisTokenBucket(second, now)];
    const key = `rl:t2:${Math.random()}`;
    const results = await Promise.all(Array.from({ length: 50 }, (_, i) => buckets[i % 2]!.take(key, 1000, 1000 / 60, 100)));
    expect(results.filter((r) => r.allowed)).toHaveLength(10);
    await second.close();
  });

  test('cache: guarda, lê e expira', async () => {
    const cache = new RedisCache(redis);
    const key = `cache:t3:${Math.random()}`;
    expect(await cache.get(key)).toBeNull();
    await cache.set(key, 'valor', 1);
    expect(await cache.get(key)).toBe('valor');
    await sleep(1_100);
    expect(await cache.get(key)).toBeNull();
  });

  test('duas cópias do gateway dividem a mesma cota e o mesmo cache', async () => {
    const tenants = { [ACME]: { name: `acme-${Math.random()}`, tokensPerMinute: 1_300 } };
    env = await setup([{ ttftMs: 100 }], { redisUrl: url, tenants });
    const copy = await setup([{ ttftMs: 100 }], { redisUrl: url, tenants });
    try {
      // 6 requests de ~1.025 tokens, divididos entre as duas cópias: só 1 cabe na cota
      const big = { ...BODY, max_tokens: 1_000 };
      const responses = await Promise.all(Array.from({ length: 6 }, (_, i) => send((i % 2 ? copy : env!).gateway.url, big)));
      expect(responses.filter((r) => r.status === 200)).toHaveLength(1);
      expect(responses.filter((r) => r.status === 429)).toHaveLength(5);

      // a resposta guardada por uma cópia é servida pela outra
      const cached = { 'x-api-key': ACME, 'x-gateway-cache': '60' };
      const body = { ...BODY, max_tokens: 50, messages: [{ role: 'user', content: `pergunta ${Math.random()}` }] };
      await waitFor(() => env!.requests().length + copy.requests().length === 6);
      await sleep(50);
      expect((await send(env.gateway.url, body, cached)).headers.get('x-gateway-cache')).toBe('miss');
      await waitFor(() => env!.requests().some((log) => log.cache === 'miss'));
      expect((await send(copy.gateway.url, body, cached)).headers.get('x-gateway-cache')).toBe('hit');
    } finally {
      await copy.close();
    }
  });

  test('Redis cai com o gateway rodando: os requests continuam passando', async () => {
    const port = await freePort();
    const own = spawn('redis-server', ['--port', String(port), '--save', '', '--appendonly', 'no'], { stdio: 'ignore' });
    await sleep(300);
    env = await setup([{}], { redisUrl: `redis://localhost:${port}` });
    expect((await send(env.gateway.url)).status).toBe(200);
    own.kill();
    await sleep(300);
    const start = Date.now();
    expect((await send(env.gateway.url)).status).toBe(200);
    expect(Date.now() - start).toBeLessThan(1_000);
  });
});
