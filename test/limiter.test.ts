import { describe, expect, test } from 'vitest';
import { Limiter, QueueFullError, QueueTimeoutError } from '../src/gateway/limiter.js';
import { sleep } from './helpers.js';

describe('limitador', () => {
  test('2 em andamento: o resto espera e entra na ordem de chegada', async () => {
    const limiter = new Limiter({ maxInFlight: 2, maxQueue: 10, maxWaitMs: 5_000 });
    const order: number[] = [];
    const start = Date.now();
    await Promise.all(
      [1, 2, 3, 4, 5].map(async (n) => {
        const release = await limiter.acquire();
        order.push(n);
        await sleep(100);
        release();
      }),
    );
    expect(order).toEqual([1, 2, 3, 4, 5]);
    expect(Date.now() - start).toBeGreaterThanOrEqual(280); // 3 rodadas de 100 ms
    expect(limiter.inFlight).toBe(0);
  });

  test('fila cheia recusa na hora', async () => {
    const limiter = new Limiter({ maxInFlight: 2, maxQueue: 2, maxWaitMs: 5_000 });
    const start = Date.now();
    const results = await Promise.allSettled(
      Array.from({ length: 10 }, async () => {
        const release = await limiter.acquire();
        await sleep(50);
        release();
      }),
    );
    const rejected = results.filter((r) => r.status === 'rejected') as PromiseRejectedResult[];
    expect(rejected).toHaveLength(6);
    expect(rejected.every((r) => r.reason instanceof QueueFullError)).toBe(true);
    expect(Date.now() - start).toBeLessThan(400);
  });

  test('espera além do máximo é recusada', async () => {
    const limiter = new Limiter({ maxInFlight: 1, maxQueue: 5, maxWaitMs: 50 });
    const release = await limiter.acquire();
    await expect(limiter.acquire()).rejects.toBeInstanceOf(QueueTimeoutError);
    expect(limiter.queued).toBe(0);
    release();
    expect(limiter.inFlight).toBe(0);
  });

  test('quem desiste de esperar sai da fila', async () => {
    const limiter = new Limiter({ maxInFlight: 1, maxQueue: 5, maxWaitMs: 5_000 });
    const release = await limiter.acquire();
    const gone = new AbortController();
    const waiting = limiter.acquire(gone.signal);
    gone.abort();
    await expect(waiting).rejects.toThrow();
    expect(limiter.queued).toBe(0);
    release();
    expect(limiter.inFlight).toBe(0);
  });

  test('devolver a mesma vaga duas vezes não libera duas', async () => {
    const limiter = new Limiter({ maxInFlight: 1, maxQueue: 5, maxWaitMs: 5_000 });
    const release = await limiter.acquire();
    release();
    release();
    expect(limiter.inFlight).toBe(0);
  });
});
