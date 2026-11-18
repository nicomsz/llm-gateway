import { describe, expect, test } from 'vitest';
import { Balancer, backoffMs, isRetryable, isUpstreamFailure, type UpstreamState } from '../src/gateway/balancer.js';
import { CircuitBreaker } from '../src/gateway/breaker.js';
import { Limiter } from '../src/gateway/limiter.js';
import { percentile } from '../src/report.js';

const states = (): UpstreamState[] =>
  ['a', 'b', 'c'].map((name) => ({
    upstream: { name, url: '', apiKey: '' },
    breaker: new CircuitBreaker({ failureThreshold: 5, openMs: 1000 }),
    limiter: new Limiter({ maxInFlight: 10, maxQueue: 10, maxWaitMs: 1000 }),
  }));

describe('classificação de erros', () => {
  test.each([429, 500, 502, 503, 504, 529])('%i: tenta em outro provedor', (status) => {
    expect(isRetryable({ status })).toBe(true);
  });
  test.each([400, 401, 403, 404, 413])('%i: não tenta de novo', (status) => {
    expect(isRetryable({ status })).toBe(false);
  });
  test('falha de conexão e tempo limite: tenta em outro', () => {
    expect(isRetryable({ networkError: true })).toBe(true);
    expect(isRetryable({ timeout: true })).toBe(true);
  });
  test('o breaker conta 5xx como falha, mas não 429 nem 400', () => {
    expect(isUpstreamFailure({ status: 529 })).toBe(true);
    expect(isUpstreamFailure({ status: 429 })).toBe(false);
    expect(isUpstreamFailure({ status: 400 })).toBe(false);
  });
});

describe('backoff', () => {
  test('dobra a cada tentativa até o teto', () => {
    expect([0, 1, 2, 3, 4, 5].map((attempt) => backoffMs(attempt, 1))).toEqual([200, 400, 800, 1600, 2000, 2000]);
  });
  test('o número aleatório encolhe a espera', () => {
    expect(backoffMs(2, 0.5)).toBe(400);
    expect(backoffMs(2, 0)).toBe(0);
  });
});

describe('balanceador', () => {
  test('rodízio: 9 requests, 3 para cada', () => {
    const all = states();
    const balancer = new Balancer(all, 'round-robin');
    const picks = Array.from({ length: 9 }, () => balancer.pick(all)!.upstream.name);
    expect(picks.join('')).toBe('abcabcabc');
  });

  test('rodízio pula quem não é candidato', () => {
    const all = states();
    const balancer = new Balancer(all, 'round-robin');
    const withoutB = [all[0]!, all[2]!];
    expect(Array.from({ length: 4 }, () => balancer.pick(withoutB)!.upstream.name).join('')).toBe('acac');
    expect(balancer.pick([])).toBeUndefined();
  });

  test('menos ocupado: escolhe quem tem menos requests em andamento', async () => {
    const all = states();
    const balancer = new Balancer(all, 'least-in-flight');
    await all[0]!.limiter.acquire();
    await all[0]!.limiter.acquire();
    await all[1]!.limiter.acquire();
    expect(balancer.pick(all)!.upstream.name).toBe('c');
  });
});

describe('percentil', () => {
  test('de 1 a 100: p50 é 50 e p99 é 99', () => {
    const values = Array.from({ length: 100 }, (_, i) => 100 - i);
    expect(percentile(values, 50)).toBe(50);
    expect(percentile(values, 99)).toBe(99);
    expect(percentile([], 50)).toBe(0);
  });
});
