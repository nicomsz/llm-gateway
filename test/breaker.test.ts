import { describe, expect, test } from 'vitest';
import { CircuitBreaker, type BreakerState } from '../src/gateway/breaker.js';

function breakerWithClock() {
  let now = 0;
  const changes: string[] = [];
  const breaker = new CircuitBreaker({ failureThreshold: 5, openMs: 10_000 }, () => now, (from: BreakerState, to: BreakerState) => changes.push(`${from}->${to}`));
  const fail = (times: number) => {
    for (let i = 0; i < times; i++) breaker.failure();
  };
  return { breaker, changes, fail, advance: (ms: number) => void (now += ms) };
}

describe('circuit breaker', () => {
  test('5 falhas seguidas abrem o circuito', () => {
    const { breaker, fail } = breakerWithClock();
    fail(4);
    expect(breaker.state).toBe('closed');
    fail(1);
    expect(breaker.state).toBe('open');
    expect(breaker.acquire()).toBe(false);
  });

  test('um sucesso zera a contagem de falhas', () => {
    const { breaker, fail } = breakerWithClock();
    fail(4);
    breaker.success();
    fail(4);
    expect(breaker.state).toBe('closed');
  });

  test('aberto: não permite antes de 10 s, e vira meio-aberto aos 10 s', () => {
    const { breaker, fail, advance } = breakerWithClock();
    fail(5);
    advance(9_000);
    expect(breaker.acquire()).toBe(false);
    expect(breaker.retryAfterMs()).toBe(1_000);
    advance(1_000);
    expect(breaker.state).toBe('half-open');
  });

  test('meio-aberto deixa passar um request só', () => {
    const { breaker, fail, advance } = breakerWithClock();
    fail(5);
    advance(10_000);
    expect(breaker.acquire()).toBe(true);
    expect(breaker.acquire()).toBe(false);
  });

  test('meio-aberto + sucesso fecha o circuito', () => {
    const { breaker, fail, advance, changes } = breakerWithClock();
    fail(5);
    advance(10_000);
    breaker.acquire();
    breaker.success();
    expect(breaker.state).toBe('closed');
    expect(changes).toEqual(['closed->open', 'open->half-open', 'half-open->closed']);
  });

  test('meio-aberto + falha abre de novo, com nova espera', () => {
    const { breaker, fail, advance } = breakerWithClock();
    fail(5);
    advance(10_000);
    breaker.acquire();
    breaker.failure();
    expect(breaker.state).toBe('open');
    expect(breaker.retryAfterMs()).toBe(10_000);
  });
});
