import { describe, expect, test } from 'vitest';
import { MemoryTokenBucket, estimateTokens } from '../src/gateway/tokenBucket.js';

// relógio falso: o teste "avança o tempo" somando na variável
function bucketWithClock() {
  let now = 1_000_000;
  const bucket = new MemoryTokenBucket(() => now);
  return { bucket, advance: (ms: number) => void (now += ms) };
}

const CAP = 1000;
const RATE = CAP / 60; // 1.000 tokens por minuto

describe('token bucket', () => {
  test('três pedidos de 400 no mesmo instante: passa, passa, nega com retry-after', async () => {
    const { bucket } = bucketWithClock();
    expect((await bucket.take('acme', CAP, RATE, 400)).allowed).toBe(true);
    expect((await bucket.take('acme', CAP, RATE, 400)).allowed).toBe(true);
    const third = await bucket.take('acme', CAP, RATE, 400);
    expect(third.allowed).toBe(false);
    expect(third.remaining).toBe(200);
    expect(third.retryAfterSec).toBe(12); // faltam 200 tokens, a 16,67 por segundo
  });

  test('depois de negar, 30 segundos de recarga liberam um pedido de 400', async () => {
    const { bucket, advance } = bucketWithClock();
    await bucket.take('acme', CAP, RATE, 400);
    await bucket.take('acme', CAP, RATE, 400);
    advance(30_000);
    const result = await bucket.take('acme', CAP, RATE, 400);
    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(300); // 200 + 500 de recarga - 400
  });

  test('o saldo nunca passa do teto', async () => {
    const { bucket, advance } = bucketWithClock();
    await bucket.take('acme', CAP, RATE, 100);
    advance(10 * 60_000);
    expect((await bucket.take('acme', CAP, RATE, 0)).remaining).toBe(CAP);
  });

  test('devolve a diferença entre a reserva e o uso real', async () => {
    const { bucket } = bucketWithClock();
    await bucket.take('acme', CAP, RATE, 600);
    await bucket.refund('acme', CAP, 400); // reservou 600, usou 200
    expect((await bucket.take('acme', CAP, RATE, 0)).remaining).toBe(800);
  });

  test('um tenant estourar não afeta o outro', async () => {
    const { bucket } = bucketWithClock();
    await bucket.take('acme', CAP, RATE, 1000);
    expect((await bucket.take('acme', CAP, RATE, 1)).allowed).toBe(false);
    expect((await bucket.take('beta', CAP, RATE, 1000)).allowed).toBe(true);
  });

  test('a estimativa soma a entrada (caracteres / 4) com o max_tokens', () => {
    expect(estimateTokens('x'.repeat(400), 500)).toBe(600);
    expect(estimateTokens('x'.repeat(400), undefined)).toBe(100 + 1024);
  });
});
