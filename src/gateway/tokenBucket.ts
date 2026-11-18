// Rate limit por tokens com o algoritmo token bucket.
// Por tenant, guardamos dois números: o saldo e o momento da última atualização.
// O saldo sobe com o tempo, numa taxa fixa, até um teto. Cada request desconta do saldo.

export interface TakeResult {
  allowed: boolean;
  /** saldo depois da operação */
  remaining: number;
  /** segundos até o pedido caber (0 quando foi permitido) */
  retryAfterSec: number;
}

export interface RateLimitStore {
  /** Tenta descontar `amount` do balde `key`. */
  take(key: string, capacity: number, ratePerSec: number, amount: number): Promise<TakeResult>;
  /** Devolve tokens reservados e não usados. */
  refund(key: string, capacity: number, amount: number): Promise<void>;
}

interface Bucket {
  tokens: number;
  ts: number;
}

/** Versão em memória: serve para um processo só. Com mais de uma cópia do gateway, use o Redis. */
export class MemoryTokenBucket implements RateLimitStore {
  private buckets = new Map<string, Bucket>();

  // o relógio entra como parâmetro para os testes avançarem o tempo sem esperar
  constructor(private now: () => number = Date.now) {}

  async take(key: string, capacity: number, ratePerSec: number, amount: number): Promise<TakeResult> {
    const now = this.now();
    const bucket = this.buckets.get(key) ?? { tokens: capacity, ts: now };
    const elapsedSec = Math.max(0, now - bucket.ts) / 1000;
    bucket.tokens = Math.min(capacity, bucket.tokens + elapsedSec * ratePerSec);
    bucket.ts = now;
    this.buckets.set(key, bucket);

    if (amount <= bucket.tokens) {
      bucket.tokens -= amount;
      return { allowed: true, remaining: Math.floor(bucket.tokens), retryAfterSec: 0 };
    }
    return { allowed: false, remaining: Math.floor(bucket.tokens), retryAfterSec: Math.ceil((amount - bucket.tokens) / ratePerSec) };
  }

  async refund(key: string, capacity: number, amount: number): Promise<void> {
    const bucket = this.buckets.get(key);
    if (bucket) bucket.tokens = Math.min(capacity, bucket.tokens + amount);
  }
}

/**
 * Estimativa do custo de um request antes de chamar o provedor.
 * Entrada: tamanho do corpo em caracteres dividido por 4. Saída: o max_tokens pedido.
 */
export function estimateTokens(rawBody: string, maxTokens: unknown): number {
  const output = typeof maxTokens === 'number' && maxTokens > 0 ? maxTokens : 1024;
  return Math.ceil(rawBody.length / 4) + output;
}
