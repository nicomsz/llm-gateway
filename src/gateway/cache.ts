// Cache de respostas: a mesma pergunta, feita de novo pelo mesmo tenant, não vai ao provedor.
import { createHash } from 'node:crypto';

/** JSON com as chaves em ordem alfabética: {"a":1,"b":2} e {"b":2,"a":1} viram o mesmo texto. */
export function stableStringify(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableStringify(object[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

/**
 * Identificação de um request para o cache.
 * O tenant entra na chave: a resposta de um tenant nunca é entregue a outro.
 * O campo `stream` sai: a resposta é a mesma, só muda o formato de entrega.
 */
export function cacheKey(tenant: string, body: Record<string, unknown>): string {
  const { stream: _stream, ...rest } = body;
  return `cache:${createHash('sha256').update(`${tenant}\n${stableStringify(rest)}`).digest('hex')}`;
}

export interface CacheStore {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlSec: number): Promise<void>;
}

export class MemoryCache implements CacheStore {
  private entries = new Map<string, { value: string; expiresAt: number }>();

  constructor(private now: () => number = Date.now) {}

  async get(key: string): Promise<string | null> {
    const entry = this.entries.get(key);
    if (!entry) return null;
    if (entry.expiresAt <= this.now()) {
      this.entries.delete(key);
      return null;
    }
    return entry.value;
  }

  async set(key: string, value: string, ttlSec: number): Promise<void> {
    this.entries.set(key, { value, expiresAt: this.now() + ttlSec * 1000 });
  }
}
