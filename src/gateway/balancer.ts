// Escolha do provedor, classificação de erros e espera entre tentativas.
import type { Upstream } from './config.js';
import type { CircuitBreaker } from './breaker.js';
import type { Limiter } from './limiter.js';

export interface UpstreamState {
  upstream: Upstream;
  breaker: CircuitBreaker;
  limiter: Limiter;
}

export interface AttemptResult {
  status?: number;
  /** falha de conexão com o provedor */
  networkError?: boolean;
  timeout?: boolean;
}

/**
 * Vale tentar o mesmo request em outro provedor?
 * Sim para falhas do provedor (conexão, tempo limite, sobrecarga).
 * Não para erros do request (400, 401...): outro provedor daria a mesma resposta.
 */
export function isRetryable(result: AttemptResult): boolean {
  if (result.networkError || result.timeout) return true;
  const status = result.status ?? 0;
  return status === 429 || status === 529 || (status >= 500 && status <= 504);
}

/** A resposta indica problema no provedor? É o que o circuit breaker conta como falha. */
export function isUpstreamFailure(result: AttemptResult): boolean {
  if (result.networkError || result.timeout) return true;
  const status = result.status ?? 0;
  return status >= 500;
}

/**
 * Espera antes da próxima tentativa: dobra a cada falha, até um teto, e é multiplicada
 * por um número aleatório entre 0 e 1 (jitter), para que requests que falharam juntos
 * não tentem de novo todos no mesmo instante.
 */
export function backoffMs(attempt: number, random: number, baseMs = 200, capMs = 2000): number {
  return Math.min(capMs, baseMs * 2 ** attempt) * random;
}

export class Balancer {
  private cursor = -1;

  constructor(
    private all: UpstreamState[],
    private strategy: 'round-robin' | 'least-in-flight',
  ) {}

  /** Escolhe entre os candidatos. `candidates` é um subconjunto de `all`. */
  pick(candidates: UpstreamState[]): UpstreamState | undefined {
    if (candidates.length === 0) return undefined;
    // percorre a lista a partir de quem vem depois do último escolhido
    const ordered: UpstreamState[] = [];
    for (let step = 1; step <= this.all.length; step++) {
      const state = this.all[(this.cursor + step) % this.all.length]!;
      if (candidates.includes(state)) ordered.push(state);
    }

    let chosen = ordered[0]!;
    if (this.strategy === 'least-in-flight') {
      // menos ocupado; no empate, vale a ordem do rodízio
      const load = (state: UpstreamState) => state.limiter.inFlight + state.limiter.queued;
      for (const state of ordered) if (load(state) < load(chosen)) chosen = state;
    }
    this.cursor = this.all.indexOf(chosen);
    return chosen;
  }
}
