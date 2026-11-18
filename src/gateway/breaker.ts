// Circuit breaker: para de enviar requests a um provedor que está falhando.
//
//   fechado      requests passam; conta falhas seguidas      N falhas     -> aberto
//   aberto       nenhum request vai para o provedor          passa openMs -> meio-aberto
//   meio-aberto  deixa passar 1 request de teste             sucesso -> fechado | falha -> aberto

export type BreakerState = 'closed' | 'open' | 'half-open';

export interface BreakerOptions {
  failureThreshold: number;
  openMs: number;
}

export class CircuitBreaker {
  private current: BreakerState = 'closed';
  private failures = 0;
  private openedAt = 0;
  private probeInFlight = false;

  constructor(
    private options: BreakerOptions,
    private now: () => number = Date.now,
    private onChange?: (from: BreakerState, to: BreakerState) => void,
  ) {}

  get state(): BreakerState {
    this.tick();
    return this.current;
  }

  /** Responde se um request poderia ser enviado agora, sem reservar nada. */
  available(): boolean {
    this.tick();
    return this.current === 'closed' || (this.current === 'half-open' && !this.probeInFlight);
  }

  /** Pede permissão para enviar um request. No estado meio-aberto, só o primeiro pedido passa. */
  acquire(): boolean {
    if (!this.available()) return false;
    if (this.current === 'half-open') this.probeInFlight = true;
    return true;
  }

  success() {
    this.failures = 0;
    this.probeInFlight = false;
    if (this.current !== 'closed') this.transition('closed');
  }

  failure() {
    this.tick();
    this.probeInFlight = false;
    if (this.current === 'half-open') return this.open();
    if (this.current === 'open') return;
    this.failures++;
    if (this.failures >= this.options.failureThreshold) this.open();
  }

  /** A tentativa terminou sem dizer nada sobre a saúde do provedor (ex.: o cliente cancelou). */
  release() {
    this.probeInFlight = false;
  }

  /** Quanto falta para o circuito aberto ir para meio-aberto. */
  retryAfterMs(): number {
    return this.state === 'open' ? Math.max(0, this.openedAt + this.options.openMs - this.now()) : 0;
  }

  private open() {
    this.openedAt = this.now();
    this.failures = 0;
    this.transition('open');
  }

  private tick() {
    if (this.current === 'open' && this.now() - this.openedAt >= this.options.openMs) this.transition('half-open');
  }

  private transition(to: BreakerState) {
    const from = this.current;
    if (from === to) return;
    this.current = to;
    this.onChange?.(from, to);
  }
}
