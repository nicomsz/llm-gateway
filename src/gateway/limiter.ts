// Limite de requests em andamento, com fila limitada.
// Quem chega com todas as vagas ocupadas espera numa fila. A fila tem tamanho máximo e
// tempo máximo de espera: passou de um dos dois, o pedido é recusado na hora.

export class QueueFullError extends Error {
  constructor() {
    super('fila cheia');
  }
}

export class QueueTimeoutError extends Error {
  constructor() {
    super('tempo máximo de espera na fila');
  }
}

export interface LimiterOptions {
  maxInFlight: number;
  maxQueue: number;
  maxWaitMs: number;
}

interface Waiter {
  grant: () => void;
}

export class Limiter {
  inFlight = 0;
  private queue: Waiter[] = [];

  constructor(private options: LimiterOptions) {}

  get queued(): number {
    return this.queue.length;
  }

  /**
   * Pede uma vaga. A Promise resolve com a função que devolve a vaga.
   * Chame essa função num bloco `finally`, para nenhum caminho de erro esquecer.
   */
  acquire(signal?: AbortSignal): Promise<() => void> {
    if (this.inFlight < this.options.maxInFlight) {
      this.inFlight++;
      return Promise.resolve(this.releaser());
    }
    if (this.queue.length >= this.options.maxQueue) return Promise.reject(new QueueFullError());

    return new Promise((resolve, reject) => {
      const leave = (error: Error) => {
        const index = this.queue.indexOf(waiter);
        if (index === -1) return;
        this.queue.splice(index, 1);
        cleanup();
        reject(error);
      };
      const onAbort = () => leave(new DOMException('cancelado', 'AbortError'));
      const timer = setTimeout(() => leave(new QueueTimeoutError()), this.options.maxWaitMs);
      const cleanup = () => {
        clearTimeout(timer);
        signal?.removeEventListener('abort', onAbort);
      };
      const waiter: Waiter = {
        // a vaga passa direto de quem saiu para quem estava esperando: inFlight não muda
        grant: () => {
          cleanup();
          resolve(this.releaser());
        },
      };
      signal?.addEventListener('abort', onAbort, { once: true });
      this.queue.push(waiter);
    });
  }

  private releaser(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = this.queue.shift();
      if (next) next.grant();
      else this.inFlight--;
    };
  }
}
