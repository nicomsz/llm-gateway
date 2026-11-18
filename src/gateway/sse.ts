// Leitura de eventos SSE enquanto os bytes passam pelo gateway.
// Os pedaços que chegam da rede não coincidem com os eventos: um evento pode vir partido
// em dois pedaços, e um pedaço pode trazer vários eventos.

export interface SseEvent {
  event: string;
  data: string;
}

export class SseParser {
  private decoder = new TextDecoder();
  private buffer = '';

  /** Recebe um pedaço e devolve os eventos que ficaram completos com ele. */
  push(chunk: Uint8Array | string): SseEvent[] {
    this.buffer += typeof chunk === 'string' ? chunk : this.decoder.decode(chunk, { stream: true });
    const events: SseEvent[] = [];
    let end: number;
    while ((end = this.buffer.indexOf('\n\n')) !== -1) {
      const block = this.buffer.slice(0, end);
      this.buffer = this.buffer.slice(end + 2);
      let event = 'message';
      const data: string[] = [];
      for (const line of block.split('\n')) {
        if (line.startsWith('event:')) event = line.slice(6).trim();
        else if (line.startsWith('data:')) data.push(line.slice(5).trim());
      }
      if (data.length > 0) events.push({ event, data: data.join('\n') });
    }
    return events;
  }
}

export interface Message {
  id: string;
  type: 'message';
  role: 'assistant';
  model: string;
  content: Array<{ type: 'text'; text: string }>;
  stop_reason: string | null;
  usage: { input_tokens: number; output_tokens: number };
}

/** Acompanha um streaming: tokens, texto acumulado e se ele terminou inteiro. */
export class StreamTracker {
  inputTokens = 0;
  private reportedOutput: number | null = null;
  text = '';
  /** chegou o message_stop */
  complete = false;
  /** o provedor mandou um evento de erro no meio */
  sawError = false;
  private id = '';
  private model = '';
  private stopReason: string | null = null;

  feed(events: SseEvent[]) {
    for (const { event, data } of events) {
      let payload: any;
      try {
        payload = JSON.parse(data);
      } catch {
        continue;
      }
      if (event === 'message_start') {
        this.id = payload.message?.id ?? '';
        this.model = payload.message?.model ?? '';
        this.inputTokens = payload.message?.usage?.input_tokens ?? 0;
      } else if (event === 'content_block_delta' && payload.delta?.type === 'text_delta') {
        this.text += payload.delta.text;
      } else if (event === 'message_delta') {
        // a contagem do message_delta é cumulativa: vale o último valor
        this.reportedOutput = payload.usage?.output_tokens ?? this.reportedOutput;
        this.stopReason = payload.delta?.stop_reason ?? this.stopReason;
      } else if (event === 'message_stop') {
        this.complete = true;
      } else if (event === 'error') {
        this.sawError = true;
      }
    }
  }

  /** Num streaming cancelado, o total ainda não chegou: estimamos pelo texto recebido. */
  get outputTokens(): number {
    return this.reportedOutput ?? Math.ceil(this.text.length / 4);
  }

  toMessage(): Message {
    return {
      id: this.id,
      type: 'message',
      role: 'assistant',
      model: this.model,
      content: [{ type: 'text', text: this.text }],
      stop_reason: this.stopReason,
      usage: { input_tokens: this.inputTokens, output_tokens: this.outputTokens },
    };
  }
}

const frame = (type: string, data: Record<string, unknown>) => `event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`;

/** Transforma uma mensagem guardada no cache na sequência de eventos SSE que o cliente espera. */
export function messageToSse(message: Message): string {
  const text = message.content.map((block) => block.text).join('');
  return [
    frame('message_start', { message: { ...message, content: [], stop_reason: null, usage: { input_tokens: message.usage.input_tokens, output_tokens: 1 } } }),
    frame('content_block_start', { index: 0, content_block: { type: 'text', text: '' } }),
    frame('content_block_delta', { index: 0, delta: { type: 'text_delta', text } }),
    frame('content_block_stop', { index: 0 }),
    frame('message_delta', { delta: { stop_reason: message.stop_reason }, usage: { output_tokens: message.usage.output_tokens } }),
    frame('message_stop', {}),
  ].join('');
}

export function errorEvent(type: string, message: string): string {
  return `event: error\ndata: ${JSON.stringify({ type: 'error', error: { type, message } })}\n\n`;
}
