// Provedor falso: imita POST /v1/messages da API de mensagens, com e sem streaming,
// com latência e falhas configuráveis. Serve para testar o gateway sem gastar com a API real.
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';

export type MockMode = 'ok' | 'down' | 'slow';

export interface MockOptions {
  port?: number;
  name?: string;
  /** espera antes do primeiro byte */
  ttftMs?: number;
  /** pausa entre os pedaços da resposta */
  chunkMs?: number;
  /** quantos pedaços a resposta tem (também é o número de tokens de saída) */
  chunks?: number;
  /** fração dos requests que recebe 529 (0 a 1) */
  errorRate?: number;
}

export interface MockStats {
  name: string;
  mode: MockMode;
  requests: number;
  /** requests em que o cliente fechou a conexão antes do fim */
  disconnects: number;
  /** pedaços de texto enviados em streaming (cada pedaço vale 1 token de saída) */
  chunksSent: number;
}

export interface Mock {
  server: Server;
  port: number;
  url: string;
  setMode(mode: MockMode): void;
  stats(): MockStats;
  close(): Promise<void>;
}

const SLOW_TTFT_MS = 10_000;

async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks).toString('utf8');
}

function sendJson(res: ServerResponse, status: number, body: unknown, headers: Record<string, string> = {}) {
  res.writeHead(status, { 'content-type': 'application/json', ...headers });
  res.end(JSON.stringify(body));
}

function sseEvent(type: string, data: Record<string, unknown>): string {
  return `event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`;
}

export async function startMock(options: MockOptions = {}): Promise<Mock> {
  const name = options.name ?? 'mock';
  const ttftMs = options.ttftMs ?? 50;
  const chunkMs = options.chunkMs ?? 10;
  const chunkCount = options.chunks ?? 5;
  const errorRate = options.errorRate ?? 0;

  let mode: MockMode = 'ok';
  let requests = 0;
  let disconnects = 0;
  let chunksSent = 0;
  let seq = 0;
  const stats = (): MockStats => ({ name, mode, requests, disconnects, chunksSent });

  const overloaded = (res: ServerResponse) =>
    sendJson(res, 529, { type: 'error', error: { type: 'overloaded_error', message: 'Overloaded' } });

  async function messages(req: IncomingMessage, res: ServerResponse) {
    requests++;
    const raw = await readBody(req);

    let body: { model?: string; stream?: boolean; messages?: unknown };
    try {
      body = JSON.parse(raw);
    } catch {
      return sendJson(res, 400, { type: 'error', error: { type: 'invalid_request_error', message: 'invalid JSON' } });
    }
    if (!Array.isArray(body.messages)) {
      return sendJson(res, 400, { type: 'error', error: { type: 'invalid_request_error', message: 'messages: field required' } });
    }
    if (mode === 'down' || Math.random() < errorRate) return overloaded(res);

    // se o cliente fechar a conexão antes do fim, paramos de esperar e de escrever
    const gone = new AbortController();
    res.on('close', () => {
      if (!res.writableEnded) {
        disconnects++;
        gone.abort();
      }
    });

    const id = `msg_${name}_${String(++seq).padStart(4, '0')}`;
    const model = body.model ?? 'mock-1';
    const words = Array.from({ length: chunkCount }, (_, i) => (i === 0 ? `resposta falsa do ${name}` : ` p${i}`));
    const usage = { input_tokens: Math.ceil(raw.length / 4), output_tokens: chunkCount };

    try {
      await sleep(mode === 'slow' ? SLOW_TTFT_MS : ttftMs, undefined, { signal: gone.signal });

      if (!body.stream) {
        await sleep(chunkMs * chunkCount, undefined, { signal: gone.signal });
        return sendJson(
          res,
          200,
          { id, type: 'message', role: 'assistant', model, content: [{ type: 'text', text: words.join('') }], stop_reason: 'end_turn', usage },
          { 'request-id': `req_${id}` },
        );
      }

      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', 'request-id': `req_${id}` });
      res.write(
        sseEvent('message_start', {
          message: { id, type: 'message', role: 'assistant', content: [], model, stop_reason: null, usage: { input_tokens: usage.input_tokens, output_tokens: 1 } },
        }),
      );
      res.write(sseEvent('content_block_start', { index: 0, content_block: { type: 'text', text: '' } }));
      for (const word of words) {
        // derrubado no meio do streaming: fecha a conexão sem terminar, como um processo que caiu
        if ((mode as MockMode) === 'down') return void res.destroy();
        res.write(sseEvent('content_block_delta', { index: 0, delta: { type: 'text_delta', text: word } }));
        chunksSent++;
        await sleep(chunkMs, undefined, { signal: gone.signal });
      }
      res.write(sseEvent('content_block_stop', { index: 0 }));
      res.write(sseEvent('message_delta', { delta: { stop_reason: 'end_turn' }, usage: { output_tokens: usage.output_tokens } }));
      res.write(sseEvent('message_stop', {}));
      res.end();
    } catch (error) {
      if (!gone.signal.aborted) throw error; // cliente desconectou: nada a fazer
    }
  }

  const server = createServer((req, res) => {
    const route = `${req.method} ${req.url}`;
    if (route === 'POST /v1/messages') {
      messages(req, res).catch(() => res.destroy());
    } else if (route === 'POST /_control') {
      readBody(req).then((raw) => {
        const next = (JSON.parse(raw || '{}') as { mode?: MockMode }).mode;
        if (next === 'ok' || next === 'down' || next === 'slow') mode = next;
        sendJson(res, 200, stats());
      }, () => res.destroy());
    } else if (route === 'GET /_control') {
      sendJson(res, 200, stats());
    } else {
      sendJson(res, 404, { type: 'error', error: { type: 'not_found_error', message: 'not found' } });
    }
  });

  await new Promise<void>((resolve) => server.listen(options.port ?? 0, resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    server,
    port,
    url: `http://localhost:${port}`,
    setMode: (next) => void (mode = next),
    stats,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}
