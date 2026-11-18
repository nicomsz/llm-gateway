// Log em JSONL: uma linha por request, cada linha um objeto JSON.
// Nunca entra aqui o conteúdo das mensagens nem as chaves de API.
import { createWriteStream, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

export interface Logger {
  write(entry: object): void;
  close(): Promise<void>;
}

export function fileLogger(path: string): Logger {
  mkdirSync(dirname(path), { recursive: true });
  const stream = createWriteStream(path, { flags: 'a' });
  return {
    write: (entry) => void stream.write(`${JSON.stringify(entry)}\n`),
    close: () => new Promise((resolve) => stream.end(() => resolve())),
  };
}

export interface MemoryLogger extends Logger {
  entries: Array<Record<string, any>>;
}

/** Guarda as linhas num array. Usado nos testes. */
export function memoryLogger(): MemoryLogger {
  const entries: Array<Record<string, any>> = [];
  return { entries, write: (entry) => void entries.push(entry as Record<string, any>), close: async () => {} };
}
