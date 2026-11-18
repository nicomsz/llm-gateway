// Sobe um provedor falso lendo a configuração de variáveis de ambiente.
//   PORT=4001 MOCK_NAME=mock-a MOCK_TTFT_MS=200 pnpm tsx src/mock/main.ts
import { startMock } from './server.js';

const num = (value: string | undefined, fallback: number) => (value === undefined ? fallback : Number(value));

const mock = await startMock({
  port: num(process.env.PORT, 4001),
  name: process.env.MOCK_NAME ?? 'mock-a',
  ttftMs: num(process.env.MOCK_TTFT_MS, 200),
  chunkMs: num(process.env.MOCK_CHUNK_MS, 50),
  chunks: num(process.env.MOCK_CHUNKS, 10),
  errorRate: num(process.env.MOCK_ERROR_RATE, 0),
});

console.log(`${mock.stats().name} em ${mock.url}`);
