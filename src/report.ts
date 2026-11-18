// Lê o log de requests e imprime latência, tokens e custo por tenant e por provedor.
//   pnpm report                     (lê logs/requests.jsonl)
//   pnpm report outro/arquivo.jsonl
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import type { RequestLog } from './gateway/server.js';

/** Percentil pelo método nearest rank: ordena e pega a posição ceil(p/100 * n) - 1. */
export function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil((p / 100) * sorted.length) - 1)]!;
}

export interface Summary {
  requests: number;
  errorPct: number;
  p50Ms: number;
  p99Ms: number;
  p50TtftMs: number;
  p99TtftMs: number;
  tokens: number;
  costUsd: number;
  cacheHitPct: number;
}

export function summarize(entries: RequestLog[]): Summary {
  const ttfts = entries.flatMap((entry) => (entry.ttftMs === undefined ? [] : [entry.ttftMs]));
  const withCache = entries.filter((entry) => entry.cache !== undefined);
  const pct = (part: number, total: number) => (total === 0 ? 0 : Math.round((part / total) * 1000) / 10);
  return {
    requests: entries.length,
    // 429 é o gateway fazendo o trabalho dele, então conta como erro só o que é falha: 5xx e conexão perdida
    errorPct: pct(entries.filter((entry) => entry.status >= 500 || (entry.status === 0 && !entry.canceled)).length, entries.length),
    p50Ms: percentile(entries.map((entry) => entry.ms), 50),
    p99Ms: percentile(entries.map((entry) => entry.ms), 99),
    p50TtftMs: percentile(ttfts, 50),
    p99TtftMs: percentile(ttfts, 99),
    tokens: entries.reduce((sum, entry) => sum + entry.inputTokens + entry.outputTokens, 0),
    costUsd: Math.round(entries.reduce((sum, entry) => sum + entry.costUsd, 0) * 1e4) / 1e4,
    cacheHitPct: pct(withCache.filter((entry) => entry.cache === 'hit').length, withCache.length),
  };
}

export function readLog(path: string): RequestLog[] {
  return readFileSync(path, 'utf8')
    .split('\n')
    .filter((line) => line.trim() !== '')
    .map((line) => JSON.parse(line) as RequestLog & { event?: string })
    .filter((entry) => entry.event === undefined); // as linhas de mudança de estado do breaker ficam de fora
}

function groupBy(entries: RequestLog[], key: (entry: RequestLog) => string): Record<string, Summary> {
  const groups = new Map<string, RequestLog[]>();
  for (const entry of entries) {
    const name = key(entry);
    groups.set(name, [...(groups.get(name) ?? []), entry]);
  }
  return Object.fromEntries([...groups].map(([name, group]) => [name, summarize(group)]));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const entries = readLog(process.argv[2] ?? 'logs/requests.jsonl');
  console.log('\nPor tenant');
  console.table(groupBy(entries, (entry) => entry.tenant ?? '(sem tenant)'));
  console.log('\nPor provedor');
  console.table(groupBy(entries, (entry) => entry.upstream ?? '(nenhum)'));
}
