import { afterEach, describe, expect, test } from 'vitest';
import { ACME, BODY, UPSTREAM_KEY, send, setup, waitFor } from './helpers.js';

let env: Awaited<ReturnType<typeof setup>>;
afterEach(() => env?.close());

describe('proxy básico', () => {
  test('sem x-api-key: 401 no formato de erro do provedor', async () => {
    env = await setup([{}]);
    const response = await send(env.gateway.url, BODY, {});
    expect(response.status).toBe(401);
    expect(await response.json()).toMatchObject({ type: 'error', error: { type: 'authentication_error' } });
    expect(env.mocks[0]!.stats().requests).toBe(0);
  });

  test('chave desconhecida: 401', async () => {
    env = await setup([{}]);
    expect((await send(env.gateway.url, BODY, { 'x-api-key': 'nao-existe' })).status).toBe(401);
    expect((await send(env.gateway.url, BODY, { 'x-api-key': 'constructor' })).status).toBe(401);
  });

  test('chave válida: devolve o JSON do provedor e grava uma linha de log', async () => {
    env = await setup([{}]);
    const response = await send(env.gateway.url);
    expect(response.status).toBe(200);
    const message = await response.json();
    expect(message.content[0].text).toContain('resposta falsa do mock-a');
    expect(response.headers.get('x-gateway-upstream')).toBe('mock-a');

    await waitFor(() => env.requests().length === 1);
    const log = env.requests()[0]!;
    expect(log).toMatchObject({ tenant: 'acme', upstream: 'mock-a', status: 200, stream: false, outputTokens: 3 });
    expect(log.inputTokens).toBe(message.usage.input_tokens);
    expect(log.costUsd).toBeGreaterThan(0);
  });

  test('o log não guarda chave nem conteúdo de mensagem', async () => {
    env = await setup([{}]);
    await send(env.gateway.url, { ...BODY, messages: [{ role: 'user', content: 'segredo-do-cliente' }] });
    await waitFor(() => env.requests().length === 1);
    const text = JSON.stringify(env.logger.entries);
    expect(text).not.toContain(ACME);
    expect(text).not.toContain(UPSTREAM_KEY);
    expect(text).not.toContain('segredo-do-cliente');
  });

  test('corpo que não é JSON: 400', async () => {
    env = await setup([{}]);
    const response = await send(env.gateway.url, 'isto não é json');
    expect(response.status).toBe(400);
    expect((await response.json()).error.type).toBe('invalid_request_error');
  });

  test('corpo maior que 1 MB: 413', async () => {
    env = await setup([{}]);
    const response = await send(env.gateway.url, { ...BODY, messages: [{ role: 'user', content: 'x'.repeat(1_100_000) }] });
    expect(response.status).toBe(413);
  });

  test('provedor lento demais: 504 perto do tempo limite', async () => {
    env = await setup([{ ttftMs: 3_000 }], { timeouts: { firstByteMs: 200 }, retry: { maxAttempts: 1 } });
    const start = Date.now();
    const response = await send(env.gateway.url);
    expect(response.status).toBe(504);
    expect(Date.now() - start).toBeLessThan(1_000);
  });

  test('provedor desligado: 502', async () => {
    env = await setup([{}], { retry: { maxAttempts: 1 } });
    await env.mocks[0]!.close();
    expect((await send(env.gateway.url)).status).toBe(502);
  });

  test('erro 400 do provedor volta ao app sem alteração e sem nova tentativa', async () => {
    env = await setup([{}, {}, {}]);
    const response = await send(env.gateway.url, { model: 'mock-1', max_tokens: 10 }); // sem messages
    expect(response.status).toBe(400);
    expect((await response.json()).error.message).toBe('messages: field required');
    await waitFor(() => env.requests().length === 1);
    expect(env.requests()[0]!.attempts).toHaveLength(1);
  });

  test('rota desconhecida: 404', async () => {
    env = await setup([{}]);
    expect((await fetch(`${env.gateway.url}/outra`)).status).toBe(404);
  });
});
