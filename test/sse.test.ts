import { describe, expect, test } from 'vitest';
import { SseParser, StreamTracker, messageToSse } from '../src/gateway/sse.js';

describe('leitor de SSE', () => {
  test('junta um evento que chegou partido em dois pedaços', () => {
    const parser = new SseParser();
    expect(parser.push('event: mess')).toEqual([]);
    expect(parser.push('age_stop\ndata: {"type":"message_stop"}\n\n')).toEqual([{ event: 'message_stop', data: '{"type":"message_stop"}' }]);
  });

  test('separa três eventos que chegaram num pedaço só', () => {
    const parser = new SseParser();
    const events = parser.push('event: a\ndata: 1\n\nevent: b\ndata: 2\n\nevent: c\ndata: 3\n\n');
    expect(events.map((e) => e.event)).toEqual(['a', 'b', 'c']);
  });

  test('caractere acentuado partido entre dois pedaços de bytes', () => {
    const bytes = new TextEncoder().encode('event: x\ndata: {"t":"ação"}\n\n');
    const cut = bytes.indexOf(0xc3) + 1; // no meio do "ç"
    const parser = new SseParser();
    const events = [...parser.push(bytes.slice(0, cut)), ...parser.push(bytes.slice(cut))];
    expect(JSON.parse(events[0]!.data).t).toBe('ação');
  });

  test('o tracker lê tokens e texto, e a mensagem montada volta a ser SSE', () => {
    const message = {
      id: 'msg_1', type: 'message' as const, role: 'assistant' as const, model: 'mock-1',
      content: [{ type: 'text' as const, text: 'olá mundo' }], stop_reason: 'end_turn', usage: { input_tokens: 12, output_tokens: 6 },
    };
    const tracker = new StreamTracker();
    tracker.feed(new SseParser().push(messageToSse(message)));
    expect(tracker.complete).toBe(true);
    expect(tracker.toMessage()).toEqual(message);
  });

  test('streaming cancelado: sem o total, a saída é estimada pelo texto recebido', () => {
    const tracker = new StreamTracker();
    tracker.feed([{ event: 'content_block_delta', data: '{"delta":{"type":"text_delta","text":"12345678"}}' }]);
    expect(tracker.complete).toBe(false);
    expect(tracker.outputTokens).toBe(2);
  });
});
