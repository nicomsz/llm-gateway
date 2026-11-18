// Sobe os três provedores falsos usados na demonstração: dois rápidos e um lento.
import { startMock } from './server.js';

const mocks = await Promise.all([
  startMock({ port: 4001, name: 'mock-a', ttftMs: 200, chunkMs: 50, chunks: 10 }),
  startMock({ port: 4002, name: 'mock-b', ttftMs: 800, chunkMs: 50, chunks: 10 }),
  startMock({ port: 4003, name: 'mock-c', ttftMs: 200, chunkMs: 50, chunks: 10 }),
]);

for (const mock of mocks) console.log(`${mock.stats().name} em ${mock.url}`);
console.log('\nDerrubar um:  curl -X POST localhost:4003/_control -d \'{"mode":"down"}\'');
console.log('Voltar:       curl -X POST localhost:4003/_control -d \'{"mode":"ok"}\'');
