// Sobe o gateway lendo config.json (ou o arquivo em GATEWAY_CONFIG).
import { loadConfig } from './config.js';
import { startGateway } from './server.js';

const config = loadConfig();
const gateway = await startGateway(config);

console.log(`gateway em ${gateway.url}`);
console.log(`provedores: ${config.upstreams.map((upstream) => upstream.name).join(', ')}`);
console.log(`rate limit e cache: ${config.redisUrl ? 'Redis' : 'memória do processo'}`);
console.log(`estado ao vivo: curl -s ${gateway.url}/_status`);

// encerra com calma: fecha o servidor e termina de gravar o log antes de sair
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => void gateway.close().then(() => process.exit(0)));
}
