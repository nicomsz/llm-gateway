# Resultados dos experimentos

Gerado por `pnpm bench` em 2026-11-18.
Ambiente: Intel(R) Xeon(R) Processor @ 2.10GHz, 2 núcleos, Node v22.23.4, linux.
Tudo roda na mesma máquina, com provedores falsos de latência fixa.

## A. Custo de passar pelo gateway

1 provedor falso que responde em 100 ms; 2.000 requests, 20 ao mesmo tempo, depois de 200 de aquecimento.

| caminho | requests | erros % | p50 ms | p99 ms | req/s |
| --- | --- | --- | --- | --- | --- |
| direto no provedor | 2000 | 0 | 102.9 | 113.2 | 193 |
| pelo gateway | 2000 | 0 | 103.9 | 119.4 | 189 |

## B. Um provedor fora do ar

3 provedores de 100 ms, 1 deles devolvendo 529 em tudo; 150 requests por segundo durante 8 s. Latência só dos requests com sucesso.

| configuração | requests | erros % | p50 ms | p99 ms | chamadas ao provedor caído |
| --- | --- | --- | --- | --- | --- |
| round-robin, sem failover | 1200 | 33.3 | 104 | 112 | 400 |
| least-in-flight, sem failover | 1200 | 87.2 | 104 | 126 | 1046 |
| least-in-flight + failover | 1200 | 0 | 206 | 303 | 1196 |
| least-in-flight + failover + circuit breaker | 1200 | 0 | 104 | 115 | 8 |

## C. Um provedor travado

3 provedores de 100 ms, 1 deles sem responder; tempo limite de 1 s por tentativa; 150 requests por segundo durante 15 s.

| configuração | requests | erros % | p50 ms | p99 ms | requests acima de 1 s |
| --- | --- | --- | --- | --- | --- |
| sem circuit breaker | 2250 | 0 | 104 | 1270 | 138 |
| com circuit breaker | 2250 | 0 | 104 | 118 | 16 |

## D. Um tenant barulhento

Provedor com capacidade para ~100 requests por segundo. O tenant acme envia 300 por segundo (cota: 6.000 tokens por minuto); o tenant beta envia 20 por segundo em média, em intervalos aleatórios, durante 15 s.

| configuração | beta: requests | beta: erros % | beta: p50 ms | beta: p99 ms | acme: atendidos | acme: 429 | acme: 503 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| sem rate limit | 305 | 75.1 | 1137 | 1154 | 1665 | 0 | 3435 |
| com rate limit | 305 | 0 | 104 | 112 | 245 | 4855 | 0 |

## E. Cache de respostas

1.000 requests, 10 ao mesmo tempo; 30% deles repetem uma de 10 perguntas frequentes (premissa do teste). Custo calculado com um preço de exemplo.

| configuração | requests | chamadas ao provedor | respostas do cache % | custo US$ | média ms |
| --- | --- | --- | --- | --- | --- |
| sem cache | 1000 | 1000 | 0 | 0.159 | 105 |
| com cache | 1000 | 689 | 31.1 | 0.1095 | 73 |

## F. Streaming

Provedor que envia o primeiro pedaço em 200 ms e depois 20 pedaços de texto, um a cada 50 ms; 300 requests, 20 ao mesmo tempo, depois de 40 de aquecimento.

| caminho | requests | primeiro pedaço p50 ms | primeiro pedaço p99 ms | resposta completa p50 ms |
| --- | --- | --- | --- | --- |
| direto no provedor | 300 | 202 | 207 | 1208 |
| pelo gateway | 300 | 203 | 221 | 1208 |

## G. Cancelamento

100 requests com streaming; o cliente fecha a conexão 400 ms depois de enviar. Cada resposta completa teria 20 pedaços de texto.

| requests cancelados | cancelamentos que chegaram ao provedor | pedaços que o provedor gerou | pedaços sem cancelamento |
| --- | --- | --- | --- |
| 100 | 100 | 429 | 2000 |
