# Teste de carga (`bun run test:load`)

Diferencial opcional. Não há meta de RPS: o objetivo é mostrar como o sistema se comporta sob contenção e se as garantias se mantêm sob carga.

## Metodologia

Script: [scripts/load-test.ts](../scripts/load-test.ts). Ele usa instâncias que já estão rodando (padrão: as 3 do docker compose).

```bash
docker compose up -d --build
LOAD_DURATION_S=30 LOAD_CONCURRENCY=48 LOAD_WALLETS=200 bun run test:load
```

- **Carga**: 48 clientes concorrentes em loop fechado (cada cliente espera a resposta antes de enviar a próxima) durante 30 s, em round-robin entre as 3 instâncias.
- **Mix**: 80% `BET` entre 1,00 e 5,00 e 20% `WIN` entre 1,00 e 10,00. Os valores são gerados como strings, a partir de centavos inteiros.
- **Contenção proposital**: 20% das requisições vão para **uma única wallet quente**. Isso serializa essa fatia no lock da linha.
- **Duplicatas**: 10% das requisições reenviam uma requisição anterior (mesma key, mesmo payload) e devem virar replay.
- **Consumidor downstream**: 4 loops drenam `wagering-events.fifo` e deduplicam por `eventId`, como um consumidor real faria.
- **Ao final**: o script espera a outbox drenar, coleta `/metrics` de todas as instâncias (diferença entre antes e depois) e **reconcilia todas as wallets**. O processo termina com erro se alguma estiver inconsistente.
- **Métricas reportadas**: throughput, p50/p95/p99/max, códigos de status, taxa de erro (5xx + erros de rede), conflitos de lock (`contended` = espera > 50 ms, `lock_timeout`, `deadlock`, `unique_race`), replays, outbox (publicados, backlog no fim, tempo de drenagem, lag médio e máximo) e o resultado da reconciliação.

## Resultado 1 — CI (GitHub Actions, docker compose com LocalStack)

Ambiente: runner `ubuntu-latest` (2 vCPU, 7 GB). Na **mesma VM** rodam PostgreSQL 17, LocalStack 4.4, 3 instâncias da aplicação e o gerador de carga. Job `load` em [.github/workflows/ci.yml](../.github/workflows/ci.yml).

| Métrica | Valor |
|---|---|
| Requisições | 4.518 em 30,5 s |
| Throughput | **147,9 req/s** |
| Latência p50 / p95 / p99 / max | 56,3 ms / 1.980,9 ms / 4.409,2 ms / 11.246,9 ms |
| Status | 4.105 × `201`, 413 × `200` (replays) |
| Taxa de erro | **0** (sem 5xx e sem erro de rede) |
| Conflitos de lock | 810 esperas contendidas; 0 lock timeouts; 0 deadlocks; 0 corridas de unique |
| Replays idempotentes | 413 (todos com o resultado original) |
| Outbox | 8.617 publicados; backlog de 86 no fim da carga; **drenada 1,2 s depois**; lag médio 0,69 s; maior idade pendente 9,3 s |
| Consumidor downstream | 8.617 mensagens recebidas = **8.617 `eventId` únicos** (0 duplicatas, 0 perdas) |
| Reconciliação | **201/201 wallets consistentes** |

## Resultado 2 — máquina local do autor (sem Docker)

Ambiente: Windows 11, 16 GB, PostgreSQL 17.9 nativo, 3 processos `bun src/main.ts` e um servidor SQS compatível (moto) no lugar do LocalStack, porque esta máquina não tem virtualização para rodar Docker. Execução de 20 s com os mesmos parâmetros.

| Métrica | Valor |
|---|---|
| Throughput | **600,3 req/s** (12.134 requisições) |
| Latência p50 / p95 / p99 | 14,6 ms / 649,8 ms / 758,5 ms |
| Taxa de erro | 0 |
| Conflitos de lock | 1.340 esperas contendidas; 0 timeouts; 0 deadlocks |
| Reconciliação | 201/201 consistentes |
| Outbox | **não comparável**: o moto (um único processo Python) virou o gargalo. Com milhares de mensagens FIFO acumuladas, um `SendMessageBatch` chegou a levar 14,5 s **mesmo numa fila vazia**. A publicação caiu para cerca de 10 eventos/s. A mesma carga com LocalStack (resultado 1) drena em 1,2 s. |

## Análise

1. **A latência de cauda vem da wallet quente, e isso é esperado.** Com lock pessimista, os 20% do tráfego que vão para a mesma wallet ficam numa fila no PostgreSQL. A vazão dessa wallet fica limitada a 1/(tempo em que o lock fica retido) e as esperas aparecem no p95/p99. As wallets "frias" ficam no p50. É a escolha consciente do desenho: o lock enfileira os escritores, sem retrabalho e sem duplicidade. Com optimistic locking, essa fatia viraria retries e possivelmente erros. Nenhuma espera passou do `lock_timeout` de 5 s, e o pior caso no CI (11 s de máximo) foi saturação de CPU de uma VM de 2 vCPU que também roda banco, broker, 3 apps e o gerador de carga.
2. **Correção sob carga**: 0 erros, 0 deadlocks, 0 lock timeouts, replays devolvendo o resultado original e todas as wallets reconciliadas. A invariante `balance == Σ ledger` se manteve.
3. **Outbox**: com LocalStack, a publicação acompanhou a produção (backlog de 86 no fim e drenagem em 1,2 s). O consumidor recebeu exatamente um evento por `eventId`. Duplicatas continuam possíveis por desenho (at-least-once); neste experimento não houve nenhuma.
4. **O que limita o número absoluto**: CPU compartilhada no CI, conexões por instância (`DB_POOL_MAX=20`) e o tempo em que o lock fica retido. Para ir mais longe: separar o gerador de carga da VM, usar mais vCPUs, PgBouncer em modo transaction e ajustar `OUTBOX_PUBLISH_CONCURRENCY`/`OUTBOX_BATCH_SIZE`.

## Limitações do experimento

- Uma execução por ambiente, sem repetições estatísticas nem intervalo de confiança.
- Loop fechado: a taxa de chegada se adapta à latência, então o teste não mede comportamento sob taxa fixa de chegada (loop aberto).
- Gerador de carga, banco e broker na mesma máquina.
- O tráfego vai só por HTTP. A carga via SQS é exercitada nos testes distribuídos, mas não é medida aqui.
