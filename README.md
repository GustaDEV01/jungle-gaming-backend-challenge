# Distributed Wagering Processor

Serviço financeiro distribuído que processa transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) vindas de múltiplos provedores, por **HTTP** e por **SQS FIFO**, mantendo correção financeira com **múltiplas instâncias**, mensagens **duplicadas**, **fora de ordem** e **falhas de processo**.

- Enunciado original: [docs/CHALLENGE.md](docs/CHALLENGE.md)
- Decisões, concorrência, idempotência, outbox, estados e trade-offs: [ARCHITECTURE.md](ARCHITECTURE.md)

## Visão geral

| Garantia | Como |
|---|---|
| Dinheiro exato | `Money` imutável sobre `decimal.js`, strings decimais com escala 2, `numeric(20,2)` no banco. Nenhum `number` para dinheiro. |
| Sem saldo negativo / lost update | `SELECT ... FOR UPDATE` na linha da wallet (unidade de concorrência) + `CHECK (balance >= 0)` + update condicionado à versão. |
| Sem débito/crédito duplicado | Idempotência persistente (`UNIQUE idempotency_key`, `UNIQUE (provider_id, external_transaction_id)`), inbox `(consumer_name, message_id)`, reversão única por índice parcial. |
| Ledger auditável | Append-only (trigger bloqueia `UPDATE/DELETE/TRUNCATE`), aritmética por `CHECK`, encadeamento por versão, acoplamento saldo↔ledger verificado no `COMMIT`. |
| Evento só após commit | Transactional Outbox na mesma transação SQL; publisher com `FOR UPDATE SKIP LOCKED` + lease. |
| Correto com N instâncias | Nada em memória é fonte de verdade; testado com 3 processos reais. |

## Pré-requisitos

- **Docker** + **Docker Compose v2** (caminho principal), ou
- **Bun 1.4.x**, **PostgreSQL 15+** e um endpoint SQS (LocalStack) para rodar fora de containers.

## Execução com Docker Compose

```bash
docker compose up -d --build        # postgres + localstack + migrate (one-shot) + 3 instâncias
bash scripts/smoke.sh               # smoke test: 2 BETs concorrentes em instâncias diferentes + reconciliação
docker compose logs -f app-1        # logs JSON estruturados
docker compose down -v              # derruba tudo e apaga o volume do banco
```

| Serviço | Endereço |
|---|---|
| `app-1`, `app-2`, `app-3` | http://localhost:3001, :3002, :3003 |
| PostgreSQL | `localhost:5432` (postgres/postgres, db `wagering`) |
| LocalStack (SQS) | http://localhost:4566 |

As filas `wager-transactions.fifo`, `wager-transactions-dlq.fifo` (redrive após `SQS_MAX_RECEIVE_COUNT`) e `wagering-events.fifo` (eventos publicados pela outbox) são criadas de forma idempotente no startup (ou com `bun run sqs:setup`).

## Execução local (sem Docker)

```bash
bun install
cp .env.example .env                # ajuste DATABASE_URL / SQS_ENDPOINT
bun run migration:up                # migrations versionadas
bun run start                       # bun src/main.ts (ou: bun run build && bun run start:prod)
```

## Migrations

```bash
bun run migration:up                # aplica pendentes
bun run migration:down              # reverte a última
bun run migration:down -- --all     # reverte tudo
bun run migration:status
```

Migrations são classes MikroORM com SQL explícito e `down()` completo; o teste `schema-constraints` aplica `up → down → up` num banco limpo. Com `RUN_MIGRATIONS=true` a própria aplicação migra no startup, serializada por advisory lock (apenas para migrations).

## Configuração

Variáveis principais (ver [.env.example](.env.example) e [src/infrastructure/config.ts](src/infrastructure/config.ts)):

| Variável | Default | Descrição |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/wagering` | PostgreSQL |
| `DB_LOCK_TIMEOUT_MS` | `5000` | espera máxima pelo lock da wallet (depois: 503 / retry) |
| `SQS_ENDPOINT` | — | endpoint LocalStack |
| `SQS_MAX_RECEIVE_COUNT` | `5` | tentativas antes da DLQ |
| `SQS_VISIBILITY_TIMEOUT_SECONDS` | `30` | visibilidade de mensagens em processamento |
| `OUTBOX_LEASE_MS` | `30000` | lease do claim da outbox (recuperação após crash) |
| `PENDING_REFERENCE_MAX_ATTEMPTS` / `PENDING_REFERENCE_TTL_MS` | `10` / `900000` | orçamento de espera por referência |
| `SQS_CONSUMER_ENABLED`, `OUTBOX_PUBLISHER_ENABLED`, `PENDING_REFERENCE_WORKER_ENABLED` | `true` | liga/desliga workers por instância |

## API

Todos os valores monetários são **strings decimais**: `{ "amount": "25.00", "currency": "BRL" }`.

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/wallets` | cria wallet (`OPENING` + `CREDIT` na mesma transação quando saldo inicial > 0) |
| `GET` | `/wallets/:walletId` | saldo e versão |
| `GET` | `/wallets/:walletId/ledger?cursor=&limit=50` | ledger paginado (cursor opaco e estável) |
| `POST` | `/wallets/:walletId/reconciliation` | saldo armazenado × reconstruído pelo ledger |
| `POST` | `/wagering/transactions` | submete transação (header `Idempotency-Key` obrigatório) |
| `GET` | `/wagering/transactions/:transactionId` | consulta por id interno |
| `GET` | `/providers/:providerId/wagering/transactions/:externalTransactionId` | consulta por id do provedor |
| `GET` | `/health/live`, `/health/ready` | liveness / readiness (PostgreSQL + SQS), sem autenticação |
| `GET` | `/metrics` | métricas Prometheus |

### Status HTTP (consistentes em todos os endpoints)

| Situação | Status | Corpo |
|---|---|---|
| Processada (primeira vez) | `201` | `{ transactionId, status: "PROCESSED", balance, idempotentReplay: false }` |
| Replay idêntico | `200` (ou o status original: `202`/`422`) | mesmo resultado original, `idempotentReplay: true`, header `Idempotent-Replay: true` |
| Aceita, aguardando referência | `202` | `status: "PENDING_REFERENCE"`, `balance: null` |
| Rejeição por regra de negócio | `422` | `status: "REJECTED"`, `failureCode` |
| Payload inválido / sem `Idempotency-Key` | `400` | `error.code: VALIDATION_ERROR` / `IDEMPOTENCY_KEY_REQUIRED` |
| Mesma key, payload diferente | `409` | `error.code: IDEMPOTENCY_KEY_CONFLICT` |
| Mesmo id externo com outra key | `409` | `error.code: EXTERNAL_TRANSACTION_CONFLICT` |
| Wallet duplicada | `409` | `error.code: WALLET_ALREADY_EXISTS` |
| Recurso inexistente (consultas) | `404` | `error.code: NOT_FOUND` |
| Falha transitória (lock timeout, banco indisponível) | `503` + `Retry-After` | `error.code: TRANSIENT_INFRASTRUCTURE_ERROR`, `retryable: true` |

`failureCode`s e semântica de reenvio: ver [ARCHITECTURE.md § Códigos de falha](ARCHITECTURE.md#códigos-de-falha).

### Exemplo

```bash
curl -s -X POST localhost:3001/wallets -H 'content-type: application/json' \
  -d '{"playerId":"player-1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'

curl -s -X POST localhost:3001/wagering/transactions -H 'content-type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"player-1",
       "walletId":"<id>","roundId":"round-987","gameId":"fortune-chimp","kind":"BET",
       "money":{"amount":"25.00","currency":"BRL"}}'
```

Provedores válidos (tabela `providers`, semeada na migration): `provider-a`, `provider-b`, `provider-c`.

### Mensagem SQS (`wager-transactions.fifo`)

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-07-29T15:00:00.000Z",
  "data": { "providerId": "provider-a", "externalTransactionId": "transaction-123",
            "idempotencyKey": "provider-a:transaction-123", "playerId": "...", "walletId": "...",
            "roundId": "round-987", "gameId": "fortune-chimp", "kind": "BET",
            "money": { "amount": "25.00", "currency": "BRL" } }
}
```

Use `MessageGroupId = walletId` (ordem por wallet como otimização; a correção não depende dela).

## Testes

Todos os testes de integração, concorrência e distribuídos usam **PostgreSQL e SQS reais** — nenhum mock de banco ou fila. Cada arquivo cria um banco novo (migrations aplicadas do zero) e filas próprias.

```bash
# dentro do compose (PostgreSQL + LocalStack em containers):
docker compose up -d postgres localstack
docker compose --profile test run --rm tests

# ou localmente, apontando para PostgreSQL/SQS existentes:
TEST_PG_URL=postgresql://postgres:postgres@localhost:5432 SQS_ENDPOINT=http://localhost:4566 bun run test:all
bun run test:unit | test:integration | test:concurrency | test:distributed
```

| Suíte | Conteúdo |
|---|---|
| `test/unit` | `Money` (escala, entradas inválidas, sem arredondamento silencioso), invariantes da `Wallet`, ledger, regras BET/WIN/LOSS/REFUND/ROLLBACK, máquina de estados, conflito de moeda, hash canônico/payload divergente, backoff da outbox |
| `test/integration` | constraints do schema e migrations reversíveis; atomicidade inbox+tx+wallet+ledger+outbox; API HTTP; SQS (inbox, redelivery, DLQ, retry, shutdown); outbox (publishers concorrentes, nada antes do commit, retry, lease); referências fora de ordem |
| `test/concurrency` | Teste A (2×80 sobre 100, repetido 25×), Teste B (50 iguais em paralelo), payloads divergentes, hot wallet, REFUND×ROLLBACK concorrentes, wallets em paralelo, prova de ausência de lock global |
| `test/distributed` | Teste D com **3 processos reais**; worker morto após commit e antes do ack; publisher morto após claim / após publicar; SIGKILL de todas as instâncias no meio da carga e reinício |

Invariante final verificada: `wallet.balance == saldo reconstruído pelo ledger` (por wallet e global).

## Estrutura

```
src/
  domain/           Money, Wallet, WalletLedgerEntry, WagerTransaction, WagerSettlement,
                    InboxMessage, OutboxMessage, IntegrationEvent (+ eventos) — sem ORM/Nest
  application/      use cases (ProcessWagerTransaction, CreateWallet, ResolvePendingReference,
                    queries/reconciliação), portas (UnitOfWork, ReadModel, Clock, Ids, Metrics)
  infrastructure/   MikroORM (schemas, mappers, unit of work, migrations), SQS (consumer,
                    outbox publisher, pending-reference worker), observabilidade, contratos
  http/             controllers, mapeamento de erros, AuthGuard (ponto de extensão)
test/               unit | integration | concurrency | distributed | support
```

## Limitações

- Autenticação não implementada (ponto de extensão `ProviderIdentityPort` + `ProviderAuthGuard`); desenho proposto no ARCHITECTURE.md.
- Uma wallet por transação (sem partidas dobradas).
- Ordenação de eventos publicados é por agregado em melhor esforço; consumidores devem deduplicar por `eventId` e ordenar por `walletVersion`.
- Teste de carga (`test:load`) não implementado.
- Ver lista completa em [ARCHITECTURE.md § Limitações](ARCHITECTURE.md#limitações).
