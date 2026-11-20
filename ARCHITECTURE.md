# Arquitetura

Este documento registra as decisões, as garantias e os trade-offs da solução. O critério de desempate em todas as decisões foi: **a invariante financeira tem de valer mesmo que a aplicação esteja errada, duplicada ou morta no meio do caminho** — portanto o banco é a última linha de defesa, e nada em memória é fonte de verdade.

## 1. Visão geral

```
            HTTP (Nest/Express)                    SQS FIFO wager-transactions.fifo
                   │                                          │
                   ▼                                          ▼
        WageringController                         SqsWagerConsumer (inbox, ack após commit,
                   │                                          │   backoff, DLQ, shutdown)
                   └──────────────┬───────────────────────────┘
                                  ▼
                 ProcessWagerTransactionUseCase   ◄── mesmo use case para as duas entradas
                                  │
                       TransactionRunner (1 transação SQL, READ COMMITTED)
                                  │
     inbox ─ lock wallet ─ idempotência ─ WagerSettlement (domínio) ─ tx ─ wallet ─ ledger ─ outbox
                                  │
                               COMMIT
                                  │
            ┌─────────────────────┴───────────────────────┐
            ▼                                             ▼
  OutboxPublisher (SKIP LOCKED + lease)        PendingReferenceWorker (SKIP LOCKED + lease)
            │                                             │
            ▼                                             ▼
  SQS FIFO wagering-events.fifo               ResolvePendingReferenceUseCase
```

Camadas (dependências apontam para dentro):

| Camada | Conteúdo | Depende de |
|---|---|---|
| `domain/` | `Money`, `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, `WagerSettlement`, `InboxMessage`, `OutboxMessage`, `IntegrationEvent` + eventos, `FailureCode` | apenas `decimal.js` |
| `application/` | use cases, portas (`WageringUnitOfWork`, `TransactionRunner`, `WageringReadModel`, `Clock`, `IdGenerator`, `Metrics`, `AppLogger`), hash canônico | domínio |
| `infrastructure/` | MikroORM (schemas, mappers, unit of work, migrations), SQS, observabilidade, contratos zod | aplicação + domínio |
| `http/` | controllers, filtro de erros, guard de autenticação | aplicação |

O domínio não conhece NestJS nem MikroORM: as entidades do ORM são *records* (`EntitySchema`) separados, traduzidos por mappers. Construtores são `private`; criação por factories (`create`, `open`, `opening`, `enqueue`, `receive`) e reconstrução por `rehydrate` (que não revalida transições).

## 2. Modelo de domínio

### Money

- Value object imutável (`Object.freeze`) sobre um clone isolado de `decimal.js` (precisão 40, nunca altera a configuração global).
- Entrada e saída sempre como **string decimal**; `toJSON()` → `{ amount: "25.00", currency: "BRL" }` com escala fixa 2.
- Gramática estrita `^-?(0|[1-9]\d*)(\.\d{1,2})?$`: rejeita `NaN`, `Infinity`, notação científica, string vazia, espaços, `+`, zeros à esquerda, mais de 2 casas. **Nunca arredonda**: `10.005` é erro, não `10.01`.
- Até 18 dígitos inteiros (cabe em `numeric(20,2)`); overflow em operações é erro.
- Operações entre moedas diferentes lançam `CurrencyMismatchError`. `Money.fromNonNegative` é usado nos contratos de entrada.
- **Escala fixa de 2 casas também na entrada**: `"25"` e `"25.0"` são recusados (`400` no HTTP, DLQ `invalid_contract` na fila), assim como `"25.001"`. O contrato não normaliza: o que entra é exatamente o que é gravado e devolvido.
- Nos contratos (zod) o valor é `z.string()`: um JSON number (`25.5`) é **400**, nunca chega ao domínio.

### Wallet (aggregate root)

- `debit()` / `credit()` devolvem o `WalletLedgerEntry` correspondente — saldo e ledger só mudam juntos.
- A entrada é construída e validada **antes** de mutar o estado; se for inválida a wallet não muda.
- `version` inicia em 1 e incrementa **exatamente 1 por mudança de saldo**; a entrada carrega a versão resultante (`walletVersion`).
- `debit()` acima do saldo lança `InsufficientFundsError`; movimentos ≤ 0 são recusados (não existe lançamento sem efeito financeiro).

### WalletLedgerEntry

Sem campos mutáveis e sem métodos de transição (`readonly` + `Object.freeze`). `create()` valida `balanceBefore ± money === balanceAfter`, saldos não negativos, valor positivo.

### WagerTransaction e estados

```
              ┌───────────────► PROCESSED  (terminal)
              │
  PENDING ────┼───────────────► REJECTED   (terminal, failureCode)
     │        │
     │        └───────────────► FAILED     (terminal, failureCode = INTERNAL_PROCESSING_ERROR)
     ▼
PENDING_REFERENCE ──(reagenda: attempts+1)──► PENDING_REFERENCE
     │
     └──► PROCESSED | REJECTED | FAILED
```

- `PENDING` existe só em memória, dentro da transação SQL que decide o resultado; o que é persistido já é `PENDING_REFERENCE` ou terminal.
- Transicionar a partir de um estado terminal lança `InvalidTransactionStateError` (erro de programação). O banco reforça: trigger `wager_tx_guard` recusa `UPDATE` de linha terminal, alteração de colunas imutáveis e `DELETE`.
- `resultBalance` guarda o saldo observado no momento da decisão — é o que o replay devolve.
- `OPENING` só pode ser criado pela factory interna `opening()`; `create()` o recusa, o contrato zod não o aceita e o banco exige `kind = 'OPENING' ⇔ provider_id = 'internal'`.

### WagerSettlement (serviço de domínio puro)

Decide e aplica o resultado de uma transação contra uma wallet **já travada**. Ordem das validações:

1. `playerId` dono da wallet → senão `PLAYER_WALLET_MISMATCH`;
2. moeda da operação = moeda da wallet → senão `CURRENCY_MISMATCH`;
3. se há referência: ausente → `PENDING_REFERENCE`; mesmo provider/player/wallet/moeda/rodada → senão `REFERENCE_MISMATCH`; tipo permitido → senão `INVALID_REFERENCE_KIND`; referência ainda pendente → continua `PENDING_REFERENCE`; referência não `PROCESSED` → `REFERENCE_NOT_PROCESSED`; reversão com valor diferente → `REVERSAL_AMOUNT_MISMATCH`; referência já revertida → `REFERENCE_ALREADY_REVERSED`;
4. aplica: `LOSS` sem movimento; débito sem saldo → `INSUFFICIENT_FUNDS` (BET) ou `REVERSAL_INSUFFICIENT_FUNDS` (reversão).

## 3. Regras de negócio e interpretações adotadas

| Operação | Efeito | Referência |
|---|---|---|
| `BET` | débito, 1 `DEBIT` | proibida |
| `WIN` | crédito, 1 `CREDIT` | opcional → `BET` da mesma rodada |
| `LOSS` | nenhum, sem ledger (valor ≥ 0, normalmente `0.00`) | opcional → `BET` |
| `REFUND` | crédito, 1 `CREDIT` | obrigatória → `BET` `PROCESSED` |
| `ROLLBACK` | inverso da referência | obrigatória → `BET` (crédito), `WIN` ou `REFUND` (débito) |

Interpretações documentadas:

1. **Uma transação é revertida no máximo uma vez, por qualquer tipo de reversão.** O enunciado diz "não pode ser revertida duas vezes pelo mesmo tipo"; aplicamos a regra mais forte porque `REFUND` + `ROLLBACK` sobre a mesma `BET` creditariam o valor duas vezes (crédito duplicado é eliminatório). Garantido no domínio (`isReversed`) e no schema (índice único parcial `wager_tx_single_reversal_uq` sobre `reference_transaction_id` para `REFUND/ROLLBACK PROCESSED`).
2. **REFUND × ROLLBACK concorrentes sobre a mesma referência**: ambos travam a mesma wallet, portanto são serializados; o primeiro a obter o lock é aplicado, o segundo vê a reversão já commitada e é `REJECTED` com `REFERENCE_ALREADY_REVERSED`. Se, por hipótese, dois escapassem do lock, o índice único parcial faria o segundo commit falhar (rollback completo) — testado em `wallet-concurrency.test.ts`.
3. `ROLLBACK` de um `REFUND` é permitido (debita o reembolso). Depois disso a `BET` original continua "revertida uma vez" e não pode ser reembolsada novamente. Um `ROLLBACK` não pode ser revertido (`INVALID_REFERENCE_KIND`).

   **Por que a regra é "uma reversão por transação" e não "uma por tipo".** Exemplo com saldo inicial de 100,00:

   | # | Operação | Leitura literal "por tipo" | Regra adotada |
   |---|---|---|---|
   | 1 | `BET` 40,00 | 60,00 | 60,00 |
   | 2 | `REFUND` da BET 1 | 100,00 | 100,00 |
   | 3 | `ROLLBACK` da BET 1 | **140,00** (outro tipo, seria aceito: crédito duplicado) | 100,00 — `REJECTED REFERENCE_ALREADY_REVERSED` |
   | 4 | `ROLLBACK` do REFUND 2 | — | 60,00 (debita o reembolso; aposta volta a valer) |
   | 5 | `REFUND` da BET 1 de novo | — | 60,00 — `REJECTED REFERENCE_ALREADY_REVERSED` |
   | 6 | `ROLLBACK` do REFUND 2 de novo | — | 60,00 — `REJECTED REFERENCE_ALREADY_REVERSED` |

   Na leitura literal, os passos 2 e 3 devolveriam a mesma aposta duas vezes. A regra adotada garante que cada transação produz no máximo um lançamento de reversão. O efeito líquido de qualquer cadeia BET → REFUND → ROLLBACK(REFUND) fica sempre igual a zero ou a um único débito da aposta, nunca negativo para a casa. Testes: `wager-rules.test.ts` (cadeia e contraexemplo), `reversals.test.ts` (banco real, 10 ROLLBACKs concorrentes do mesmo REFUND → 1 débito), `wallet-concurrency.test.ts` (REFUND×ROLLBACK concorrentes, 15×) e `schema-constraints.test.ts` (o índice recusa a segunda reversão mesmo sem a aplicação).
4. `WIN` que referencia uma `BET` já revertida é rejeitado (`REFERENCE_ALREADY_REVERSED`): não se paga prêmio de aposta cancelada.
5. `WIN`/`LOSS` sem referência são aceitos (provedores que não correlacionam); com referência, valem as mesmas validações de pertencimento.
6. Valor zero só é aceito em `LOSS`; nos demais tipos é `400`.
7. **Wallet ou provider inexistente**: rejeição de negócio `422` (`WALLET_NOT_FOUND`/`UNKNOWN_PROVIDER`) **sem persistência** — não há agregado ao qual anexar uma transação auditável (a FK exige wallet existente). Pela fila a mensagem é confirmada (ack) e a inbox registrada. Um reenvio depois que a wallet existir é processado normalmente.
8. Uma referência que chega depois que a transação dependente expirou (`REFERENCE_NOT_FOUND`) é processada normalmente; a dependente continua `REJECTED` (terminal) — o provedor decide reenviar com outro id.
9. Replay de uma transação que estava `PENDING_REFERENCE` e depois foi resolvida devolve o resultado final.

## 4. Concorrência

**Unidade de concorrência: a linha da wallet.** Estratégia: *pessimistic locking* com `SELECT ... FOR UPDATE` (MikroORM `LockMode.PESSIMISTIC_WRITE`) dentro de uma transação `READ COMMITTED`.

Protocolo de `ProcessWagerTransactionUseCase` (uma única transação SQL):

1. **[SQS]** `INSERT inbox ... ON CONFLICT DO NOTHING` — uma redelivery concorrente bloqueia no índice único até o primeiro commitar e então vira no-op.
2. Caminho rápido sem lock: se a `Idempotency-Key` já existe, replay/conflito (linhas terminais são imutáveis, então a leitura é estável).
3. Valida o provider.
4. `SELECT wallet FOR UPDATE` — serializa todos os escritores **desta** wallet, em qualquer instância.
5. Re-verifica a key e o `(provider_id, external_transaction_id)`: em `READ COMMITTED` cada statement enxerga um snapshot novo, então quem esperou o lock vê o que o anterior commitou. É assim que 50 requisições idênticas viram 1 débito + 49 replays.
6. Resolve a referência, decide no domínio, grava transação, `UPDATE wallets ... WHERE id = ? AND version = ?` (defesa em profundidade: o update falha se a versão não for a esperada), ledger, eventos da outbox e completa a inbox.
7. `COMMIT`. Só então o adaptador responde (HTTP) ou faz ack (SQS).

Por que pessimista e não otimista: a carteira "quente" é o caso normal em apostas (muitas operações por segundo no mesmo jogador). Com optimistic locking cada conflito vira retry da transação inteira, com latência e risco de esgotar tentativas; o lock de linha enfileira os escritores no próprio PostgreSQL, sem trabalho desperdiçado. O custo é a espera, que é limitada por `lock_timeout` (`DB_LOCK_TIMEOUT_MS`, padrão 5 s) → `503 TRANSIENT_INFRASTRUCTURE_ERROR` (HTTP) ou retry com backoff (SQS), sempre seguro de repetir com a mesma key.

Garantias adicionais:

- **Sem lock global**: só a linha da wallet é travada; wallets diferentes correm em paralelo (teste que segura o lock de uma wallet numa sessão externa e mostra que outra wallet responde em < 800 ms).
- **Sem deadlock entre wallets**: cada transação trava uma única wallet. O worker de referências trava na mesma ordem do caminho principal (wallet → linha da transação). Mesmo assim, `40P01`/`40001` são re-executados (até `DB_MAX_TX_RETRIES`, com jitter) e contados em métrica.
- **Corridas que escapam do lock** (ex.: mesma key em duas wallets diferentes, travas diferentes) terminam numa violação de `UNIQUE`; a transação inteira faz rollback e o use case reexecuta uma vez, caindo no caminho de replay/conflito.
- **Múltiplas instâncias**: nada depende de mutex local ou memória compartilhada; o Teste D roda 3 processos reais.
- O FIFO do SQS (`MessageGroupId = walletId`) é otimização de ordem/latência; a correção não depende dele.

## 5. Consistência financeira e constraints

O schema aplica as garantias sozinho (migração `Migration20261120000001_initial_schema`):

| Garantia | Constraint |
|---|---|
| saldo não negativo | `wallets_balance_non_negative CHECK (balance_amount >= 0)` |
| uma wallet por jogador+moeda | `UNIQUE (player_id, currency)` |
| idempotência | `UNIQUE (idempotency_key)` global |
| id externo único por provider | `UNIQUE (provider_id, external_transaction_id)` |
| reversão única | índice único parcial `wager_tx_single_reversal_uq` |
| estados terminais imutáveis, sem delete | trigger `wager_tx_guard` |
| consistência de status | CHECKs: `failure_code ⇔ REJECTED/FAILED`, `processed_at` em terminais, `result_balance` em `PROCESSED`, agenda em `PENDING_REFERENCE`, referência obrigatória em `REFUND/ROLLBACK`, proibida em `BET/OPENING`, `OPENING ⇔ internal`, valor > 0 exceto `LOSS` |
| ledger append-only | triggers recusam `UPDATE`, `DELETE`, `TRUNCATE` |
| aritmética do lançamento | `ledger_entry_balanced CHECK` (`after = before ± amount`), valores ≥ 0, `amount > 0` |
| um lançamento por transação/wallet | `UNIQUE (wallet_id, transaction_id)` |
| ledger pertence à wallet da transação | FK composta `(transaction_id, wallet_id) → wager_transactions(id, wallet_id)` |
| sem lost update estrutural | `UNIQUE (wallet_id, wallet_version)`: duas escritas nunca registram a mesma versão |
| saldo ↔ ledger | *constraint triggers* `DEFERRABLE INITIALLY DEFERRED` verificados no `COMMIT`: toda mudança de saldo (e o saldo de abertura) precisa do lançamento daquela versão terminando naquele saldo; versão muda +1 por mudança e nunca sem mudança; cada lançamento encadeia no anterior (`balance_before = balance_after` da versão anterior) e a wallet atingiu sua versão |
| dinheiro exato | `numeric(20,2)` em todas as colunas monetárias, mapeado como `decimal` → string |
| inbox | `PRIMARY KEY (consumer_name, message_id)` |

Os triggers de acoplamento fazem apenas buscas O(1) por índice (nenhuma soma global). A consistência entre saldo e ledger resulta da transação atômica + regras de domínio + lock; os triggers garantem que um bug na aplicação não consiga commitar um estado incoerente, e a reconciliação verifica.

**Reconciliação** (`POST /wallets/:id/reconciliation`): um único statement SQL (um snapshot) lê o saldo armazenado, soma os lançamentos com sinal em `numeric`, conta entradas e quebras de encadeamento e compara versões. Divergências **não são corrigidas**: são logadas (`level=error`), contadas em `wagering_reconciliations_total{result="divergent"}` e retornadas com `consistent: false`.

## 6. Idempotência

- `Idempotency-Key` é obrigatório no HTTP (`400 IDEMPOTENCY_KEY_REQUIRED`) e é a fonte da verdade; na fila vem em `data.idempotencyKey`. Default recomendado: `"{providerId}:{externalTransactionId}"`.
- **Persistente**: a própria linha de `wager_transactions` guarda key, `payload_hash` e o resultado (status, `failureCode`, `referenceTransactionId`, `result_balance_amount`). Nada de cache em memória.
- **payloadHash** = SHA-256 (hex) do **JSON canônico** do subconjunto de negócio: `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount (sempre com 2 casas, garantido pelo contrato), currency}, referenceExternalTransactionId` (omitido se ausente). JSON canônico: chaves ordenadas recursivamente por code unit UTF-16, membros `undefined` omitidos, sem espaços; números são recusados. Header, `messageId`, `occurredAt` e correlation id **não** entram no hash — por isso a mesma operação via HTTP e via SQS é reconhecida como a mesma.
- Requisição idêntica → mesmo resultado, `idempotentReplay: true`, mesmo status de negócio (200 para processada; 202/422 preservados), **saldo observado na época** (não o atual).
- Mesma key com payload diferente → `409 IDEMPOTENCY_KEY_CONFLICT` (nunca replay). Mesmo id externo com outra key → `409 EXTERNAL_TRANSACTION_CONFLICT`.
- HTTP e SQS usam o mesmo use case; a inbox adicionalmente deduplica a **mensagem** (mesmo `messageId` com payload diferente → DLQ `inbox_payload_conflict`).

## 7. Inbox, Outbox e mensageria

### Consumidor SQS (`wager-transactions.fifo`)

- Long polling; mensagens de grupos diferentes em paralelo, do mesmo grupo em ordem.
- Inbox `(consumer_name, message_id)` gravada **na mesma transação** do efeito financeiro: "linha na inbox ⇔ efeito commitado".
- **Ack (DeleteMessage) só depois do `COMMIT`.** Crash entre commit e ack → redelivery → inbox detecta → ack sem novo efeito (teste com processo real morto nesse ponto).
- Classificação de erros:
  - negócio (processada, rejeitada, duplicada, wallet/provider inexistente) → **ack**;
  - transitório (lock timeout, banco indisponível, erro inesperado) → `ChangeMessageVisibility` com backoff exponencial (`SQS_RETRY_BASE_SECONDS·2^(n-1)`, teto `SQS_RETRY_MAX_SECONDS`), DLQ ao atingir `SQS_MAX_RECEIVE_COUNT`;
  - permanente (JSON inválido, contrato inválido, conflito de idempotência, conflito de inbox) → **DLQ imediata** com atributos `dlqReason`, `receiveCount`, `originalMessageId`.
  - A fila também tem `RedrivePolicy` para a DLQ (rede de segurança se o envio explícito falhar).
- Em falha transitória, as mensagens seguintes do mesmo grupo no lote voltam para a fila (visibilidade 0), preservando a ordem do grupo.
- **Shutdown** (`SIGTERM` → `enableShutdownHooks`): para o polling (aborta o long poll), termina as mensagens em andamento (até `SQS_SHUTDOWN_GRACE_MS`), devolve as não iniciadas com visibilidade 0, depois para o worker de referências e o publisher, e por fim fecha o pool.

### Transactional Outbox

- Eventos são gravados em `outbox_messages` **na mesma transação** que a transação/saldo/ledger/inbox. Nada é publicado antes do commit (o publisher só enxerga linhas commitadas — testado com uma transação aberta segurando uma linha).
- `OutboxPublisher` (em todas as instâncias):
  1. **claim**: um único statement auto-commit `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED LIMIT n)` empurra `next_attempt_at` para frente (lease `OUTBOX_LEASE_MS`). Publishers concorrentes nunca pegam a mesma linha ao mesmo tempo;
  2. **publica** em `wagering-events.fifo` (`MessageGroupId = aggregateId`, `MessageDeduplicationId = eventId`) em *lanes* paralelas (`OUTBOX_PUBLISH_CONCURRENCY`, padrão 4): todos os eventos de um agregado caem na mesma lane (hash do `aggregateId`) e, dentro dela, os lotes de 10 seguem a ordem de ocorrência (UUID v7). Se um lote falha, o resto da lane é reagendado junto com ele, para não ultrapassar o evento que falhou;
  3. **marca** `published_at`; falha → `attempts+1` e `next_attempt_at` com backoff exponencial (`OutboxMessage.scheduleRetry`).
- Crash entre 1 e 3: o lease expira e outra instância publica. Entrega **at-least-once**: uma publicação duplicada carrega o **mesmo `eventId`** e corpo idêntico; o broker FIFO descarta duplicatas na janela de 5 min e o consumidor deve deduplicar por `eventId`.
- Não há limite de tentativas na outbox: evento confirmado nunca é descartado; o atraso é visível em `wagering_outbox_oldest_pending_age_seconds` / `wagering_outbox_pending_messages` (outbox lag).

### Eventos

Envelope `IntegrationEvent<T>` (classe abstrata; `eventType` e `version` no tipo concreto): `eventId, eventType, aggregateId (walletId), correlationId, causationId, occurredAt (ISO-8601), version, data`. `data` carrega `MoneyProps` (strings), nunca `Money`.

| Evento | Quando |
|---|---|
| `WagerTransactionProcessed` | qualquer transação aplicada, inclusive `LOSS` e `OPENING` |
| `WagerTransactionRejected` | rejeição de negócio persistida (inclusive expiração de referência) |
| `WalletBalanceChanged` | somente quando o saldo muda (com `balanceBefore/After`, `walletVersion`) |
| `WagerTransactionPendingReference` | ao estacionar a transação (uma vez, não a cada retry) |

### Referências fora de ordem

- Referência ausente → `PENDING_REFERENCE` com `reference_attempts = 1` e `next_reference_check_at` (backoff).
- `PendingReferenceWorker` (todas as instâncias): claim com `FOR UPDATE SKIP LOCKED` + lease; para cada linha, `ResolvePendingReferenceUseCase` trava a wallet, depois a linha, confere que ainda está pendente e reavalia pelo mesmo `WagerSettlement`.
- Backoff exponencial `PENDING_REFERENCE_BASE_DELAY_MS·2^(n-1)` com teto `PENDING_REFERENCE_MAX_DELAY_MS`.
- **Orçamento**: `PENDING_REFERENCE_MAX_ATTEMPTS = 10` **ou** `PENDING_REFERENCE_TTL_MS = 15 min`, o que vier primeiro. Justificativa: provedores normalmente reenviam a operação original em segundos; 1 s → 60 s com 10 tentativas cobre ~5 min de atraso contínuo, e o TTL limita o tempo em que o resultado fica indefinido para o provedor. Antes de expirar, uma última verificação: se a referência chegou, aplica.
- Esgotado: `REJECTED` + `REFERENCE_NOT_FOUND` + evento `WagerTransactionRejected`.
- Erro permanente (não transitório) durante a resolução → `FAILED` + `INTERNAL_PROCESSING_ERROR` (terminal, auditável) em vez de loop infinito.

## 8. Recuperação após falhas

| Falha | Comportamento |
|---|---|
| processo morre **antes** do commit | rollback automático; nada visível; HTTP sem resposta → cliente reenvia com a mesma key; SQS → mensagem volta após a visibilidade |
| processo morre **depois** do commit e **antes** do ack | redelivery absorvida pela inbox (ack sem efeito) |
| processo morre com outbox reivindicada | lease expira, outra instância publica |
| processo morre após publicar e antes de marcar | republica com o mesmo `eventId` (duplicata segura) |
| PostgreSQL indisponível | HTTP `503` + `Retry-After`; SQS retry com backoff → DLQ após o limite; readiness `503` |
| SQS indisponível | consumer faz backoff no polling; outbox acumula e publica depois; readiness `503` |
| lock contendido além do `lock_timeout` | `503` / retry; métrica `wagering_lock_conflicts_total{kind="lock_timeout"}` |
| reinício de todas as instâncias | estado reconstruído do banco; teste com SIGKILL das 3 instâncias no meio da carga comprova consistência final |

## 9. Códigos de falha

| `failureCode` | Persistido | Significado | O provedor deve |
|---|---|---|---|
| `INSUFFICIENT_FUNDS` | sim | `BET` acima do saldo | desistir (resultado final) |
| `REVERSAL_INSUFFICIENT_FUNDS` | sim | `ROLLBACK` que deixaria saldo negativo (distinto do anterior) | escalar operacionalmente |
| `CURRENCY_MISMATCH` | sim | moeda ≠ moeda da wallet | corrigir payload |
| `PLAYER_WALLET_MISMATCH` | sim | jogador não é dono da wallet | corrigir payload |
| `REFERENCE_MISMATCH` | sim | referência de outro jogador/wallet/moeda/rodada | corrigir payload |
| `INVALID_REFERENCE_KIND` | sim | tipo de referência não permitido | corrigir payload |
| `REFERENCE_NOT_PROCESSED` | sim | referência `REJECTED`/`FAILED` | desistir |
| `REFERENCE_ALREADY_REVERSED` | sim | referência já revertida | desistir (já está revertida) |
| `REVERSAL_AMOUNT_MISMATCH` | sim | valor ≠ valor da referência | corrigir payload |
| `REFERENCE_NOT_FOUND` | sim | referência não chegou no orçamento | reenviar com novo id, se for o caso |
| `WALLET_NOT_FOUND` | não | wallet inexistente | corrigir / criar a wallet e reenviar |
| `UNKNOWN_PROVIDER` | não | provider não cadastrado/ativo | corrigir |
| `INTERNAL_PROCESSING_ERROR` | sim (`FAILED`) | erro permanente não-negócio | contatar operação |

Erros de transporte (HTTP) usam `error.code`: `VALIDATION_ERROR`, `IDEMPOTENCY_KEY_REQUIRED`, `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`, `WALLET_ALREADY_EXISTS`, `NOT_FOUND`, `TRANSIENT_INFRASTRUCTURE_ERROR` (`retryable: true`), `INTERNAL_ERROR`.

## 10. Status HTTP

| Classe | Status | Pode reenviar? |
|---|---|---|
| processada | `201` (nova) / `200` (replay) | — |
| aceita, pendente | `202` | sim, mesma key (devolve o estado atual/final) |
| rejeição de negócio | `422` + `failureCode` | não com o mesmo payload |
| payload inválido | `400` | não; corrigir |
| conflito de idempotência | `409` | não |
| recurso inexistente | `404` | — |
| falha transitória | `503` + `Retry-After` | **sim, mesma key** |

## 11. ORM e estratégia transacional

**MikroORM 6** (preferencial no enunciado). Uso:

- `EntitySchema` (sem decorators) para records de persistência; o domínio fica livre do ORM.
- `em.fork().transactional(..., { isolationLevel: READ_COMMITTED })` por unidade de trabalho; `set local lock_timeout/statement_timeout` em cada transação.
- `LockMode.PESSIMISTIC_WRITE` para `SELECT ... FOR UPDATE` (wallet e transação pendente).
- Escritas explícitas (`em.insert`, `em.nativeUpdate`, `em.insertMany`) em vez de `flush` implícito: no caminho financeiro a **ordem dos statements e os locks** precisam ser determinísticos e visíveis no código; o Unit of Work implícito não acrescentaria garantia e esconderia a ordem.
- SQL nativo (`em.execute`) onde o ORM não expressa a semântica: `ON CONFLICT DO NOTHING` (inbox), claims com `FOR UPDATE SKIP LOCKED` (outbox e worker), reconciliação em um statement.
- `Money` mapeado como `decimal` → **string** (`numeric(20,2)`), reidratado por `Money.from`.
- Migrations versionadas como classes TS com SQL explícito, lista explícita (`migrationsList`) e `down()` completo.

## 12. Observabilidade

- **Logs JSON** (uma linha por evento) com `instanceId`, `correlationId` (header `X-Correlation-Id` ou gerado; na fila, `correlationId` ou `messageId`), `messageId`, `transactionId`, `walletId`, `providerId`. Nunca o payload completo (campos `payload/body/authorization/...` são redigidos).
- **Métricas Prometheus** em `/metrics`:
  - `wagering_transactions_total{status,kind,source}` — transações por status;
  - `wagering_duplicates_total{kind}` — replays, duplicatas de inbox, corridas de unique;
  - `wagering_retries_total{component}` — consumer, outbox, referências, transação SQL;
  - `wagering_dlq_messages_total{reason}` e `wagering_dlq_depth`;
  - `wagering_lock_conflicts_total{kind}` e `wagering_wallet_lock_wait_seconds`;
  - `wagering_outbox_lag_seconds`, `wagering_outbox_pending_messages`, `wagering_outbox_oldest_pending_age_seconds`;
  - `wagering_processing_duration_seconds{source}` — latência;
  - `wagering_reconciliations_total{result}`.
- **Health**: `/health/live` (processo responde, sem dependências) e `/health/ready` (PostgreSQL + SQS com timeout de 2 s; `503` se algum cair). Ambos sem autenticação.

## 13. Autenticação (não implementada — decisão)

Autenticação não pontua e competiria com correção financeira; foi deixada como ponto de extensão explícito:

- `ProviderIdentityPort` + `NoopProviderIdentity` + `ProviderAuthGuard` aplicado aos controllers de negócio (não ao health).
- Desenho proposto: **Keycloak** (ou Zitadel) no compose; cada provedor é um *client* OIDC com *client credentials*. O guard valida o JWT (assinatura via JWKS em cache, `iss`, `aud`, `exp`) e expõe `azp` como `providerId`; o use case passa a exigir `body.providerId === identity.providerId` (`403` caso contrário). Escopos (`wagering:write`, `wallets:read`) por rota. Mensagens da fila continuam canal interno confiável, mas o `providerId` delas segue as mesmas validações de domínio (provider cadastrado e ativo).

## 14. Testes

Ver [README § Testes](README.md#testes). Pontos de método:

- Infra real: cada arquivo cria um banco novo (aplica as migrations) e filas próprias; nenhum mock de banco ou fila.
- Paralelismo real: requisições disparadas com `Promise.all` contra o servidor HTTP, cada uma em sua conexão; Teste D e crash tests com **processos do SO** (`bun src/main.ts`), mortos por `process.exit` (fault injection) ou SIGKILL.
- Fault injection (`FAULT_INJECTION`) é inerte em produção (variável não definida) e cobre: crash após commit/antes do ack, crash após claim da outbox, crash após publicar/antes de marcar, falha transitória por `gameId`.
- Cenários repetidos (Teste A 25×, REFUND×ROLLBACK 15×) para pegar intermitência.
- Invariante final em todas as suítes: `wallet.balance == Σ ledger` por wallet e global.

## 15. Trade-offs

| Decisão | Alternativa | Por quê |
|---|---|---|
| Lock pessimista por wallet | optimistic + retry; `UPDATE ... WHERE balance >= x` | hot wallet é o caso comum; fila no banco sem retrabalho; o update condicionado sozinho não cobre idempotência/referências que precisam ler antes de decidir |
| `READ COMMITTED` | `SERIALIZABLE` | com o lock explícito, RC é suficiente e não gera falhas de serialização em massa; a re-leitura após o lock é intencional |
| Resultado da idempotência na própria transação | tabela de idempotência separada | uma única linha = uma única fonte de verdade, sem estado "em andamento" para limpar |
| Claim com lease (auto-commit) | segurar `FOR UPDATE` durante a publicação | não mantém transação aberta durante I/O de rede; o custo é duplicata possível após lease expirado, que já é aceita (at-least-once) |
| Rejeição sem persistência para wallet/provider inexistente | persistir com FK nula | manter FKs fortes; não há agregado auditável |
| Reversão única por qualquer tipo | "por tipo" literal | evita crédito duplicado REFUND+ROLLBACK |
| Constraint triggers de acoplamento | só aplicação | o banco recusa estado incoerente mesmo com bug; custo O(1) por linha |

## 16. Limitações

- Autenticação não implementada (seção 13).
- Moeda única por wallet; o modelo é multi-moeda (wallet por moeda) e os conflitos de moeda são testados, mas não há conversão.
- Ledger de entrada simples (partidas dobradas não implementadas).
- Ordem dos eventos por agregado: preservada dentro de um claim (lanes) e após falha (o resto da lane espera). Entre **instâncias diferentes** que reivindicam eventos do mesmo agregado em ciclos próximos, a ordem de chegada ao broker não é garantida; consumidores devem deduplicar por `eventId` e ordenar por `walletVersion`.
- `FAILED` só é atribuído pelo worker de referências (erro permanente); falhas permanentes no caminho síncrono retornam `500` sem persistir (nada foi commitado).
- Teste de carga (`bun run test:load`, [docs/LOAD_TEST.md](docs/LOAD_TEST.md)) é de execução única, em loop fechado, com gerador e infraestrutura na mesma máquina.
- Validação local do autor feita sem Docker (máquina sem virtualização): PostgreSQL 17 nativo e um servidor SQS compatível; a validação em containers (PostgreSQL + LocalStack) é feita pelo CI (`.github/workflows/ci.yml`) e pelo perfil `test` do compose.
