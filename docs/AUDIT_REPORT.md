# Auditoria adversarial — relatório

Branch: `audit/adversarial`, a partir de `main@43be715`. Ambiente local: Windows 11, PostgreSQL 17.9 nativo e servidor SQS compatível (moto), porque a máquina não tem Docker. A validação em containers (PostgreSQL + LocalStack) é feita pelo CI desta branch.

Este documento tem duas partes:

- **Fase 1 — diagnóstico**, escrita **antes** de qualquer alteração de código;
- **Fase 2 — correções**, acrescentada depois.

---

## Fase 1 — diagnóstico (sem alterações de código)

### Resumo executivo

- **Nenhuma falha eliminatória (E1–E8) foi reproduzida.** Os cenários adversariais de concorrência, idempotência, crash, queda real do PostgreSQL, lock timeout, lease expirado e redelivery paralela mantiveram as invariantes: saldo nunca negativo, uma aplicação por operação, `saldo == Σ ledger` e nenhum evento antes do commit.
- **Dois bugs confirmados de gravidade CRÍTICA**, que não são eliminatórios por si só, mas tocam E4 e E7:
  - **AUD-01**: o replay de uma rejeição `CURRENCY_MISMATCH` devolve o saldo original com a **moeda errada**;
  - **AUD-02**: o schema aceita lançamento de ledger para transação `REJECTED` (e em moeda diferente da wallet), e a reconciliação não percebe.
- **Um problema ALTO de desempenho**, confirmado por EXPLAIN (AUD-03): o custo do claim da outbox cresce com o backlog da wallet quente (2,5 s por claim com 200 mil eventos pendentes).
- Problemas MÉDIO/BAIXO estão listados com recomendação, sem correção automática.

### Checklist dos critérios eliminatórios (com evidência)

| Critério | Resultado | Evidência adversarial |
|---|---|---|
| E1 — dinheiro sem `number` | ok | varredura estática (`no-float-money.test.ts`); escala fixa de 2 casas; `numeric(20,2)` lido como string; corpos com número → 400 |
| E2 — sem saldo negativo | ok | ADV-01/02 (100 concorrentes), ADV-05/06 (lock timeout), ADV-14 (SIGKILL); `wallets.balance_amount >= 0` em todas as execuções |
| E3 — sem duplicação | ok | ADV-01 (100× mesma key → 1 débito), ADV-03 (abort do cliente + retry, 20×), ADV-07 (mesmo `messageId` em 20 grupos paralelos → 1 linha na inbox), ADV-13 (retry após queda do banco → 1 aplicação), ADV-14 |
| E4 — idempotência persistente | **ok, com bug no conteúdo do replay** | ADV-02 (mesma key, payloads divergentes em 2 wallets → 1 efeito, demais 409/200). **AUD-01**: replay de `CURRENCY_MISMATCH` muda a moeda do saldo |
| E5 — múltiplas instâncias | ok | ADV-14/15 com processos reais; nenhum estado de correção em memória (revisão de código) |
| E6 — outbox | ok (com custo, AUD-03) | ADV-08: publisher travado além do lease → nada perdido, ordem por wallet mantida, duplicatas idênticas |
| E7 — ledger auditável | **ok na aplicação, com brecha no schema** | ADV-11: UPDATE/DELETE/TRUNCATE recusados. **AUD-02**: crédito vinculado a transação REJECTED é aceito pelo banco |
| E8 — testes reais | ok | todos os cenários usam PostgreSQL e SQS reais e processos do SO; ADV-13 derruba o PostgreSQL de verdade |

### Bugs confirmados

**AUD-01 — CRÍTICO — replay devolve o saldo observado na moeda errada (E4)**

- **Onde**: `src/infrastructure/persistence/mappers.ts:56` (`resultBalance: money(r.resultBalanceAmount, currency)`). `wager_transactions` só tem a coluna `currency`, que é a moeda da **operação**.
- **Condição**: transação rejeitada com `CURRENCY_MISMATCH` (operação em USD numa wallet BRL). O saldo observado é BRL, mas é gravado só o valor e reidratado como USD.
- **Reprodução** (ADV-04):
  - resposta original: `422 {"amount":"100.00","currency":"BRL"}`;
  - replay: `422 {"amount":"100.00","currency":"USD"}` com `idempotentReplay=true`;
  - `GET /wagering/transactions/:id`: `{"amount":"100.00","currency":"USD"}`.
- **Esperado**: o replay devolve exatamente o resultado original (README §7.7).
- **Correção**: persistir a moeda do saldo observado (`result_balance_currency`), com garantia no schema de que é a moeda da wallet.
- **Risco de eliminação**: baixo a médio. É o caminho do "replay retorna o resultado original" que os avaliadores testam, num caso de borda.

**AUD-02 — CRÍTICO — o schema aceita lançamento que não corresponde a uma movimentação válida (E7)**

- **Onde**: migration `Migration20261120000001_initial_schema`. Os triggers de acoplamento validam saldo × ledger, mas nada valida ledger × transação.
- **Reprodução**:
  - ADV-09: num único `BEGIN…COMMIT` direto no banco, `UPDATE wallets` (100,00 → 1.100,00, v2) mais `INSERT` de um CRÉDITO de 1.000,00 com `transaction_id` de uma BET `REJECTED`. O COMMIT foi aceito, e a reconciliação respondeu `consistent: true`, porque saldo e ledger "batem";
  - ADV-10: lançamento em USD numa wallet BRL também é aceito.
- **Condição**: escrita direta no banco ou um bug futuro na aplicação.
- **Esperado**: o README §6.4 diz que "qualquer transação REJECTED não gera lançamento", e o §5.9 pede que as garantias valham no schema.
- **Correção**: verificação deferida, O(1), no INSERT do ledger: a transação precisa estar `PROCESSED` e não ser `LOSS`; o valor e a moeda têm de coincidir com os da transação e da wallet; a direção tem de ser coerente com o tipo (ROLLBACK = inverso da referência). Na volta, uma transação `PROCESSED` que mexe em saldo precisa ter lançamento. A moeda do ledger fica presa à moeda da wallet por FK composta.
- **Risco de eliminação**: baixo a médio (defesa em profundidade do E7).

### Riscos e problemas não eliminatórios

| ID | Gravidade | Tipo | Descrição | Evidência | Recomendação |
|---|---|---|---|---|---|
| AUD-03 | ALTO | confirmado (desempenho) | O claim head-of-line da outbox percorre, por anti-join, todos os "seguidores" vencidos da wallet quente antes de achar os heads. Custo O(backlog): 42 ms com 6 mil pendentes, 581 ms com 50 mil, 2.550 ms com 200 mil. Cada claim publica no máximo 20 eventos da wallet quente, então um backlog grande dessa wallet (ex.: broker fora do ar por minutos) drena devagar | EXPLAIN ANALYZE (`Nested Loop Anti Join` 2,3 s) | Reescrever o claim com varredura das wallets distintas pelo índice (`WITH RECURSIVE` + `LATERAL ... LIMIT 1`). Medido: 315 ms com 200 mil (≈8×) e 125 ms com 50 mil. Subir `OUTBOX_MAX_EVENTS_PER_AGGREGATE`. Não corrigido automaticamente (ALTO) |
| AUD-04 | MÉDIO | confirmado | Corpo acima do limite (300 KB) → `500 INTERNAL_ERROR retryable:true`, em vez de 413 não-retentável | sondagem HTTP | Mapear erros do body-parser com `status` 4xx para 400/413 com `retryable:false` |
| AUD-05 | MÉDIO | risco | `zod@3.25.76` tem aviso GHSA-mrhx-c7xc-m3w5 (memória em coleções grandes); `ajv@8.13.0` (moderado, transitiva das ferramentas de migration) | `bun audit` | Não explorável na configuração atual: schemas sem arrays e corpo ≤ 100 KB. Atualizar zod para ≥ 4.6.6 quando possível |
| AUD-06 | MÉDIO | risco | `idempotency_key` é global e não está vinculada ao provider autenticado: sem autenticação, qualquer cliente pode ocupar a key de outro provider (o legítimo recebe 409) | leitura de código | Com o IdP: exigir `key` com prefixo do `providerId` autenticado, ou unicidade `(provider_id, idempotency_key)` |
| AUD-07 | BAIXO | risco | O consumer não estende a visibilidade (*heartbeat*). Um lote mais lento que o `VisibilityTimeout` é reentregue a outro consumer. A inbox impede efeito duplicado (E3 mantido), mas há trabalho e logs extras | leitura de código | Estender a visibilidade enquanto processa, ou processar lotes menores |
| AUD-08 | BAIXO | risco | Timeout de aquisição do pool (`Knex: Timeout acquiring a connection`) não é classificado como transitório → HTTP 500 em vez de 503 | leitura de código | Incluir a mensagem em `isConnectionError` |
| AUD-09 | BAIXO | melhoria | `outbox_messages` e `inbox_messages` crescem sem retenção (os índices parciais mantêm os caminhos quentes pequenos) | leitura de código | Job de retenção/arquivamento |
| AUD-10 | BAIXO | melhoria | `POST /reconciliation` varre o ledger inteiro e é aberto (sem autenticação): custo alto em wallets enormes | leitura de código | Autenticação e limitação de taxa |

### Cenários adversariais executados (fase 1)

| # | Cenário | Teste | Resultado |
|---|---|---|---|
| 1 | 100 requisições simultâneas com a mesma key | ADV-01 | passou |
| 2 | Mesma key, payloads divergentes, 2 wallets | ADV-02 | passou |
| 3 | REFUND × ROLLBACK concorrentes | suíte existente (`wallet-concurrency`, 15×) | passou |
| 4 | 10 ROLLBACKs simultâneos de um REFUND | suíte existente (`reversals`) | passou |
| 5 | Referências fora de ordem com várias instâncias | ADV-15 (grupos distintos, 2 processos) | passou |
| 6 | Reinício durante transações pendentes | ADV-14 (SIGKILL) | passou |
| 7 | Falha do PostgreSQL durante a operação | ADV-13 (PostgreSQL parado de verdade) | passou (rodado localmente; destrutivo, por isso controlado por variável de ambiente) |
| 8 | Falha do SQS na publicação | suíte existente (`outbox`, fila inexistente) | passou |
| 9 | Publisher travado além do lease | ADV-08 | passou |
| 10 | Dois+ publishers na mesma wallet | suíte existente (3 publishers) | passou |
| 11 | Primeiro evento falhando | suíte existente (`fail_publish`) | passou |
| 12 | Lock timeout durante a operação | ADV-05 (HTTP), ADV-06 (SQS) | passou |
| 13 | Falha após o commit, antes da resposta/ack | ADV-03 (abort do cliente), suíte existente (crash antes do ack) | passou |
| 14 | Reentrega após a janela de dedup do SQS | ADV-07 e suíte existente (dedup id aleatório = fora da janela) | passou |
| 15 | Violação de constraints por SQL direto | ADV-09/10/11 | **ADV-09 e ADV-10 falharam (AUD-02)** |
| — | Replay de rejeição por moeda | ADV-04 | **falhou (AUD-01)** |

### Desempenho (fase 1)

- **Claim da outbox**: ver AUD-03.
- **Triggers de acoplamento saldo↔ledger**: 800 movimentações sequenciais, com triggers 1,70 ms/tx e sem 1,55 ms/tx. Sobrecarga ≈ 0,16 ms/tx (≈ 10% da transação SQL; desprezível perto do p50 HTTP de ~15 ms).
- **Head-of-line blocking**: por desenho, um evento que falha segura apenas a própria wallet; as demais seguem (teste existente com `fail_publish`). Não há *starvation* entre wallets: os heads são ordenados por `next_attempt_at`.
- **Lock da wallet**: sob carga (`docs/LOAD_TEST.md`), zero lock timeouts e zero deadlocks. A cauda vem da wallet quente, que é serializada.

### Cobertura que faltava na suíte existente (antes da auditoria)

- Nenhum teste verificava a **moeda** do saldo num replay de rejeição (por isso o AUD-01 passou).
- Nenhum teste tentava lançar no ledger com transação inválida (AUD-02).
- O teste de redelivery mandava as cópias **no mesmo grupo** (consumo sequencial); a inserção concorrente na inbox não era exercitada (coberta agora pelo ADV-07).
- Não havia queda real do PostgreSQL, lock timeout, publisher travado além do lease nem abort do cliente.

### Prontidão (fase 1)

Não há falha eliminatória conhecida. Os dois CRÍTICOS devem ser corrigidos antes da entrega, porque são baratos, localizados e tocam exatamente E4 e E7.
