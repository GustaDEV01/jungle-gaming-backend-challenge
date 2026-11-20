import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { SQL } from 'bun';
import { FaultInjector } from '../../src/infrastructure/messaging/fault-injection';
import { OutboxPublisher } from '../../src/infrastructure/messaging/outbox-publisher';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import { JsonLogger } from '../../src/infrastructure/observability/logger';
import { PromMetrics } from '../../src/infrastructure/observability/metrics';
import { createOrm } from '../../src/infrastructure/persistence/orm';
import {
  assertGlobalInvariant,
  configFor,
  createWallet,
  deleteQueues,
  drain,
  dropDatabase,
  http,
  ledgerStats,
  queueDepth,
  queues,
  reconcile,
  sendRaw,
  sql,
  sqsClient,
  startApp,
  submit,
  testEnv,
  uniqueName,
  waitFor,
  wagerBody,
  wagerMessage,
  type RunningApp,
  type TestEnv,
} from '../support/infra';

/**
 * Adversarial audit scenarios not covered by the main suite. Real PostgreSQL
 * and a real SQS endpoint; no mocks (the "slow SQS" below only delays real calls).
 */
let t: TestEnv;
let app: RunningApp;
let db: SQL;
let q: QueueUrls;
const orms: MikroORM[] = [];
const logger = new JsonLogger({ service: 'audit', instanceId: 'audit' }, 'error');

beforeAll(async () => {
  t = await testEnv('adv', { DB_POOL_MAX: '60', DB_LOCK_TIMEOUT_MS: '700', OUTBOX_PUBLISHER_ENABLED: 'false' });
  q = await queues(t.env);
  app = await startApp(t.env);
  db = sql(t.databaseUrl);
});

afterAll(async () => {
  await app?.close();
  for (const orm of orms) await orm.close(true);
  await db?.close();
  await deleteQueues(q);
  await dropDatabase(t.databaseUrl);
});

async function txCountByKey(key: string): Promise<number> {
  const [{ n }] = await db`select count(*)::int as n from wager_transactions where idempotency_key = ${key}`;
  return n;
}

describe('ADV — idempotency under pressure', () => {
  test('ADV-01: 100 identical requests with the same key at once => 1 transaction, 1 debit, 1x201 + 99x200 with the original balance', async () => {
    const w = await createWallet(app.url, '100.00');
    const ext = uniqueName('adv01');
    const results = await Promise.all(Array.from({ length: 100 }, () => submit(app.url, { ...w, kind: 'BET', amount: '10.00', externalTransactionId: ext })));
    expect(results.filter((r) => r.status === 201)).toHaveLength(1);
    expect(results.filter((r) => r.status === 200)).toHaveLength(99);
    expect(results.filter((r) => r.status >= 500)).toHaveLength(0);
    expect(results.every((r) => r.body.balance?.amount === '90.00')).toBe(true);
    expect(await txCountByKey(`provider-a:${ext}`)).toBe(1);
    expect(await ledgerStats(db, w.walletId)).toMatchObject({ debits: 1, balance: '90.00' });
  });

  test('ADV-02: same key, divergent payloads on TWO different wallets (50 + 50 concurrently) => exactly one effect overall', async () => {
    const w1 = await createWallet(app.url, '100.00');
    const w2 = await createWallet(app.url, '100.00');
    const key = uniqueName('adv02-key');
    const ext = uniqueName('adv02');
    const results = await Promise.all(
      Array.from({ length: 100 }, (_, i) =>
        submit(app.url, { ...(i % 2 ? w1 : w2), kind: 'BET', amount: '10.00', externalTransactionId: ext }, key),
      ),
    );
    expect(results.filter((r) => r.status === 201)).toHaveLength(1);
    expect(results.filter((r) => r.status >= 500)).toHaveLength(0);
    for (const r of results) expect([200, 201, 409]).toContain(r.status);
    expect(await txCountByKey(key)).toBe(1);
    const s1 = await ledgerStats(db, w1.walletId);
    const s2 = await ledgerStats(db, w2.walletId);
    expect(s1.debits + s2.debits).toBe(1);
  });

  test('ADV-03: client gives up (abort) right after sending; retries with the same key => one effect, original result', async () => {
    const w = await createWallet(app.url, '1000.00');
    for (let i = 0; i < 20; i++) {
      const body = wagerBody({ ...w, kind: 'BET', amount: '1.00' });
      const key = `provider-a:${body.externalTransactionId}`;
      const ctrl = new AbortController();
      const pending = fetch(`${app.url}/wagering/transactions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'Idempotency-Key': key },
        body: JSON.stringify(body),
        signal: ctrl.signal,
      }).catch(() => null);
      await Bun.sleep(i % 4 === 0 ? 0 : Math.floor(Math.random() * 15));
      ctrl.abort();
      await pending;
      const retry = await http('POST', `${app.url}/wagering/transactions`, body, { 'Idempotency-Key': key });
      expect([200, 201]).toContain(retry.status);
      expect(await txCountByKey(key)).toBe(1);
    }
    const stats = await ledgerStats(db, w.walletId);
    expect(stats.debits).toBe(20);
    expect(stats.balance).toBe('980.00');
  });

  test('ADV-04: replay of a CURRENCY_MISMATCH rejection returns the ORIGINAL observed balance (same currency)', async () => {
    const w = await createWallet(app.url, '100.00', 'BRL');
    const first = await submit(app.url, { ...w, kind: 'BET', amount: '5.00', currency: 'USD' });
    expect(first.status).toBe(422);
    expect(first.body.failureCode).toBe('CURRENCY_MISMATCH');
    const replay = await submit(app.url, { ...w, kind: 'BET', amount: '5.00', currency: 'USD', externalTransactionId: first.externalTransactionId });
    expect(replay.body.idempotentReplay).toBe(true);
    expect(replay.body.balance).toEqual(first.body.balance); // original: { amount: '100.00', currency: 'BRL' }
    const get = await http('GET', `${app.url}/wagering/transactions/${first.body.transactionId}`);
    expect(get.body.balance).toEqual(first.body.balance);
  });
});

describe('ADV — lock timeout during a financial operation', () => {
  test('ADV-05: HTTP: wallet locked longer than lock_timeout => 503 retryable, nothing persisted; retry succeeds once', async () => {
    const w = await createWallet(app.url, '100.00');
    const holder = sql(t.databaseUrl);
    const body = wagerBody({ ...w, kind: 'BET', amount: '10.00' });
    const key = `provider-a:${body.externalTransactionId}`;
    try {
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const locked = holder.begin(async (trx) => {
        await trx`select id from wallets where id = ${w.walletId} for update`;
        await gate;
      });
      await Bun.sleep(150);
      const res = await http('POST', `${app.url}/wagering/transactions`, body, { 'Idempotency-Key': key });
      expect(res.status).toBe(503);
      expect(res.body.error).toMatchObject({ code: 'TRANSIENT_INFRASTRUCTURE_ERROR', retryable: true, details: { reason: 'lock_timeout' } });
      expect(res.headers.get('retry-after')).toBeTruthy();
      expect(await txCountByKey(key)).toBe(0);
      release();
      await locked;
    } finally {
      await holder.close();
    }
    const retry = await http('POST', `${app.url}/wagering/transactions`, body, { 'Idempotency-Key': key });
    expect(retry.status).toBe(201);
    expect(await ledgerStats(db, w.walletId)).toMatchObject({ debits: 1, balance: '90.00' });
  });

  test('ADV-06: SQS: lock timeout is transient => message retried with backoff and applied exactly once', async () => {
    const w = await createWallet(app.url, '100.00');
    const holder = sql(t.databaseUrl);
    const msg = wagerMessage({ ...w, kind: 'BET', amount: '10.00' });
    try {
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const locked = holder.begin(async (trx) => {
        await trx`select id from wallets where id = ${w.walletId} for update`;
        await gate;
      });
      await Bun.sleep(150);
      await sendRaw(q.wager, JSON.stringify(msg), w.walletId);
      await Bun.sleep(2_000); // at least one lock timeout happens meanwhile
      expect(await txCountByKey(String(msg.data.idempotencyKey))).toBe(0);
      release();
      await locked;
    } finally {
      await holder.close();
    }
    await waitFor(async () => (await txCountByKey(String(msg.data.idempotencyKey))) === 1, 'applied after the lock was released', 20_000);
    await waitFor(async () => {
      const d = await queueDepth(q.wager);
      return d.visible + d.inFlight === 0;
    }, 'acked');
    expect(await ledgerStats(db, w.walletId)).toMatchObject({ debits: 1, balance: '90.00' });
  });
});

describe('ADV — SQS concurrency', () => {
  test('ADV-07: the same messageId delivered 20x in 20 DIFFERENT groups (processed in parallel) => 1 inbox row, 1 debit', async () => {
    const w = await createWallet(app.url, '100.00');
    const msg = wagerMessage({ ...w, kind: 'BET', amount: '10.00' });
    await Promise.all(Array.from({ length: 20 }, (_, i) => sendRaw(q.wager, JSON.stringify(msg), `g-${i}-${w.walletId}`)));
    await waitFor(async () => {
      const d = await queueDepth(q.wager);
      return d.visible + d.inFlight === 0;
    }, 'all copies consumed', 30_000);
    const [{ n }] = await db`select count(*)::int as n from inbox_messages where message_id = ${msg.messageId}`;
    expect(n).toBe(1);
    expect(await ledgerStats(db, w.walletId)).toMatchObject({ debits: 1, balance: '90.00' });
  });
});

describe('ADV — outbox', () => {
  async function publisher(name: string, client: SQSClient, overrides: Record<string, string> = {}) {
    const orm = await createOrm(t.databaseUrl, 5);
    orms.push(orm);
    return new OutboxPublisher(orm, client, q.events, configFor(t.env, overrides).outbox, new FaultInjector(undefined), new PromMetrics(false), logger, name);
  }

  test('ADV-08: publisher stalled longer than its lease while another publisher takes over => nothing lost, per-wallet order kept, duplicates identical', async () => {
    await drain(q.events, 500, 3_000);
    await db`update outbox_messages set published_at = now() where published_at is null`; // isolate this scenario
    const wallets = await Promise.all(Array.from({ length: 3 }, () => createWallet(app.url, '100.00')));
    for (let i = 0; i < 3; i++) await Promise.all(wallets.map((w) => submit(app.url, { ...w, kind: 'BET', amount: '1.00' })));
    const [{ n: total }] = await db`select count(*)::int as n from outbox_messages where published_at is null`;
    expect(total).toBe(24);

    const real = sqsClient();
    // Real SQS calls, delayed beyond the lease (a stalled / paused process).
    const slowClient = { send: async (cmd: unknown) => (await Bun.sleep(2_500), real.send(cmd as never)) } as unknown as SQSClient;
    const slow = await publisher('slow', slowClient, { OUTBOX_LEASE_MS: '1000' });
    const fast = await publisher('fast', real, { OUTBOX_LEASE_MS: '1000' });
    const slowRun = slow.publishOnce();
    await Bun.sleep(1_300); // lease of the slow publisher has expired
    await waitFor(async () => {
      await fast.publishOnce();
      const [{ n }] = await db`select count(*)::int as n from outbox_messages where published_at is null`;
      return n === 0;
    }, 'fast publisher drains', 20_000, 100);
    await slowRun; // the stalled publisher wakes up and re-sends what it had claimed

    const received = await drain(q.events);
    const byWallet = new Map<string, string[]>();
    const bodies = new Map<string, Set<string>>();
    for (const m of received) {
      const e = JSON.parse(m.Body!) as { eventId: string; aggregateId: string };
      const list = byWallet.get(e.aggregateId) ?? [];
      if (!list.includes(e.eventId)) list.push(e.eventId);
      byWallet.set(e.aggregateId, list);
      bodies.set(e.eventId, new Set([...(bodies.get(e.eventId) ?? []), m.Body!]));
    }
    for (const w of wallets) {
      const expected = (await db`select id from outbox_messages where aggregate_id = ${w.walletId} order by seq`).map((r: { id: string }) => r.id);
      expect(byWallet.get(w.walletId)).toEqual(expected);
    }
    for (const set of bodies.values()) expect(set.size).toBe(1);
  });
});

describe('ADV — direct SQL attacks on the ledger (schema as last line of defence)', () => {
  /** Creates a wallet and a REJECTED transaction we can try to abuse. */
  async function fixture() {
    const w = await createWallet(app.url, '100.00');
    const rejected = await submit(app.url, { ...w, kind: 'BET', amount: '999.00' });
    expect(rejected.body.status).toBe('REJECTED');
    return { ...w, rejectedId: rejected.body.transactionId as string };
  }

  async function attempt(fn: (tx: SQL) => Promise<unknown>): Promise<string | null> {
    try {
      await db.begin(fn as never);
      return null;
    } catch (error) {
      return String((error as Error).message);
    }
  }

  test('ADV-09: a CREDIT entry attached to a REJECTED transaction (with matching wallet update) is refused', async () => {
    const f = await fixture();
    const error = await attempt(async (tx) => {
      await tx`update wallets set balance_amount = 1100.00, version = 2 where id = ${f.walletId}`;
      await tx`insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
               values (gen_random_uuid(), ${f.walletId}, ${f.rejectedId}, 'CREDIT', 1000.00, 'BRL', 100.00, 1100.00, 2, now())`;
    });
    expect(error).not.toBeNull();
  });

  test('ADV-10: a ledger entry in a currency different from the wallet currency is refused', async () => {
    const f = await fixture();
    const [opening] = await db`select transaction_id from wallet_ledger_entries where wallet_id = ${f.walletId}`;
    const error = await attempt(async (tx) => {
      await tx`update wallets set balance_amount = 90.00, version = 2 where id = ${f.walletId}`;
      await tx`insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
               values (gen_random_uuid(), ${f.walletId}, ${f.rejectedId}, 'DEBIT', 10.00, 'USD', 100.00, 90.00, 2, now())`;
    });
    expect(opening).toBeDefined();
    expect(error).not.toBeNull();
  });

  /** Inserts a wager transaction row directly (bypassing the application). */
  async function rawTx(tx: SQL, f: { walletId: string; playerId: string }, kind: string, amount: string, extra: { status?: string; result?: string; resultCurrency?: string } = {}) {
    const id = crypto.randomUUID();
    const ext = uniqueName('raw');
    const status = extra.status ?? 'PROCESSED';
    await tx`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id,
               kind, amount, currency, status, result_balance_amount, result_balance_currency, created_at, processed_at)
             values (${id}, 'provider-a', ${ext}, ${ext}, ${'b'.repeat(64)}, ${f.walletId}, ${f.playerId}, 'r', 'g',
               ${kind}, ${amount}, 'BRL', ${status}, ${extra.result ?? '100.00'}, ${extra.resultCurrency ?? 'BRL'}, now(), now())`;
    return id;
  }
  async function move(tx: SQL, walletId: string, txId: string, direction: string, amount: string, before: string, after: string) {
    await tx`update wallets set balance_amount = ${after}, version = 2 where id = ${walletId}`;
    await tx`insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
             values (gen_random_uuid(), ${walletId}, ${txId}, ${direction}, ${amount}, 'BRL', ${before}, ${after}, 2, now())`;
  }

  test('ADV-16: ledger amount different from the transaction amount is refused', async () => {
    const f = await fixture();
    expect(await attempt(async (tx) => move(tx, f.walletId, await rawTx(tx, f, 'WIN', '10.00', { result: '1100.00' }), 'CREDIT', '1000.00', '100.00', '1100.00'))).toMatch(/differs from transaction/);
  });

  test('ADV-17: a PROCESSED balance-moving transaction without its ledger entry is refused', async () => {
    const f = await fixture();
    expect(await attempt(async (tx) => rawTx(tx, f, 'BET', '10.00', { result: '90.00' }))).toMatch(/has no ledger entry/);
  });

  test('ADV-18: a ledger entry for a LOSS is refused', async () => {
    const f = await fixture();
    expect(await attempt(async (tx) => move(tx, f.walletId, await rawTx(tx, f, 'LOSS', '5.00', { result: '105.00' }), 'CREDIT', '5.00', '100.00', '105.00'))).toMatch(/LOSS transaction/);
  });

  test('ADV-19: a WIN booked as DEBIT (wrong direction) is refused', async () => {
    const f = await fixture();
    expect(await attempt(async (tx) => move(tx, f.walletId, await rawTx(tx, f, 'WIN', '10.00', { result: '90.00' }), 'DEBIT', '10.00', '100.00', '90.00'))).toMatch(/direction DEBIT is not valid/);
  });

  test('ADV-20: an observed balance stored in a currency other than the wallet currency is refused', async () => {
    const f = await fixture();
    expect(await attempt(async (tx) => rawTx(tx, f, 'BET', '10.00', { status: 'REJECTED', resultCurrency: 'USD' }))).not.toBeNull();
  });

  test('ADV-21: control — a correct movement written directly (PROCESSED WIN + CREDIT + wallet update) is accepted', async () => {
    const f = await fixture();
    expect(await attempt(async (tx) => move(tx, f.walletId, await rawTx(tx, f, 'WIN', '10.00', { result: '110.00' }), 'CREDIT', '10.00', '100.00', '110.00'))).toBeNull();
    expect((await reconcile(app.url, f.walletId)).consistent).toBe(true);
  });

  test('ADV-11: balance changes through UPDATE alone, DELETE/UPDATE/TRUNCATE of the ledger and deletion of transactions are refused', async () => {
    const f = await fixture();
    expect(await attempt(async (tx) => tx`update wallets set balance_amount = 5000.00, version = 2 where id = ${f.walletId}`)).not.toBeNull();
    expect(await attempt(async (tx) => tx`delete from wallet_ledger_entries where wallet_id = ${f.walletId}`)).not.toBeNull();
    expect(await attempt(async (tx) => tx`update wallet_ledger_entries set amount = 1 where wallet_id = ${f.walletId}`)).not.toBeNull();
    expect(await attempt(async (tx) => tx`truncate wallet_ledger_entries cascade`)).not.toBeNull();
    expect(await attempt(async (tx) => tx`truncate wallets cascade`)).not.toBeNull();
    expect(await attempt(async (tx) => tx`delete from wager_transactions where id = ${f.rejectedId}`)).not.toBeNull();
    expect(await attempt(async (tx) => tx`update wager_transactions set status = 'PROCESSED' where id = ${f.rejectedId}`)).not.toBeNull();
    expect((await reconcile(app.url, f.walletId)).consistent).toBe(true);
  });
});

describe('ADV — global invariant', () => {
  test('ADV-12: after every scenario: balance == sum(ledger) for every wallet, nothing negative', async () => {
    expect((await assertGlobalInvariant(db)).inconsistent).toBe(0);
    const [{ negative }] = await db`select count(*)::int as negative from wallets where balance_amount < 0`;
    expect(negative).toBe(0);
  });
});
