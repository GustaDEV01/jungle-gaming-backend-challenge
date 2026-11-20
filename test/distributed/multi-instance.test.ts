import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import {
  assertGlobalInvariant,
  createWallet,
  deleteQueues,
  dropDatabase,
  ledgerStats,
  pool,
  queueDepth,
  queues,
  reconcile,
  sendWager,
  sql,
  submit,
  testEnv,
  uniqueName,
  waitFor,
  wagerMessage,
  type TestEnv,
} from '../support/infra';
import { metric, spawnInstance, type Instance } from '../support/processes';

/**
 * Test D — three REAL processes (separate OS processes, pools, consumers and
 * workers) share one PostgreSQL and one set of SQS queues.
 */
let t: TestEnv;
let db: SQL;
let q: QueueUrls;
let nodes: Instance[] = [];

beforeAll(async () => {
  t = await testEnv('multi', { DB_POOL_MAX: '25', SQS_VISIBILITY_TIMEOUT_SECONDS: '10' });
  q = await queues(t.env);
  nodes = await Promise.all(['node-1', 'node-2', 'node-3'].map((n) => spawnInstance(t.env, n)));
  db = sql(t.databaseUrl);
});

afterAll(async () => {
  await Promise.all(nodes.map((n) => n.kill()));
  await db?.close();
  await deleteQueues(q);
  await dropDatabase(t.databaseUrl);
});

const node = (i: number) => nodes[i % nodes.length]!;

describe('Test D — three instances', () => {
  test('Test A across instances: 2 x BET 80.00 on 100.00 hit different processes (x10 wallets)', async () => {
    const wallets = await Promise.all(Array.from({ length: 10 }, (_, i) => createWallet(node(i).url, '100.00')));
    const results = await Promise.all(
      wallets.flatMap((w, i) => [
        submit(node(i).url, { ...w, kind: 'BET', amount: '80.00' }),
        submit(node(i + 1).url, { ...w, kind: 'BET', amount: '80.00' }),
      ]),
    );
    for (let i = 0; i < wallets.length; i++) {
      const pair = [results[2 * i]!.body.status, results[2 * i + 1]!.body.status].sort();
      expect(pair).toEqual(['PROCESSED', 'REJECTED']);
      const stats = await ledgerStats(db, wallets[i]!.walletId);
      expect(stats).toMatchObject({ balance: '20.00', debits: 1 });
    }
  });

  test('Test B across instances: the same BET 60x spread over 3 processes => one debit', async () => {
    const w = await createWallet(node(0).url, '100.00');
    const externalTransactionId = uniqueName('dup');
    const results = await Promise.all(
      Array.from({ length: 60 }, (_, i) => submit(node(i).url, { ...w, kind: 'BET', amount: '10.00', externalTransactionId })),
    );
    expect(new Set(results.map((r) => r.body.transactionId)).size).toBe(1);
    expect(results.filter((r) => r.status === 201)).toHaveLength(1);
    expect(results.every((r) => r.body.balance.amount === '90.00')).toBe(true);
    expect((await ledgerStats(db, w.walletId)).debits).toBe(1);
  });

  test('mixed HTTP + SQS load with duplicated messages, consumed by all 3 consumers', async () => {
    const wallets = await Promise.all(Array.from({ length: 8 }, (_, i) => createWallet(node(i).url, '200.00')));
    const messages = wallets.flatMap((w) =>
      Array.from({ length: 12 }, (_, j) => wagerMessage({ ...w, kind: j % 4 === 0 ? 'WIN' : 'BET', amount: j % 4 === 0 ? '5.00' : '9.00' })),
    );
    // every message is delivered twice (redelivery) and HTTP traffic hits the same wallets at the same time
    const sends = pool([...messages, ...messages].map((m) => () => sendWager(q.wager, m)));
    const httpOps = wallets.flatMap((w, i) =>
      Array.from({ length: 10 }, (_, j) => submit(node(i + j).url, { ...w, kind: 'BET', amount: '7.00' })),
    );
    await Promise.all([sends, ...httpOps]);

    await waitFor(async () => {
      const [{ n }] = await db`select count(*)::int as n from inbox_messages where message_id = any(${messages.map((m) => m.messageId)})`;
      return n === messages.length;
    }, 'all messages consumed', 90_000, 250);
    await waitFor(async () => {
      const d = await queueDepth(q.wager);
      return d.visible + d.inFlight === 0;
    }, 'queue drained', 60_000, 250);

    // exactly one transaction per message, despite double delivery
    const [{ n: txCount }] = await db`select count(*)::int as n from wager_transactions
      where external_transaction_id = any(${messages.map((m) => String(m.data.externalTransactionId))})`;
    expect(txCount).toBe(messages.length);
    for (const w of wallets) expect((await reconcile(node(0).url, w.walletId)).consistent).toBe(true);
    // balance-affecting processed transactions == ledger entries, for these wallets
    const [{ processed, entries }] = await db`
      select (select count(*)::int from wager_transactions where wallet_id = any(${wallets.map((w) => w.walletId)})
                and status = 'PROCESSED' and kind <> 'LOSS') as processed,
             (select count(*)::int from wallet_ledger_entries where wallet_id = any(${wallets.map((w) => w.walletId)})) as entries`;
    expect(entries).toBe(processed);

    // the work really was distributed: at least two processes consumed from SQS
    const perNode = await Promise.all(nodes.map((n) => metric(n, /^wagering_transactions_total\{.*source="sqs"/)));
    expect(perNode.filter((v) => v > 0).length).toBeGreaterThanOrEqual(2);
    const duplicates = await Promise.all(nodes.map((n) => metric(n, /^wagering_duplicates_total\{kind="inbox_duplicate"/)));
    expect(duplicates.reduce((a, b) => a + b, 0)).toBeGreaterThan(0);
  });

  test('pending references resolved by three competing workers, each exactly once', async () => {
    const w = await createWallet(node(0).url, '1000.00');
    const refunds = await Promise.all(
      Array.from({ length: 15 }, (_, i) => submit(node(i).url, { ...w, kind: 'REFUND', amount: '10.00', referenceExternalTransactionId: `late-${w.walletId}-${i}` })),
    );
    expect(refunds.every((r) => r.status === 202)).toBe(true);
    await Promise.all(
      Array.from({ length: 15 }, (_, i) => submit(node(i + 1).url, { ...w, kind: 'BET', amount: '10.00', externalTransactionId: `late-${w.walletId}-${i}` })),
    );
    await waitFor(async () => {
      const [{ n }] = await db`select count(*)::int as n from wager_transactions where wallet_id = ${w.walletId} and kind = 'REFUND' and status = 'PROCESSED'`;
      return n === 15;
    }, 'refunds resolved', 30_000, 200);
    const stats = await ledgerStats(db, w.walletId);
    expect(stats).toMatchObject({ balance: '1000.00', debits: 15, credits: 16 });
  });

  test('global invariant over everything the three instances did', async () => {
    const { wallets, inconsistent } = await assertGlobalInvariant(db);
    expect(wallets).toBeGreaterThan(0);
    expect(inconsistent).toBe(0);
    const [{ negative }] = await db`select count(*)::int as negative from wallet_ledger_entries where balance_after < 0`;
    expect(negative).toBe(0);
  });
});
