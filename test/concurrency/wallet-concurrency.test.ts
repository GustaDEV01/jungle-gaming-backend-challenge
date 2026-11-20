import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import {
  assertGlobalInvariant,
  createWallet,
  deleteQueues,
  dropDatabase,
  http,
  ledgerStats,
  queues,
  reconcile,
  sql,
  startApp,
  submit,
  testEnv,
  uniqueName,
  type RunningApp,
  type TestEnv,
} from '../support/infra';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';

/**
 * Real parallelism against a real PostgreSQL: requests are fired concurrently
 * over HTTP (Promise.all), each on its own pooled connection.
 */
let t: TestEnv;
let app: RunningApp;
let db: SQL;
let q: QueueUrls;

beforeAll(async () => {
  t = await testEnv('conc', { SQS_CONSUMER_ENABLED: 'false', DB_POOL_MAX: '60' });
  q = await queues(t.env);
  app = await startApp(t.env);
  db = sql(t.databaseUrl);
});

afterAll(async () => {
  await app?.close();
  await db?.close();
  await deleteQueues(q);
  await dropDatabase(t.databaseUrl);
});

describe('Test A — two concurrent BETs of 80.00 on a 100.00 wallet', () => {
  test('exactly one PROCESSED, one REJECTED (INSUFFICIENT_FUNDS), final 20.00, one DEBIT — repeated 25 times', async () => {
    for (let round = 0; round < 25; round++) {
      const { walletId, playerId } = await createWallet(app.url, '100.00');
      const [a, b] = await Promise.all([
        submit(app.url, { walletId, playerId, kind: 'BET', amount: '80.00' }),
        submit(app.url, { walletId, playerId, kind: 'BET', amount: '80.00' }),
      ]);
      const statuses = [a.body.status, b.body.status].sort();
      expect(statuses).toEqual(['PROCESSED', 'REJECTED']);
      const rejected = a.body.status === 'REJECTED' ? a : b;
      expect(rejected.status).toBe(422);
      expect(rejected.body.failureCode).toBe('INSUFFICIENT_FUNDS');

      const stats = await ledgerStats(db, walletId);
      expect(stats.balance).toBe('20.00');
      expect(stats.debits).toBe(1);
      expect(stats.entries).toBe(2); // OPENING credit + one debit

      // retrying both requests never duplicates the debit
      const [ra, rb] = await Promise.all([
        submit(app.url, { walletId, playerId, kind: 'BET', amount: '80.00', externalTransactionId: a.externalTransactionId }),
        submit(app.url, { walletId, playerId, kind: 'BET', amount: '80.00', externalTransactionId: b.externalTransactionId }),
      ]);
      expect(ra.body.idempotentReplay).toBe(true);
      expect(rb.body.idempotentReplay).toBe(true);
      expect(ra.body.status).toBe(a.body.status);
      expect(rb.body.status).toBe(b.body.status);
      expect((await ledgerStats(db, walletId)).debits).toBe(1);
      expect((await reconcile(app.url, walletId)).consistent).toBe(true);
    }
  });
});

describe('Test B — the same BET sent 50 times in parallel', () => {
  test('single debit, one original response and 49 replays with the original balance', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const externalTransactionId = uniqueName('same-bet');
    const results = await Promise.all(
      Array.from({ length: 50 }, () => submit(app.url, { walletId, playerId, kind: 'BET', amount: '25.00', externalTransactionId })),
    );
    expect(results.every((r) => r.body.status === 'PROCESSED')).toBe(true);
    expect(new Set(results.map((r) => r.body.transactionId)).size).toBe(1);
    expect(results.filter((r) => r.body.idempotentReplay === false)).toHaveLength(1);
    expect(results.filter((r) => r.status === 201)).toHaveLength(1);
    expect(results.filter((r) => r.status === 200)).toHaveLength(49);
    expect(results.every((r) => r.body.balance.amount === '75.00')).toBe(true);

    const stats = await ledgerStats(db, walletId);
    expect(stats.debits).toBe(1);
    expect(stats.balance).toBe('75.00');
    const [{ count }] = await db`select count(*)::int as count from wager_transactions where wallet_id = ${walletId} and kind = 'BET'`;
    expect(count).toBe(1);
  });

  test('50 parallel requests with the same key but divergent payloads: one wins, others conflict (409)', async () => {
    const { walletId, playerId } = await createWallet(app.url, '1000.00');
    const externalTransactionId = uniqueName('divergent');
    const key = `provider-a:${externalTransactionId}`;
    const results = await Promise.all(
      Array.from({ length: 50 }, (_, i) =>
        submit(app.url, { walletId, playerId, kind: 'BET', amount: `${(i % 5) + 1}.00`, externalTransactionId }, key),
      ),
    );
    const created = results.filter((r) => r.status === 201);
    expect(created).toHaveLength(1);
    const winnerAmount = created[0]!.body.balance.amount;
    for (const r of results) {
      if (r.status === 201) continue;
      // identical payload to the winner => replay; otherwise => conflict
      expect([200, 409]).toContain(r.status);
      if (r.status === 409) expect(r.body.error.code).toBe('IDEMPOTENCY_KEY_CONFLICT');
      if (r.status === 200) expect(r.body.balance.amount).toBe(winnerAmount);
    }
    expect((await ledgerStats(db, walletId)).debits).toBe(1);
  });
});

describe('Hot wallet — many concurrent mixed operations', () => {
  test('balance never negative, no lost update, ledger == balance', async () => {
    const { walletId, playerId } = await createWallet(app.url, '50.00');
    const ops = Array.from({ length: 200 }, (_, i) =>
      submit(app.url, { walletId, playerId, kind: i % 3 === 0 ? 'WIN' : 'BET', amount: i % 3 === 0 ? '3.00' : '7.00' }),
    );
    const results = await Promise.all(ops);
    expect(results.every((r) => [201, 422].includes(r.status))).toBe(true);
    const processedBets = results.filter((r) => r.body.kind === 'BET' && r.body.status === 'PROCESSED').length;
    const wins = results.filter((r) => r.body.kind === 'WIN' && r.body.status === 'PROCESSED').length;
    const rejected = results.filter((r) => r.body.status === 'REJECTED');
    expect(rejected.every((r) => r.body.failureCode === 'INSUFFICIENT_FUNDS')).toBe(true);

    const stats = await ledgerStats(db, walletId);
    // 50.00 + 3.00 * wins - 7.00 * processedBets, computed with integers in cents
    const expectedCents = 5000 + 300 * wins - 700 * processedBets;
    expect(stats.balance).toBe(`${Math.floor(expectedCents / 100)}.${String(expectedCents % 100).padStart(2, '0')}`);
    expect(stats.min_balance_after?.startsWith('-')).toBe(false);
    expect(stats.entries).toBe(1 + processedBets + wins);
    const rec = await reconcile(app.url, walletId);
    expect(rec.consistent).toBe(true);
    const [{ version }] = await db`select version from wallets where id = ${walletId}`;
    expect(version).toBe(stats.entries); // version increments exactly once per balance change
  });
});

describe('Concurrent REFUND and ROLLBACK on the same BET', () => {
  test('exactly one reversal is applied; the other is REJECTED (REFERENCE_ALREADY_REVERSED) — repeated 15 times', async () => {
    for (let i = 0; i < 15; i++) {
      const { walletId, playerId } = await createWallet(app.url, '100.00');
      const bet = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '40.00' });
      expect(bet.status).toBe(201);
      const ref = bet.externalTransactionId;
      const attempts = await Promise.all([
        submit(app.url, { walletId, playerId, kind: 'REFUND', amount: '40.00', referenceExternalTransactionId: ref }),
        submit(app.url, { walletId, playerId, kind: 'ROLLBACK', amount: '40.00', referenceExternalTransactionId: ref }),
        submit(app.url, { walletId, playerId, kind: 'REFUND', amount: '40.00', referenceExternalTransactionId: ref }),
        submit(app.url, { walletId, playerId, kind: 'ROLLBACK', amount: '40.00', referenceExternalTransactionId: ref }),
      ]);
      expect(attempts.filter((r) => r.body.status === 'PROCESSED')).toHaveLength(1);
      const rejected = attempts.filter((r) => r.body.status === 'REJECTED');
      expect(rejected).toHaveLength(3);
      expect(rejected.every((r) => r.body.failureCode === 'REFERENCE_ALREADY_REVERSED')).toBe(true);
      const stats = await ledgerStats(db, walletId);
      expect(stats.balance).toBe('100.00');
      expect(stats.credits).toBe(2); // opening + one reversal
    }
  });
});

describe('Test C — different wallets in parallel', () => {
  test('20 wallets x 15 operations concurrently, all consistent', async () => {
    const wallets = await Promise.all(Array.from({ length: 20 }, () => createWallet(app.url, '100.00')));
    const results = await Promise.all(
      wallets.flatMap((w) => Array.from({ length: 15 }, () => submit(app.url, { ...w, kind: 'BET', amount: '10.00' }))),
    );
    expect(results.every((r) => [201, 422].includes(r.status))).toBe(true);
    for (const w of wallets) {
      const stats = await ledgerStats(db, w.walletId);
      expect(stats.debits).toBe(10); // exactly 10 x 10.00 fit in 100.00
      expect(stats.balance).toBe('0.00');
    }
  });

  test('no global lock: a wallet locked by another session does not block other wallets', async () => {
    const a = await createWallet(app.url, '100.00');
    const b = await createWallet(app.url, '100.00');
    const holder = sql(t.databaseUrl);
    try {
      // Hold wallet A's row lock in an open transaction from an independent session.
      const locked = holder.begin(async (tx) => {
        await tx`select id from wallets where id = ${a.walletId} for update`;
        await Bun.sleep(1500);
      });
      await Bun.sleep(200);
      const startedA = performance.now();
      const pendingA = submit(app.url, { ...a, kind: 'BET', amount: '1.00' });
      const startedB = performance.now();
      const resB = await submit(app.url, { ...b, kind: 'BET', amount: '1.00' });
      const elapsedB = performance.now() - startedB;
      expect(resB.status).toBe(201);
      expect(elapsedB).toBeLessThan(800); // B was not blocked by A's lock
      const resA = await pendingA;
      expect(resA.status).toBe(201);
      expect(performance.now() - startedA).toBeGreaterThan(1000); // A really waited for its own wallet lock
      await locked;
    } finally {
      await holder.close();
    }
  });
});

describe('Global invariant', () => {
  test('wallet.balance == balance rebuilt from the ledger for every wallet', async () => {
    const result = await assertGlobalInvariant(db);
    expect(result.wallets).toBeGreaterThan(0);
    expect(result.inconsistent).toBe(0);
    const health = await http('GET', `${app.url}/health/ready`);
    expect(health.status).toBe(200);
  });
});
