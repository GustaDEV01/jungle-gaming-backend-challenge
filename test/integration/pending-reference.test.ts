import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import {
  createWallet,
  deleteQueues,
  dropDatabase,
  http,
  ledgerStats,
  queues,
  reconcile,
  sendWager,
  sql,
  startApp,
  submit,
  testEnv,
  uniqueName,
  waitFor,
  wagerMessage,
  type RunningApp,
  type TestEnv,
} from '../support/infra';

let t: TestEnv;
let app: RunningApp;
let db: SQL;
let q: QueueUrls;

beforeAll(async () => {
  t = await testEnv('pending', { PENDING_REFERENCE_MAX_ATTEMPTS: '5', PENDING_REFERENCE_BASE_DELAY_MS: '100', PENDING_REFERENCE_MAX_DELAY_MS: '300' });
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

async function status(walletId: string, externalId: string) {
  const [row] = await db`select status, failure_code, reference_attempts from wager_transactions where wallet_id = ${walletId} and external_transaction_id = ${externalId}`;
  return row as { status: string; failure_code: string | null; reference_attempts: number } | undefined;
}

async function events(walletId: string, type: string) {
  return db`select payload from outbox_messages where aggregate_id = ${walletId} and event_type = ${type}`;
}

describe('out-of-order references', () => {
  test('REFUND delivered before its BET is parked, then applied once the BET arrives', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const betId = uniqueName('late-bet');
    const refund = await submit(app.url, { walletId, playerId, kind: 'REFUND', amount: '40.00', referenceExternalTransactionId: betId });
    expect(refund.status).toBe(202);
    expect(refund.body.status).toBe('PENDING_REFERENCE');
    expect((await events(walletId, 'WagerTransactionPendingReference')).length).toBe(1);

    const bet = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '40.00', externalTransactionId: betId });
    expect(bet.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '60.00' } });

    await waitFor(async () => (await status(walletId, refund.externalTransactionId))?.status === 'PROCESSED', 'refund resolved');
    const stats = await ledgerStats(db, walletId);
    expect(stats.balance).toBe('100.00');
    expect(stats.debits).toBe(1);
    expect(stats.credits).toBe(2);

    // A replay now returns the final outcome
    const replay = await submit(app.url, { walletId, playerId, kind: 'REFUND', amount: '40.00', referenceExternalTransactionId: betId, externalTransactionId: refund.externalTransactionId });
    expect(replay.status).toBe(200);
    expect(replay.body).toMatchObject({ status: 'PROCESSED', idempotentReplay: true, balance: { amount: '100.00' }, referenceTransactionId: bet.body.transactionId });
    expect((await events(walletId, 'WagerTransactionPendingReference')).length).toBe(1); // not re-emitted on every retry
    expect((await reconcile(app.url, walletId)).consistent).toBe(true);
  });

  test('ROLLBACK delivered before its reference over SQS (out of order)', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const winId = uniqueName('win');
    const rollback = wagerMessage({ walletId, playerId, kind: 'ROLLBACK', amount: '30.00', referenceExternalTransactionId: winId });
    const win = wagerMessage({ walletId, playerId, kind: 'WIN', amount: '30.00', externalTransactionId: winId });
    await sendWager(q.wager, rollback); // same message group: ROLLBACK really is consumed first
    await sendWager(q.wager, win);
    await waitFor(async () => (await status(walletId, String(rollback.data.externalTransactionId)))?.status === 'PROCESSED', 'rollback resolved', 20_000);
    const stats = await ledgerStats(db, walletId);
    expect(stats.balance).toBe('100.00'); // +30 (WIN) -30 (ROLLBACK)
    expect(stats.entries).toBe(3);
    expect((await status(walletId, winId))?.status).toBe('PROCESSED');
  });

  test('chain: ROLLBACK -> WIN -> BET all out of order', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const betId = uniqueName('bet');
    const winId = uniqueName('win');
    const rb = await submit(app.url, { walletId, playerId, kind: 'ROLLBACK', amount: '25.00', referenceExternalTransactionId: winId });
    const win = await submit(app.url, { walletId, playerId, kind: 'WIN', amount: '25.00', externalTransactionId: winId, referenceExternalTransactionId: betId });
    expect(rb.status).toBe(202);
    expect(win.status).toBe(202);
    await submit(app.url, { walletId, playerId, kind: 'BET', amount: '10.00', externalTransactionId: betId });
    await waitFor(async () => (await status(walletId, rb.externalTransactionId))?.status === 'PROCESSED', 'chain resolved', 20_000);
    expect((await ledgerStats(db, walletId)).balance).toBe('90.00');
  });

  test('reference that never arrives: REJECTED with REFERENCE_NOT_FOUND after the retry budget, with event', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const refund = await submit(app.url, { walletId, playerId, kind: 'REFUND', amount: '10.00', referenceExternalTransactionId: uniqueName('ghost') });
    expect(refund.status).toBe(202);
    const final = await waitFor(async () => {
      const s = await status(walletId, refund.externalTransactionId);
      return s?.status === 'REJECTED' ? s : undefined;
    }, 'expiry', 20_000);
    expect(final.failure_code).toBe('REFERENCE_NOT_FOUND');
    expect(final.reference_attempts).toBe(5);
    const rejected = await events(walletId, 'WagerTransactionRejected');
    expect(rejected.length).toBe(1);
    expect(rejected[0].payload.data.failureCode).toBe('REFERENCE_NOT_FOUND');
    expect((await ledgerStats(db, walletId)).entries).toBe(1); // only the opening credit
    const res = await http('GET', `${app.url}/wagering/transactions/${refund.body.transactionId}`);
    expect(res.body).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_NOT_FOUND' });
  });

  test('reference arrives but is invalid (wrong amount) => REJECTED by the worker, not left pending', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const betId = uniqueName('bet');
    const refund = await submit(app.url, { walletId, playerId, kind: 'REFUND', amount: '11.00', referenceExternalTransactionId: betId });
    await submit(app.url, { walletId, playerId, kind: 'BET', amount: '10.00', externalTransactionId: betId });
    const s = await waitFor(async () => {
      const r = await status(walletId, refund.externalTransactionId);
      return r?.status === 'REJECTED' ? r : undefined;
    }, 'rejected');
    expect(s.failure_code).toBe('REVERSAL_AMOUNT_MISMATCH');
  });
});
