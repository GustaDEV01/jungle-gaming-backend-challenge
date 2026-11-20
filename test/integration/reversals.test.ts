import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import { createWallet, deleteQueues, dropDatabase, ledgerStats, queues, reconcile, sql, startApp, submit, testEnv, type RunningApp, type TestEnv } from '../support/infra';

/**
 * Reversal rules against the real database. Adopted rule: a transaction is
 * reverted AT MOST ONCE, whatever the reversal kind (REFUND or ROLLBACK).
 * The literal "once per kind" reading would allow REFUND + ROLLBACK of the
 * same BET, i.e. a duplicate credit — an eliminatory failure.
 */
let t: TestEnv;
let app: RunningApp;
let db: SQL;
let q: QueueUrls;

beforeAll(async () => {
  t = await testEnv('reversal', { SQS_CONSUMER_ENABLED: 'false', PENDING_REFERENCE_WORKER_ENABLED: 'false' });
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

describe('reversal rules', () => {
  test('BET -> REFUND -> ROLLBACK(REFUND): balances, ledger and the "reverted once" rule on both references', async () => {
    const w = await createWallet(app.url, '100.00');
    const bet = await submit(app.url, { ...w, kind: 'BET', amount: '40.00' });
    expect(bet.body.balance.amount).toBe('60.00');
    const refund = await submit(app.url, { ...w, kind: 'REFUND', amount: '40.00', referenceExternalTransactionId: bet.externalTransactionId });
    expect(refund.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00' }, referenceTransactionId: bet.body.transactionId });
    const rbRefund = await submit(app.url, { ...w, kind: 'ROLLBACK', amount: '40.00', referenceExternalTransactionId: refund.externalTransactionId });
    expect(rbRefund.status).toBe(201);
    expect(rbRefund.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '60.00' }, referenceTransactionId: refund.body.transactionId });

    const again = await Promise.all([
      submit(app.url, { ...w, kind: 'REFUND', amount: '40.00', referenceExternalTransactionId: bet.externalTransactionId }),
      submit(app.url, { ...w, kind: 'ROLLBACK', amount: '40.00', referenceExternalTransactionId: bet.externalTransactionId }),
      submit(app.url, { ...w, kind: 'ROLLBACK', amount: '40.00', referenceExternalTransactionId: refund.externalTransactionId }),
    ]);
    expect(again.map((r) => r.body.failureCode)).toEqual(['REFERENCE_ALREADY_REVERSED', 'REFERENCE_ALREADY_REVERSED', 'REFERENCE_ALREADY_REVERSED']);
    const rbOfRb = await submit(app.url, { ...w, kind: 'ROLLBACK', amount: '40.00', referenceExternalTransactionId: rbRefund.externalTransactionId });
    expect(rbOfRb.body.failureCode).toBe('INVALID_REFERENCE_KIND');

    const ledger = await db`select direction, amount::text as amount from wallet_ledger_entries where wallet_id = ${w.walletId} order by wallet_version`;
    expect(ledger).toEqual([
      { direction: 'CREDIT', amount: '100.00' }, // opening
      { direction: 'DEBIT', amount: '40.00' }, // BET
      { direction: 'CREDIT', amount: '40.00' }, // REFUND
      { direction: 'DEBIT', amount: '40.00' }, // ROLLBACK(REFUND)
    ]);
    expect((await reconcile(app.url, w.walletId)).consistent).toBe(true);
  });

  test('ROLLBACK of a REFUND without funds is rejected with REVERSAL_INSUFFICIENT_FUNDS and stays auditable', async () => {
    const w = await createWallet(app.url, '40.00');
    const bet = await submit(app.url, { ...w, kind: 'BET', amount: '40.00' }); // 0.00
    const refund = await submit(app.url, { ...w, kind: 'REFUND', amount: '40.00', referenceExternalTransactionId: bet.externalTransactionId }); // 40.00
    await submit(app.url, { ...w, kind: 'BET', amount: '30.00' }); // 10.00
    const rb = await submit(app.url, { ...w, kind: 'ROLLBACK', amount: '40.00', referenceExternalTransactionId: refund.externalTransactionId });
    expect(rb.status).toBe(422);
    expect(rb.body).toMatchObject({ status: 'REJECTED', failureCode: 'REVERSAL_INSUFFICIENT_FUNDS', balance: { amount: '10.00' } });
    const [row] = await db`select status, failure_code, reference_transaction_id from wager_transactions where id = ${rb.body.transactionId}`;
    expect(row).toEqual({ status: 'REJECTED', failure_code: 'REVERSAL_INSUFFICIENT_FUNDS', reference_transaction_id: refund.body.transactionId });
    expect((await ledgerStats(db, w.walletId)).balance).toBe('10.00');
  });

  test('ten concurrent ROLLBACKs of the same REFUND: exactly one debit', async () => {
    const w = await createWallet(app.url, '100.00');
    const bet = await submit(app.url, { ...w, kind: 'BET', amount: '25.00' });
    const refund = await submit(app.url, { ...w, kind: 'REFUND', amount: '25.00', referenceExternalTransactionId: bet.externalTransactionId });
    const results = await Promise.all(
      Array.from({ length: 10 }, () => submit(app.url, { ...w, kind: 'ROLLBACK', amount: '25.00', referenceExternalTransactionId: refund.externalTransactionId })),
    );
    expect(results.filter((r) => r.body.status === 'PROCESSED')).toHaveLength(1);
    expect(results.filter((r) => r.body.failureCode === 'REFERENCE_ALREADY_REVERSED')).toHaveLength(9);
    const stats = await ledgerStats(db, w.walletId);
    expect(stats).toMatchObject({ balance: '75.00', debits: 2, credits: 2 });
  });
});
