import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import { createWallet, deleteQueues, dropDatabase, http, queues, reconcile, startApp, submit, testEnv, uniqueName, wagerBody, type RunningApp, type TestEnv } from '../support/infra';

let t: TestEnv;
let app: RunningApp;
let q: QueueUrls;

beforeAll(async () => {
  t = await testEnv('http', { SQS_CONSUMER_ENABLED: 'false', PENDING_REFERENCE_WORKER_ENABLED: 'false' });
  q = await queues(t.env);
  app = await startApp(t.env);
});

afterAll(async () => {
  await app?.close();
  await deleteQueues(q);
  await dropDatabase(t.databaseUrl);
});

describe('POST /wallets', () => {
  test('creates the wallet with version 1 and the opening balance', async () => {
    const playerId = uniqueName('p');
    const res = await http('POST', `${app.url}/wallets`, { playerId, initialBalance: { amount: '1000.00', currency: 'BRL' } });
    expect(res.status).toBe(201);
    expect(res.body).toMatchObject({ playerId, balance: { amount: '1000.00', currency: 'BRL' }, version: 1 });
    expect(res.headers.get('x-correlation-id')).toBeTruthy();
  });

  test('duplicate (playerId, currency) is a 409 conflict; another currency is allowed', async () => {
    const playerId = uniqueName('p');
    const body = { playerId, initialBalance: { amount: '1.00', currency: 'BRL' } };
    expect((await http('POST', `${app.url}/wallets`, body)).status).toBe(201);
    const dup = await http('POST', `${app.url}/wallets`, body);
    expect(dup.status).toBe(409);
    expect(dup.body.error.code).toBe('WALLET_ALREADY_EXISTS');
    expect((await http('POST', `${app.url}/wallets`, { playerId, initialBalance: { amount: '1.00', currency: 'USD' } })).status).toBe(201);
  });

  test('concurrent duplicates: exactly one wallet', async () => {
    const playerId = uniqueName('p');
    const results = await Promise.all(
      Array.from({ length: 10 }, () => http('POST', `${app.url}/wallets`, { playerId, initialBalance: { amount: '5.00', currency: 'BRL' } })),
    );
    expect(results.filter((r) => r.status === 201)).toHaveLength(1);
    expect(results.filter((r) => r.status === 409)).toHaveLength(9);
  });

  test.each([
    [{ playerId: 'p', initialBalance: { amount: 10, currency: 'BRL' } }],
    [{ playerId: 'p', initialBalance: { amount: '10.001', currency: 'BRL' } }],
    [{ playerId: 'p', initialBalance: { amount: '-1.00', currency: 'BRL' } }],
    [{ playerId: 'p', initialBalance: { amount: '1e2', currency: 'BRL' } }],
    [{ playerId: 'p', initialBalance: { amount: 'NaN', currency: 'BRL' } }],
    [{ playerId: 'p', initialBalance: { amount: '1.00', currency: 'brl' } }],
    [{ playerId: '', initialBalance: { amount: '1.00', currency: 'BRL' } }],
  ])('invalid payload %j => 400', async (body) => {
    const res = await http('POST', `${app.url}/wallets`, body);
    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('VALIDATION_ERROR');
  });
});

describe('POST /wagering/transactions — validation', () => {
  test('Idempotency-Key is mandatory', async () => {
    const { walletId, playerId } = await createWallet(app.url);
    const res = await http('POST', `${app.url}/wagering/transactions`, wagerBody({ walletId, playerId, kind: 'BET', amount: '1.00' }));
    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('IDEMPOTENCY_KEY_REQUIRED');
  });

  test.each([
    ['number amount', { money: { amount: 25.5, currency: 'BRL' } }],
    ['scientific notation', { money: { amount: '2.5e1', currency: 'BRL' } }],
    ['3 decimals', { money: { amount: '1.999', currency: 'BRL' } }],
    ['no decimals ("25")', { money: { amount: '25', currency: 'BRL' } }],
    ['one decimal ("25.0")', { money: { amount: '25.0', currency: 'BRL' } }],
    ['OPENING is internal', { kind: 'OPENING' }],
    ['unknown kind', { kind: 'JACKPOT' }],
    ['REFUND without reference', { kind: 'REFUND' }],
    ['BET with reference', { referenceExternalTransactionId: 'x' }],
    ['zero BET', { money: { amount: '0.00', currency: 'BRL' } }],
    ['extra field', { foo: 'bar' }],
    ['walletId not uuid', { walletId: 'abc' }],
  ])('%s => 400', async (_name, patch) => {
    const { walletId, playerId } = await createWallet(app.url);
    const body = { ...wagerBody({ walletId, playerId, kind: 'BET', amount: '1.00' }), ...patch };
    const res = await http('POST', `${app.url}/wagering/transactions`, body, { 'Idempotency-Key': uniqueName('k') });
    expect(res.status).toBe(400);
    expect(res.body.error.code).toBe('VALIDATION_ERROR');
  });

  test('unknown wallet / provider => 422 business rejection, nothing persisted', async () => {
    const { playerId } = await createWallet(app.url);
    const missing = await submit(app.url, { walletId: '01a1fa1b-0000-7000-8000-000000000000', playerId, kind: 'BET', amount: '1.00' });
    expect(missing.status).toBe(422);
    expect(missing.body).toMatchObject({ transactionId: null, status: 'REJECTED', failureCode: 'WALLET_NOT_FOUND' });
    const { walletId } = await createWallet(app.url);
    const provider = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '1.00', providerId: 'unknown-provider' });
    expect(provider.status).toBe(422);
    expect(provider.body.failureCode).toBe('UNKNOWN_PROVIDER');
    const internal = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '1.00', providerId: 'internal' });
    expect(internal.body.failureCode).toBe('UNKNOWN_PROVIDER');
  });
});

describe('POST /wagering/transactions — flows and idempotency', () => {
  test('replay returns the ORIGINAL result (balance observed at that time), not the current balance', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const first = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '25.00' });
    expect(first.status).toBe(201);
    expect(first.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' }, idempotentReplay: false });
    expect(first.headers.get('idempotent-replay')).toBe('false');

    await submit(app.url, { walletId, playerId, kind: 'BET', amount: '50.00' }); // balance is now 25.00
    const replay = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '25.00', externalTransactionId: first.externalTransactionId });
    expect(replay.status).toBe(200);
    expect(replay.body).toMatchObject({ transactionId: first.body.transactionId, balance: { amount: '75.00' }, idempotentReplay: true });
    expect(replay.headers.get('idempotent-replay')).toBe('true');
    expect((await http('GET', `${app.url}/wallets/${walletId}`)).body.balance.amount).toBe('25.00');
  });

  test('replay of a REJECTED transaction returns the same rejection', async () => {
    const { walletId, playerId } = await createWallet(app.url, '10.00');
    const first = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '25.00' });
    expect(first.status).toBe(422);
    await submit(app.url, { walletId, playerId, kind: 'WIN', amount: '100.00' }); // now there would be enough funds
    const replay = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '25.00', externalTransactionId: first.externalTransactionId });
    expect(replay.status).toBe(422);
    expect(replay.body).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', idempotentReplay: true, balance: { amount: '10.00' } });
  });

  test('same key + different payload => 409 IDEMPOTENCY_KEY_CONFLICT (not a replay)', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const first = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '25.00' });
    const conflict = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '26.00', externalTransactionId: first.externalTransactionId });
    expect(conflict.status).toBe(409);
    expect(conflict.body.error).toMatchObject({ code: 'IDEMPOTENCY_KEY_CONFLICT', retryable: false });
  });

  test('same external id under another key => 409 EXTERNAL_TRANSACTION_CONFLICT', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const first = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '25.00' });
    const other = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '25.00', externalTransactionId: first.externalTransactionId }, uniqueName('other-key'));
    expect(other.status).toBe(409);
    expect(other.body.error.code).toBe('EXTERNAL_TRANSACTION_CONFLICT');
  });

  test('BET -> WIN -> LOSS -> REFUND -> ROLLBACK full flow with balances and ledger', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const bet = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '20.00', roundId: 'r-a' });
    const win = await submit(app.url, { walletId, playerId, kind: 'WIN', amount: '50.00', roundId: 'r-a', referenceExternalTransactionId: bet.externalTransactionId });
    expect(win.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '130.00' }, referenceTransactionId: bet.body.transactionId });
    const bet2 = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '10.00', roundId: 'r-b' });
    const loss = await submit(app.url, { walletId, playerId, kind: 'LOSS', amount: '0.00', roundId: 'r-b', referenceExternalTransactionId: bet2.externalTransactionId });
    expect(loss.status).toBe(201);
    expect(loss.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '120.00' } });
    const bet3 = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '15.00', roundId: 'r-c' });
    const refund = await submit(app.url, { walletId, playerId, kind: 'REFUND', amount: '15.00', roundId: 'r-c', referenceExternalTransactionId: bet3.externalTransactionId });
    expect(refund.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '120.00' } });
    const rollbackWin = await submit(app.url, { walletId, playerId, kind: 'ROLLBACK', amount: '50.00', roundId: 'r-a', referenceExternalTransactionId: win.externalTransactionId });
    expect(rollbackWin.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '70.00' } });
    const rollbackRefund = await submit(app.url, { walletId, playerId, kind: 'ROLLBACK', amount: '15.00', roundId: 'r-c', referenceExternalTransactionId: refund.externalTransactionId });
    expect(rollbackRefund.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '55.00' } });

    // reversal mistakes
    const wrongAmount = await submit(app.url, { walletId, playerId, kind: 'REFUND', amount: '19.99', roundId: 'r-a', referenceExternalTransactionId: bet.externalTransactionId });
    expect(wrongAmount.body.failureCode).toBe('REVERSAL_AMOUNT_MISMATCH');
    const wrongRound = await submit(app.url, { walletId, playerId, kind: 'REFUND', amount: '20.00', roundId: 'r-z', referenceExternalTransactionId: bet.externalTransactionId });
    expect(wrongRound.body.failureCode).toBe('REFERENCE_MISMATCH');
    const twice = await submit(app.url, { walletId, playerId, kind: 'ROLLBACK', amount: '50.00', roundId: 'r-a', referenceExternalTransactionId: win.externalTransactionId });
    expect(twice.body.failureCode).toBe('REFERENCE_ALREADY_REVERSED');

    const ledger = await http('GET', `${app.url}/wallets/${walletId}/ledger?limit=200`);
    // OPENING, BET, WIN, BET2, BET3, REFUND, ROLLBACK(WIN), ROLLBACK(REFUND) — LOSS and rejections have no entry
    expect(ledger.body.entries.map((e: { direction: string }) => e.direction)).toEqual([
      'CREDIT', 'DEBIT', 'CREDIT', 'DEBIT', 'DEBIT', 'CREDIT', 'DEBIT', 'DEBIT',
    ]);
    const wallet = await http('GET', `${app.url}/wallets/${walletId}`);
    expect(wallet.body).toMatchObject({ balance: { amount: '55.00' }, version: 8 });
    expect(await reconcile(app.url, walletId)).toMatchObject({ consistent: true, checkedEntries: 8 });
  });

  test('currency conflict is a persisted business rejection', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const res = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '1.00', currency: 'USD' });
    expect(res.status).toBe(422);
    expect(res.body.failureCode).toBe('CURRENCY_MISMATCH');
    expect(res.body.transactionId).toBeTruthy();
  });

  test('reference missing => 202 PENDING_REFERENCE', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const res = await submit(app.url, { walletId, playerId, kind: 'REFUND', amount: '1.00', referenceExternalTransactionId: 'not-arrived-yet' });
    expect(res.status).toBe(202);
    expect(res.body).toMatchObject({ status: 'PENDING_REFERENCE', balance: null });
  });
});

describe('queries', () => {
  test('GET transaction by id and by provider external id; 404s', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const bet = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '5.00' });
    const byId = await http('GET', `${app.url}/wagering/transactions/${bet.body.transactionId}`);
    expect(byId.status).toBe(200);
    expect(byId.body).toMatchObject({ transactionId: bet.body.transactionId, kind: 'BET', status: 'PROCESSED', money: { amount: '5.00', currency: 'BRL' } });
    const byExt = await http('GET', `${app.url}/providers/provider-a/wagering/transactions/${bet.externalTransactionId}`);
    expect(byExt.body.transactionId).toBe(bet.body.transactionId);
    expect((await http('GET', `${app.url}/wagering/transactions/01a1fa1b-0000-7000-8000-000000000000`)).status).toBe(404);
    expect((await http('GET', `${app.url}/wagering/transactions/not-a-uuid`)).status).toBe(404);
    expect((await http('GET', `${app.url}/providers/provider-a/wagering/transactions/nope`)).status).toBe(404);
    expect((await http('GET', `${app.url}/wallets/01a1fa1b-0000-7000-8000-000000000000`)).status).toBe(404);
  });

  test('ledger pagination: opaque, stable cursor; entries appended meanwhile are neither skipped nor repeated', async () => {
    const { walletId, playerId } = await createWallet(app.url, '1000.00');
    for (let i = 0; i < 9; i++) await submit(app.url, { walletId, playerId, kind: 'BET', amount: '1.00' });
    const page1 = await http('GET', `${app.url}/wallets/${walletId}/ledger?limit=4`);
    expect(page1.body.entries).toHaveLength(4);
    expect(typeof page1.body.nextCursor).toBe('string');
    // new entries appended between pages
    for (let i = 0; i < 3; i++) await submit(app.url, { walletId, playerId, kind: 'WIN', amount: '2.00' });
    const seen: number[] = page1.body.entries.map((e: { walletVersion: number }) => e.walletVersion);
    let cursor = page1.body.nextCursor;
    while (cursor) {
      const page = await http('GET', `${app.url}/wallets/${walletId}/ledger?limit=4&cursor=${cursor}`);
      seen.push(...page.body.entries.map((e: { walletVersion: number }) => e.walletVersion));
      cursor = page.body.nextCursor;
    }
    expect(seen).toEqual(Array.from({ length: 13 }, (_, i) => i + 1));
    expect((await http('GET', `${app.url}/wallets/${walletId}/ledger?cursor=garbage`)).status).toBe(400);
    expect((await http('GET', `${app.url}/wallets/${walletId}/ledger?limit=1000`)).status).toBe(400);
  });

  test('reconciliation response shape', async () => {
    const { walletId, playerId } = await createWallet(app.url, '975.00');
    await submit(app.url, { walletId, playerId, kind: 'WIN', amount: '25.00' });
    const res = await http('POST', `${app.url}/wallets/${walletId}/reconciliation`);
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      walletId,
      storedBalance: { amount: '1000.00', currency: 'BRL' },
      calculatedBalance: { amount: '1000.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 2,
    });
  });
});

describe('health and metrics', () => {
  test('live, ready and metrics', async () => {
    expect((await http('GET', `${app.url}/health/live`)).body).toEqual({ status: 'ok' });
    const ready = await http('GET', `${app.url}/health/ready`);
    expect(ready.status).toBe(200);
    expect(ready.body.checks.postgres.status).toBe('up');
    expect(ready.body.checks.sqs.status).toBe('up');
    const metrics = await fetch(`${app.url}/metrics`).then((r) => r.text());
    for (const name of [
      'wagering_transactions_total',
      'wagering_duplicates_total',
      'wagering_lock_conflicts_total',
      'wagering_processing_duration_seconds',
      'wagering_outbox_pending_messages',
      'wagering_retries_total',
      'wagering_dlq_messages_total',
    ]) {
      expect(metrics).toContain(name);
    }
  });
});
