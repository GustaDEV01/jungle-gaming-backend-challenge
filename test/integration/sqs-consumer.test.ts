import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { Message } from '@aws-sdk/client-sqs';
import type { SQL } from 'bun';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import {
  createWallet,
  deleteQueues,
  drain,
  dropDatabase,
  ledgerStats,
  queueDepth,
  queues,
  reconcile,
  sendRaw,
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

/** Real SQS FIFO queues (LocalStack / SQS-compatible endpoint) + real PostgreSQL. */
let t: TestEnv;
let app: RunningApp;
let db: SQL;
let q: QueueUrls;
const dlqSeen: Message[] = [];

beforeAll(async () => {
  t = await testEnv('sqs', {
    FAULT_INJECTION: 'transient_game:flaky-game:2,transient_game:doomed-game:always',
    SQS_MAX_RECEIVE_COUNT: '3',
  });
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

async function txByExternal(externalId: string) {
  const rows = await db`select id, status, failure_code from wager_transactions where provider_id = 'provider-a' and external_transaction_id = ${externalId}`;
  return rows[0] as { id: string; status: string; failure_code: string | null } | undefined;
}

async function inboxCount(messageId: string): Promise<number> {
  const [{ n }] = await db`select count(*)::int as n from inbox_messages where message_id = ${messageId}`;
  return n;
}

/** Collects DLQ messages until one with the given messageId shows up. */
async function waitDlq(messageId: string, timeoutMs = 30_000): Promise<Message> {
  return waitFor(
    async () => {
      dlqSeen.push(...(await drain(q.wagerDlq, 300, 2_000)));
      return dlqSeen.find((m) => (m.Body ?? '').includes(messageId));
    },
    `DLQ message ${messageId}`,
    timeoutMs,
    200,
  );
}

describe('SQS consumer', () => {
  test('processes a message through the same use case and acks after commit', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const msg = wagerMessage({ walletId, playerId, kind: 'BET', amount: '30.00' });
    await sendWager(q.wager, msg);
    const tx = await waitFor(() => txByExternal(String(msg.data.externalTransactionId)), 'transaction');
    expect(tx.status).toBe('PROCESSED');
    expect(await inboxCount(msg.messageId)).toBe(1);
    expect((await ledgerStats(db, walletId)).balance).toBe('70.00');
    await waitFor(async () => (await queueDepth(q.wager)).visible + (await queueDepth(q.wager)).inFlight === 0, 'queue empty');
  });

  test('redelivery of the same messageId (5x) produces a single effect', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const msg = wagerMessage({ walletId, playerId, kind: 'BET', amount: '10.00' });
    // Different SQS dedup ids: the broker cannot help, only the inbox can.
    await Promise.all(Array.from({ length: 5 }, () => sendWager(q.wager, msg)));
    await waitFor(() => txByExternal(String(msg.data.externalTransactionId)), 'transaction');
    await waitFor(async () => {
      const d = await queueDepth(q.wager);
      return d.visible + d.inFlight === 0;
    }, 'all redeliveries consumed');
    expect(await inboxCount(msg.messageId)).toBe(1);
    const stats = await ledgerStats(db, walletId);
    expect(stats.debits).toBe(1);
    expect(stats.balance).toBe('90.00');
  });

  test('same operation via HTTP and via SQS (different messageId, same idempotency key) => one debit', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const http = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '15.00' });
    expect(http.status).toBe(201);
    const msg = wagerMessage({ walletId, playerId, kind: 'BET', amount: '15.00', externalTransactionId: http.externalTransactionId });
    await sendWager(q.wager, msg);
    await waitFor(async () => (await inboxCount(msg.messageId)) === 1, 'inbox row');
    expect((await ledgerStats(db, walletId)).debits).toBe(1);
    const [inbox] = await db`select transaction_id from inbox_messages where message_id = ${msg.messageId}`;
    expect(inbox.transaction_id).toBe(http.body.transactionId);
  });

  test('business rejection is terminal: persisted as REJECTED and acked (no retry, no DLQ)', async () => {
    const { walletId, playerId } = await createWallet(app.url, '5.00');
    const msg = wagerMessage({ walletId, playerId, kind: 'BET', amount: '50.00' });
    await sendWager(q.wager, msg);
    const tx = await waitFor(() => txByExternal(String(msg.data.externalTransactionId)), 'rejected transaction');
    expect(tx).toMatchObject({ status: 'REJECTED', failure_code: 'INSUFFICIENT_FUNDS' });
    expect(await inboxCount(msg.messageId)).toBe(1);
  });

  test('malformed JSON and contract violations go straight to the DLQ', async () => {
    const marker = uniqueName('malformed');
    await sendRaw(q.wager, `{not json ${marker}`, 'g-malformed');
    const badAmount = wagerMessage({ walletId: '01a1fa1b-0000-7000-8000-000000000001', playerId: 'p', kind: 'BET', amount: '1.00' });
    (badAmount.data.money as { amount: unknown }).amount = 12.5; // a JSON number is never money
    await sendWager(q.wager, badAmount);
    const m1 = await waitDlq(marker);
    expect(m1.MessageAttributes?.dlqReason?.StringValue).toBe('malformed_json');
    const m2 = await waitDlq(badAmount.messageId);
    expect(m2.MessageAttributes?.dlqReason?.StringValue).toBe('invalid_contract');
  });

  test('same messageId with a different payload => DLQ (inbox payload conflict), no second effect', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const msg = wagerMessage({ walletId, playerId, kind: 'BET', amount: '10.00' });
    await sendWager(q.wager, msg);
    await waitFor(() => txByExternal(String(msg.data.externalTransactionId)), 'first');
    const forged = { ...msg, data: { ...msg.data, money: { amount: '99.00', currency: 'BRL' } } };
    await sendWager(q.wager, forged);
    const dlq = await waitDlq(`"amount":"99.00"`);
    expect(dlq.MessageAttributes?.dlqReason?.StringValue).toBe('inbox_payload_conflict');
    expect((await ledgerStats(db, walletId)).debits).toBe(1);
  });

  test('idempotency key conflict over SQS => DLQ', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const first = await submit(app.url, { walletId, playerId, kind: 'BET', amount: '10.00' });
    const msg = wagerMessage({ walletId, playerId, kind: 'BET', amount: '11.00', externalTransactionId: first.externalTransactionId });
    await sendWager(q.wager, msg);
    const dlq = await waitDlq(msg.messageId);
    expect(dlq.MessageAttributes?.dlqReason?.StringValue).toBe('idempotency_key_conflict');
    expect((await ledgerStats(db, walletId)).debits).toBe(1);
  });

  test('transient failures are retried with backoff and then succeed (exactly one effect)', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const msg = wagerMessage({ walletId, playerId, kind: 'BET', amount: '20.00', gameId: 'flaky-game' });
    const started = Date.now();
    await sendWager(q.wager, msg);
    const tx = await waitFor(() => txByExternal(String(msg.data.externalTransactionId)), 'retried transaction', 40_000);
    expect(tx.status).toBe('PROCESSED');
    // two failures with 1s and 2s visibility backoff happened before the success
    expect(Date.now() - started).toBeGreaterThanOrEqual(2_500);
    expect((await ledgerStats(db, walletId)).debits).toBe(1);
    const metrics = await fetch(`${app.url}/metrics`).then((r) => r.text());
    expect(metrics).toMatch(/wagering_retries_total\{component="sqs_consumer"\} [1-9]/);
  });

  test('a message that always fails transiently ends in the DLQ after maxReceiveCount, with no effect', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const msg = wagerMessage({ walletId, playerId, kind: 'BET', amount: '20.00', gameId: 'doomed-game' });
    await sendWager(q.wager, msg);
    const dlq = await waitDlq(msg.messageId, 45_000);
    expect(dlq.MessageAttributes?.dlqReason?.StringValue).toBe('retries_exhausted');
    expect(Number(dlq.MessageAttributes?.receiveCount?.StringValue)).toBe(3);
    expect(await txByExternal(String(msg.data.externalTransactionId))).toBeUndefined();
    expect((await ledgerStats(db, walletId)).debits).toBe(0);
    const metrics = await fetch(`${app.url}/metrics`).then((r) => r.text());
    expect(metrics).toContain('wagering_dlq_messages_total{reason="retries_exhausted"}');
  });

  test('graceful shutdown: the consumer stops; messages sent while down are processed by the next instance', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    await app.close(); // SIGTERM path: stop polling, finish in-flight, stop publishers, close the pool
    const msg = wagerMessage({ walletId, playerId, kind: 'BET', amount: '1.00' });
    await sendWager(q.wager, msg);
    await Bun.sleep(1_500);
    expect(await txByExternal(String(msg.data.externalTransactionId))).toBeUndefined(); // nobody consumed it
    app = await startApp(t.env);
    const tx = await waitFor(() => txByExternal(String(msg.data.externalTransactionId)), 'processed after restart');
    expect(tx.status).toBe('PROCESSED');
    expect((await reconcile(app.url, walletId)).consistent).toBe(true);
  });
});
