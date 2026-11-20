import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import {
  createWallet,
  deleteQueues,
  dropDatabase,
  ledgerStats,
  pool,
  queueDepth,
  queues,
  sendWager,
  sql,
  testEnv,
  waitFor,
  wagerMessage,
  type TestEnv,
} from '../support/infra';
import { spawnInstance, type Instance } from '../support/processes';

/**
 * Real SIGTERM to a real process while it is consuming. On Windows a signal
 * cannot be delivered gracefully (TerminateProcess), so this runs on Linux
 * (CI / docker compose); the in-process close() path is covered in sqs-consumer.test.ts.
 */
const isWindows = process.platform === 'win32';
let t: TestEnv;
let db: SQL;
let q: QueueUrls;
const spawned: Instance[] = [];

beforeAll(async () => {
  if (isWindows) return;
  t = await testEnv('sigterm', { SQS_VISIBILITY_TIMEOUT_SECONDS: '5', SQS_BATCH_SIZE: '10' });
  q = await queues(t.env);
  db = sql(t.databaseUrl);
});

afterAll(async () => {
  if (isWindows) return;
  await Promise.all(spawned.map((i) => i.kill()));
  await db?.close();
  await deleteQueues(q);
  await dropDatabase(t.databaseUrl);
});

describe('graceful shutdown', () => {
  test.skipIf(isWindows)('SIGTERM while consuming: in-flight work finishes, nothing is lost or duplicated', async () => {
    const a = await spawnInstance(t.env, 'sigterm-a');
    spawned.push(a);
    const { walletId, playerId } = await createWallet(a.url, '1000.00');
    const messages = Array.from({ length: 40 }, () => wagerMessage({ walletId, playerId, kind: 'BET', amount: '1.00' }));
    await pool(messages.map((m) => () => sendWager(q.wager, m)), 8);

    const ids = messages.map((m) => m.messageId);
    const consumed = async () => (await db`select count(*)::int as n from inbox_messages where message_id = any(${ids})`)[0].n as number;
    await waitFor(async () => (await consumed()) >= 1, 'first message consumed', 20_000, 50);

    a.proc.kill('SIGTERM');
    const started = Date.now();
    await a.exitCode();
    expect(Date.now() - started).toBeLessThan(15_000);
    const logs = await a.logs();
    expect(logs).toContain('shutting down');
    expect(logs).toContain('sqs consumer stopped');

    const b = await spawnInstance(t.env, 'sigterm-b');
    spawned.push(b);
    await waitFor(async () => (await consumed()) === messages.length, 'all messages consumed by the next instance', 60_000, 200);
    await waitFor(async () => {
      const d = await queueDepth(q.wager);
      return d.visible + d.inFlight === 0;
    }, 'queue drained', 30_000, 200);
    const stats = await ledgerStats(db, walletId);
    expect(stats.debits).toBe(messages.length);
    expect(stats.balance).toBe('960.00');
  });
});
