import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import {
  createWallet,
  deleteQueues,
  dropDatabase,
  ledgerStats,
  pool,
  queues,
  reconcile,
  sendRaw,
  sql,
  submit,
  testEnv,
  uniqueName,
  waitFor,
  wagerMessage,
  type TestEnv,
} from '../support/infra';
import { spawnInstance, type Instance } from '../support/processes';

let t: TestEnv;
let db: SQL;
let q: QueueUrls;
const spawned: Instance[] = [];

beforeAll(async () => {
  t = await testEnv('advmp', { SQS_VISIBILITY_TIMEOUT_SECONDS: '4', PENDING_REFERENCE_LEASE_MS: '2000', PENDING_REFERENCE_MAX_ATTEMPTS: '30', DB_POOL_MAX: '15' });
  q = await queues(t.env);
  db = sql(t.databaseUrl);
});

afterAll(async () => {
  await Promise.all(spawned.map((i) => i.kill()));
  await db?.close();
  await deleteQueues(q);
  await dropDatabase(t.databaseUrl);
});

async function spawn(name: string, extra: Record<string, string> = {}) {
  const i = await spawnInstance(t.env, name, extra);
  spawned.push(i);
  return i;
}

describe('ADV — restarts and out-of-order references across processes', () => {
  test('ADV-14: SIGKILL while pending references are being resolved and BETs consumed; survivors finish exactly once', async () => {
    const a = await spawn('advmp-a');
    const w = await createWallet(a.url, '1000.00');
    const betIds = Array.from({ length: 12 }, () => uniqueName('late-bet'));
    const refunds = await Promise.all(
      betIds.map((id) => submit(a.url, { ...w, kind: 'REFUND', amount: '10.00', referenceExternalTransactionId: id })),
    );
    expect(refunds.every((r) => r.status === 202)).toBe(true);
    const bets = betIds.map((id) => wagerMessage({ ...w, kind: 'BET', amount: '10.00', externalTransactionId: id }));
    await pool(bets.map((m) => () => sendRaw(q.wager, JSON.stringify(m), w.walletId)), 6);
    await Bun.sleep(400);
    await a.kill(); // mid-flight: some BETs consumed, some refunds claimed by A's worker (leased)

    const b = await spawn('advmp-b');
    await spawn('advmp-c');
    await waitFor(async () => {
      const [{ n }] = await db`select count(*)::int as n from wager_transactions where wallet_id = ${w.walletId} and kind = 'REFUND' and status = 'PROCESSED'`;
      return n === 12;
    }, 'all refunds resolved by the survivors', 90_000, 300);
    const [{ betsProcessed }] = await db`select count(*)::int as "betsProcessed" from wager_transactions where wallet_id = ${w.walletId} and kind = 'BET' and status = 'PROCESSED'`;
    expect(betsProcessed).toBe(12);
    expect(await ledgerStats(db, w.walletId)).toMatchObject({ balance: '1000.00', debits: 12, credits: 13 });
    expect((await reconcile(b.url, w.walletId)).consistent).toBe(true);
  });

  test('ADV-15: ROLLBACKs delivered before their WINs, spread over message groups and 2 consumers => each applied once', async () => {
    const nodes = spawned.filter((i) => i.proc.exitCode === null && i.proc.signalCode === null && !i.proc.killed);
    expect(nodes.length).toBeGreaterThanOrEqual(2);
    const w = await createWallet(nodes[0]!.url, '100.00');
    const winIds = Array.from({ length: 10 }, () => uniqueName('win'));
    const rollbacks = winIds.map((id) => wagerMessage({ ...w, kind: 'ROLLBACK', amount: '5.00', referenceExternalTransactionId: id }));
    const wins = winIds.map((id) => wagerMessage({ ...w, kind: 'WIN', amount: '5.00', externalTransactionId: id }));
    // different groups => no FIFO ordering at all between them; every rollback is sent first
    await pool(rollbacks.map((m, i) => () => sendRaw(q.wager, JSON.stringify(m), `rb-${i}`)), 5);
    await Bun.sleep(500);
    await pool(wins.map((m, i) => () => sendRaw(q.wager, JSON.stringify(m), `win-${i}`)), 5);
    await waitFor(async () => {
      const [{ n }] = await db`select count(*)::int as n from wager_transactions where wallet_id = ${w.walletId} and kind = 'ROLLBACK' and status = 'PROCESSED'`;
      return n === 10;
    }, 'rollbacks resolved', 60_000, 300);
    expect(await ledgerStats(db, w.walletId)).toMatchObject({ balance: '100.00', credits: 11, debits: 10 });
  });
});
