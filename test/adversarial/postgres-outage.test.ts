import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import { createWallet, deleteQueues, dropDatabase, http, ledgerStats, queues, reconcile, sendWager, sql, testEnv, waitFor, wagerBody, wagerMessage, type TestEnv } from '../support/infra';
import { spawnInstance, type Instance } from '../support/processes';

/**
 * Real PostgreSQL outage: the server process is stopped while a real
 * application process is running, then started again.
 *
 * Destructive for every other connection, so it only runs when explicitly enabled:
 *   ADV_PG_CTL=<path to pg_ctl> ADV_PG_DATA=<data dir> bun test test/adversarial/postgres-outage.test.ts
 */
const pgCtl = process.env.ADV_PG_CTL;
const pgData = process.env.ADV_PG_DATA;
const enabled = Boolean(pgCtl && pgData);

let t: TestEnv;
let q: QueueUrls;
let node: Instance | undefined;

async function pg(action: 'stop' | 'start'): Promise<void> {
  const args = action === 'stop' ? ['-D', pgData!, '-m', 'fast', 'stop'] : ['-D', pgData!, '-o', '-p 5432 -c max_connections=300', '-l', `${pgData}/../pg.log`, '-w', 'start'];
  const proc = Bun.spawn([pgCtl!, ...args], { stdout: 'ignore', stderr: 'ignore' });
  // pg_ctl start keeps the handles of the postmaster; do not wait on it, poll instead.
  if (action === 'stop') await proc.exited;
  await waitFor(
    async () => {
      const probe = Bun.spawn([pgCtl!.replace(/pg_ctl(\.exe)?$/, 'pg_isready$1'), '-h', 'localhost', '-p', '5432'], { stdout: 'ignore', stderr: 'ignore' });
      const code = await probe.exited;
      return action === 'stop' ? code !== 0 : code === 0;
    },
    `postgres ${action}`,
    60_000,
    250,
  );
}

beforeAll(async () => {
  if (!enabled) return;
  t = await testEnv('pgdown', { DB_POOL_MAX: '10', SQS_RETRY_BASE_SECONDS: '2', SQS_RETRY_MAX_SECONDS: '4', SQS_MAX_RECEIVE_COUNT: '8' });
  q = await queues(t.env);
  node = await spawnInstance(t.env, 'pgdown-node');
});

afterAll(async () => {
  if (!enabled) return;
  await node?.kill();
  await deleteQueues(q);
  await dropDatabase(t.databaseUrl);
});

describe('ADV — PostgreSQL outage', () => {
  test.skipIf(!enabled)('ADV-13: DB down => 503 + readiness down + liveness up; after recovery the same request and queued message apply exactly once', async () => {
    const w = await createWallet(node!.url, '100.00');
    const body = wagerBody({ ...w, kind: 'BET', amount: '10.00' });
    const key = `provider-a:${body.externalTransactionId}`;
    const msg = wagerMessage({ ...w, kind: 'BET', amount: '7.00' });

    await pg('stop');
    try {
      const during = await http('POST', `${node!.url}/wagering/transactions`, body, { 'Idempotency-Key': key });
      const ready = await http('GET', `${node!.url}/health/ready`);
      const live = await http('GET', `${node!.url}/health/live`);
      const read = await http('GET', `${node!.url}/wallets/${w.walletId}`);
      await sendWager(q.wager, msg);
      await Bun.sleep(3_000); // the consumer meets the outage
      console.log(JSON.stringify({ during: [during.status, during.body?.error], ready: ready.status, live: live.status, read: [read.status, read.body?.error] }));
      expect(during.status).toBe(503);
      expect(during.body.error).toMatchObject({ code: 'TRANSIENT_INFRASTRUCTURE_ERROR', retryable: true });
      expect(ready.status).toBe(503);
      expect(live.status).toBe(200);
      expect(read.status).toBe(503);
    } finally {
      await pg('start');
    }

    // The pool must recover by itself (no process restart).
    const after = await waitFor(async () => {
      const r = await http('POST', `${node!.url}/wagering/transactions`, body, { 'Idempotency-Key': key });
      return r.status === 201 || r.status === 200 ? r : undefined;
    }, 'request accepted after recovery', 30_000, 500);
    expect([200, 201]).toContain(after.status);

    const db: SQL = sql(t.databaseUrl);
    try {
      await waitFor(async () => {
        const [{ n }] = await db`select count(*)::int as n from inbox_messages where message_id = ${msg.messageId}`;
        return n === 1;
      }, 'queued message applied after recovery', 60_000, 500);
      expect(await ledgerStats(db, w.walletId)).toMatchObject({ debits: 2, balance: '83.00' });
      const [{ n }] = await db`select count(*)::int as n from wager_transactions where idempotency_key = ${key}`;
      expect(n).toBe(1);
      expect((await reconcile(node!.url, w.walletId)).consistent).toBe(true);
      expect((await http('GET', `${node!.url}/health/ready`)).status).toBe(200);
    } finally {
      await db.close();
    }
  });
});
