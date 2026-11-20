import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import {
  assertGlobalInvariant,
  createWallet,
  deleteQueues,
  drain,
  dropDatabase,
  http,
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
 * Process crashes are real: the application process exits (fault injection
 * calls process.exit) or is SIGKILLed. Recovery is done by other processes.
 */
let t: TestEnv;
let db: SQL;
let q: QueueUrls;
const spawned: Instance[] = [];

async function spawn(name: string, extra: Record<string, string> = {}): Promise<Instance> {
  const instance = await spawnInstance(t.env, name, extra);
  spawned.push(instance);
  return instance;
}

beforeAll(async () => {
  t = await testEnv('crash', { SQS_VISIBILITY_TIMEOUT_SECONDS: '3', OUTBOX_LEASE_MS: '2000', DB_POOL_MAX: '20' });
  q = await queues(t.env);
  db = sql(t.databaseUrl);
});

afterAll(async () => {
  await Promise.all(spawned.map((i) => i.kill()));
  await db?.close();
  await deleteQueues(q);
  await dropDatabase(t.databaseUrl);
});

describe('crash recovery', () => {
  test('worker killed AFTER commit and BEFORE ack: redelivery is absorbed by the inbox (single debit)', async () => {
    const api = await spawn('api', { SQS_CONSUMER_ENABLED: 'false' });
    const { walletId, playerId } = await createWallet(api.url, '100.00');
    const victim = await spawn('victim', { FAULT_INJECTION: 'crash_after_commit_before_ack', OUTBOX_PUBLISHER_ENABLED: 'false' });

    const msg = wagerMessage({ walletId, playerId, kind: 'BET', amount: '30.00' });
    await sendWager(q.wager, msg);
    expect(await victim.exitCode()).toBe(137); // died right after COMMIT

    // committed, but never acked
    const [tx] = await db`select status from wager_transactions where external_transaction_id = ${String(msg.data.externalTransactionId)}`;
    expect(tx.status).toBe('PROCESSED');
    const depth = await queueDepth(q.wager);
    expect(depth.visible + depth.inFlight).toBe(1);

    // another instance takes over after the visibility timeout
    const rescuer = await spawn('rescuer');
    await waitFor(async () => {
      const d = await queueDepth(q.wager);
      return d.visible + d.inFlight === 0;
    }, 'redelivered message acked', 30_000, 250);
    const stats = await ledgerStats(db, walletId);
    expect(stats).toMatchObject({ debits: 1, balance: '70.00' });
    const [{ n }] = await db`select count(*)::int as n from inbox_messages where message_id = ${msg.messageId}`;
    expect(n).toBe(1);
    expect(await metric(rescuer, /^wagering_duplicates_total\{kind="inbox_duplicate"/)).toBeGreaterThanOrEqual(1);
    await rescuer.kill();
    await api.kill();
  });

  test('publisher killed after claiming: committed events are published by another instance after the lease', async () => {
    await drain(q.events, 500, 3_000);
    const victim = await spawn('pub-victim', { FAULT_INJECTION: 'crash_after_outbox_claim', SQS_CONSUMER_ENABLED: 'false', OUTBOX_POLL_INTERVAL_MS: '1000' });
    const playerId = uniqueName('p');
    // The HTTP response may or may not arrive before the publisher claims and crashes: the commit is what matters.
    await http('POST', `${victim.url}/wallets`, { playerId, initialBalance: { amount: '50.00', currency: 'BRL' } }).catch(() => undefined);
    expect(await victim.exitCode()).toBe(137);
    const [w] = await db`select id from wallets where player_id = ${playerId}`;
    expect(w).toBeDefined();
    const before = await db`select id, published_at from outbox_messages where aggregate_id = ${w.id}`;
    expect(before.length).toBe(2);
    expect(before.every((r: { published_at: Date | null }) => r.published_at === null)).toBe(true);

    const heir = await spawn('pub-heir', { SQS_CONSUMER_ENABLED: 'false' });
    await waitFor(async () => {
      const [{ n }] = await db`select count(*)::int as n from outbox_messages where aggregate_id = ${w.id} and published_at is not null`;
      return n === 2;
    }, 'events published by the heir', 20_000, 200);
    const received = await drain(q.events);
    const ids = new Set(received.filter((m) => (m.Body ?? '').includes(w.id)).map((m) => m.MessageAttributes?.eventId?.StringValue));
    expect(ids).toEqual(new Set(before.map((r: { id: string }) => r.id)));
    await heir.kill();
  });

  test('publisher killed after publishing but before marking: re-published with the same eventId (safe duplicate)', async () => {
    await drain(q.events, 500, 3_000);
    const victim = await spawn('dup-victim', { FAULT_INJECTION: 'crash_after_publish_before_mark', SQS_CONSUMER_ENABLED: 'false', OUTBOX_POLL_INTERVAL_MS: '1000' });
    const playerId = uniqueName('p');
    await http('POST', `${victim.url}/wallets`, { playerId, initialBalance: { amount: '50.00', currency: 'BRL' } }).catch(() => undefined);
    expect(await victim.exitCode()).toBe(137);
    const [w] = await db`select id from wallets where player_id = ${playerId}`;
    const heir = await spawn('dup-heir', { SQS_CONSUMER_ENABLED: 'false' });
    await waitFor(async () => {
      const [{ n }] = await db`select count(*)::int as n from outbox_messages where aggregate_id = ${w.id} and published_at is not null`;
      return n === 2;
    }, 'republished', 20_000, 200);
    const received = (await drain(q.events)).filter((m) => (m.Body ?? '').includes(w.id));
    const ids = received.map((m) => m.MessageAttributes?.eventId?.StringValue);
    expect(new Set(ids).size).toBe(2); // two distinct events, possibly delivered more than once
    expect(received.length).toBeGreaterThanOrEqual(2);
    await heir.kill();
  });

  test('restart of every instance mid-load: final state is consistent and nothing is duplicated or lost', async () => {
    const nodes = await Promise.all(['r-1', 'r-2', 'r-3'].map((n) => spawn(n)));
    const wallets = await Promise.all(Array.from({ length: 6 }, (_, i) => createWallet(nodes[i % 3]!.url, '500.00')));
    const messages = wallets.flatMap((w) =>
      Array.from({ length: 15 }, (_, j) =>
        wagerMessage({ ...w, kind: j % 5 === 0 ? 'WIN' : 'BET', amount: j % 5 === 0 ? '4.00' : '6.00' }),
      ),
    );
    const sending = pool(messages.map((m) => () => sendWager(q.wager, m)), 12);
    const httpLoad = Promise.all(
      wallets.flatMap((w, i) => Array.from({ length: 6 }, (_, j) => submit(nodes[(i + j) % 3]!.url, { ...w, kind: 'BET', amount: '3.00' }).catch(() => null))),
    );
    await Bun.sleep(700);
    await Promise.all(nodes.map((n) => n.kill())); // SIGKILL everything mid-flight
    await sending;
    const httpResults = await httpLoad;

    const restarted = await Promise.all(['r-1b', 'r-2b', 'r-3b'].map((n) => spawn(n)));
    await waitFor(async () => {
      const [{ n }] = await db`select count(*)::int as n from inbox_messages where message_id = any(${messages.map((m) => m.messageId)})`;
      return n === messages.length;
    }, 'every message processed after restart', 120_000, 300);
    await waitFor(async () => {
      const [{ n }] = await db`select count(*)::int as n from outbox_messages where published_at is null`;
      return n === 0;
    }, 'outbox drained after restart', 60_000, 300);

    // exactly one transaction per message
    const [{ n: txs }] = await db`select count(*)::int as n from wager_transactions where external_transaction_id = any(${messages.map((m) => String(m.data.externalTransactionId))})`;
    expect(txs).toBe(messages.length);
    // HTTP requests that got an answer are durable
    for (const r of httpResults) {
      if (r && r.status === 201) {
        const [row] = await db`select status from wager_transactions where id = ${r.body.transactionId}`;
        expect(row.status).toBe('PROCESSED');
      }
    }
    for (const w of wallets) expect((await reconcile(restarted[0]!.url, w.walletId)).consistent).toBe(true);
    expect((await assertGlobalInvariant(db)).inconsistent).toBe(0);
  });
});
