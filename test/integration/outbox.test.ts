import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { SQL } from 'bun';
import { FaultInjector } from '../../src/infrastructure/messaging/fault-injection';
import { OutboxPublisher } from '../../src/infrastructure/messaging/outbox-publisher';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import { JsonLogger } from '../../src/infrastructure/observability/logger';
import { PromMetrics } from '../../src/infrastructure/observability/metrics';
import { createOrm } from '../../src/infrastructure/persistence/orm';
import {
  configFor,
  createWallet,
  deleteQueues,
  drain,
  dropDatabase,
  queues,
  sql,
  sqsClient,
  startApp,
  submit,
  testEnv,
  waitFor,
  type RunningApp,
  type TestEnv,
} from '../support/infra';

let t: TestEnv;
let app: RunningApp;
let db: SQL;
let q: QueueUrls;
const orms: MikroORM[] = [];

const logger = new JsonLogger({ service: 'test', instanceId: 'test' }, 'error');

async function publisher(name: string, queueUrl: string, overrides: Record<string, string> = {}, faults = new FaultInjector(undefined)) {
  const orm = await createOrm(t.databaseUrl, 5);
  orms.push(orm);
  const config = configFor(t.env, overrides);
  return new OutboxPublisher(orm, sqsClient(), queueUrl, config.outbox, faults, new PromMetrics(false), logger, name);
}

async function pending(): Promise<number> {
  const [{ n }] = await db`select count(*)::int as n from outbox_messages where published_at is null`;
  return n;
}

beforeAll(async () => {
  // The app's own publisher is disabled: these tests drive publishers explicitly.
  t = await testEnv('outbox', { SQS_CONSUMER_ENABLED: 'false', OUTBOX_PUBLISHER_ENABLED: 'false', OUTBOX_BATCH_SIZE: '10', OUTBOX_LEASE_MS: '1500' });
  q = await queues(t.env);
  app = await startApp(t.env);
  db = sql(t.databaseUrl);
});

afterAll(async () => {
  await app?.close();
  for (const orm of orms) await orm.close(true);
  await db?.close();
  await deleteQueues(q);
  await dropDatabase(t.databaseUrl);
});

describe('transactional outbox', () => {
  test('two concurrent publishers over the same outbox: every event published once, none lost', async () => {
    const wallets = await Promise.all(Array.from({ length: 15 }, () => createWallet(app.url, '100.00')));
    await Promise.all(wallets.flatMap((w) => [1, 2, 3].map(() => submit(app.url, { ...w, kind: 'BET', amount: '10.00' }))));
    const total = await pending();
    expect(total).toBe(15 * 2 + 45 * 2); // opening (2 events) + 3 bets x 2 events

    const p1 = await publisher('pub-1', q.events);
    const p2 = await publisher('pub-2', q.events);
    const published = { 'pub-1': 0, 'pub-2': 0 };
    const loop = async (p: OutboxPublisher, name: 'pub-1' | 'pub-2') => {
      for (let i = 0; i < 200 && (await pending()) > 0; i++) published[name] += await p.publishOnce();
    };
    await Promise.all([loop(p1, 'pub-1'), loop(p2, 'pub-2')]);

    expect(await pending()).toBe(0);
    expect(published['pub-1'] + published['pub-2']).toBe(total); // no row published twice by the two publishers
    expect(published['pub-1']).toBeGreaterThan(0);
    expect(published['pub-2']).toBeGreaterThan(0);

    const messages = await drain(q.events);
    const ids = messages.map((m) => m.MessageAttributes?.eventId?.StringValue);
    expect(new Set(ids).size).toBe(total);
    const [{ n: rows }] = await db`select count(*)::int as n from outbox_messages`;
    expect(rows).toBe(total);
    // envelope shape
    const sample = JSON.parse(messages[0]!.Body!);
    expect(sample).toMatchObject({ eventId: expect.any(String), eventType: expect.any(String), aggregateId: expect.any(String), version: 1 });
  });

  test('nothing is published before COMMIT: an uncommitted outbox row is invisible to the publisher', async () => {
    const p = await publisher('pub-uncommitted', q.events);
    const holder = sql(t.databaseUrl);
    const eventId = Bun.randomUUIDv7();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const tx = holder.begin(async (trx) => {
      await trx`insert into outbox_messages (id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at)
                values (${eventId}, 'agg', 'Test', ${{ eventId }}, now(), 0, now())`;
      await gate; // transaction still open
    });
    await Bun.sleep(200);
    expect(await p.publishOnce()).toBe(0);
    expect((await drain(q.events, 500, 2_000)).length).toBe(0);
    release();
    await tx;
    expect(await p.publishOnce()).toBe(1);
    const received = await drain(q.events);
    expect(received.map((m) => m.MessageAttributes?.eventId?.StringValue)).toEqual([eventId]);
    await holder.close();
  });

  test('publication failure is retried with exponential backoff and eventually published', async () => {
    const { walletId, playerId } = await createWallet(app.url, '10.00');
    await submit(app.url, { walletId, playerId, kind: 'BET', amount: '1.00' });
    const broken = await publisher('pub-broken', `${q.events.replace(/[^/]+$/, '')}does-not-exist.fifo`);
    await broken.publishOnce();
    const failed = await db`select attempts, last_error, next_attempt_at > now() as delayed from outbox_messages where published_at is null order by seq`;
    expect(failed.length).toBe(4);
    // the head failed and got a retry; the 3 events behind it were held back (not sent), all delayed
    expect(failed[0]).toMatchObject({ attempts: 1, delayed: true });
    expect(failed[0].last_error).toBeTruthy();
    for (const row of failed.slice(1)) expect(row).toMatchObject({ attempts: 0, last_error: null, delayed: true });
    await broken.publishOnce(); // second failure only once due again
    const good = await publisher('pub-good', q.events);
    await waitFor(async () => {
      await good.publishOnce();
      return (await pending()) === 0;
    }, 'outbox drained after backoff', 15_000, 200);
    const [{ max }] = await db`select max(attempts)::int as max from outbox_messages where aggregate_id = ${walletId}`;
    expect(max).toBeGreaterThanOrEqual(1);
    expect((await drain(q.events)).length).toBe(4);
  });

  test('crash after claim: the lease expires and another publisher delivers; duplicates keep the same eventId', async () => {
    const { walletId, playerId } = await createWallet(app.url, '10.00');
    await submit(app.url, { walletId, playerId, kind: 'BET', amount: '1.00' });
    // Publisher A claims (leases) the rows and "dies" before publishing.
    const claimed = await db`
      with due as (select id from outbox_messages where published_at is null and next_attempt_at <= now() for update skip locked)
      update outbox_messages o set next_attempt_at = now() + interval '1500 milliseconds' from due where o.id = due.id returning o.id`;
    expect(claimed.length).toBe(4);
    const b = await publisher('pub-b', q.events);
    expect(await b.publishOnce()).toBe(0); // leased: not visible yet
    await Bun.sleep(1_700);
    expect(await b.publishOnce()).toBe(4);
    expect(await pending()).toBe(0);

    // A duplicate publication (e.g. crash after SendMessage, before marking) re-sends the same eventId:
    const [row] = await db`select id, aggregate_id, payload from outbox_messages where aggregate_id = ${walletId} limit 1`;
    const { SendMessageCommand } = await import('@aws-sdk/client-sqs');
    await sqsClient().send(
      new SendMessageCommand({
        QueueUrl: q.events,
        MessageBody: JSON.stringify(row.payload),
        MessageGroupId: row.aggregate_id,
        MessageDeduplicationId: row.id,
        MessageAttributes: { eventId: { DataType: 'String', StringValue: row.id } },
      }),
    );
    const received = await drain(q.events);
    const ids = received.map((m) => m.MessageAttributes?.eventId?.StringValue);
    expect(new Set(ids).size).toBe(4); // consumers de-duplicate by eventId (the broker may also drop the copy)
    for (const id of new Set(ids)) {
      const bodies = new Set(received.filter((m) => m.MessageAttributes?.eventId?.StringValue === id).map((m) => m.Body));
      expect(bodies.size).toBe(1); // a duplicate is byte-identical => safe to discard
    }
  });

  test('strict per-wallet order with THREE concurrent publishers while the first event of a wallet fails and is retried', async () => {
    await drain(q.events, 500, 3_000);
    expect(await pending()).toBe(0);
    // 6 wallets x (opening: 2 events + 4 BETs x 2 events) = 60 events, 10 per wallet
    const wallets = await Promise.all(Array.from({ length: 6 }, () => createWallet(app.url, '100.00')));
    for (let i = 0; i < 4; i++) await Promise.all(wallets.map((w) => submit(app.url, { ...w, kind: 'BET', amount: '1.00' })));
    expect(await pending()).toBe(60);
    const victim = wallets[0]!.walletId;
    const [victimHead] = await db`select id from outbox_messages where aggregate_id = ${victim} order by seq limit 1`;

    // One shared injector: the victim's first publication is rejected twice, whichever publisher tries.
    const faults = new FaultInjector(`fail_publish:${victim}:2`);
    // Tiny claims so every wallet's sequence is split across many cycles and publishers.
    const small = { OUTBOX_BATCH_SIZE: '2', OUTBOX_MAX_EVENTS_PER_AGGREGATE: '3', OUTBOX_RETRY_BASE_MS: '150', OUTBOX_RETRY_MAX_MS: '300' };
    const pubs = await Promise.all(['p1', 'p2', 'p3'].map((n) => publisher(n, q.events, small, faults)));
    const counts = [0, 0, 0];
    await Promise.all(
      pubs.map(async (p, i) => {
        const started = Date.now();
        while (Date.now() - started < 20_000 && (await pending()) > 0) {
          const n = await p.publishOnce();
          counts[i]! += n;
          if (n === 0) await Bun.sleep(30);
        }
      }),
    );
    expect(await pending()).toBe(0);
    expect(counts.reduce((a, b) => a + b, 0)).toBe(60);
    expect(counts.filter((c) => c > 0).length).toBeGreaterThanOrEqual(2); // work really was shared

    const [head] = await db`select attempts from outbox_messages where id = ${victimHead.id}`;
    expect(head.attempts).toBe(2); // failed twice, then published

    // Order as seen by a consumer (first occurrence of each eventId) == commit order (seq), per wallet.
    const received = await drain(q.events);
    const seen = new Map<string, string[]>();
    for (const m of received) {
      const body = JSON.parse(m.Body!) as { eventId: string; aggregateId: string };
      const list = seen.get(body.aggregateId) ?? [];
      if (!list.includes(body.eventId)) list.push(body.eventId);
      seen.set(body.aggregateId, list);
    }
    for (const w of wallets) {
      const expected = (await db`select id from outbox_messages where aggregate_id = ${w.walletId} order by seq`).map((r: { id: string }) => r.id);
      expect(seen.get(w.walletId)).toEqual(expected);
      // and walletVersion of the balance events is strictly increasing in delivery order
      const versions = received
        .map((m) => JSON.parse(m.Body!) as { aggregateId: string; eventType: string; data: { walletVersion?: number } })
        .filter((e) => e.aggregateId === w.walletId && e.eventType === 'WalletBalanceChanged')
        .map((e) => e.data.walletVersion!);
      expect(versions).toEqual([1, 2, 3, 4, 5]);
    }
  });
});
