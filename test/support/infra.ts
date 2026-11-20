import { DeleteQueueCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SQSClient, SendMessageCommand, type Message } from '@aws-sdk/client-sqs';
import type { INestApplication } from '@nestjs/common';
import { SQL } from 'bun';
import { createApp } from '../../src/bootstrap';
import { WorkersLifecycle } from '../../src/app.module';
import { loadConfig, type AppConfig } from '../../src/infrastructure/config';
import { createSqsClient, ensureQueues, type QueueUrls } from '../../src/infrastructure/messaging/sqs';
import { runMigrations } from '../../src/infrastructure/persistence/migrate';

/**
 * Real infrastructure only: PostgreSQL and an SQS endpoint (LocalStack in
 * docker compose / CI; any SQS-compatible endpoint locally). No mocks.
 */
export const PG_BASE_URL = process.env.TEST_PG_URL ?? 'postgresql://postgres:postgres@localhost:5432';
export const SQS_ENDPOINT = process.env.SQS_ENDPOINT ?? 'http://localhost:4566';

const adminUrl = () => `${PG_BASE_URL}/postgres`;

export function uniqueName(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`.toLowerCase();
}

/** Creates a brand new database and runs all migrations on it. */
export async function freshDatabase(prefix: string): Promise<string> {
  const name = uniqueName(`wt_${prefix}`).replace(/[^a-z0-9_]/g, '_');
  const admin = new SQL(adminUrl());
  try {
    await admin.unsafe(`create database ${name}`);
  } finally {
    await admin.close();
  }
  const url = `${PG_BASE_URL}/${name}`;
  await runMigrations(url, 'up');
  return url;
}

export async function dropDatabase(url: string): Promise<void> {
  const name = url.slice(url.lastIndexOf('/') + 1);
  const admin = new SQL(adminUrl());
  try {
    await admin.unsafe(`drop database if exists ${name} with (force)`);
  } catch {
    /* best effort */
  } finally {
    await admin.close();
  }
}

export function sql(url: string): SQL {
  return new SQL(url, { max: 5 });
}

export interface TestEnv {
  databaseUrl: string;
  queuePrefix: string;
  env: Record<string, string>;
}

/** Environment for one test file: own database, own queues, fast timers. */
export async function testEnv(prefix: string, extra: Record<string, string> = {}): Promise<TestEnv> {
  const databaseUrl = await freshDatabase(prefix);
  const queuePrefix = uniqueName(prefix).replace(/[^a-z0-9_-]/g, '-').slice(0, 30);
  const env: Record<string, string> = {
    DATABASE_URL: databaseUrl,
    SQS_ENDPOINT,
    AWS_REGION: 'us-east-1',
    AWS_ACCESS_KEY_ID: 'test',
    AWS_SECRET_ACCESS_KEY: 'test',
    SQS_WAGER_QUEUE: `${queuePrefix}-wager.fifo`,
    SQS_WAGER_DLQ: `${queuePrefix}-wager-dlq.fifo`,
    SQS_EVENTS_QUEUE: `${queuePrefix}-events.fifo`,
    SQS_WAIT_TIME_SECONDS: '1',
    SQS_VISIBILITY_TIMEOUT_SECONDS: '5',
    SQS_MAX_RECEIVE_COUNT: '3',
    SQS_RETRY_BASE_SECONDS: '1',
    SQS_RETRY_MAX_SECONDS: '2',
    SQS_SHUTDOWN_GRACE_MS: '5000',
    OUTBOX_POLL_INTERVAL_MS: '100',
    OUTBOX_LEASE_MS: '3000',
    OUTBOX_RETRY_BASE_MS: '200',
    OUTBOX_RETRY_MAX_MS: '1000',
    PENDING_REFERENCE_POLL_INTERVAL_MS: '100',
    PENDING_REFERENCE_BASE_DELAY_MS: '100',
    PENDING_REFERENCE_MAX_DELAY_MS: '300',
    PENDING_REFERENCE_MAX_ATTEMPTS: '5',
    PENDING_REFERENCE_LEASE_MS: '2000',
    DB_POOL_MAX: '30',
    LOG_LEVEL: process.env.TEST_LOG_LEVEL ?? 'error',
    ...extra,
  };
  return { databaseUrl, queuePrefix, env };
}

export function configFor(env: Record<string, string>, overrides: Record<string, string> = {}): AppConfig {
  return loadConfig({ ...env, ...overrides });
}

export interface RunningApp {
  app: INestApplication;
  url: string;
  config: AppConfig;
  workers: WorkersLifecycle;
  close(): Promise<void>;
}

/** Real Nest application in this process, listening on a random port. */
export async function startApp(env: Record<string, string>, overrides: Record<string, string> = {}): Promise<RunningApp> {
  const config = configFor(env, { PORT: '0', INSTANCE_ID: uniqueName('inproc'), ...overrides });
  const app = await createApp(config);
  await app.listen(0, '127.0.0.1');
  const address = app.getHttpServer().address() as { port: number };
  let closed = false;
  return {
    app,
    url: `http://127.0.0.1:${address.port}`,
    config,
    workers: app.get(WorkersLifecycle),
    close: async () => {
      if (closed) return;
      closed = true;
      await app.close();
    },
  };
}

// ------------------------------------------------------------------ HTTP

export interface HttpResult<T = any> {
  status: number;
  body: T;
  headers: Headers;
}

export async function http<T = any>(method: string, url: string, body?: unknown, headers: Record<string, string> = {}): Promise<HttpResult<T>> {
  const res = await fetch(url, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
  });
  const text = await res.text();
  let parsed: unknown = text;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    /* plain text */
  }
  return { status: res.status, body: parsed as T, headers: res.headers };
}

export async function createWallet(baseUrl: string, amount = '100.00', currency = 'BRL', playerId = uniqueName('player')) {
  const res = await http('POST', `${baseUrl}/wallets`, { playerId, initialBalance: { amount, currency } });
  if (res.status !== 201) throw new Error(`wallet creation failed: ${res.status} ${JSON.stringify(res.body)}`);
  return { walletId: res.body.id as string, playerId };
}

export interface WagerInput {
  walletId: string;
  playerId: string;
  kind: 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
  amount: string;
  currency?: string;
  externalTransactionId?: string;
  referenceExternalTransactionId?: string;
  providerId?: string;
  roundId?: string;
  gameId?: string;
}

export function wagerBody(input: WagerInput) {
  const body: Record<string, unknown> = {
    providerId: input.providerId ?? 'provider-a',
    externalTransactionId: input.externalTransactionId ?? uniqueName('ext'),
    playerId: input.playerId,
    walletId: input.walletId,
    roundId: input.roundId ?? 'round-1',
    gameId: input.gameId ?? 'fortune-chimp',
    kind: input.kind,
    money: { amount: input.amount, currency: input.currency ?? 'BRL' },
  };
  if (input.referenceExternalTransactionId) body.referenceExternalTransactionId = input.referenceExternalTransactionId;
  return body;
}

export async function submit(baseUrl: string, input: WagerInput, key?: string) {
  const body = wagerBody(input);
  const idempotencyKey = key ?? `${body.providerId}:${body.externalTransactionId}`;
  const res = await http('POST', `${baseUrl}/wagering/transactions`, body, { 'Idempotency-Key': idempotencyKey });
  return { ...res, externalTransactionId: body.externalTransactionId as string, idempotencyKey };
}

export async function reconcile(baseUrl: string, walletId: string) {
  const res = await http('POST', `${baseUrl}/wallets/${walletId}/reconciliation`);
  if (res.status !== 200) throw new Error(`reconciliation failed ${res.status}`);
  return res.body as { consistent: boolean; storedBalance: { amount: string }; calculatedBalance: { amount: string }; checkedEntries: number };
}

// ------------------------------------------------------------------ SQS

export function sqsClient(): SQSClient {
  return createSqsClient(loadConfig({ SQS_ENDPOINT }).sqs);
}

export async function queues(env: Record<string, string>): Promise<QueueUrls> {
  return ensureQueues(sqsClient(), configFor(env).sqs);
}

export function wagerMessage(input: WagerInput & { messageId?: string; idempotencyKey?: string }) {
  const data = wagerBody(input) as Record<string, unknown>;
  data.idempotencyKey = input.idempotencyKey ?? `${data.providerId}:${data.externalTransactionId}`;
  return {
    messageId: input.messageId ?? uniqueName('msg'),
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data,
  };
}

/** Sends with a random dedup id, so broker de-duplication never hides a redelivery from our inbox. */
export async function sendRaw(queueUrl: string, body: string, groupId: string): Promise<void> {
  await sqsClient().send(
    new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: body, MessageGroupId: groupId, MessageDeduplicationId: crypto.randomUUID() }),
  );
}

export async function sendWager(queueUrl: string, message: ReturnType<typeof wagerMessage>): Promise<void> {
  await sendRaw(queueUrl, JSON.stringify(message), String(message.data.walletId));
}

export async function queueDepth(queueUrl: string): Promise<{ visible: number; inFlight: number }> {
  const res = await sqsClient().send(
    new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'] }),
  );
  return {
    visible: Number.parseInt(res.Attributes?.ApproximateNumberOfMessages ?? '0', 10),
    inFlight: Number.parseInt(res.Attributes?.ApproximateNumberOfMessagesNotVisible ?? '0', 10),
  };
}

/** Drains a queue (deleting what it reads) until it stays empty for `quietMs`. */
export async function drain(queueUrl: string, quietMs = 1500, maxMs = 30_000): Promise<Message[]> {
  const client = sqsClient();
  const all: Message[] = [];
  const started = Date.now();
  let lastSeen = Date.now();
  while (Date.now() - lastSeen < quietMs && Date.now() - started < maxMs) {
    const res = await client.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: ['All'], VisibilityTimeout: 30 }),
    );
    const msgs = res.Messages ?? [];
    if (msgs.length) {
      lastSeen = Date.now();
      all.push(...msgs);
      const { DeleteMessageBatchCommand } = await import('@aws-sdk/client-sqs');
      await client.send(
        new DeleteMessageBatchCommand({ QueueUrl: queueUrl, Entries: msgs.map((m, i) => ({ Id: String(i), ReceiptHandle: m.ReceiptHandle! })) }),
      );
    }
  }
  return all;
}

export async function deleteQueues(urls: QueueUrls): Promise<void> {
  const client = sqsClient();
  for (const url of [urls.wager, urls.wagerDlq, urls.events]) {
    await client.send(new DeleteQueueCommand({ QueueUrl: url })).catch(() => undefined);
  }
}

// ------------------------------------------------------------------ waiting

export async function waitFor<T>(fn: () => Promise<T | undefined | null | false>, what: string, timeoutMs = 20_000, everyMs = 100): Promise<T> {
  const started = Date.now();
  let last: unknown;
  while (Date.now() - started < timeoutMs) {
    try {
      const value = await fn();
      if (value) return value as T;
    } catch (error) {
      last = error;
    }
    await Bun.sleep(everyMs);
  }
  throw new Error(`timed out waiting for ${what}${last ? `: ${(last as Error).message}` : ''}`);
}

// ------------------------------------------------------------------ DB assertions

export async function ledgerStats(db: SQL, walletId: string) {
  const [row] = await db`
    select count(*)::int as entries,
           count(*) filter (where direction = 'DEBIT')::int as debits,
           count(*) filter (where direction = 'CREDIT')::int as credits,
           (select balance_amount::text from wallets where id = ${walletId}) as balance,
           (select min(balance_after)::text from wallet_ledger_entries where wallet_id = ${walletId}) as min_balance_after
      from wallet_ledger_entries where wallet_id = ${walletId}`;
  return row as { entries: number; debits: number; credits: number; balance: string; min_balance_after: string | null };
}

/** Global invariant for all tests: wallet.balance == balance rebuilt from the ledger, for every wallet. */
export async function assertGlobalInvariant(db: SQL): Promise<{ wallets: number; inconsistent: number }> {
  const [row] = await db`
    select count(*)::int as wallets,
           count(*) filter (where w.balance_amount <> coalesce(l.total, 0))::int as inconsistent
      from wallets w
      left join (select wallet_id, sum(case when direction = 'CREDIT' then amount else -amount end) as total
                   from wallet_ledger_entries group by wallet_id) l on l.wallet_id = w.id`;
  return row as { wallets: number; inconsistent: number };
}
