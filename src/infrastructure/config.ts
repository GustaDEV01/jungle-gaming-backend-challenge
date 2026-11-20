import { hostname } from 'node:os';

export const APP_CONFIG = Symbol('APP_CONFIG');

export interface AppConfig {
  port: number;
  instanceId: string;
  logLevel: 'debug' | 'info' | 'warn' | 'error';
  database: {
    url: string;
    poolMax: number;
    lockTimeoutMs: number;
    statementTimeoutMs: number;
    maxTxRetries: number;
  };
  sqs: {
    endpoint: string | undefined;
    region: string;
    accessKeyId: string;
    secretAccessKey: string;
    wagerQueueName: string;
    wagerDlqName: string;
    eventsQueueName: string;
    consumerEnabled: boolean;
    consumerName: string;
    waitTimeSeconds: number;
    visibilityTimeoutSeconds: number;
    maxReceiveCount: number;
    batchSize: number;
    retryBaseSeconds: number;
    retryMaxSeconds: number;
    shutdownGraceMs: number;
  };
  outbox: {
    enabled: boolean;
    pollIntervalMs: number;
    batchSize: number;
    /** Parallel publishing lanes (events of one aggregate always share a lane). */
    publishConcurrency: number;
    leaseMs: number;
    baseDelayMs: number;
    maxDelayMs: number;
  };
  pendingReference: {
    enabled: boolean;
    pollIntervalMs: number;
    batchSize: number;
    leaseMs: number;
    baseDelayMs: number;
    maxDelayMs: number;
    maxAttempts: number;
    ttlMs: number;
  };
  /** Test-only fault injection, e.g. "crash_after_commit_before_ack". Never set in production. */
  faultInjection: string | undefined;
}

type Env = Record<string, string | undefined>;

function int(env: Env, key: string, fallback: number): number {
  const raw = env[key];
  if (raw === undefined || raw === '') return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isFinite(value)) throw new Error(`Invalid integer for ${key}: ${raw}`);
  return value;
}

function bool(env: Env, key: string, fallback: boolean): boolean {
  const raw = env[key];
  if (raw === undefined || raw === '') return fallback;
  return raw === 'true' || raw === '1';
}

export function loadConfig(env: Env = process.env): AppConfig {
  return {
    port: int(env, 'PORT', 3000),
    instanceId: env.INSTANCE_ID ?? `${hostname()}-${process.pid}`,
    logLevel: (env.LOG_LEVEL as AppConfig['logLevel']) ?? 'info',
    database: {
      url: env.DATABASE_URL ?? 'postgresql://postgres:postgres@localhost:5432/wagering',
      poolMax: int(env, 'DB_POOL_MAX', 20),
      lockTimeoutMs: int(env, 'DB_LOCK_TIMEOUT_MS', 5_000),
      statementTimeoutMs: int(env, 'DB_STATEMENT_TIMEOUT_MS', 15_000),
      maxTxRetries: int(env, 'DB_MAX_TX_RETRIES', 3),
    },
    sqs: {
      endpoint: env.SQS_ENDPOINT || undefined,
      region: env.AWS_REGION ?? 'us-east-1',
      accessKeyId: env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: env.AWS_SECRET_ACCESS_KEY ?? 'test',
      wagerQueueName: env.SQS_WAGER_QUEUE ?? 'wager-transactions.fifo',
      wagerDlqName: env.SQS_WAGER_DLQ ?? 'wager-transactions-dlq.fifo',
      eventsQueueName: env.SQS_EVENTS_QUEUE ?? 'wagering-events.fifo',
      consumerEnabled: bool(env, 'SQS_CONSUMER_ENABLED', true),
      consumerName: env.SQS_CONSUMER_NAME ?? 'wager-transactions-consumer',
      waitTimeSeconds: int(env, 'SQS_WAIT_TIME_SECONDS', 10),
      visibilityTimeoutSeconds: int(env, 'SQS_VISIBILITY_TIMEOUT_SECONDS', 30),
      maxReceiveCount: int(env, 'SQS_MAX_RECEIVE_COUNT', 5),
      batchSize: int(env, 'SQS_BATCH_SIZE', 10),
      retryBaseSeconds: int(env, 'SQS_RETRY_BASE_SECONDS', 2),
      retryMaxSeconds: int(env, 'SQS_RETRY_MAX_SECONDS', 60),
      shutdownGraceMs: int(env, 'SQS_SHUTDOWN_GRACE_MS', 10_000),
    },
    outbox: {
      enabled: bool(env, 'OUTBOX_PUBLISHER_ENABLED', true),
      pollIntervalMs: int(env, 'OUTBOX_POLL_INTERVAL_MS', 500),
      batchSize: int(env, 'OUTBOX_BATCH_SIZE', 100),
      publishConcurrency: int(env, 'OUTBOX_PUBLISH_CONCURRENCY', 4),
      leaseMs: int(env, 'OUTBOX_LEASE_MS', 30_000),
      baseDelayMs: int(env, 'OUTBOX_RETRY_BASE_MS', 1_000),
      maxDelayMs: int(env, 'OUTBOX_RETRY_MAX_MS', 300_000),
    },
    pendingReference: {
      enabled: bool(env, 'PENDING_REFERENCE_WORKER_ENABLED', true),
      pollIntervalMs: int(env, 'PENDING_REFERENCE_POLL_INTERVAL_MS', 1_000),
      batchSize: int(env, 'PENDING_REFERENCE_BATCH_SIZE', 50),
      leaseMs: int(env, 'PENDING_REFERENCE_LEASE_MS', 30_000),
      baseDelayMs: int(env, 'PENDING_REFERENCE_BASE_DELAY_MS', 1_000),
      maxDelayMs: int(env, 'PENDING_REFERENCE_MAX_DELAY_MS', 60_000),
      maxAttempts: int(env, 'PENDING_REFERENCE_MAX_ATTEMPTS', 10),
      ttlMs: int(env, 'PENDING_REFERENCE_TTL_MS', 15 * 60_000),
    },
    faultInjection: env.FAULT_INJECTION || undefined,
  };
}
