export const METRICS = Symbol('METRICS');
export const LOGGER = Symbol('LOGGER');

export type DuplicateKind = 'idempotent_replay' | 'inbox_duplicate' | 'unique_race';
export type LockConflictKind = 'lock_timeout' | 'deadlock' | 'serialization_failure' | 'unique_race' | 'contended';

/** Metrics port (implemented with prom-client). */
export interface Metrics {
  transactionOutcome(status: string, kind: string, source: string): void;
  duplicateDetected(kind: DuplicateKind, source: string): void;
  lockConflict(kind: LockConflictKind): void;
  lockWait(seconds: number): void;
  processingLatency(source: string, seconds: number): void;
  retry(component: 'sqs_consumer' | 'outbox' | 'pending_reference' | 'db_transaction'): void;
  dlq(reason: string): void;
  outboxPublished(count: number): void;
  outboxLag(seconds: number): void;
  reconciliation(consistent: boolean): void;
}

export type LogFields = Record<string, string | number | boolean | null | undefined>;

/** Structured logger port. Never log full financial payloads. */
export interface AppLogger {
  info(msg: string, fields?: LogFields): void;
  warn(msg: string, fields?: LogFields): void;
  error(msg: string, fields?: LogFields): void;
  debug(msg: string, fields?: LogFields): void;
}
