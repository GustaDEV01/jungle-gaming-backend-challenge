import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';
import type { DuplicateKind, LockConflictKind, Metrics } from '../../application/observability';

/**
 * Prometheus metrics (exposed on GET /metrics). One Registry per process
 * instance, so several app instances can coexist in one test process.
 */
export class PromMetrics implements Metrics {
  readonly registry = new Registry();

  private readonly transactions = new Counter({
    name: 'wagering_transactions_total',
    help: 'Wager transactions by final status, kind and source',
    labelNames: ['status', 'kind', 'source'],
    registers: [this.registry],
  });
  private readonly duplicates = new Counter({
    name: 'wagering_duplicates_total',
    help: 'Duplicates detected (idempotent replays, inbox redeliveries, unique races)',
    labelNames: ['kind', 'source'],
    registers: [this.registry],
  });
  private readonly lockConflicts = new Counter({
    name: 'wagering_lock_conflicts_total',
    help: 'Lock conflicts: contended waits, lock timeouts, deadlocks, serialization failures, unique races',
    labelNames: ['kind'],
    registers: [this.registry],
  });
  private readonly lockWaitHist = new Histogram({
    name: 'wagering_wallet_lock_wait_seconds',
    help: 'Time spent waiting for the wallet row lock',
    buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [this.registry],
  });
  private readonly latency = new Histogram({
    name: 'wagering_processing_duration_seconds',
    help: 'End-to-end use case latency',
    labelNames: ['source'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    registers: [this.registry],
  });
  private readonly retries = new Counter({
    name: 'wagering_retries_total',
    help: 'Retries by component',
    labelNames: ['component'],
    registers: [this.registry],
  });
  private readonly dlqCounter = new Counter({
    name: 'wagering_dlq_messages_total',
    help: 'Messages moved to the DLQ by this instance, by reason',
    labelNames: ['reason'],
    registers: [this.registry],
  });
  private readonly published = new Counter({
    name: 'wagering_outbox_published_total',
    help: 'Outbox messages published',
    registers: [this.registry],
  });
  private readonly lag = new Histogram({
    name: 'wagering_outbox_lag_seconds',
    help: 'Delay between event occurrence (commit) and publication',
    buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300],
    registers: [this.registry],
  });
  readonly outboxPending = new Gauge({
    name: 'wagering_outbox_pending_messages',
    help: 'Unpublished outbox messages (sampled by the publisher)',
    registers: [this.registry],
  });
  readonly outboxOldestAge = new Gauge({
    name: 'wagering_outbox_oldest_pending_age_seconds',
    help: 'Age of the oldest unpublished outbox message (outbox lag)',
    registers: [this.registry],
  });
  readonly dlqDepth = new Gauge({
    name: 'wagering_dlq_depth',
    help: 'Approximate number of messages in the DLQ (sampled)',
    registers: [this.registry],
  });
  private readonly reconciliations = new Counter({
    name: 'wagering_reconciliations_total',
    help: 'Reconciliations by result',
    labelNames: ['result'],
    registers: [this.registry],
  });

  constructor(defaultMetrics = true) {
    if (defaultMetrics) collectDefaultMetrics({ register: this.registry });
  }

  transactionOutcome(status: string, kind: string, source: string): void {
    this.transactions.inc({ status, kind, source });
  }
  duplicateDetected(kind: DuplicateKind, source: string): void {
    this.duplicates.inc({ kind, source });
  }
  lockConflict(kind: LockConflictKind): void {
    this.lockConflicts.inc({ kind });
  }
  lockWait(seconds: number): void {
    this.lockWaitHist.observe(seconds);
  }
  processingLatency(source: string, seconds: number): void {
    this.latency.observe({ source }, seconds);
  }
  retry(component: 'sqs_consumer' | 'outbox' | 'pending_reference' | 'db_transaction'): void {
    this.retries.inc({ component });
  }
  dlq(reason: string): void {
    this.dlqCounter.inc({ reason });
  }
  outboxPublished(count: number): void {
    this.published.inc(count);
  }
  outboxLag(seconds: number): void {
    this.lag.observe(seconds);
  }
  reconciliation(consistent: boolean): void {
    this.reconciliations.inc({ result: consistent ? 'consistent' : 'divergent' });
  }
}
