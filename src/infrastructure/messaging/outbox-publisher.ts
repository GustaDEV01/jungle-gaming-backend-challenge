import { SendMessageBatchCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { AppLogger } from '../../application/observability';
import { OutboxMessage } from '../../domain/outbox-message';
import type { AppConfig } from '../config';
import type { PromMetrics } from '../observability/metrics';
import type { FaultInjector } from './fault-injection';

/** Stable lane for an aggregate (djb2). */
function laneOf(aggregateId: string, lanes: number): number {
  let h = 5381;
  for (let i = 0; i < aggregateId.length; i++) h = ((h << 5) + h + aggregateId.charCodeAt(i)) | 0;
  return Math.abs(h) % lanes;
}

interface ClaimedRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown> | string;
  occurred_at: Date | string;
  attempts: number;
}

/**
 * Transactional outbox publisher. Safe with N concurrent publishers (any
 * number of instances):
 *
 * 1. CLAIM — one auto-committed statement selects due rows with
 *    FOR UPDATE SKIP LOCKED and pushes their next_attempt_at forward by a
 *    lease. Concurrent publishers never claim the same row at the same time.
 * 2. PUBLISH to SQS FIFO (MessageGroupId = aggregateId, MessageDeduplicationId = eventId).
 * 3. MARK published (or schedule a retry with exponential backoff).
 *
 * If the process dies between 1 and 3 the lease expires and another instance
 * publishes again: delivery is at-least-once. Duplicates are harmless: the
 * broker de-duplicates by eventId within its window and consumers must
 * de-duplicate by eventId (it is stable across re-publications).
 * Events are only ever read from committed rows, so nothing is published
 * before the financial transaction commits.
 */
export class OutboxPublisher {
  private timer?: ReturnType<typeof setTimeout>;
  private running = false;
  private current?: Promise<number>;

  constructor(
    private readonly orm: MikroORM,
    private readonly sqs: SQSClient,
    private readonly eventsQueueUrl: string,
    private readonly config: AppConfig['outbox'],
    private readonly faults: FaultInjector,
    private readonly metrics: PromMetrics,
    private readonly logger: AppLogger,
    private readonly instanceId: string,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    const tick = async () => {
      if (!this.running) return;
      try {
        this.current = this.publishOnce();
        const published = await this.current;
        // Drain quickly while there is backlog, otherwise wait the poll interval.
        this.schedule(tick, published >= this.config.batchSize ? 0 : this.config.pollIntervalMs);
      } catch (error) {
        this.logger.error('outbox publish cycle failed', { error: (error as Error).message });
        this.schedule(tick, this.config.pollIntervalMs * 2);
      }
    };
    this.schedule(tick, 0);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) clearTimeout(this.timer);
    await this.current?.catch(() => undefined);
  }

  private schedule(fn: () => void, ms: number): void {
    if (this.running) this.timer = setTimeout(fn, ms);
  }

  /** One claim/publish/mark cycle. Returns how many messages were published. */
  async publishOnce(): Promise<number> {
    const em = this.orm.em.fork();
    const claimed = await em.execute<ClaimedRow[]>(
      `with due as (
         select id from outbox_messages
          where published_at is null and next_attempt_at <= now()
          order by next_attempt_at, id
          limit ?
          for update skip locked)
       update outbox_messages o
          set next_attempt_at = now() + (? * interval '1 millisecond')
         from due where o.id = due.id
       returning o.id, o.aggregate_id, o.event_type, o.payload, o.occurred_at, o.attempts`,
      [this.config.batchSize, this.config.leaseMs],
    );
    await this.sampleBacklog();
    if (claimed.length === 0) return 0;
    this.faults.crashIf('crash_after_outbox_claim');

    // Lanes: every event of one aggregate goes to the same lane and lanes run in
    // parallel. Inside a lane, chunks are sent sequentially in occurrence order
    // (uuid v7), so per-wallet order is preserved while throughput scales.
    const rows = [...claimed].sort((a, b) => (a.id < b.id ? -1 : 1));
    const laneCount = Math.max(1, this.config.publishConcurrency);
    const lanes = new Map<number, ClaimedRow[]>();
    for (const row of rows) {
      const lane = laneOf(row.aggregate_id, laneCount);
      lanes.set(lane, [...(lanes.get(lane) ?? []), row]);
    }
    const counts = await Promise.all([...lanes.values()].map((laneRows) => this.publishLane(em, laneRows)));
    const published = counts.reduce((a, b) => a + b, 0);
    if (published > 0) this.logger.debug('outbox published', { count: published, instanceId: this.instanceId });
    return published;
  }

  private async publishLane(em: ReturnType<MikroORM['em']['fork']>, rows: ClaimedRow[]): Promise<number> {
    let published = 0;
    for (let i = 0; i < rows.length; i += 10) {
      const chunk = rows.slice(i, i + 10);
      const ok: string[] = [];
      const failed = new Map<string, string>();
      try {
        const res = await this.sqs.send(
          new SendMessageBatchCommand({
            QueueUrl: this.eventsQueueUrl,
            Entries: chunk.map((row, idx) => ({
              Id: `m${idx}`,
              MessageBody: typeof row.payload === 'string' ? row.payload : JSON.stringify(row.payload),
              MessageGroupId: row.aggregate_id,
              MessageDeduplicationId: row.id,
              MessageAttributes: {
                eventType: { DataType: 'String', StringValue: row.event_type },
                eventId: { DataType: 'String', StringValue: row.id },
              },
            })),
          }),
        );
        for (const s of res.Successful ?? []) ok.push(chunk[Number.parseInt(s.Id!.slice(1), 10)]!.id);
        for (const f of res.Failed ?? []) failed.set(chunk[Number.parseInt(f.Id!.slice(1), 10)]!.id, f.Message ?? f.Code ?? 'failed');
      } catch (error) {
        for (const row of chunk) failed.set(row.id, (error as Error).message);
      }
      this.faults.crashIf('crash_after_publish_before_mark');

      if (ok.length > 0) {
        await em.execute(`update outbox_messages set published_at = now(), last_error = null where id = any(?::uuid[]) and published_at is null`, [
          `{${ok.join(',')}}`,
        ]);
        published += ok.length;
        this.metrics.outboxPublished(ok.length);
        const now = Date.now();
        for (const row of chunk.filter((r) => ok.includes(r.id))) {
          this.metrics.outboxLag((now - new Date(row.occurred_at).getTime()) / 1000);
        }
      }
      for (const [id, reason] of failed) {
        const row = chunk.find((r) => r.id === id)!;
        const message = OutboxMessage.rehydrate({
          id,
          aggregateId: row.aggregate_id,
          eventType: row.event_type,
          payload: {},
          occurredAt: new Date(row.occurred_at),
          attempts: row.attempts,
        });
        message.scheduleRetry(new Date(), { baseDelayMs: this.config.baseDelayMs, maxDelayMs: this.config.maxDelayMs });
        await em.execute(
          `update outbox_messages set attempts = ?, next_attempt_at = ?, last_error = ? where id = ? and published_at is null`,
          [message.attempts, message.nextAttemptAt!, reason.slice(0, 500), id],
        );
        this.metrics.retry('outbox');
        this.logger.warn('outbox publish failed, retry scheduled', { eventId: id, attempts: message.attempts, error: reason });
      }
      if (failed.size > 0) {
        // Keep order: later events of this lane wait for the failed ones (same due time, re-claimed in id order).
        const rest = rows.slice(i + 10).map((r) => r.id);
        if (rest.length > 0) {
          await em.execute(
            `update outbox_messages set next_attempt_at = (select max(next_attempt_at) from outbox_messages where id = any(?::uuid[]))
              where id = any(?::uuid[]) and published_at is null`,
            [`{${[...failed.keys()].join(',')}}`, `{${rest.join(',')}}`],
          );
        }
        break;
      }
    }
    return published;
  }

  private async sampleBacklog(): Promise<void> {
    try {
      const [row] = await this.orm.em.fork().execute<{ pending: number; oldest: string | null }[]>(
        `select count(*)::int as pending, extract(epoch from (now() - min(occurred_at)))::text as oldest
           from outbox_messages where published_at is null`,
      );
      this.metrics.outboxPending.set(row?.pending ?? 0);
      this.metrics.outboxOldestAge.set(row?.oldest ? Number.parseFloat(row.oldest) : 0);
    } catch {
      /* sampling only */
    }
  }
}
