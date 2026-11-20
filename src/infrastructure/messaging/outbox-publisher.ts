import { SendMessageBatchCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { AppLogger } from '../../application/observability';
import { OutboxMessage } from '../../domain/outbox-message';
import type { AppConfig } from '../config';
import type { PromMetrics } from '../observability/metrics';
import type { FaultInjector } from './fault-injection';

interface ClaimedRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown> | string;
  occurred_at: Date | string;
  attempts: number;
  /** bigint as text: per-aggregate commit order. */
  seq: string;
}

type Em = ReturnType<MikroORM['em']['fork']>;

/**
 * Transactional outbox publisher with STRICT per-aggregate (per-wallet)
 * ordering, safe with N concurrent publishers (any number of instances).
 *
 * 1. CLAIM (one auto-committed statement) — "head-of-line" per aggregate:
 *    only the oldest unpublished event of an aggregate (lowest `seq`) is
 *    eligible, and only when it is due. Eligible heads are locked with
 *    FOR UPDATE SKIP LOCKED, and the head plus the next events of the same
 *    aggregate (up to `maxEventsPerAggregate`) get a lease. While that lease
 *    (or a retry backoff) is pending on the head, no other publisher can
 *    publish ANY event of that aggregate — a later event can never overtake
 *    an earlier one, even across instances or after a failure.
 * 2. PUBLISH in rounds: round k sends the k-th event of every claimed
 *    aggregate, so a SendMessageBatch never contains two events of one
 *    aggregate. Batches of a round go out in parallel; rounds are sequential.
 *    MessageGroupId = aggregateId, MessageDeduplicationId = eventId.
 * 3. MARK published. If an event fails it gets a retry with exponential
 *    backoff, it stays the head, and the rest of that aggregate is held back
 *    behind it (head-of-line blocking, as FIFO semantics require).
 *
 * If the process dies between 1 and 3 the lease expires and another instance
 * publishes again: delivery is at-least-once. A re-publication carries the same
 * eventId and body; the broker drops it within its dedup window and consumers
 * must de-duplicate by eventId. Only committed rows are ever read, so nothing
 * is published before the financial transaction commits.
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
      `with heads as (
         select o.id, o.aggregate_id
           from outbox_messages o
          where o.published_at is null
            and o.next_attempt_at <= now()
            and not exists (select 1 from outbox_messages p
                             where p.aggregate_id = o.aggregate_id and p.published_at is null and p.seq < o.seq)
          order by o.next_attempt_at, o.seq
          limit ?
          for update skip locked),
       batch as (
         select m.id, row_number() over (partition by m.aggregate_id order by m.seq) as rn
           from outbox_messages m
           join heads h on h.aggregate_id = m.aggregate_id
          where m.published_at is null)
       update outbox_messages o
          set next_attempt_at = now() + (? * interval '1 millisecond')
         from batch
        where o.id = batch.id and batch.rn <= ?
       returning o.id, o.aggregate_id, o.event_type, o.payload, o.occurred_at, o.attempts, o.seq::text as seq`,
      [this.config.batchSize, this.config.leaseMs, this.config.maxEventsPerAggregate],
    );
    await this.sampleBacklog();
    if (claimed.length === 0) return 0;
    this.faults.crashIf('crash_after_outbox_claim');

    // Per aggregate, in commit order.
    const queues = new Map<string, ClaimedRow[]>();
    for (const row of [...claimed].sort((a, b) => (BigInt(a.seq) < BigInt(b.seq) ? -1 : 1))) {
      queues.set(row.aggregate_id, [...(queues.get(row.aggregate_id) ?? []), row]);
    }

    let published = 0;
    for (let round = 0; queues.size > 0; round++) {
      const roundRows: ClaimedRow[] = [];
      for (const [aggregate, rows] of queues) {
        const row = rows[round];
        if (row) roundRows.push(row);
        else queues.delete(aggregate);
      }
      if (roundRows.length === 0) break;

      const chunks: ClaimedRow[][] = [];
      for (let i = 0; i < roundRows.length; i += 10) chunks.push(roundRows.slice(i, i + 10));
      const ok: ClaimedRow[] = [];
      const failed = new Map<string, { row: ClaimedRow; reason: string }>();
      for (let i = 0; i < chunks.length; i += Math.max(1, this.config.publishConcurrency)) {
        const group = chunks.slice(i, i + Math.max(1, this.config.publishConcurrency));
        const results = await Promise.all(group.map((chunk) => this.send(chunk)));
        for (const r of results) {
          ok.push(...r.ok);
          for (const [id, f] of r.failed) failed.set(id, f);
        }
      }
      this.faults.crashIf('crash_after_publish_before_mark');

      if (ok.length > 0) {
        await em.execute(`update outbox_messages set published_at = now(), last_error = null where id = any(?::uuid[]) and published_at is null`, [
          `{${ok.map((r) => r.id).join(',')}}`,
        ]);
        published += ok.length;
        this.metrics.outboxPublished(ok.length);
        const now = Date.now();
        for (const row of ok) this.metrics.outboxLag((now - new Date(row.occurred_at).getTime()) / 1000);
      }
      for (const { row, reason } of failed.values()) {
        await this.holdBack(em, row, queues.get(row.aggregate_id)?.slice(round + 1) ?? [], reason);
        queues.delete(row.aggregate_id);
      }
    }
    if (published > 0) this.logger.debug('outbox published', { count: published, instanceId: this.instanceId });
    return published;
  }

  private async send(chunk: ClaimedRow[]): Promise<{ ok: ClaimedRow[]; failed: Map<string, { row: ClaimedRow; reason: string }> }> {
    const ok: ClaimedRow[] = [];
    const failed = new Map<string, { row: ClaimedRow; reason: string }>();
    const toSend: ClaimedRow[] = [];
    for (const row of chunk) {
      if (this.faults.shouldFailPublish(row.aggregate_id)) failed.set(row.id, { row, reason: 'injected broker rejection' });
      else toSend.push(row);
    }
    if (toSend.length === 0) return { ok, failed };
    try {
      const res = await this.sqs.send(
        new SendMessageBatchCommand({
          QueueUrl: this.eventsQueueUrl,
          Entries: toSend.map((row, idx) => ({
            Id: `m${idx}`,
            MessageBody: typeof row.payload === 'string' ? row.payload : JSON.stringify(row.payload),
            MessageGroupId: row.aggregate_id,
            MessageDeduplicationId: row.id,
            MessageAttributes: {
              eventType: { DataType: 'String', StringValue: row.event_type },
              eventId: { DataType: 'String', StringValue: row.id },
              sequence: { DataType: 'Number', StringValue: row.seq },
            },
          })),
        }),
      );
      for (const s of res.Successful ?? []) ok.push(toSend[Number.parseInt(s.Id!.slice(1), 10)]!);
      for (const f of res.Failed ?? []) {
        const row = toSend[Number.parseInt(f.Id!.slice(1), 10)]!;
        failed.set(row.id, { row, reason: f.Message ?? f.Code ?? 'failed' });
      }
    } catch (error) {
      for (const row of toSend) failed.set(row.id, { row, reason: (error as Error).message });
    }
    return { ok, failed };
  }

  /**
   * The failed event is rescheduled with backoff and stays the head of its
   * aggregate; the events claimed behind it get the same due time, so they are
   * re-claimed together, in order, once the head is published.
   */
  private async holdBack(em: Em, row: ClaimedRow, behind: ClaimedRow[], reason: string): Promise<void> {
    const message = OutboxMessage.rehydrate({
      id: row.id,
      aggregateId: row.aggregate_id,
      eventType: row.event_type,
      payload: {},
      occurredAt: new Date(row.occurred_at),
      attempts: row.attempts,
    });
    message.scheduleRetry(new Date(), { baseDelayMs: this.config.baseDelayMs, maxDelayMs: this.config.maxDelayMs });
    await em.execute(`update outbox_messages set attempts = ?, next_attempt_at = ?, last_error = ? where id = ? and published_at is null`, [
      message.attempts,
      message.nextAttemptAt!,
      reason.slice(0, 500),
      row.id,
    ]);
    if (behind.length > 0) {
      await em.execute(`update outbox_messages set next_attempt_at = ? where id = any(?::uuid[]) and published_at is null`, [
        message.nextAttemptAt!,
        `{${behind.map((r) => r.id).join(',')}}`,
      ]);
    }
    this.metrics.retry('outbox');
    this.logger.warn('outbox publish failed, retry scheduled', {
      eventId: row.id,
      aggregateId: row.aggregate_id,
      attempts: message.attempts,
      heldBack: behind.length,
      error: reason,
    });
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
