import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { canonicalHash } from '../../application/canonical-json';
import {
  ExternalTransactionConflictError,
  IdempotencyConflictError,
  InboxPayloadConflictError,
  TransientInfrastructureError,
} from '../../application/errors';
import type { AppLogger, Metrics } from '../../application/observability';
import type { ProcessWagerTransactionUseCase } from '../../application/process-wager-transaction.use-case';
import type { WagerCommand } from '../../application/wager-command';
import { DomainError } from '../../domain/errors';
import type { AppConfig } from '../config';
import { toWagerCommand, wagerMessageSchema } from '../contracts';
import type { FaultInjector } from './fault-injection';
import type { QueueUrls } from './sqs';

type Decision =
  | { action: 'ack'; reason: string }
  | { action: 'retry'; reason: string }
  | { action: 'dlq'; reason: string };

/**
 * SQS FIFO consumer for wager-transactions.fifo.
 *
 * - reuses ProcessWagerTransactionUseCase (same path as HTTP);
 * - de-duplicates through the persistent inbox (consumerName, messageId) written
 *   in the same SQL transaction as the financial effect;
 * - deletes (acks) ONLY after the use case returned, i.e. after COMMIT;
 * - business outcome (processed / rejected / duplicate) -> ack;
 *   transient error -> visibility backoff, DLQ after maxReceiveCount;
 *   permanent error (malformed message, idempotency conflict) -> DLQ immediately;
 * - on shutdown: stops polling, finishes in-flight messages and returns the
 *   ones not started (visibility 0).
 */
export class SqsWagerConsumer {
  private running = false;
  private stopping = false;
  private loop?: Promise<void>;
  private poll?: AbortController;
  private readonly inFlight = new Set<Promise<void>>();
  private lastDlqSample = 0;

  constructor(
    private readonly sqs: SQSClient,
    private readonly queues: QueueUrls,
    private readonly config: AppConfig['sqs'],
    private readonly useCase: ProcessWagerTransactionUseCase,
    private readonly faults: FaultInjector,
    private readonly metrics: Metrics & { dlqDepth?: { set(v: number): void } },
    private readonly logger: AppLogger,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.stopping = false;
    this.loop = this.run();
    this.logger.info('sqs consumer started', { queue: this.config.wagerQueueName, consumerName: this.config.consumerName });
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.stopping = true;
    this.poll?.abort();
    const grace = new Promise<void>((resolve) => setTimeout(resolve, this.config.shutdownGraceMs));
    await Promise.race([Promise.allSettled([...this.inFlight]).then(() => undefined), grace]);
    await this.loop?.catch(() => undefined);
    this.running = false;
    this.logger.info('sqs consumer stopped', { inFlight: this.inFlight.size });
  }

  private async run(): Promise<void> {
    let idleBackoff = 250;
    while (!this.stopping) {
      try {
        this.poll = new AbortController();
        const res = await this.sqs.send(
          new ReceiveMessageCommand({
            QueueUrl: this.queues.wager,
            MaxNumberOfMessages: Math.min(Math.max(this.config.batchSize, 1), 10),
            WaitTimeSeconds: this.config.waitTimeSeconds,
            VisibilityTimeout: this.config.visibilityTimeoutSeconds,
            MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
            MessageAttributeNames: ['All'],
          }),
          { abortSignal: this.poll.signal },
        );
        idleBackoff = 250;
        const messages = res.Messages ?? [];
        if (messages.length > 0) {
          const batch = this.processBatch(messages);
          this.inFlight.add(batch);
          await batch.finally(() => this.inFlight.delete(batch));
        }
        await this.sampleDlqDepth();
      } catch (error) {
        if (this.stopping) break;
        this.logger.error('sqs receive failed', { error: (error as Error).message });
        await Bun.sleep(idleBackoff);
        idleBackoff = Math.min(idleBackoff * 2, 10_000);
      }
    }
  }

  /** Groups run in parallel; messages of one group run in order. */
  private async processBatch(messages: Message[]): Promise<void> {
    const groups = new Map<string, Message[]>();
    for (const m of messages) {
      const group = m.Attributes?.MessageGroupId ?? 'default';
      groups.set(group, [...(groups.get(group) ?? []), m]);
    }
    await Promise.all(
      [...groups.values()].map(async (group) => {
        for (let i = 0; i < group.length; i++) {
          const message = group[i]!;
          if (this.stopping) {
            // Not started yet: give it back immediately instead of waiting for the visibility timeout.
            await this.release(group.slice(i));
            return;
          }
          const decision = await this.handle(message);
          if (decision.action === 'retry') {
            // Keep FIFO order inside the group: the rest goes back too.
            await this.release(group.slice(i + 1));
            return;
          }
        }
      }),
    );
  }

  private async handle(message: Message): Promise<Decision> {
    const receiveCount = Number.parseInt(message.Attributes?.ApproximateReceiveCount ?? '1', 10);
    let decision: Decision;
    let parsed: { messageId?: string; correlationId?: string; data?: { walletId?: string; providerId?: string } } = {};
    try {
      let raw: unknown;
      try {
        raw = JSON.parse(message.Body ?? '');
      } catch {
        return await this.toDlq(message, 'malformed_json', receiveCount);
      }
      parsed = (raw ?? {}) as typeof parsed;
      const result = wagerMessageSchema.safeParse(raw);
      if (!result.success) return await this.toDlq(message, 'invalid_contract', receiveCount);
      const envelope = result.data;
      this.faults.beforeWagerMessage(envelope.data.gameId, receiveCount);

      const { idempotencyKey, ...business } = envelope.data;
      const cmd: WagerCommand = toWagerCommand(business, idempotencyKey);
      const outcome = await this.useCase.execute(cmd, {
        source: 'sqs',
        correlationId: envelope.correlationId ?? envelope.messageId,
        inbox: {
          consumerName: this.config.consumerName,
          messageId: envelope.messageId,
          payloadHash: canonicalHash({ type: envelope.type, data: envelope.data }),
        },
      });
      // COMMITTED at this point.
      this.faults.crashIf('crash_after_commit_before_ack');
      await this.ack(message);
      decision = { action: 'ack', reason: outcome.type };
    } catch (error) {
      decision = this.classify(error);
      const fields = {
        messageId: parsed.messageId,
        correlationId: parsed.correlationId ?? parsed.messageId,
        walletId: parsed.data?.walletId,
        providerId: parsed.data?.providerId,
        receiveCount,
        reason: decision.reason,
        error: (error as Error).message,
      };
      if (decision.action === 'dlq') {
        this.logger.warn('permanent failure, moving message to DLQ', fields);
        return await this.toDlq(message, decision.reason, receiveCount);
      }
      if (receiveCount >= this.config.maxReceiveCount) {
        this.logger.error('retry budget exhausted, moving message to DLQ', fields);
        return await this.toDlq(message, 'retries_exhausted', receiveCount);
      }
      const delay = Math.min(this.config.retryBaseSeconds * 2 ** (receiveCount - 1), this.config.retryMaxSeconds);
      this.metrics.retry('sqs_consumer');
      this.logger.warn('transient failure, message will be retried', { ...fields, retryInSeconds: delay });
      await this.changeVisibility(message, delay);
    }
    return decision;
  }

  private classify(error: unknown): Decision {
    if (error instanceof TransientInfrastructureError) return { action: 'retry', reason: error.reason };
    if (
      error instanceof IdempotencyConflictError ||
      error instanceof ExternalTransactionConflictError ||
      error instanceof InboxPayloadConflictError
    ) {
      return { action: 'dlq', reason: error.code.toLowerCase() };
    }
    if (error instanceof DomainError) return { action: 'dlq', reason: error.code.toLowerCase() };
    // Unknown error: treat as transient; the receive-count budget bounds it and ends in the DLQ.
    return { action: 'retry', reason: 'unexpected_error' };
  }

  private async toDlq(message: Message, reason: string, receiveCount: number): Promise<Decision> {
    try {
      await this.sqs.send(
        new SendMessageCommand({
          QueueUrl: this.queues.wagerDlq,
          MessageBody: message.Body ?? '',
          MessageGroupId: message.Attributes?.MessageGroupId ?? 'dlq',
          // Same SQS message moved twice (crash between send and delete) is de-duplicated by the broker.
          MessageDeduplicationId: (message.MessageId ?? crypto.randomUUID()).slice(0, 128),
          MessageAttributes: {
            dlqReason: { DataType: 'String', StringValue: reason },
            receiveCount: { DataType: 'Number', StringValue: String(receiveCount) },
            originalMessageId: { DataType: 'String', StringValue: message.MessageId ?? 'unknown' },
          },
        }),
      );
      await this.ack(message);
      this.metrics.dlq(reason);
      return { action: 'dlq', reason };
    } catch (error) {
      // Could not move it: leave it; the redrive policy will move it after maxReceiveCount.
      this.logger.error('failed to move message to DLQ', { reason, error: (error as Error).message });
      return { action: 'retry', reason: 'dlq_send_failed' };
    }
  }

  private async ack(message: Message): Promise<void> {
    await this.sqs.send(new DeleteMessageCommand({ QueueUrl: this.queues.wager, ReceiptHandle: message.ReceiptHandle! }));
  }

  private async changeVisibility(message: Message, seconds: number): Promise<void> {
    try {
      await this.sqs.send(
        new ChangeMessageVisibilityCommand({ QueueUrl: this.queues.wager, ReceiptHandle: message.ReceiptHandle!, VisibilityTimeout: seconds }),
      );
    } catch (error) {
      this.logger.warn('change visibility failed', { error: (error as Error).message });
    }
  }

  private async release(messages: Message[]): Promise<void> {
    await Promise.all(messages.map((m) => this.changeVisibility(m, 0)));
  }

  private async sampleDlqDepth(): Promise<void> {
    if (Date.now() - this.lastDlqSample < 15_000 || !this.metrics.dlqDepth) return;
    this.lastDlqSample = Date.now();
    try {
      const attrs = await this.sqs.send(
        new GetQueueAttributesCommand({ QueueUrl: this.queues.wagerDlq, AttributeNames: ['ApproximateNumberOfMessages'] }),
      );
      this.metrics.dlqDepth.set(Number.parseInt(attrs.Attributes?.ApproximateNumberOfMessages ?? '0', 10));
    } catch {
      /* sampling only */
    }
  }
}
