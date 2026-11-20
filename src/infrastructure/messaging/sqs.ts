import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import type { AppConfig } from '../config';

export const SQS_CLIENT = Symbol('SQS_CLIENT');
export const QUEUE_URLS = Symbol('QUEUE_URLS');

export interface QueueUrls {
  wager: string;
  wagerDlq: string;
  events: string;
}

export function createSqsClient(config: AppConfig['sqs']): SQSClient {
  return new SQSClient({
    region: config.region,
    endpoint: config.endpoint,
    credentials: { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey },
    maxAttempts: 3,
  });
}

async function createFifo(client: SQSClient, name: string, attributes: Record<string, string>): Promise<string> {
  const res = await client.send(
    new CreateQueueCommand({
      QueueName: name,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false', ...attributes },
    }),
  );
  return res.QueueUrl!;
}

/**
 * Idempotent queue provisioning:
 *   wager-transactions-dlq.fifo
 *   wager-transactions.fifo  (RedrivePolicy -> DLQ after maxReceiveCount receives)
 *   wagering-events.fifo     (integration events published by the outbox)
 */
export async function ensureQueues(client: SQSClient, config: AppConfig['sqs']): Promise<QueueUrls> {
  const wagerDlq = await createFifo(client, config.wagerDlqName, { MessageRetentionPeriod: '1209600' });
  const dlqAttrs = await client.send(new GetQueueAttributesCommand({ QueueUrl: wagerDlq, AttributeNames: ['QueueArn'] }));
  const dlqArn = dlqAttrs.Attributes?.QueueArn;
  if (!dlqArn) throw new Error('DLQ ARN not available');
  const wager = await createFifo(client, config.wagerQueueName, {
    VisibilityTimeout: String(config.visibilityTimeoutSeconds),
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: String(config.maxReceiveCount) }),
  });
  const events = await createFifo(client, config.eventsQueueName, {});
  return { wager, wagerDlq, events };
}

export async function resolveQueueUrls(client: SQSClient, config: AppConfig['sqs']): Promise<QueueUrls> {
  const url = async (QueueName: string) => (await client.send(new GetQueueUrlCommand({ QueueName }))).QueueUrl!;
  return { wager: await url(config.wagerQueueName), wagerDlq: await url(config.wagerDlqName), events: await url(config.eventsQueueName) };
}
