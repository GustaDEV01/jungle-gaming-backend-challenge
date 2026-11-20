import { GetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { Controller, Get, Header, Inject, Res } from '@nestjs/common';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { Response } from 'express';
import { PromMetrics } from '../infrastructure/observability/metrics';
import { MIKRO_ORM } from '../infrastructure/persistence/orm';
import { QUEUE_URLS, SQS_CLIENT, type QueueUrls } from '../infrastructure/messaging/sqs';

async function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timeout after ${ms}ms`)), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

/** Health endpoints are intentionally unauthenticated. */
@Controller()
export class HealthController {
  constructor(
    @Inject(MIKRO_ORM) private readonly orm: MikroORM,
    @Inject(SQS_CLIENT) private readonly sqs: SQSClient,
    @Inject(QUEUE_URLS) private readonly queues: QueueUrls,
    @Inject(PromMetrics) private readonly metrics: PromMetrics,
  ) {}

  /** Liveness: the process is up and the event loop answers. No dependency checks. */
  @Get('health/live')
  live() {
    return { status: 'ok' };
  }

  /** Readiness: PostgreSQL and SQS reachable. 503 otherwise (take the instance out of rotation). */
  @Get('health/ready')
  async ready(@Res({ passthrough: true }) res: Response) {
    const check = async (fn: () => Promise<unknown>) => {
      const started = performance.now();
      try {
        await withTimeout(fn(), 2_000);
        return { status: 'up', latencyMs: Math.round(performance.now() - started) };
      } catch (error) {
        return { status: 'down', error: (error as Error).message };
      }
    };
    const [postgres, sqs] = await Promise.all([
      check(() => this.orm.em.fork().execute('select 1')),
      check(() => this.sqs.send(new GetQueueAttributesCommand({ QueueUrl: this.queues.wager, AttributeNames: ['QueueArn'] }))),
    ]);
    const ok = postgres.status === 'up' && sqs.status === 'up';
    res.status(ok ? 200 : 503);
    return { status: ok ? 'ready' : 'not_ready', checks: { postgres, sqs } };
  }

  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4')
  async prometheus() {
    return this.metrics.registry.metrics();
  }
}
