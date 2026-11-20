import type { SQSClient } from '@aws-sdk/client-sqs';
import type { MikroORM } from '@mikro-orm/postgresql';
import {
  type BeforeApplicationShutdown,
  type DynamicModule,
  Inject,
  Injectable,
  Module,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { CreateWalletUseCase } from './application/create-wallet.use-case';
import { LOGGER, METRICS, type AppLogger } from './application/observability';
import { CLOCK, ID_GENERATOR, READ_MODEL, TRANSACTION_RUNNER, type Clock, type IdGenerator } from './application/ports';
import {
  PENDING_REFERENCE_POLICY,
  ProcessWagerTransactionUseCase,
  type PendingReferencePolicy,
} from './application/process-wager-transaction.use-case';
import { WageringQueries } from './application/queries';
import { ResolvePendingReferenceUseCase } from './application/resolve-pending-reference.use-case';
import { NoopProviderIdentity, PROVIDER_IDENTITY, ProviderAuthGuard } from './http/auth';
import { WageringController, WalletsController } from './http/controllers';
import { HealthController } from './http/health.controller';
import { APP_CONFIG, type AppConfig } from './infrastructure/config';
import { FAULT_INJECTOR, FaultInjector } from './infrastructure/messaging/fault-injection';
import { OutboxPublisher } from './infrastructure/messaging/outbox-publisher';
import { PendingReferenceWorker } from './infrastructure/messaging/pending-reference-worker';
import { QUEUE_URLS, SQS_CLIENT, createSqsClient, ensureQueues, type QueueUrls } from './infrastructure/messaging/sqs';
import { SqsWagerConsumer } from './infrastructure/messaging/sqs-wager-consumer';
import { JsonLogger } from './infrastructure/observability/logger';
import { PromMetrics } from './infrastructure/observability/metrics';
import { MikroOrmReadModel } from './infrastructure/persistence/mikro-orm-read-model';
import { MikroOrmTransactionRunner } from './infrastructure/persistence/mikro-orm-unit-of-work';
import { MIKRO_ORM, createOrm } from './infrastructure/persistence/orm';

/** Starts the background workers after bootstrap and stops them, in order, on shutdown. */
@Injectable()
export class WorkersLifecycle implements OnApplicationBootstrap, BeforeApplicationShutdown, OnApplicationShutdown {
  readonly consumer: SqsWagerConsumer;
  readonly outbox: OutboxPublisher;
  readonly pendingReferences: PendingReferenceWorker;

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    @Inject(MIKRO_ORM) private readonly orm: MikroORM,
    @Inject(SQS_CLIENT) sqs: SQSClient,
    @Inject(QUEUE_URLS) queues: QueueUrls,
    @Inject(FAULT_INJECTOR) faults: FaultInjector,
    @Inject(PromMetrics) metrics: PromMetrics,
    @Inject(LOGGER) private readonly logger: AppLogger,
    @Inject(ProcessWagerTransactionUseCase) wagering: ProcessWagerTransactionUseCase,
    @Inject(ResolvePendingReferenceUseCase) resolver: ResolvePendingReferenceUseCase,
  ) {
    this.consumer = new SqsWagerConsumer(sqs, queues, config.sqs, wagering, faults, metrics, logger);
    this.outbox = new OutboxPublisher(orm, sqs, queues.events, config.outbox, faults, metrics, logger, config.instanceId);
    this.pendingReferences = new PendingReferenceWorker(orm, resolver, config.pendingReference, logger);
  }

  onApplicationBootstrap(): void {
    if (this.config.sqs.consumerEnabled) this.consumer.start();
    if (this.config.outbox.enabled) this.outbox.start();
    if (this.config.pendingReference.enabled) this.pendingReferences.start();
    if (this.config.faultInjection) this.logger.warn('FAULT INJECTION ENABLED (test only)', { faultInjection: this.config.faultInjection });
  }

  /** SIGTERM: stop intake first, let in-flight work finish, then the publishers. */
  async beforeApplicationShutdown(signal?: string): Promise<void> {
    this.logger.info('shutting down', { signal: signal ?? 'close' });
    await this.consumer.stop();
    await this.pendingReferences.stop();
    await this.outbox.stop();
  }

  async onApplicationShutdown(): Promise<void> {
    await this.orm.close(true);
  }
}

@Module({})
export class AppModule {
  static register(config: AppConfig): DynamicModule {
    const logger = new JsonLogger({ service: 'wagering-processor', instanceId: config.instanceId }, config.logLevel);
    const metrics = new PromMetrics();
    return {
      module: AppModule,
      controllers: [WalletsController, WageringController, HealthController],
      providers: [
        { provide: APP_CONFIG, useValue: config },
        { provide: LOGGER, useValue: logger },
        { provide: JsonLogger, useValue: logger },
        { provide: PromMetrics, useValue: metrics },
        { provide: METRICS, useValue: metrics },
        { provide: CLOCK, useValue: { now: () => new Date() } satisfies Clock },
        { provide: ID_GENERATOR, useValue: { next: () => Bun.randomUUIDv7() } satisfies IdGenerator },
        {
          provide: PENDING_REFERENCE_POLICY,
          useValue: {
            baseDelayMs: config.pendingReference.baseDelayMs,
            maxDelayMs: config.pendingReference.maxDelayMs,
            maxAttempts: config.pendingReference.maxAttempts,
            ttlMs: config.pendingReference.ttlMs,
          } satisfies PendingReferencePolicy,
        },
        { provide: MIKRO_ORM, useFactory: () => createOrm(config.database.url, config.database.poolMax) },
        {
          provide: TRANSACTION_RUNNER,
          inject: [MIKRO_ORM],
          useFactory: (orm: MikroORM) =>
            new MikroOrmTransactionRunner(
              orm,
              {
                lockTimeoutMs: config.database.lockTimeoutMs,
                statementTimeoutMs: config.database.statementTimeoutMs,
                maxRetries: config.database.maxTxRetries,
              },
              metrics,
              logger,
            ),
        },
        { provide: READ_MODEL, inject: [MIKRO_ORM], useFactory: (orm: MikroORM) => new MikroOrmReadModel(orm) },
        { provide: SQS_CLIENT, useFactory: () => createSqsClient(config.sqs) },
        { provide: QUEUE_URLS, inject: [SQS_CLIENT], useFactory: (sqs: SQSClient) => ensureQueues(sqs, config.sqs) },
        { provide: FAULT_INJECTOR, useValue: new FaultInjector(config.faultInjection) },
        { provide: PROVIDER_IDENTITY, useValue: new NoopProviderIdentity() },
        ProviderAuthGuard,
        ProcessWagerTransactionUseCase,
        ResolvePendingReferenceUseCase,
        CreateWalletUseCase,
        WageringQueries,
        WorkersLifecycle,
      ],
    };
  }
}
