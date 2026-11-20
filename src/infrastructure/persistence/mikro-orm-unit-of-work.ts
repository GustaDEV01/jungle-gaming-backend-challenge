import { IsolationLevel, LockMode } from '@mikro-orm/core';
import type { EntityManager, MikroORM } from '@mikro-orm/postgresql';
import { TransientInfrastructureError, UniqueConstraintViolation } from '../../application/errors';
import type { AppLogger, Metrics } from '../../application/observability';
import type { InboxInsertResult, ProviderRecord, TransactionRunner, WageringUnitOfWork } from '../../application/ports';
import type { IntegrationEvent } from '../../domain/events/integration-event';
import type { InboxMessage } from '../../domain/inbox-message';
import type { WalletLedgerEntry } from '../../domain/ledger-entry';
import { OutboxMessage } from '../../domain/outbox-message';
import type { Wallet } from '../../domain/wallet';
import type { WagerTransaction } from '../../domain/wager-transaction';
import { ledgerToRecord, transactionToDomain, transactionToRecord, walletToDomain, walletToRecord } from './mappers';
import { PG, isConnectionError, pgErrorInfo } from './pg-errors';
import { InboxRecord, LedgerEntryRecord, OutboxRecord, ProviderEntity, WagerTransactionRecord, WalletRecord } from './schemas';

/** Thrown when the conditional wallet UPDATE matched no row (should be impossible under the row lock). */
export class WalletVersionConflictError extends Error {
  constructor(walletId: string, expected: number) {
    super(`Wallet ${walletId} was modified concurrently (expected version ${expected})`);
  }
}

/**
 * MikroORM implementation of the unit of work. Every call goes through the
 * transactional EntityManager fork, i.e. the same connection and SQL transaction.
 * Writes use explicit statements (insert / nativeUpdate) so the statement
 * order is deterministic; locks use LockMode.PESSIMISTIC_WRITE (SELECT ... FOR UPDATE).
 */
export class MikroOrmUnitOfWork implements WageringUnitOfWork {
  constructor(private readonly em: EntityManager) {}

  async insertInbox(message: InboxMessage): Promise<InboxInsertResult> {
    const rows = await this.em.execute<{ message_id: string }[]>(
      `insert into inbox_messages (consumer_name, message_id, payload_hash, received_at)
       values (?, ?, ?, ?) on conflict (consumer_name, message_id) do nothing returning message_id`,
      [message.consumerName, message.messageId, message.payloadHash, message.receivedAt],
    );
    if (rows.length === 1) return { inserted: true };
    const existing = await this.em.execute<{ payload_hash: string }[]>(
      `select payload_hash from inbox_messages where consumer_name = ? and message_id = ?`,
      [message.consumerName, message.messageId],
    );
    return { inserted: false, existingPayloadHash: (existing[0]?.payload_hash ?? '').trim() };
  }

  async completeInbox(message: InboxMessage, transactionId: string | undefined): Promise<void> {
    await this.em.nativeUpdate(
      InboxRecord,
      { consumerName: message.consumerName, messageId: message.messageId },
      { processedAt: message.processedAt ?? new Date(), transactionId: transactionId ?? null },
    );
  }

  async findProvider(providerId: string): Promise<ProviderRecord | null> {
    const p = await this.em.findOne(ProviderEntity, { id: providerId }, { disableIdentityMap: true });
    return p ? { id: p.id, active: p.active, isInternal: p.isInternal } : null;
  }

  async lockWallet(walletId: string): Promise<Wallet | null> {
    const r = await this.em.findOne(WalletRecord, { id: walletId }, { lockMode: LockMode.PESSIMISTIC_WRITE, refresh: true });
    return r ? walletToDomain(r) : null;
  }

  async findWallet(walletId: string): Promise<Wallet | null> {
    const r = await this.em.findOne(WalletRecord, { id: walletId }, { disableIdentityMap: true });
    return r ? walletToDomain(r) : null;
  }

  async insertWallet(wallet: Wallet): Promise<void> {
    await this.em.insert(WalletRecord, walletToRecord(wallet));
  }

  async updateWalletBalance(wallet: Wallet, expectedVersion: number): Promise<void> {
    const affected = await this.em.nativeUpdate(
      WalletRecord,
      { id: wallet.id, version: expectedVersion },
      { balanceAmount: wallet.balance.amount, version: wallet.version, updatedAt: wallet.updatedAt },
    );
    if (affected !== 1) throw new WalletVersionConflictError(wallet.id, expectedVersion);
  }

  async findTransactionByIdempotencyKey(key: string): Promise<WagerTransaction | null> {
    const r = await this.em.findOne(WagerTransactionRecord, { idempotencyKey: key }, { disableIdentityMap: true });
    return r ? transactionToDomain(r) : null;
  }

  async findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null> {
    const r = await this.em.findOne(WagerTransactionRecord, { providerId, externalTransactionId }, { disableIdentityMap: true });
    return r ? transactionToDomain(r) : null;
  }

  async lockTransaction(id: string): Promise<WagerTransaction | null> {
    const r = await this.em.findOne(WagerTransactionRecord, { id }, { lockMode: LockMode.PESSIMISTIC_WRITE, refresh: true });
    return r ? transactionToDomain(r) : null;
  }

  async isReversed(transactionId: string): Promise<boolean> {
    const count = await this.em.count(WagerTransactionRecord, {
      referenceTransactionId: transactionId,
      kind: { $in: ['REFUND', 'ROLLBACK'] },
      status: 'PROCESSED',
    });
    return count > 0;
  }

  async insertTransaction(tx: WagerTransaction, correlationId: string): Promise<void> {
    await this.em.insert(WagerTransactionRecord, transactionToRecord(tx, correlationId));
  }

  async updateTransaction(tx: WagerTransaction): Promise<void> {
    const r = transactionToRecord(tx, null);
    await this.em.nativeUpdate(
      WagerTransactionRecord,
      { id: tx.id },
      {
        status: r.status,
        failureCode: r.failureCode,
        referenceTransactionId: r.referenceTransactionId,
        resultBalanceAmount: r.resultBalanceAmount,
        resultBalanceCurrency: r.resultBalanceCurrency,
        referenceAttempts: r.referenceAttempts,
        nextReferenceCheckAt: r.nextReferenceCheckAt,
        processedAt: r.processedAt,
      },
    );
  }

  async appendLedgerEntry(entry: WalletLedgerEntry): Promise<void> {
    await this.em.insert(LedgerEntryRecord, ledgerToRecord(entry));
  }

  async enqueueEvents(events: IntegrationEvent<unknown>[]): Promise<void> {
    if (events.length === 0) return;
    const records: OutboxRecord[] = events.map((event) => {
      const m = OutboxMessage.enqueue(event).toState();
      return {
        id: m.id,
        aggregateId: m.aggregateId,
        eventType: m.eventType,
        // The domain payload is frozen; the ORM gets its own plain copy.
        payload: structuredClone(m.payload) as Record<string, unknown>,
        occurredAt: m.occurredAt,
        attempts: m.attempts,
        nextAttemptAt: m.nextAttemptAt ?? m.occurredAt,
        publishedAt: null,
        lastError: null,
      };
    });
    await this.em.insertMany(OutboxRecord, records);
  }
}

export interface RunnerOptions {
  lockTimeoutMs: number;
  statementTimeoutMs: number;
  maxRetries: number;
}

/**
 * Runs a unit of work in one READ COMMITTED SQL transaction. Per-transaction
 * lock_timeout bounds how long a hot wallet can make a caller wait. Deadlocks
 * and serialization failures are retried with jittered backoff; everything
 * else is mapped to application errors.
 */
export class MikroOrmTransactionRunner implements TransactionRunner {
  constructor(
    private readonly orm: MikroORM,
    private readonly options: RunnerOptions,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async run<T>(work: (uow: WageringUnitOfWork) => Promise<T>, label: string): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.orm.em.fork().transactional(
          async (em) => {
            await em.execute(`set local lock_timeout = ${Math.trunc(this.options.lockTimeoutMs)}`);
            await em.execute(`set local statement_timeout = ${Math.trunc(this.options.statementTimeoutMs)}`);
            return work(new MikroOrmUnitOfWork(em as EntityManager));
          },
          { isolationLevel: IsolationLevel.READ_COMMITTED },
        );
      } catch (error) {
        const info = pgErrorInfo(error);
        if (info.code === PG.uniqueViolation) {
          throw new UniqueConstraintViolation(info.constraint ?? 'unknown', error);
        }
        const retryable = info.code === PG.deadlockDetected || info.code === PG.serializationFailure;
        if (retryable) {
          this.metrics.lockConflict(info.code === PG.deadlockDetected ? 'deadlock' : 'serialization_failure');
          if (attempt <= this.options.maxRetries) {
            this.metrics.retry('db_transaction');
            this.logger.warn('retrying transaction', { label, attempt, sqlState: info.code });
            await Bun.sleep(10 * 2 ** attempt + Math.floor(Math.random() * 20));
            continue;
          }
          throw new TransientInfrastructureError(`${label}: retries exhausted`, 'concurrency_retries_exhausted', error);
        }
        if (info.code === PG.lockNotAvailable) {
          this.metrics.lockConflict('lock_timeout');
          throw new TransientInfrastructureError(`${label}: wallet lock timeout`, 'lock_timeout', error);
        }
        if (info.code === PG.queryCanceled) {
          throw new TransientInfrastructureError(`${label}: statement timeout`, 'statement_timeout', error);
        }
        if (isConnectionError(error)) {
          throw new TransientInfrastructureError(`${label}: database unavailable`, 'database_unavailable', error);
        }
        throw error;
      }
    }
  }
}
