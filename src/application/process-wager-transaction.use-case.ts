import { Inject, Injectable } from '@nestjs/common';
import type { EventContext, IntegrationEvent } from '../domain/events/integration-event';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../domain/events/wagering-events';
import { FailureCode } from '../domain/failure-codes';
import { InboxMessage } from '../domain/inbox-message';
import type { WalletLedgerEntry } from '../domain/ledger-entry';
import { Money } from '../domain/money';
import { backoffDelayMs } from '../domain/outbox-message';
import type { Wallet } from '../domain/wallet';
import { WagerSettlement, type SettlementOutcome } from '../domain/wager-settlement';
import { WagerTransaction, WagerTransactionStatus } from '../domain/wager-transaction';
import {
  ExternalTransactionConflictError,
  IdempotencyConflictError,
  InboxPayloadConflictError,
  UniqueConstraintViolation,
} from './errors';
import { LOGGER, METRICS, type AppLogger, type Metrics } from './observability';
import { CLOCK, ID_GENERATOR, TRANSACTION_RUNNER, type Clock, type IdGenerator, type TransactionRunner, type WageringUnitOfWork } from './ports';
import { toView, wagerPayloadHash, type ProcessingMeta, type WagerCommand, type WagerResult } from './wager-command';

export const PENDING_REFERENCE_POLICY = Symbol('PENDING_REFERENCE_POLICY');

export interface PendingReferencePolicy {
  baseDelayMs: number;
  maxDelayMs: number;
  /** Re-checks before giving up. */
  maxAttempts: number;
  /** Absolute age limit, whichever comes first. */
  ttlMs: number;
}

/**
 * The single wagering use case. HTTP and SQS adapters both call `execute`.
 *
 * Concurrency protocol (one SQL transaction, READ COMMITTED):
 *   1. [SQS] INSERT inbox ON CONFLICT DO NOTHING        -> duplicate message => no-op
 *   2. lock-free idempotency fast path (terminal rows are immutable)
 *   3. SELECT wallet FOR UPDATE                         -> serialises every writer of this wallet
 *   4. re-check idempotency key / external id (fresh snapshot after the lock)
 *   5. resolve reference, settle in the domain, persist tx + wallet + ledger + outbox (+ inbox)
 *   6. COMMIT — only then the adapter acks/answers.
 * UNIQUE constraints are the last line of defence: a race that slips through
 * (e.g. same key on two different wallets) ends in a constraint violation, the
 * transaction rolls back and is re-run once, which then takes the replay/conflict path.
 */
@Injectable()
export class ProcessWagerTransactionUseCase {
  constructor(
    @Inject(TRANSACTION_RUNNER) private readonly runner: TransactionRunner,
    @Inject(CLOCK) private readonly clock: Clock,
    @Inject(ID_GENERATOR) private readonly ids: IdGenerator,
    @Inject(PENDING_REFERENCE_POLICY) private readonly policy: PendingReferencePolicy,
    @Inject(METRICS) private readonly metrics: Metrics,
    @Inject(LOGGER) private readonly logger: AppLogger,
  ) {}

  async execute(cmd: WagerCommand, meta: ProcessingMeta): Promise<WagerResult> {
    const started = performance.now();
    const payloadHash = wagerPayloadHash(cmd);
    try {
      const result = await this.executeWithRaceRetry(cmd, payloadHash, meta);
      this.observe(result, cmd, meta);
      return result;
    } finally {
      this.metrics.processingLatency(meta.source, (performance.now() - started) / 1000);
    }
  }

  private async executeWithRaceRetry(cmd: WagerCommand, payloadHash: string, meta: ProcessingMeta): Promise<WagerResult> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.runner.run((uow) => this.process(uow, cmd, payloadHash, meta), 'wager.process');
      } catch (error) {
        if (error instanceof UniqueConstraintViolation && attempt < 3) {
          // Lost a race on a unique key; everything was rolled back. Re-run:
          // the winner is now committed and visible, so we replay or conflict.
          this.metrics.lockConflict('unique_race');
          this.metrics.duplicateDetected('unique_race', meta.source);
          this.logger.warn('unique constraint race, retrying', {
            constraint: error.constraint,
            providerId: cmd.providerId,
            walletId: cmd.walletId,
            correlationId: meta.correlationId,
          });
          continue;
        }
        throw error;
      }
    }
  }

  private async process(uow: WageringUnitOfWork, cmd: WagerCommand, payloadHash: string, meta: ProcessingMeta): Promise<WagerResult> {
    const now = this.clock.now();

    // 1. inbox de-duplication (same SQL transaction as the effect)
    let inbox: InboxMessage | undefined;
    if (meta.inbox) {
      inbox = InboxMessage.receive({ ...meta.inbox, receivedAt: now });
      const inserted = await uow.insertInbox(inbox);
      if (!inserted.inserted) {
        if (inserted.existingPayloadHash !== meta.inbox.payloadHash) throw new InboxPayloadConflictError(meta.inbox.messageId);
        return { type: 'duplicate_message', messageId: meta.inbox.messageId };
      }
    }

    // 2. lock-free fast path for replays
    const early = await uow.findTransactionByIdempotencyKey(cmd.idempotencyKey);
    if (early) return this.complete(uow, inbox, this.replay(early, payloadHash), now);

    const provider = await uow.findProvider(cmd.providerId);
    if (!provider || !provider.active || provider.isInternal) {
      await this.finishInbox(uow, inbox, undefined);
      return { type: 'unpersisted_rejection', failureCode: FailureCode.UnknownProvider };
    }

    // 3. the wallet row lock is the unit of concurrency
    const lockStarted = performance.now();
    const wallet = await uow.lockWallet(cmd.walletId);
    const waited = (performance.now() - lockStarted) / 1000;
    this.metrics.lockWait(waited);
    if (waited > 0.05) this.metrics.lockConflict('contended');
    if (!wallet) {
      await this.finishInbox(uow, inbox, undefined);
      return { type: 'unpersisted_rejection', failureCode: FailureCode.WalletNotFound };
    }

    // 4. re-check under the lock: a concurrent duplicate may have committed while we waited
    const existing = await uow.findTransactionByIdempotencyKey(cmd.idempotencyKey);
    if (existing) return this.complete(uow, inbox, this.replay(existing, payloadHash), now);
    const sameExternal = await uow.findTransactionByExternalId(cmd.providerId, cmd.externalTransactionId);
    if (sameExternal) {
      throw new ExternalTransactionConflictError(cmd.providerId, cmd.externalTransactionId, sameExternal.id);
    }

    // 5. decide and apply
    const tx = WagerTransaction.create({
      id: this.ids.next(),
      providerId: cmd.providerId,
      externalTransactionId: cmd.externalTransactionId,
      idempotencyKey: cmd.idempotencyKey,
      payloadHash,
      walletId: cmd.walletId,
      playerId: cmd.playerId,
      roundId: cmd.roundId,
      gameId: cmd.gameId,
      kind: cmd.kind,
      money: Money.fromNonNegative(cmd.money),
      referenceExternalTransactionId: cmd.referenceExternalTransactionId,
      createdAt: now,
    });
    const expectedVersion = wallet.version;
    const outcome = await this.settle(uow, tx, wallet, now);

    await uow.insertTransaction(tx, meta.correlationId);
    await this.persistOutcome(uow, tx, wallet, expectedVersion, outcome, now, meta.correlationId, meta.inbox?.messageId);
    await this.finishInbox(uow, inbox, tx.id);
    return { type: 'transaction', transaction: toView(tx), idempotentReplay: false };
  }

  /** Shared with the pending-reference worker. Caller holds the wallet lock. */
  async settle(uow: WageringUnitOfWork, tx: WagerTransaction, wallet: Wallet, now: Date): Promise<SettlementOutcome> {
    let reference = null;
    let alreadyReversed = false;
    if (tx.referenceExternalTransactionId !== undefined) {
      reference = await uow.findTransactionByExternalId(tx.providerId, tx.referenceExternalTransactionId);
      if (reference) alreadyReversed = await uow.isReversed(reference.id);
    }
    const nextCheckAt = new Date(now.getTime() + backoffDelayMs(tx.referenceAttempts + 1, this.policy));
    return WagerSettlement.settle(tx, wallet, { transaction: reference, alreadyReversed }, {
      ledgerEntryId: this.ids.next(),
      at: now,
      nextReferenceCheckAt: nextCheckAt,
    });
  }

  /**
   * Persists the effects of a settlement: wallet + ledger (when the balance
   * moved) and the integration events, all in the caller's SQL transaction.
   */
  async persistOutcome(
    uow: WageringUnitOfWork,
    tx: WagerTransaction,
    wallet: Wallet,
    expectedVersion: number,
    outcome: SettlementOutcome,
    now: Date,
    correlationId: string,
    causationId: string | undefined,
    options: { emitPendingEvent?: boolean } = {},
  ): Promise<void> {
    const ctx = (): EventContext => ({
      eventId: this.ids.next(),
      correlationId,
      causationId: causationId ?? tx.id,
      occurredAt: now,
    });
    const events: IntegrationEvent<unknown>[] = [];
    let entry: WalletLedgerEntry | null = null;
    switch (outcome.type) {
      case 'processed':
        entry = outcome.entry;
        events.push(WagerTransactionProcessed.from(tx, ctx()));
        break;
      case 'rejected':
        events.push(WagerTransactionRejected.from(tx, ctx()));
        break;
      case 'pending_reference':
        if (options.emitPendingEvent ?? true) events.push(WagerTransactionPendingReference.from(tx, ctx()));
        break;
    }
    if (entry) {
      await uow.updateWalletBalance(wallet, expectedVersion);
      await uow.appendLedgerEntry(entry);
      events.push(WalletBalanceChanged.from(wallet, entry, ctx()));
    }
    if (events.length > 0) await uow.enqueueEvents(events);
  }

  private replay(existing: WagerTransaction, payloadHash: string): WagerResult {
    if (!existing.matchesPayload(payloadHash)) {
      throw new IdempotencyConflictError(existing.idempotencyKey, existing.id);
    }
    // The stored outcome (status, failureCode, balance observed at that time) is returned as is.
    return { type: 'transaction', transaction: toView(existing), idempotentReplay: true };
  }

  private async complete(uow: WageringUnitOfWork, inbox: InboxMessage | undefined, result: WagerResult, _now: Date): Promise<WagerResult> {
    await this.finishInbox(uow, inbox, result.type === 'transaction' ? result.transaction.transactionId : undefined);
    return result;
  }

  private async finishInbox(uow: WageringUnitOfWork, inbox: InboxMessage | undefined, transactionId: string | undefined): Promise<void> {
    if (!inbox) return;
    inbox.markProcessed(this.clock.now());
    await uow.completeInbox(inbox, transactionId);
  }

  private observe(result: WagerResult, cmd: WagerCommand, meta: ProcessingMeta): void {
    const base = { correlationId: meta.correlationId, providerId: cmd.providerId, walletId: cmd.walletId, messageId: meta.inbox?.messageId };
    switch (result.type) {
      case 'transaction': {
        const t = result.transaction;
        if (result.idempotentReplay) this.metrics.duplicateDetected('idempotent_replay', meta.source);
        else this.metrics.transactionOutcome(t.status, t.kind, meta.source);
        this.logger.info('wager transaction handled', {
          ...base,
          transactionId: t.transactionId,
          kind: t.kind,
          status: t.status,
          failureCode: t.failureCode,
          idempotentReplay: result.idempotentReplay,
        });
        if (t.status === WagerTransactionStatus.PendingReference && !result.idempotentReplay) {
          this.logger.info('reference missing, transaction parked', { ...base, transactionId: t.transactionId });
        }
        break;
      }
      case 'unpersisted_rejection':
        this.metrics.transactionOutcome('REJECTED', cmd.kind, meta.source);
        this.logger.info('wager transaction rejected (not persisted)', { ...base, failureCode: result.failureCode });
        break;
      case 'duplicate_message':
        this.metrics.duplicateDetected('inbox_duplicate', meta.source);
        this.logger.info('duplicate message ignored', base);
        break;
    }
  }
}
