import { Inject, Injectable } from '@nestjs/common';
import { FailureCode } from '../domain/failure-codes';
import { WagerSettlement } from '../domain/wager-settlement';
import { WagerTransaction, WagerTransactionStatus } from '../domain/wager-transaction';
import { LOGGER, METRICS, type AppLogger, type Metrics } from './observability';
import { CLOCK, TRANSACTION_RUNNER, type Clock, type TransactionRunner } from './ports';
import {
  PENDING_REFERENCE_POLICY,
  ProcessWagerTransactionUseCase,
  type PendingReferencePolicy,
} from './process-wager-transaction.use-case';

export type ResolutionResult = 'processed' | 'rejected' | 'still_pending' | 'expired' | 'skipped';

/**
 * Re-evaluates one PENDING_REFERENCE transaction. Lock order is the same as
 * the main path (wallet first, then the transaction row) so it cannot deadlock
 * with it. Called by the scheduled worker after it claimed the row.
 */
@Injectable()
export class ResolvePendingReferenceUseCase {
  constructor(
    @Inject(TRANSACTION_RUNNER) private readonly runner: TransactionRunner,
    @Inject(CLOCK) private readonly clock: Clock,
    @Inject(PENDING_REFERENCE_POLICY) private readonly policy: PendingReferencePolicy,
    private readonly wagering: ProcessWagerTransactionUseCase,
    @Inject(METRICS) private readonly metrics: Metrics,
    @Inject(LOGGER) private readonly logger: AppLogger,
  ) {}

  async resolve(transactionId: string, walletId: string): Promise<ResolutionResult> {
    const result = await this.runner.run(async (uow) => {
      const wallet = await uow.lockWallet(walletId);
      const tx = await uow.lockTransaction(transactionId);
      if (!wallet || !tx || tx.status !== WagerTransactionStatus.PendingReference) return 'skipped' as const;

      const now = this.clock.now();
      const expectedVersion = wallet.version;
      const expired =
        tx.referenceAttempts >= this.policy.maxAttempts || now.getTime() - tx.createdAt.getTime() >= this.policy.ttlMs;

      // Even when the budget is exhausted, give the reference one last look:
      // if it arrived in the meantime we must apply, not reject.
      const persisted = tx.toState();
      const outcome = await this.wagering.settle(uow, tx, wallet, now);
      if (outcome.type === 'pending_reference' && expired) {
        // settle() rescheduled the in-memory copy; expire from the persisted state instead.
        const fresh = WagerTransaction.rehydrate(persisted);
        const expiredOutcome = WagerSettlement.expireReference(fresh, wallet, now);
        await uow.updateTransaction(fresh);
        await this.wagering.persistOutcome(uow, fresh, wallet, expectedVersion, expiredOutcome, now, fresh.id, undefined);
        return 'expired' as const;
      }
      await uow.updateTransaction(tx);
      await this.wagering.persistOutcome(uow, tx, wallet, expectedVersion, outcome, now, tx.id, undefined, {
        emitPendingEvent: false, // the PendingReference event was emitted when the transaction was parked
      });
      if (outcome.type === 'processed') return 'processed' as const;
      if (outcome.type === 'rejected') return 'rejected' as const;
      return 'still_pending' as const;
    }, 'wager.resolve_pending_reference');

    this.record(result, transactionId, walletId);
    return result;
  }

  /**
   * Permanent, non-business failure while resolving (e.g. a domain invariant
   * error on corrupted data): the transaction becomes FAILED with
   * INTERNAL_PROCESSING_ERROR — terminal and auditable — instead of looping forever.
   */
  async markFailed(transactionId: string, walletId: string, reason: string): Promise<void> {
    await this.runner.run(async (uow) => {
      const wallet = await uow.lockWallet(walletId);
      const tx = await uow.lockTransaction(transactionId);
      if (!wallet || !tx || tx.status !== WagerTransactionStatus.PendingReference) return;
      tx.fail(FailureCode.InternalProcessingError, this.clock.now());
      await uow.updateTransaction(tx);
    }, 'wager.mark_failed');
    this.metrics.transactionOutcome('FAILED', 'PENDING_REFERENCE_RESOLUTION', 'worker');
    this.logger.error('pending reference marked FAILED', { transactionId, walletId, reason });
  }

  private record(result: ResolutionResult, transactionId: string, walletId: string): void {
    if (result === 'still_pending') this.metrics.retry('pending_reference');
    if (result === 'processed' || result === 'rejected' || result === 'expired') {
      this.metrics.transactionOutcome(result === 'processed' ? 'PROCESSED' : 'REJECTED', 'PENDING_REFERENCE_RESOLUTION', 'worker');
      this.logger.info('pending reference resolved', { transactionId, walletId, result });
    }
  }
}
