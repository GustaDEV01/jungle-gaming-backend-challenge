import { FailureCode } from './failure-codes';
import { LedgerDirection, WalletLedgerEntry } from './ledger-entry';
import { Wallet } from './wallet';
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from './wager-transaction';

export interface ReferenceLookup {
  /** Transaction found by (providerId, referenceExternalTransactionId), or null. */
  transaction: WagerTransaction | null;
  /** True when a PROCESSED REFUND/ROLLBACK already reverted the reference. */
  alreadyReversed: boolean;
}

export interface SettlementContext {
  ledgerEntryId: string;
  at: Date;
  /** When the reference is missing: when to look again. */
  nextReferenceCheckAt: Date;
}

export type SettlementOutcome =
  | { type: 'processed'; entry: WalletLedgerEntry | null }
  | { type: 'rejected'; code: FailureCode }
  | { type: 'pending_reference' };

/**
 * Pure domain service that decides and applies the outcome of a wager
 * transaction against a wallet. It must be called while the caller holds an
 * exclusive lock on the wallet (the application layer guarantees that).
 *
 * The transaction (PENDING or PENDING_REFERENCE) and the wallet are mutated
 * in memory; persistence is the caller's job, inside one SQL transaction.
 */
export class WagerSettlement {
  static settle(tx: WagerTransaction, wallet: Wallet, reference: ReferenceLookup, ctx: SettlementContext): SettlementOutcome {
    if (tx.walletId !== wallet.id) {
      throw new Error(`Settlement wiring error: transaction wallet ${tx.walletId} != ${wallet.id}`);
    }
    const reject = (code: FailureCode, referenceId?: string): SettlementOutcome => {
      tx.reject(code, ctx.at, wallet.balance, referenceId);
      return { type: 'rejected', code };
    };

    if (tx.playerId !== wallet.playerId) return reject(FailureCode.PlayerWalletMismatch);
    if (tx.money.currency !== wallet.currency) return reject(FailureCode.CurrencyMismatch);

    let ref: WagerTransaction | undefined;
    if (tx.referenceExternalTransactionId !== undefined) {
      const found = reference.transaction;
      if (!found) {
        tx.markPendingReference(ctx.nextReferenceCheckAt);
        return { type: 'pending_reference' };
      }
      if (
        found.providerId !== tx.providerId ||
        found.playerId !== tx.playerId ||
        found.walletId !== tx.walletId ||
        found.money.currency !== tx.money.currency ||
        found.roundId !== tx.roundId
      ) {
        return reject(FailureCode.ReferenceMismatch, found.id);
      }
      if (!tx.canReference(found)) return reject(FailureCode.InvalidReferenceKind, found.id);

      if (found.status === WagerTransactionStatus.PendingReference || found.status === WagerTransactionStatus.Pending) {
        // The reference exists but is itself waiting (e.g. ROLLBACK of a WIN whose BET is missing).
        tx.markPendingReference(ctx.nextReferenceCheckAt);
        return { type: 'pending_reference' };
      }
      if (found.status !== WagerTransactionStatus.Processed) return reject(FailureCode.ReferenceNotProcessed, found.id);

      if (tx.isReversal() && !tx.money.equals(found.money)) return reject(FailureCode.ReversalAmountMismatch, found.id);
      // A transaction is reverted at most once, whichever reversal kind does it.
      // WIN on an already refunded/rolled back BET is also refused.
      if (reference.alreadyReversed && tx.kind !== WagerTransactionKind.Loss) {
        return reject(FailureCode.ReferenceAlreadyReversed, found.id);
      }
      ref = found;
    }

    if (!tx.affectsBalance()) {
      tx.markProcessed(ref?.id, ctx.at, wallet.balance);
      return { type: 'processed', entry: null };
    }

    const direction = tx.ledgerDirectionFor(ref);
    if (direction === LedgerDirection.Debit && !wallet.canDebit(tx.money)) {
      return reject(tx.isReversal() ? FailureCode.ReversalInsufficientFunds : FailureCode.InsufficientFunds, ref?.id);
    }
    const entry = wallet.move(direction, tx.money, { transactionId: tx.id, ledgerEntryId: ctx.ledgerEntryId, at: ctx.at });
    tx.markProcessed(ref?.id, ctx.at, wallet.balance);
    return { type: 'processed', entry };
  }

  /** Retry budget exhausted for a PENDING_REFERENCE transaction. */
  static expireReference(tx: WagerTransaction, wallet: Wallet, at: Date): SettlementOutcome {
    tx.reject(FailureCode.ReferenceNotFound, at, wallet.balance);
    return { type: 'rejected', code: FailureCode.ReferenceNotFound };
  }
}
