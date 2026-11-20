import type { FailureCode } from '../failure-codes';
import type { LedgerDirection, WalletLedgerEntry } from '../ledger-entry';
import type { MoneyProps } from '../money';
import type { Wallet } from '../wallet';
import type { WagerTransaction, WagerTransactionKind } from '../wager-transaction';
import { type EventContext, IntegrationEvent } from './integration-event';

interface TransactionEventBase {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

function base(tx: WagerTransaction): TransactionEventBase {
  const data: TransactionEventBase = {
    transactionId: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    gameId: tx.gameId,
    kind: tx.kind,
    money: tx.money.toJSON(),
  };
  if (tx.referenceExternalTransactionId !== undefined) data.referenceExternalTransactionId = tx.referenceExternalTransactionId;
  return data;
}

export interface WagerTransactionProcessedData extends TransactionEventBase {
  referenceTransactionId?: string;
  balance: MoneyProps;
  processedAt: string;
}

/** Any applied transaction, LOSS included. */
export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    const data: WagerTransactionProcessedData = {
      ...base(tx),
      balance: tx.resultBalance!.toJSON(),
      processedAt: tx.processedAt!.toISOString(),
    };
    if (tx.referenceTransactionId) data.referenceTransactionId = tx.referenceTransactionId;
    return new WagerTransactionProcessed({ ...ctx, aggregateId: tx.walletId, data });
  }
}

export interface WagerTransactionRejectedData extends TransactionEventBase {
  failureCode: FailureCode;
  rejectedAt: string;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    return new WagerTransactionRejected({
      ...ctx,
      aggregateId: tx.walletId,
      data: { ...base(tx), failureCode: tx.failureCode!, rejectedAt: tx.processedAt!.toISOString() },
    });
  }
}

export interface WagerTransactionPendingReferenceData extends TransactionEventBase {
  referenceExternalTransactionId: string;
  attempt: number;
  nextCheckAt: string;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference({
      ...ctx,
      aggregateId: tx.walletId,
      data: {
        ...base(tx),
        referenceExternalTransactionId: tx.referenceExternalTransactionId!,
        attempt: tx.referenceAttempts,
        nextCheckAt: tx.nextReferenceCheckAt!.toISOString(),
      },
    });
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

/** Emitted only when the balance changes. */
export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged({
      ...ctx,
      aggregateId: wallet.id,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: entry.walletVersion,
      },
    });
  }
}
