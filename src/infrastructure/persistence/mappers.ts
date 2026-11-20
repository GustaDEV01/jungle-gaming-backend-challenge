import type { FailureCode } from '../../domain/failure-codes';
import { type LedgerDirection, WalletLedgerEntry } from '../../domain/ledger-entry';
import { Money } from '../../domain/money';
import { Wallet } from '../../domain/wallet';
import { WagerTransaction, type WagerTransactionKind, type WagerTransactionStatus } from '../../domain/wager-transaction';
import type { LedgerEntryRecord, WagerTransactionRecord, WalletRecord } from './schemas';

/** The pg driver returns numeric as string; trim the char(n) padding defensively. */
const money = (amount: string, currency: string): Money => Money.from({ amount: String(amount), currency: currency.trim() });
const date = (value: Date | string): Date => (value instanceof Date ? value : new Date(value));

export function walletToDomain(r: WalletRecord): Wallet {
  return Wallet.rehydrate({
    id: r.id,
    playerId: r.playerId,
    currency: r.currency.trim(),
    balance: money(r.balanceAmount, r.currency),
    version: r.version,
    createdAt: date(r.createdAt),
    updatedAt: date(r.updatedAt),
  });
}

export function walletToRecord(w: Wallet): WalletRecord {
  return {
    id: w.id,
    playerId: w.playerId,
    currency: w.currency,
    balanceAmount: w.balance.amount,
    version: w.version,
    createdAt: w.createdAt,
    updatedAt: w.updatedAt,
  };
}

export function transactionToDomain(r: WagerTransactionRecord): WagerTransaction {
  const currency = r.currency.trim();
  return WagerTransaction.rehydrate({
    id: r.id,
    providerId: r.providerId,
    externalTransactionId: r.externalTransactionId,
    idempotencyKey: r.idempotencyKey,
    payloadHash: r.payloadHash.trim(),
    walletId: r.walletId,
    playerId: r.playerId,
    roundId: r.roundId,
    gameId: r.gameId,
    kind: r.kind as WagerTransactionKind,
    money: money(r.amount, currency),
    referenceExternalTransactionId: r.referenceExternalTransactionId ?? undefined,
    createdAt: date(r.createdAt),
    status: r.status as WagerTransactionStatus,
    referenceTransactionId: r.referenceTransactionId ?? undefined,
    failureCode: (r.failureCode ?? undefined) as FailureCode | undefined,
    processedAt: r.processedAt ? date(r.processedAt) : undefined,
    // The observed balance is in the WALLET currency (AUD-01), not necessarily the operation currency.
    resultBalance: r.resultBalanceAmount != null ? money(r.resultBalanceAmount, r.resultBalanceCurrency ?? currency) : undefined,
    referenceAttempts: r.referenceAttempts,
    nextReferenceCheckAt: r.nextReferenceCheckAt ? date(r.nextReferenceCheckAt) : undefined,
  });
}

export function transactionToRecord(tx: WagerTransaction, correlationId: string | null): WagerTransactionRecord {
  return {
    id: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    idempotencyKey: tx.idempotencyKey,
    payloadHash: tx.payloadHash,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    gameId: tx.gameId,
    kind: tx.kind,
    amount: tx.money.amount,
    currency: tx.money.currency,
    referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
    referenceTransactionId: tx.referenceTransactionId ?? null,
    status: tx.status,
    failureCode: tx.failureCode ?? null,
    resultBalanceAmount: tx.resultBalance ? tx.resultBalance.amount : null,
    resultBalanceCurrency: tx.resultBalance ? tx.resultBalance.currency : null,
    referenceAttempts: tx.referenceAttempts,
    nextReferenceCheckAt: tx.nextReferenceCheckAt ?? null,
    createdAt: tx.createdAt,
    processedAt: tx.processedAt ?? null,
    correlationId,
  };
}

export function ledgerToDomain(r: LedgerEntryRecord): WalletLedgerEntry {
  const currency = r.currency.trim();
  return WalletLedgerEntry.rehydrate({
    id: r.id,
    walletId: r.walletId,
    transactionId: r.transactionId,
    direction: r.direction as LedgerDirection,
    money: money(r.amount, currency),
    balanceBefore: money(r.balanceBefore, currency),
    balanceAfter: money(r.balanceAfter, currency),
    walletVersion: r.walletVersion,
    createdAt: date(r.createdAt),
  });
}

export function ledgerToRecord(e: WalletLedgerEntry): LedgerEntryRecord {
  return {
    id: e.id,
    walletId: e.walletId,
    transactionId: e.transactionId,
    direction: e.direction,
    amount: e.money.amount,
    currency: e.money.currency,
    balanceBefore: e.balanceBefore.amount,
    balanceAfter: e.balanceAfter.amount,
    walletVersion: e.walletVersion,
    createdAt: e.createdAt,
  };
}
