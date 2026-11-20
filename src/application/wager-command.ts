import { FailureCode } from '../domain/failure-codes';
import { Money, type MoneyProps } from '../domain/money';
import type { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '../domain/wager-transaction';
import { canonicalHash } from './canonical-json';

/** Validated input, identical for HTTP and SQS. */
export interface WagerCommand {
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: Exclude<WagerTransactionKind, WagerTransactionKind.Opening>;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export interface ProcessingMeta {
  correlationId: string;
  source: 'http' | 'sqs';
  /** Present when the command came from a queue: dedup record written in the same SQL transaction. */
  inbox?: { consumerName: string; messageId: string; payloadHash: string };
}

/**
 * payloadHash = SHA-256 of the canonical JSON of the business fields only.
 * The idempotency key (header / data.idempotencyKey) and transport metadata
 * (messageId, occurredAt, correlation id) are NOT part of it. The amount is
 * normalised to scale 2 first, so "25" and "25.00" hash the same.
 */
export function wagerPayloadHash(cmd: WagerCommand): string {
  const money = Money.from(cmd.money);
  return canonicalHash({
    providerId: cmd.providerId,
    externalTransactionId: cmd.externalTransactionId,
    playerId: cmd.playerId,
    walletId: cmd.walletId,
    roundId: cmd.roundId,
    gameId: cmd.gameId,
    kind: cmd.kind,
    money: { amount: money.amount, currency: money.currency },
    referenceExternalTransactionId: cmd.referenceExternalTransactionId,
  });
}

export interface WagerTransactionView {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  status: WagerTransactionStatus;
  referenceExternalTransactionId: string | null;
  referenceTransactionId: string | null;
  failureCode: FailureCode | null;
  /** Balance observed when the outcome was decided (null while PENDING_REFERENCE). */
  balance: MoneyProps | null;
  createdAt: string;
  processedAt: string | null;
}

export function toView(tx: WagerTransaction): WagerTransactionView {
  return {
    transactionId: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    gameId: tx.gameId,
    kind: tx.kind,
    money: tx.money.toJSON(),
    status: tx.status,
    referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
    referenceTransactionId: tx.referenceTransactionId ?? null,
    failureCode: tx.failureCode ?? null,
    balance: tx.resultBalance ? tx.resultBalance.toJSON() : null,
    createdAt: tx.createdAt.toISOString(),
    processedAt: tx.processedAt ? tx.processedAt.toISOString() : null,
  };
}

export type WagerResult =
  | { type: 'transaction'; transaction: WagerTransactionView; idempotentReplay: boolean }
  /** Business rejection with no aggregate to attach to (unknown wallet/provider): nothing persisted. */
  | { type: 'unpersisted_rejection'; failureCode: typeof FailureCode.WalletNotFound | typeof FailureCode.UnknownProvider }
  /** Queue redelivery of an already committed message. */
  | { type: 'duplicate_message'; messageId: string };
