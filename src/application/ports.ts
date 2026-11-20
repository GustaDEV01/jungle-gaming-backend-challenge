import type { IntegrationEvent } from '../domain/events/integration-event';
import type { InboxMessage } from '../domain/inbox-message';
import type { WalletLedgerEntry } from '../domain/ledger-entry';
import type { Wallet } from '../domain/wallet';
import type { WagerTransaction } from '../domain/wager-transaction';

export const CLOCK = Symbol('CLOCK');
export const ID_GENERATOR = Symbol('ID_GENERATOR');
export const TRANSACTION_RUNNER = Symbol('TRANSACTION_RUNNER');
export const READ_MODEL = Symbol('READ_MODEL');

export interface Clock {
  now(): Date;
}

export interface IdGenerator {
  /** Time ordered UUID (v7). */
  next(): string;
}

export interface ProviderRecord {
  id: string;
  active: boolean;
  isInternal: boolean;
}

export type InboxInsertResult = { inserted: true } | { inserted: false; existingPayloadHash: string };

/**
 * Everything a use case may do inside ONE SQL transaction. Implementations
 * must run every method on the same connection/transaction.
 */
export interface WageringUnitOfWork {
  /** INSERT ... ON CONFLICT DO NOTHING on (consumer_name, message_id). Blocks on a concurrent uncommitted duplicate. */
  insertInbox(message: InboxMessage): Promise<InboxInsertResult>;
  completeInbox(message: InboxMessage, transactionId: string | undefined): Promise<void>;

  findProvider(providerId: string): Promise<ProviderRecord | null>;

  /** SELECT ... FOR UPDATE on the wallet row (the unit of concurrency). */
  lockWallet(walletId: string): Promise<Wallet | null>;
  findWallet(walletId: string): Promise<Wallet | null>;
  insertWallet(wallet: Wallet): Promise<void>;
  /** Conditional update on the expected version; throws if no row matched. */
  updateWalletBalance(wallet: Wallet, expectedVersion: number): Promise<void>;

  findTransactionByIdempotencyKey(key: string): Promise<WagerTransaction | null>;
  findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null>;
  /** SELECT ... FOR UPDATE on a transaction row (pending-reference resolution). */
  lockTransaction(id: string): Promise<WagerTransaction | null>;
  /** True if a PROCESSED REFUND/ROLLBACK already reverted this transaction. */
  isReversed(transactionId: string): Promise<boolean>;
  insertTransaction(tx: WagerTransaction, correlationId: string): Promise<void>;
  updateTransaction(tx: WagerTransaction): Promise<void>;

  appendLedgerEntry(entry: WalletLedgerEntry): Promise<void>;
  enqueueEvents(events: IntegrationEvent<unknown>[]): Promise<void>;
}

export interface TransactionRunner {
  /**
   * Runs `work` inside a single SQL transaction (READ COMMITTED) and commits.
   * Retries the whole unit on deadlock / serialization failure (bounded).
   */
  run<T>(work: (uow: WageringUnitOfWork) => Promise<T>, label: string): Promise<T>;
}

export interface LedgerPage {
  entries: WalletLedgerEntry[];
  nextCursor: string | null;
}

export interface ReconciliationData {
  storedBalance: string;
  calculatedBalance: string;
  currency: string;
  checkedEntries: number;
  lastEntryVersion: number | null;
  walletVersion: number;
  chainBreaks: number;
}

/** Read side (no locks). */
export interface WageringReadModel {
  findWallet(walletId: string): Promise<Wallet | null>;
  findTransaction(id: string): Promise<WagerTransaction | null>;
  findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null>;
  /** Keyset pagination ordered by (wallet_version) — stable and append-only friendly. */
  ledgerPage(walletId: string, afterVersion: number | null, limit: number): Promise<LedgerPage>;
  /** Balance rebuilt from the ledger in SQL (sum of signed amounts) + chain verification. */
  reconcile(walletId: string): Promise<ReconciliationData | null>;
}
