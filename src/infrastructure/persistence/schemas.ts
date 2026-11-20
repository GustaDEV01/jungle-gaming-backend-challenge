import { EntitySchema } from '@mikro-orm/core';

/**
 * Persistence records. The domain never sees them: mappers translate
 * records <-> aggregates. Money columns are `numeric(20,2)` mapped as
 * `decimal` => JavaScript string (never number).
 */
export class WalletRecord {
  id!: string;
  playerId!: string;
  currency!: string;
  balanceAmount!: string;
  version!: number;
  createdAt!: Date;
  updatedAt!: Date;
}

export class WagerTransactionRecord {
  id!: string;
  providerId!: string;
  externalTransactionId!: string;
  idempotencyKey!: string;
  payloadHash!: string;
  walletId!: string;
  playerId!: string;
  roundId!: string;
  gameId!: string;
  kind!: string;
  amount!: string;
  currency!: string;
  referenceExternalTransactionId?: string | null;
  referenceTransactionId?: string | null;
  status!: string;
  failureCode?: string | null;
  resultBalanceAmount?: string | null;
  /** Wallet currency of the observed balance (may differ from `currency` on CURRENCY_MISMATCH). */
  resultBalanceCurrency?: string | null;
  referenceAttempts!: number;
  nextReferenceCheckAt?: Date | null;
  createdAt!: Date;
  processedAt?: Date | null;
  correlationId?: string | null;
}

export class LedgerEntryRecord {
  id!: string;
  walletId!: string;
  transactionId!: string;
  direction!: string;
  amount!: string;
  currency!: string;
  balanceBefore!: string;
  balanceAfter!: string;
  walletVersion!: number;
  createdAt!: Date;
}

export class InboxRecord {
  consumerName!: string;
  messageId!: string;
  payloadHash!: string;
  transactionId?: string | null;
  receivedAt!: Date;
  processedAt?: Date | null;
}

export class OutboxRecord {
  id!: string;
  aggregateId!: string;
  eventType!: string;
  payload!: Record<string, unknown>;
  occurredAt!: Date;
  attempts!: number;
  nextAttemptAt!: Date;
  publishedAt?: Date | null;
  lastError?: string | null;
}

export class ProviderEntity {
  id!: string;
  name!: string;
  active!: boolean;
  isInternal!: boolean;
  createdAt!: Date;
}

const money = { type: 'decimal', columnType: 'numeric(20,2)' } as const;
const ts = { type: 'datetime', columnType: 'timestamptz' } as const;

export const WalletSchema = new EntitySchema<WalletRecord>({
  class: WalletRecord,
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'string' },
    currency: { type: 'string', columnType: 'char(3)' },
    balanceAmount: { ...money },
    version: { type: 'integer' },
    createdAt: { ...ts },
    updatedAt: { ...ts },
  },
});

export const WagerTransactionSchema = new EntitySchema<WagerTransactionRecord>({
  class: WagerTransactionRecord,
  tableName: 'wager_transactions',
  properties: {
    id: { type: 'uuid', primary: true },
    providerId: { type: 'string' },
    externalTransactionId: { type: 'string' },
    idempotencyKey: { type: 'string' },
    payloadHash: { type: 'string', columnType: 'char(64)' },
    walletId: { type: 'uuid' },
    playerId: { type: 'string' },
    roundId: { type: 'string' },
    gameId: { type: 'string' },
    kind: { type: 'string' },
    amount: { ...money },
    currency: { type: 'string', columnType: 'char(3)' },
    referenceExternalTransactionId: { type: 'string', nullable: true },
    referenceTransactionId: { type: 'uuid', nullable: true },
    status: { type: 'string' },
    failureCode: { type: 'string', nullable: true },
    resultBalanceAmount: { ...money, nullable: true },
    resultBalanceCurrency: { type: 'string', columnType: 'char(3)', nullable: true },
    referenceAttempts: { type: 'integer' },
    nextReferenceCheckAt: { ...ts, nullable: true },
    createdAt: { ...ts },
    processedAt: { ...ts, nullable: true },
    correlationId: { type: 'string', nullable: true },
  },
});

export const LedgerEntrySchema = new EntitySchema<LedgerEntryRecord>({
  class: LedgerEntryRecord,
  tableName: 'wallet_ledger_entries',
  properties: {
    id: { type: 'uuid', primary: true },
    walletId: { type: 'uuid' },
    transactionId: { type: 'uuid' },
    direction: { type: 'string' },
    amount: { ...money },
    currency: { type: 'string', columnType: 'char(3)' },
    balanceBefore: { ...money },
    balanceAfter: { ...money },
    walletVersion: { type: 'integer' },
    createdAt: { ...ts },
  },
});

export const InboxSchema = new EntitySchema<InboxRecord>({
  class: InboxRecord,
  tableName: 'inbox_messages',
  properties: {
    consumerName: { type: 'string', primary: true },
    messageId: { type: 'string', primary: true },
    payloadHash: { type: 'string', columnType: 'char(64)' },
    transactionId: { type: 'uuid', nullable: true },
    receivedAt: { ...ts },
    processedAt: { ...ts, nullable: true },
  },
});

export const OutboxSchema = new EntitySchema<OutboxRecord>({
  class: OutboxRecord,
  tableName: 'outbox_messages',
  properties: {
    id: { type: 'uuid', primary: true },
    aggregateId: { type: 'string' },
    eventType: { type: 'string' },
    payload: { type: 'json', columnType: 'jsonb' },
    occurredAt: { ...ts },
    attempts: { type: 'integer' },
    nextAttemptAt: { ...ts },
    publishedAt: { ...ts, nullable: true },
    lastError: { type: 'text', nullable: true },
  },
});

export const ProviderSchema = new EntitySchema<ProviderEntity>({
  class: ProviderEntity,
  tableName: 'providers',
  properties: {
    id: { type: 'string', primary: true },
    name: { type: 'string' },
    active: { type: 'boolean' },
    isInternal: { type: 'boolean' },
    createdAt: { ...ts },
  },
});

export const ALL_SCHEMAS = [WalletSchema, WagerTransactionSchema, LedgerEntrySchema, InboxSchema, OutboxSchema, ProviderSchema];
