import { Inject, Injectable } from '@nestjs/common';
import { Money, type MoneyProps } from '../domain/money';
import type { LedgerDirection } from '../domain/ledger-entry';
import { InvalidArgumentError } from '../domain/errors';
import { NotFoundError } from './errors';
import { LOGGER, METRICS, type AppLogger, type Metrics } from './observability';
import { READ_MODEL, type WageringReadModel } from './ports';
import { toView, type WagerTransactionView } from './wager-command';
import { toWalletView, type WalletView } from './create-wallet.use-case';

export interface LedgerEntryView {
  id: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
  createdAt: string;
}

export interface LedgerPageView {
  walletId: string;
  entries: LedgerEntryView[];
  nextCursor: string | null;
}

export interface ReconciliationView {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
  walletVersion: number;
  lastEntryVersion: number | null;
  chainBreaks: number;
  checkedAt: string;
}

/**
 * Opaque cursor: base64url of {"v":<walletVersion>}. Ledger rows are append
 * only and wallet_version is unique per wallet, so keyset pagination on it is
 * stable: no row is skipped or repeated while new entries are appended.
 */
export function encodeCursor(walletVersion: number): string {
  return Buffer.from(JSON.stringify({ v: walletVersion }), 'utf8').toString('base64url');
}

export function decodeCursor(cursor: string): number {
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as { v?: unknown };
    if (typeof parsed.v === 'number' && Number.isInteger(parsed.v) && parsed.v >= 0) return parsed.v;
  } catch {
    /* fallthrough */
  }
  throw new InvalidArgumentError('Invalid cursor');
}

@Injectable()
export class WageringQueries {
  constructor(
    @Inject(READ_MODEL) private readonly read: WageringReadModel,
    @Inject(METRICS) private readonly metrics: Metrics,
    @Inject(LOGGER) private readonly logger: AppLogger,
  ) {}

  async wallet(walletId: string): Promise<WalletView> {
    const wallet = await this.read.findWallet(walletId);
    if (!wallet) throw new NotFoundError(`Wallet ${walletId} not found`);
    return toWalletView(wallet);
  }

  async transaction(id: string): Promise<WagerTransactionView> {
    const tx = await this.read.findTransaction(id);
    if (!tx) throw new NotFoundError(`Transaction ${id} not found`);
    return toView(tx);
  }

  async transactionByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransactionView> {
    const tx = await this.read.findTransactionByExternalId(providerId, externalTransactionId);
    if (!tx) throw new NotFoundError(`Transaction ${providerId}/${externalTransactionId} not found`);
    return toView(tx);
  }

  async ledger(walletId: string, cursor: string | undefined, limit: number): Promise<LedgerPageView> {
    const wallet = await this.read.findWallet(walletId);
    if (!wallet) throw new NotFoundError(`Wallet ${walletId} not found`);
    const after = cursor ? decodeCursor(cursor) : null;
    const page = await this.read.ledgerPage(walletId, after, limit);
    return {
      walletId,
      entries: page.entries.map((e) => ({
        id: e.id,
        transactionId: e.transactionId,
        direction: e.direction,
        money: e.money.toJSON(),
        balanceBefore: e.balanceBefore.toJSON(),
        balanceAfter: e.balanceAfter.toJSON(),
        walletVersion: e.walletVersion,
        createdAt: e.createdAt.toISOString(),
      })),
      nextCursor: page.nextCursor,
    };
  }

  /**
   * Rebuilds the balance from the ledger and compares it with the stored
   * balance. Divergences are NOT corrected: they are logged, counted and
   * reported.
   */
  async reconcile(walletId: string): Promise<ReconciliationView> {
    const data = await this.read.reconcile(walletId);
    if (!data) throw new NotFoundError(`Wallet ${walletId} not found`);
    const stored = Money.from({ amount: data.storedBalance, currency: data.currency });
    const calculated = Money.from({ amount: data.calculatedBalance, currency: data.currency });
    const difference = stored.subtract(calculated);
    const versionsMatch = data.lastEntryVersion === null ? data.walletVersion === 1 : data.lastEntryVersion === data.walletVersion;
    const consistent = difference.isZero() && data.chainBreaks === 0 && versionsMatch;
    this.metrics.reconciliation(consistent);
    if (!consistent) {
      this.logger.error('reconciliation divergence detected', {
        walletId,
        difference: difference.amount,
        chainBreaks: data.chainBreaks,
        walletVersion: data.walletVersion,
        lastEntryVersion: data.lastEntryVersion,
      });
    }
    return {
      walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: data.checkedEntries,
      walletVersion: data.walletVersion,
      lastEntryVersion: data.lastEntryVersion,
      chainBreaks: data.chainBreaks,
      checkedAt: new Date().toISOString(),
    };
  }
}
