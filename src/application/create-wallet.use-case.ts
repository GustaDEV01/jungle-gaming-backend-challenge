import { Inject, Injectable } from '@nestjs/common';
import { WagerTransactionProcessed, WalletBalanceChanged } from '../domain/events/wagering-events';
import type { IntegrationEvent } from '../domain/events/integration-event';
import { Money, type MoneyProps } from '../domain/money';
import { Wallet } from '../domain/wallet';
import { WagerTransaction } from '../domain/wager-transaction';
import { canonicalHash } from './canonical-json';
import { UniqueConstraintViolation, WalletAlreadyExistsError } from './errors';
import { LOGGER, type AppLogger } from './observability';
import { CLOCK, ID_GENERATOR, TRANSACTION_RUNNER, type Clock, type IdGenerator, type TransactionRunner } from './ports';

export const INTERNAL_PROVIDER_ID = 'internal';

export interface CreateWalletCommand {
  playerId: string;
  initialBalance: MoneyProps;
}

export interface WalletView {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
  createdAt: string;
  updatedAt: string;
}

export function toWalletView(wallet: Wallet): WalletView {
  return {
    id: wallet.id,
    playerId: wallet.playerId,
    balance: wallet.balance.toJSON(),
    version: wallet.version,
    createdAt: wallet.createdAt.toISOString(),
    updatedAt: wallet.updatedAt.toISOString(),
  };
}

/**
 * Opens a wallet. A positive initial balance creates the internal OPENING
 * transaction and its CREDIT ledger entry in the SAME SQL transaction as the
 * wallet row (plus the outbox events). UNIQUE (player_id, currency) turns a
 * concurrent duplicate into WalletAlreadyExistsError.
 */
@Injectable()
export class CreateWalletUseCase {
  constructor(
    @Inject(TRANSACTION_RUNNER) private readonly runner: TransactionRunner,
    @Inject(CLOCK) private readonly clock: Clock,
    @Inject(ID_GENERATOR) private readonly ids: IdGenerator,
    @Inject(LOGGER) private readonly logger: AppLogger,
  ) {}

  async execute(cmd: CreateWalletCommand, correlationId: string): Promise<WalletView> {
    const initialBalance = Money.fromNonNegative(cmd.initialBalance);
    try {
      return await this.runner.run(async (uow) => {
        const now = this.clock.now();
        const walletId = this.ids.next();
        const openingTxId = this.ids.next();
        const { wallet, openingEntry } = Wallet.open({
          id: walletId,
          playerId: cmd.playerId,
          initialBalance,
          opening: { transactionId: openingTxId, ledgerEntryId: this.ids.next() },
          at: now,
        });
        await uow.insertWallet(wallet);

        if (openingEntry) {
          const externalId = `opening:${walletId}`;
          const opening = WagerTransaction.opening({
            id: openingTxId,
            providerId: INTERNAL_PROVIDER_ID,
            externalTransactionId: externalId,
            idempotencyKey: `${INTERNAL_PROVIDER_ID}:${externalId}`,
            payloadHash: canonicalHash({ walletId, playerId: cmd.playerId, initialBalance: initialBalance.toJSON() }),
            walletId,
            playerId: cmd.playerId,
            roundId: 'opening',
            gameId: 'internal',
            money: initialBalance,
            createdAt: now,
          });
          opening.markProcessed(undefined, now, wallet.balance);
          await uow.insertTransaction(opening, correlationId);
          await uow.appendLedgerEntry(openingEntry);
          const ctx = () => ({ eventId: this.ids.next(), correlationId, causationId: openingTxId, occurredAt: now });
          const events: IntegrationEvent<unknown>[] = [
            WagerTransactionProcessed.from(opening, ctx()),
            WalletBalanceChanged.from(wallet, openingEntry, ctx()),
          ];
          await uow.enqueueEvents(events);
        }
        this.logger.info('wallet opened', { walletId, correlationId, currency: wallet.currency });
        return toWalletView(wallet);
      }, 'wallet.create');
    } catch (error) {
      if (error instanceof UniqueConstraintViolation && error.constraint === 'wallets_player_currency_unique') {
        throw new WalletAlreadyExistsError(cmd.playerId, initialBalance.currency);
      }
      throw error;
    }
  }
}
