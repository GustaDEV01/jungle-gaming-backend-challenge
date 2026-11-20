import { CurrencyMismatchError, InsufficientFundsError, InvalidArgumentError } from './errors';
import { LedgerDirection, WalletLedgerEntry } from './ledger-entry';
import { Money } from './money';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface MovementContext {
  transactionId: string;
  ledgerEntryId: string;
  at: Date;
}

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  /** Needed only when initialBalance > 0 (OPENING transaction + CREDIT entry). */
  opening?: { transactionId: string; ledgerEntryId: string };
  at: Date;
}

export interface OpenedWallet {
  wallet: Wallet;
  /** CREDIT entry for the opening balance; null when the wallet opens with zero. */
  openingEntry: WalletLedgerEntry | null;
}

/**
 * Wallet aggregate root. Every balance change goes through debit()/credit(),
 * which return the matching ledger entry, so balance and ledger can only move
 * together. `version` starts at 1 and increments only when the balance changes.
 */
export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: OpenWalletProps): OpenedWallet {
    if (!props.id || !props.playerId) {
      throw new InvalidArgumentError('Wallet id and playerId are required');
    }
    if (props.initialBalance.isNegative()) {
      throw new InvalidArgumentError('Initial balance must not be negative');
    }
    const currency = props.initialBalance.currency;
    const wallet = new Wallet(props.id, props.playerId, currency, props.initialBalance, 1, props.at, props.at);

    if (props.initialBalance.isZero()) {
      return { wallet, openingEntry: null };
    }
    if (!props.opening) {
      throw new InvalidArgumentError('Opening transaction ids are required for a positive initial balance');
    }
    const openingEntry = WalletLedgerEntry.create({
      id: props.opening.ledgerEntryId,
      walletId: wallet.id,
      transactionId: props.opening.transactionId,
      direction: LedgerDirection.Credit,
      money: props.initialBalance,
      balanceBefore: Money.zero(currency),
      balanceAfter: props.initialBalance,
      walletVersion: 1,
      createdAt: props.at,
    });
    return { wallet, openingEntry };
  }

  /** Reconstruction from persistence — does not revalidate. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(state.id, state.playerId, state.currency, state.balance, state.version, state.createdAt, state.updatedAt);
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  canDebit(money: Money): boolean {
    this.assertSameCurrency(money);
    return !this._balance.isLessThan(money);
  }

  /** @throws InsufficientFundsError when the balance would become negative. */
  debit(money: Money, ctx: MovementContext): WalletLedgerEntry {
    this.assertMovement(money);
    if (this._balance.isLessThan(money)) {
      throw new InsufficientFundsError(`Balance ${this._balance} is lower than ${money}`);
    }
    return this.apply(LedgerDirection.Debit, money, ctx);
  }

  credit(money: Money, ctx: MovementContext): WalletLedgerEntry {
    this.assertMovement(money);
    return this.apply(LedgerDirection.Credit, money, ctx);
  }

  /** Applies a movement in an explicit direction (used by reversals). */
  move(direction: LedgerDirection, money: Money, ctx: MovementContext): WalletLedgerEntry {
    return direction === LedgerDirection.Debit ? this.debit(money, ctx) : this.credit(money, ctx);
  }

  toState(): WalletState {
    return {
      id: this.id,
      playerId: this.playerId,
      currency: this.currency,
      balance: this._balance,
      version: this._version,
      createdAt: this.createdAt,
      updatedAt: this._updatedAt,
    };
  }

  private apply(direction: LedgerDirection, money: Money, ctx: MovementContext): WalletLedgerEntry {
    const before = this._balance;
    const after = direction === LedgerDirection.Credit ? before.add(money) : before.subtract(money);
    const nextVersion = this._version + 1;
    // Build (and validate) the entry before mutating state: if the entry is
    // invalid the aggregate stays untouched.
    const entry = WalletLedgerEntry.create({
      id: ctx.ledgerEntryId,
      walletId: this.id,
      transactionId: ctx.transactionId,
      direction,
      money,
      balanceBefore: before,
      balanceAfter: after,
      walletVersion: nextVersion,
      createdAt: ctx.at,
    });
    this._balance = after;
    this._version = nextVersion;
    this._updatedAt = ctx.at;
    return entry;
  }

  private assertMovement(money: Money): void {
    this.assertSameCurrency(money);
    if (!money.isPositive()) {
      throw new InvalidArgumentError('Movement amount must be positive');
    }
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
