import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InsufficientFundsError, InvalidArgumentError, UnbalancedLedgerEntryError } from '../../src/domain/errors';
import { LedgerDirection, WalletLedgerEntry } from '../../src/domain/ledger-entry';
import { Money } from '../../src/domain/money';
import { Wallet } from '../../src/domain/wallet';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const at = new Date('2026-11-20T12:00:00Z');
let seq = 0;
const ctx = () => ({ transactionId: `tx-${++seq}`, ledgerEntryId: `le-${seq}`, at });

function openWallet(initial = '100.00') {
  return Wallet.open({ id: 'w-1', playerId: 'p-1', initialBalance: brl(initial), opening: { transactionId: 'open-tx', ledgerEntryId: 'open-le' }, at });
}

describe('Wallet', () => {
  test('opens with version 1 and an OPENING credit entry when the initial balance is positive', () => {
    const { wallet, openingEntry } = openWallet('100.00');
    expect(wallet.version).toBe(1);
    expect(wallet.balance.amount).toBe('100.00');
    expect(openingEntry?.direction).toBe(LedgerDirection.Credit);
    expect(openingEntry?.balanceBefore.amount).toBe('0.00');
    expect(openingEntry?.balanceAfter.amount).toBe('100.00');
    expect(openingEntry?.walletVersion).toBe(1);
  });

  test('opens with zero balance without a ledger entry', () => {
    const { wallet, openingEntry } = openWallet('0.00');
    expect(wallet.version).toBe(1);
    expect(openingEntry).toBeNull();
  });

  test('debit/credit return the matching ledger entry and bump version by exactly 1', () => {
    const { wallet } = openWallet('100.00');
    const d = wallet.debit(brl('80.00'), ctx());
    expect(wallet.balance.amount).toBe('20.00');
    expect(wallet.version).toBe(2);
    expect(d.balanceBefore.amount).toBe('100.00');
    expect(d.balanceAfter.amount).toBe('20.00');
    expect(d.walletVersion).toBe(2);
    const c = wallet.credit(brl('5.50'), ctx());
    expect(wallet.balance.amount).toBe('25.50');
    expect(wallet.version).toBe(3);
    expect(c.isBalanced()).toBe(true);
  });

  test('never goes negative: debit above balance throws and leaves state untouched', () => {
    const { wallet } = openWallet('100.00');
    wallet.debit(brl('80.00'), ctx());
    expect(() => wallet.debit(brl('80.00'), ctx())).toThrow(InsufficientFundsError);
    expect(wallet.balance.amount).toBe('20.00');
    expect(wallet.version).toBe(2);
  });

  test('debit of the whole balance is allowed (reaches exactly zero)', () => {
    const { wallet } = openWallet('10.00');
    wallet.debit(brl('10.00'), ctx());
    expect(wallet.balance.isZero()).toBe(true);
  });

  test('rejects operations in another currency', () => {
    const { wallet } = openWallet('100.00');
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    expect(() => wallet.debit(usd, ctx())).toThrow(CurrencyMismatchError);
    expect(() => wallet.credit(usd, ctx())).toThrow(CurrencyMismatchError);
    expect(wallet.version).toBe(1);
  });

  test('rejects zero or negative movements (no ledger entry without financial effect)', () => {
    const { wallet } = openWallet('100.00');
    expect(() => wallet.credit(brl('0.00'), ctx())).toThrow(InvalidArgumentError);
    expect(() => wallet.debit(brl('-1.00'), ctx())).toThrow(InvalidArgumentError);
    expect(wallet.version).toBe(1);
  });

  test('rehydrate does not revalidate', () => {
    const w = Wallet.rehydrate({ id: 'w', playerId: 'p', currency: 'BRL', balance: brl('3.00'), version: 7, createdAt: at, updatedAt: at });
    expect(w.version).toBe(7);
    expect(w.balance.amount).toBe('3.00');
  });
});

describe('WalletLedgerEntry', () => {
  const base = {
    id: 'e',
    walletId: 'w',
    transactionId: 't',
    walletVersion: 2,
    createdAt: at,
  };

  test('create validates the arithmetic', () => {
    expect(() =>
      WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Debit, money: brl('10.00'), balanceBefore: brl('50.00'), balanceAfter: brl('45.00') }),
    ).toThrow(UnbalancedLedgerEntryError);
    const ok = WalletLedgerEntry.create({
      ...base,
      direction: LedgerDirection.Debit,
      money: brl('10.00'),
      balanceBefore: brl('50.00'),
      balanceAfter: brl('40.00'),
    });
    expect(ok.isBalanced()).toBe(true);
    expect(ok.signedAmount().amount).toBe('-10.00');
  });

  test('rejects negative balances and non-positive amounts', () => {
    expect(() =>
      WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Debit, money: brl('10.00'), balanceBefore: brl('5.00'), balanceAfter: brl('-5.00') }),
    ).toThrow(UnbalancedLedgerEntryError);
    expect(() =>
      WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Credit, money: brl('0.00'), balanceBefore: brl('5.00'), balanceAfter: brl('5.00') }),
    ).toThrow(InvalidArgumentError);
  });

  test('is structurally immutable', () => {
    const e = WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Credit, money: brl('1.00'), balanceBefore: brl('0.00'), balanceAfter: brl('1.00') });
    expect(Object.isFrozen(e)).toBe(true);
    expect(() => {
      (e as unknown as { direction: string }).direction = 'DEBIT';
    }).toThrow();
  });
});
