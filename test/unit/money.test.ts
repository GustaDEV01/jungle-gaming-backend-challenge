import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InvalidMoneyError } from '../../src/domain/errors';
import { Money } from '../../src/domain/money';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

describe('Money', () => {
  test('keeps a fixed scale of 2 and serialises as decimal string', () => {
    expect(brl('25').toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
    expect(brl('25.5').amount).toBe('25.50');
    expect(brl('0.01').amount).toBe('0.01');
    expect(JSON.stringify(brl('1000.00'))).toBe('{"amount":"1000.00","currency":"BRL"}');
  });

  test('exact decimal arithmetic (no float drift)', () => {
    expect(brl('0.10').add(brl('0.20')).amount).toBe('0.30');
    expect(brl('100.00').subtract(brl('80.00')).amount).toBe('20.00');
    expect(brl('999999999999999999.99').subtract(brl('0.01')).amount).toBe('999999999999999999.98');
    let total = Money.zero('BRL');
    for (let i = 0; i < 1000; i++) total = total.add(brl('0.01'));
    expect(total.amount).toBe('10.00');
  });

  test('is immutable: operations return new instances', () => {
    const a = brl('10.00');
    const b = a.add(brl('5.00'));
    expect(a.amount).toBe('10.00');
    expect(b.amount).toBe('15.00');
    expect(Object.isFrozen(a)).toBe(true);
    expect(() => {
      (a as unknown as { currency: string }).currency = 'USD';
    }).toThrow();
  });

  test.each([
    ['NaN'],
    ['Infinity'],
    ['-Infinity'],
    ['1e3'],
    ['1E3'],
    ['2.5e-1'],
    [''],
    [' 1.00'],
    ['1.00 '],
    ['+1.00'],
    ['1.234'],
    ['0.001'],
    ['1.'],
    ['.5'],
    ['01.00'],
    ['0x10'],
    ['1,00'],
    ['1_000'],
    ['1234567890123456789.00'], // 19 integer digits > numeric(20,2)
  ])('rejects invalid amount %p', (amount) => {
    expect(() => brl(amount)).toThrow(InvalidMoneyError);
  });

  test('rejects non-string amounts (numbers never accepted)', () => {
    expect(() => Money.from({ amount: 25 as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: Number.NaN as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test('never rounds silently: more than 2 decimals is an error, not a rounding', () => {
    expect(() => brl('10.005')).toThrow(InvalidMoneyError);
    expect(() => brl('10.999')).toThrow(InvalidMoneyError);
  });

  test('rejects invalid currency codes', () => {
    for (const currency of ['brl', 'BR', 'BRLL', '', '12A']) {
      expect(() => Money.from({ amount: '1.00', currency })).toThrow(InvalidMoneyError);
    }
  });

  test('negative values: allowed internally, rejected on input contracts', () => {
    expect(brl('-5.00').isNegative()).toBe(true);
    expect(brl('5.00').negate().amount).toBe('-5.00');
    expect(() => Money.fromNonNegative({ amount: '-5.00', currency: 'BRL' })).toThrow(InvalidMoneyError);
    expect(brl('-0.00').isNegative()).toBe(false);
    expect(brl('-0.00').amount).toBe('0.00');
  });

  test('comparisons', () => {
    expect(brl('0.00').isZero()).toBe(true);
    expect(brl('0.01').isPositive()).toBe(true);
    expect(brl('79.99').isLessThan(brl('80.00'))).toBe(true);
    expect(brl('80.00').isLessThan(brl('80.00'))).toBe(false);
    expect(brl('80').equals(brl('80.00'))).toBe(true);
    expect(brl('80.00').equals(Money.from({ amount: '80.00', currency: 'USD' }))).toBe(false);
  });

  test('operations between different currencies throw a domain error', () => {
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    expect(() => brl('1.00').add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl('1.00').subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl('1.00').isLessThan(usd)).toThrow(CurrencyMismatchError);
  });

  test('overflow beyond numeric(20,2) is rejected', () => {
    expect(() => brl('999999999999999999.99').add(brl('0.01'))).toThrow(InvalidMoneyError);
  });
});
