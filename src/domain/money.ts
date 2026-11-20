import Decimal from 'decimal.js';
import { CurrencyMismatchError, InvalidMoneyError } from './errors';

/** DTO shape used in every contract (HTTP, SQS, events). */
export interface MoneyProps {
  amount: string;
  currency: string;
}

/**
 * Isolated decimal.js configuration: we never touch the global Decimal
 * settings, so no other library can change our precision/rounding.
 * 40 significant digits is far above numeric(20,2) (18 integer + 2 fraction).
 */
// Rounding mode is irrelevant in practice: add/subtract of scale-2 values is
// exact at this precision, and `ofDecimal` rejects anything with more digits.
const D = Decimal.clone({ precision: 40, rounding: Decimal.ROUND_HALF_EVEN, toExpNeg: -40, toExpPos: 40 });

export const MONEY_SCALE = 2;
/** numeric(20,2) => at most 18 integer digits. */
const MAX_INTEGER_DIGITS = 18;

/**
 * Strict decimal grammar: optional minus sign, integer part without leading
 * zeros, optional fraction of 1..2 digits. Rejects "", "NaN", "Infinity",
 * "1e3", "+1", " 1", "1.", ".5", "1.234", "0x10", "01.00".
 */
const DECIMAL_PATTERN = /^-?(0|[1-9]\d*)(\.\d{1,2})?$/;
const CURRENCY_PATTERN = /^[A-Z]{3}$/;

/**
 * Immutable monetary value object. Internally a decimal.js value with a fixed
 * scale of 2; never a JavaScript `number`.
 */
export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  /**
   * Parses a decimal string. Values with fewer than two decimal places are
   * normalised ("25" -> "25.00"); more than two decimal places are rejected
   * (never rounded silently).
   */
  static from(props: MoneyProps): Money {
    if (props === null || typeof props !== 'object') {
      throw new InvalidMoneyError('Money must be an object { amount, currency }');
    }
    const { amount, currency } = props;
    Money.assertCurrency(currency);
    if (typeof amount !== 'string') {
      throw new InvalidMoneyError('Amount must be a decimal string');
    }
    if (!DECIMAL_PATTERN.test(amount)) {
      throw new InvalidMoneyError(`Invalid amount "${amount}": expected a decimal string with at most ${MONEY_SCALE} decimal places`);
    }
    const integerPart = amount.replace('-', '').split('.')[0] ?? '';
    if (integerPart.length > MAX_INTEGER_DIGITS) {
      throw new InvalidMoneyError(`Amount "${amount}" exceeds ${MAX_INTEGER_DIGITS} integer digits`);
    }
    const value = new D(amount);
    // Normalise "-0" / "-0.00" to zero.
    return new Money(value.isZero() ? new D(0) : value, currency);
  }

  /** Same as `from` but also rejects negative amounts (input contracts). */
  static fromNonNegative(props: MoneyProps): Money {
    const money = Money.from(props);
    if (money.isNegative()) {
      throw new InvalidMoneyError('Amount must not be negative');
    }
    return money;
  }

  static zero(currency: string): Money {
    Money.assertCurrency(currency);
    return new Money(new D(0), currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.ofDecimal(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.ofDecimal(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return Money.ofDecimal(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  isNegative(): boolean {
    return this.value.lessThan(0);
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.value.equals(other.value);
  }

  /** Always exactly two decimal places, never exponential notation. */
  get amount(): string {
    return this.value.toFixed(MONEY_SCALE);
  }

  toJSON(): MoneyProps {
    return { amount: this.amount, currency: this.currency };
  }

  toString(): string {
    return `${this.amount} ${this.currency}`;
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }

  private static ofDecimal(value: Decimal, currency: string): Money {
    // Addition/subtraction of two scale-2 values is always scale-2; this
    // guards against any future operation that could introduce more digits.
    if (value.decimalPlaces() > MONEY_SCALE) {
      throw new InvalidMoneyError('Operation produced more than two decimal places');
    }
    if (value.abs().truncated().toFixed(0).length > MAX_INTEGER_DIGITS) {
      throw new InvalidMoneyError(`Result exceeds ${MAX_INTEGER_DIGITS} integer digits`);
    }
    return new Money(value.isZero() ? new D(0) : value, currency);
  }

  private static assertCurrency(currency: unknown): asserts currency is string {
    if (typeof currency !== 'string' || !CURRENCY_PATTERN.test(currency)) {
      throw new InvalidMoneyError(`Invalid currency "${String(currency)}": expected ISO-4217 alpha code`);
    }
  }
}
