/**
 * Domain errors. They carry a stable, machine readable `code` so adapters
 * (HTTP, SQS) can map them without parsing messages.
 */
export abstract class DomainError extends Error {
  abstract readonly code: string;

  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Invalid value supplied to a value object or factory (e.g. a malformed amount). */
export class InvalidMoneyError extends DomainError {
  readonly code = 'INVALID_MONEY';
  constructor(message: string) {
    super(message);
  }
}

export class CurrencyMismatchError extends DomainError {
  readonly code = 'CURRENCY_MISMATCH';
  constructor(
    readonly expected: string,
    readonly actual: string,
  ) {
    super(`Currency mismatch: expected ${expected}, got ${actual}`);
  }
}

export class InsufficientFundsError extends DomainError {
  readonly code = 'INSUFFICIENT_FUNDS';
  constructor(message = 'Insufficient funds') {
    super(message);
  }
}

export class InvalidArgumentError extends DomainError {
  readonly code = 'INVALID_ARGUMENT';
  constructor(message: string) {
    super(message);
  }
}

/** Ledger arithmetic does not hold (balanceBefore ± money !== balanceAfter). */
export class UnbalancedLedgerEntryError extends DomainError {
  readonly code = 'UNBALANCED_LEDGER_ENTRY';
  constructor(message: string) {
    super(message);
  }
}

/**
 * Attempt to transition a transaction out of a terminal state. This is a
 * programming error, never a business path.
 */
export class InvalidTransactionStateError extends DomainError {
  readonly code = 'INVALID_TRANSACTION_STATE';
  constructor(message: string) {
    super(message);
  }
}
