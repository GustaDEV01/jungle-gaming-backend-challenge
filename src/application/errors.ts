/**
 * Application-level errors. Adapters map them to transport semantics:
 * HTTP status codes / SQS ack-retry-DLQ decisions.
 */
export abstract class ApplicationError extends Error {
  abstract readonly code: string;
  protected constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Same idempotency key, different payload hash. Never a replay. */
export class IdempotencyConflictError extends ApplicationError {
  readonly code = 'IDEMPOTENCY_KEY_CONFLICT';
  constructor(
    readonly idempotencyKey: string,
    readonly existingTransactionId: string,
  ) {
    super(`Idempotency key "${idempotencyKey}" was already used with a different payload`);
  }
}

/** (providerId, externalTransactionId) already registered under another idempotency key. */
export class ExternalTransactionConflictError extends ApplicationError {
  readonly code = 'EXTERNAL_TRANSACTION_CONFLICT';
  constructor(
    readonly providerId: string,
    readonly externalTransactionId: string,
    readonly existingTransactionId: string,
  ) {
    super(`Transaction ${providerId}/${externalTransactionId} already exists with another idempotency key`);
  }
}

/** Same inbox messageId with a different payload: the producer reused an id. */
export class InboxPayloadConflictError extends ApplicationError {
  readonly code = 'INBOX_PAYLOAD_CONFLICT';
  constructor(readonly messageId: string) {
    super(`Message ${messageId} was already consumed with a different payload`);
  }
}

export class WalletAlreadyExistsError extends ApplicationError {
  readonly code = 'WALLET_ALREADY_EXISTS';
  constructor(
    readonly playerId: string,
    readonly currency: string,
  ) {
    super(`Wallet already exists for player ${playerId} in ${currency}`);
  }
}

export class NotFoundError extends ApplicationError {
  readonly code = 'NOT_FOUND';
  constructor(message: string) {
    super(message);
  }
}

/**
 * Raised by the persistence adapter when a UNIQUE constraint rejects a write.
 * Use cases translate it (race on idempotency key, duplicate wallet...).
 */
export class UniqueConstraintViolation extends Error {
  constructor(
    readonly constraint: string,
    override readonly cause?: unknown,
  ) {
    super(`Unique constraint violated: ${constraint}`);
    this.name = 'UniqueConstraintViolation';
  }
}

/**
 * Transient infrastructure failure (lock timeout, deadlock retries exhausted,
 * DB/SQS unreachable). Safe to retry with the same idempotency key.
 */
export class TransientInfrastructureError extends ApplicationError {
  readonly code = 'TRANSIENT_INFRASTRUCTURE_ERROR';
  constructor(
    message: string,
    readonly reason: string,
    override readonly cause?: unknown,
  ) {
    super(message);
  }
}
