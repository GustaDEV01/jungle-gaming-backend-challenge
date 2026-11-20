import { InvalidArgumentError, InvalidTransactionStateError } from './errors';
import type { FailureCode } from './failure-codes';
import { LedgerDirection } from './ledger-entry';
import { Money } from './money';

export enum WagerTransactionKind {
  Opening = 'OPENING', // internal: wallet opening credit
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  Pending = 'PENDING', // accepted, not yet applied (in-memory only within a SQL transaction)
  PendingReference = 'PENDING_REFERENCE', // waiting for the referenced transaction
  Processed = 'PROCESSED', // applied (terminal)
  Rejected = 'REJECTED', // business rule violation (terminal)
  Failed = 'FAILED', // permanent infrastructure error (terminal, auditable)
}

/** Kinds that external providers may submit (OPENING is internal only). */
export const EXTERNAL_KINDS = [
  WagerTransactionKind.Bet,
  WagerTransactionKind.Win,
  WagerTransactionKind.Loss,
  WagerTransactionKind.Refund,
  WagerTransactionKind.Rollback,
] as const;

const TERMINAL = new Set<WagerTransactionStatus>([
  WagerTransactionStatus.Processed,
  WagerTransactionStatus.Rejected,
  WagerTransactionStatus.Failed,
]);

/**
 * Valid transitions. Terminal states have no outgoing edge.
 * PENDING_REFERENCE -> PENDING_REFERENCE is the "still missing, retry later" reschedule.
 */
const TRANSITIONS: Record<WagerTransactionStatus, readonly WagerTransactionStatus[]> = {
  [WagerTransactionStatus.Pending]: [
    WagerTransactionStatus.PendingReference,
    WagerTransactionStatus.Processed,
    WagerTransactionStatus.Rejected,
    WagerTransactionStatus.Failed,
  ],
  [WagerTransactionStatus.PendingReference]: [
    WagerTransactionStatus.PendingReference,
    WagerTransactionStatus.Processed,
    WagerTransactionStatus.Rejected,
    WagerTransactionStatus.Failed,
  ],
  [WagerTransactionStatus.Processed]: [],
  [WagerTransactionStatus.Rejected]: [],
  [WagerTransactionStatus.Failed]: [],
};

/** Which kinds each kind may reference. */
const ALLOWED_REFERENCE_KINDS: Partial<Record<WagerTransactionKind, readonly WagerTransactionKind[]>> = {
  [WagerTransactionKind.Win]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Loss]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Refund]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Rollback]: [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Refund],
};

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
}

export interface WagerTransactionState extends CreateWagerTransactionProps {
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
  /** Wallet balance observed when the outcome was decided (returned on replay). */
  resultBalance?: Money;
  referenceAttempts: number;
  nextReferenceCheckAt?: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    /** provider id of the referenced transaction — not the internal id */
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
    private _resultBalance?: Money,
    private _referenceAttempts: number = 0,
    private _nextReferenceCheckAt?: Date,
  ) {}

  /** Born PENDING. Validates reference requirements per kind. OPENING is rejected here. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (props.kind === WagerTransactionKind.Opening) {
      throw new InvalidArgumentError('OPENING is internal and cannot be submitted');
    }
    return WagerTransaction.build(props);
  }

  /** Internal OPENING credit created together with the wallet. */
  static opening(props: Omit<CreateWagerTransactionProps, 'kind' | 'referenceExternalTransactionId'>): WagerTransaction {
    return WagerTransaction.build({ ...props, kind: WagerTransactionKind.Opening });
  }

  static rehydrate(s: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      s.id,
      s.providerId,
      s.externalTransactionId,
      s.idempotencyKey,
      s.payloadHash,
      s.walletId,
      s.playerId,
      s.roundId,
      s.gameId,
      s.kind,
      s.money,
      s.referenceExternalTransactionId,
      s.createdAt,
      s.status,
      s.referenceTransactionId,
      s.failureCode,
      s.processedAt,
      s.resultBalance,
      s.referenceAttempts,
      s.nextReferenceCheckAt,
    );
  }

  private static build(props: CreateWagerTransactionProps): WagerTransaction {
    const required: (keyof CreateWagerTransactionProps)[] = [
      'id',
      'providerId',
      'externalTransactionId',
      'idempotencyKey',
      'payloadHash',
      'walletId',
      'playerId',
      'roundId',
      'gameId',
    ];
    for (const key of required) {
      if (typeof props[key] !== 'string' || (props[key] as string).length === 0) {
        throw new InvalidArgumentError(`${key} is required`);
      }
    }
    if (props.money.isNegative()) {
      throw new InvalidArgumentError('Amount must not be negative');
    }
    const kind = props.kind;
    const ref = props.referenceExternalTransactionId;
    const requiresReference = kind === WagerTransactionKind.Refund || kind === WagerTransactionKind.Rollback;
    if (requiresReference && !ref) {
      throw new InvalidArgumentError(`${kind} requires referenceExternalTransactionId`);
    }
    if ((kind === WagerTransactionKind.Bet || kind === WagerTransactionKind.Opening) && ref) {
      throw new InvalidArgumentError(`${kind} must not reference another transaction`);
    }
    if (ref !== undefined && ref === props.externalTransactionId) {
      throw new InvalidArgumentError('A transaction cannot reference itself');
    }
    // Every kind that moves balance needs a positive amount; LOSS may carry 0.
    if (kind !== WagerTransactionKind.Loss && !props.money.isPositive()) {
      throw new InvalidArgumentError(`${kind} amount must be positive`);
    }
    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      kind,
      props.money,
      ref,
      props.createdAt,
      WagerTransactionStatus.Pending,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }
  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }
  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }
  get processedAt(): Date | undefined {
    return this._processedAt;
  }
  get resultBalance(): Money | undefined {
    return this._resultBalance;
  }
  get referenceAttempts(): number {
    return this._referenceAttempts;
  }
  get nextReferenceCheckAt(): Date | undefined {
    return this._nextReferenceCheckAt;
  }

  // ---- transitions (throw InvalidTransactionStateError from terminal states)

  markProcessed(referenceTransactionId: string | undefined, at: Date, balanceAfter: Money): void {
    this.transitionTo(WagerTransactionStatus.Processed);
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
    this._resultBalance = balanceAfter;
    this._nextReferenceCheckAt = undefined;
  }

  /** Reference not available yet: schedule a re-check. Increments the attempt counter. */
  markPendingReference(nextCheckAt: Date): void {
    this.transitionTo(WagerTransactionStatus.PendingReference);
    this._referenceAttempts += 1;
    this._nextReferenceCheckAt = nextCheckAt;
  }

  reject(code: FailureCode, at: Date, observedBalance: Money | undefined, referenceTransactionId?: string): void {
    this.transitionTo(WagerTransactionStatus.Rejected);
    this._failureCode = code;
    this._processedAt = at;
    this._resultBalance = observedBalance;
    this._referenceTransactionId = referenceTransactionId;
    this._nextReferenceCheckAt = undefined;
  }

  fail(code: FailureCode, at: Date): void {
    this.transitionTo(WagerTransactionStatus.Failed);
    this._failureCode = code;
    this._processedAt = at;
    this._nextReferenceCheckAt = undefined;
  }

  // ---- domain queries

  isTerminal(): boolean {
    return TERMINAL.has(this._status);
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  isReversal(): boolean {
    return this.requiresReference();
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  canReference(reference: WagerTransaction): boolean {
    return (ALLOWED_REFERENCE_KINDS[this.kind] ?? []).includes(reference.kind);
  }

  /**
   * Direction of the ledger entry this transaction produces. ROLLBACK inverts
   * the direction of its reference (BET -> CREDIT, WIN/REFUND -> DEBIT).
   */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback: {
        if (!reference) {
          throw new InvalidArgumentError('ROLLBACK direction requires the referenced transaction');
        }
        return reference.ledgerDirectionFor() === LedgerDirection.Debit ? LedgerDirection.Credit : LedgerDirection.Debit;
      }
      case WagerTransactionKind.Loss:
        throw new InvalidArgumentError('LOSS does not produce ledger entries');
    }
  }

  toState(): WagerTransactionState {
    return {
      id: this.id,
      providerId: this.providerId,
      externalTransactionId: this.externalTransactionId,
      idempotencyKey: this.idempotencyKey,
      payloadHash: this.payloadHash,
      walletId: this.walletId,
      playerId: this.playerId,
      roundId: this.roundId,
      gameId: this.gameId,
      kind: this.kind,
      money: this.money,
      referenceExternalTransactionId: this.referenceExternalTransactionId,
      createdAt: this.createdAt,
      status: this._status,
      referenceTransactionId: this._referenceTransactionId,
      failureCode: this._failureCode,
      processedAt: this._processedAt,
      resultBalance: this._resultBalance,
      referenceAttempts: this._referenceAttempts,
      nextReferenceCheckAt: this._nextReferenceCheckAt,
    };
  }

  private transitionTo(next: WagerTransactionStatus): void {
    if (!TRANSITIONS[this._status].includes(next)) {
      throw new InvalidTransactionStateError(`Transaction ${this.id}: invalid transition ${this._status} -> ${next}`);
    }
    this._status = next;
  }
}
