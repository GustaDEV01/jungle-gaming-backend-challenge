/**
 * Stable, machine readable failure codes. Documented in ARCHITECTURE.md.
 *
 * Each code tells the provider what to do next:
 * - "fix"   : the payload/context is wrong; resending the same payload will never succeed.
 * - "final" : definitive business outcome; do not resend.
 */
export const FailureCode = {
  /** BET larger than the available balance. (final) */
  InsufficientFunds: 'INSUFFICIENT_FUNDS',
  /** REFUND/ROLLBACK that would make the balance negative. Distinct from a BET without funds. (final) */
  ReversalInsufficientFunds: 'REVERSAL_INSUFFICIENT_FUNDS',
  /** Operation currency differs from the wallet currency. (fix) */
  CurrencyMismatch: 'CURRENCY_MISMATCH',
  /** playerId in the payload does not own the wallet. (fix) */
  PlayerWalletMismatch: 'PLAYER_WALLET_MISMATCH',
  /** walletId does not exist. Not persisted (no aggregate to attach to). (fix) */
  WalletNotFound: 'WALLET_NOT_FOUND',
  /** providerId is not registered/active. Not persisted. (fix) */
  UnknownProvider: 'UNKNOWN_PROVIDER',
  /** Referenced transaction never arrived within the retry budget. (final) */
  ReferenceNotFound: 'REFERENCE_NOT_FOUND',
  /** Reference belongs to another player/wallet/currency/round. (fix) */
  ReferenceMismatch: 'REFERENCE_MISMATCH',
  /** Reference kind not allowed (REFUND -> BET only; ROLLBACK -> BET|WIN|REFUND; WIN/LOSS -> BET). (fix) */
  InvalidReferenceKind: 'INVALID_REFERENCE_KIND',
  /** Referenced transaction is REJECTED or FAILED, so there is nothing to revert. (final) */
  ReferenceNotProcessed: 'REFERENCE_NOT_PROCESSED',
  /** Referenced transaction was already reverted (by a REFUND or a ROLLBACK). (final) */
  ReferenceAlreadyReversed: 'REFERENCE_ALREADY_REVERSED',
  /** REFUND/ROLLBACK amount differs from the referenced amount (partial reversal unsupported). (fix) */
  ReversalAmountMismatch: 'REVERSAL_AMOUNT_MISMATCH',
  /** Permanent, non-business error while processing (FAILED status). Operator attention required. */
  InternalProcessingError: 'INTERNAL_PROCESSING_ERROR',
} as const;

export type FailureCode = (typeof FailureCode)[keyof typeof FailureCode];

export const ALL_FAILURE_CODES: readonly FailureCode[] = Object.values(FailureCode);
