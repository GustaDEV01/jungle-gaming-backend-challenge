import { describe, expect, test } from 'bun:test';
import { wagerPayloadHash, type WagerCommand } from '../../src/application/wager-command';
import { canonicalJson } from '../../src/application/canonical-json';
import { InvalidArgumentError, InvalidTransactionStateError } from '../../src/domain/errors';
import { FailureCode } from '../../src/domain/failure-codes';
import { LedgerDirection } from '../../src/domain/ledger-entry';
import { Money } from '../../src/domain/money';
import { OutboxMessage, backoffDelayMs } from '../../src/domain/outbox-message';
import { Wallet } from '../../src/domain/wallet';
import { WagerSettlement, type ReferenceLookup } from '../../src/domain/wager-settlement';
import { WagerTransaction, WagerTransactionKind as K, WagerTransactionStatus as S } from '../../src/domain/wager-transaction';
import { WalletBalanceChanged } from '../../src/domain/events/wagering-events';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const at = new Date('2026-11-20T12:00:00Z');
let n = 0;

function wallet(balance = '100.00') {
  return Wallet.open({ id: 'w-1', playerId: 'p-1', initialBalance: brl(balance), opening: { transactionId: 'o', ledgerEntryId: 'ol' }, at }).wallet;
}

function tx(kind: K, amount: string, extra: Partial<Parameters<typeof WagerTransaction.create>[0]> = {}) {
  const id = `ext-${++n}`;
  return WagerTransaction.create({
    id: `tx-${n}`,
    providerId: 'provider-a',
    externalTransactionId: id,
    idempotencyKey: `provider-a:${id}`,
    payloadHash: 'a'.repeat(64),
    walletId: 'w-1',
    playerId: 'p-1',
    roundId: 'r-1',
    gameId: 'g-1',
    kind,
    money: brl(amount),
    createdAt: at,
    ...extra,
  });
}

const none: ReferenceLookup = { transaction: null, alreadyReversed: false };
const ref = (t: WagerTransaction, alreadyReversed = false): ReferenceLookup => ({ transaction: t, alreadyReversed });
const ctx = () => ({ ledgerEntryId: `le-${++n}`, at, nextReferenceCheckAt: new Date(at.getTime() + 1000) });

function processed(kind: K, amount: string, w: Wallet, reference?: WagerTransaction) {
  const t = tx(kind, amount, reference ? { referenceExternalTransactionId: reference.externalTransactionId } : {});
  const outcome = WagerSettlement.settle(t, w, reference ? ref(reference) : none, ctx());
  expect(outcome.type).toBe('processed');
  return t;
}

describe('BET', () => {
  test('debits and produces one DEBIT entry', () => {
    const w = wallet('100.00');
    const bet = tx(K.Bet, '25.00');
    const outcome = WagerSettlement.settle(bet, w, none, ctx());
    expect(outcome.type).toBe('processed');
    if (outcome.type === 'processed') expect(outcome.entry?.direction).toBe(LedgerDirection.Debit);
    expect(bet.status).toBe(S.Processed);
    expect(bet.resultBalance?.amount).toBe('75.00');
    expect(w.balance.amount).toBe('75.00');
  });

  test('rejects with INSUFFICIENT_FUNDS and does not touch the wallet', () => {
    const w = wallet('100.00');
    processed(K.Bet, '80.00', w);
    const second = tx(K.Bet, '80.00');
    const outcome = WagerSettlement.settle(second, w, none, ctx());
    expect(outcome).toEqual({ type: 'rejected', code: FailureCode.InsufficientFunds });
    expect(second.status).toBe(S.Rejected);
    expect(second.resultBalance?.amount).toBe('20.00');
    expect(w.balance.amount).toBe('20.00');
    expect(w.version).toBe(2);
  });

  test('currency conflict is rejected', () => {
    const w = wallet('100.00');
    const usdBet = tx(K.Bet, '1.00', { money: Money.from({ amount: '1.00', currency: 'USD' }) });
    expect(WagerSettlement.settle(usdBet, w, none, ctx())).toEqual({ type: 'rejected', code: FailureCode.CurrencyMismatch });
    expect(w.version).toBe(1);
  });

  test('player that does not own the wallet is rejected', () => {
    const w = wallet('100.00');
    const t = tx(K.Bet, '1.00', { playerId: 'someone-else' });
    expect(WagerSettlement.settle(t, w, none, ctx())).toEqual({ type: 'rejected', code: FailureCode.PlayerWalletMismatch });
  });

  test('BET cannot reference and needs a positive amount', () => {
    expect(() => tx(K.Bet, '1.00', { referenceExternalTransactionId: 'x' })).toThrow(InvalidArgumentError);
    expect(() => tx(K.Bet, '0.00')).toThrow(InvalidArgumentError);
  });
});

describe('WIN', () => {
  test('credits, with or without reference to a BET of the same round', () => {
    const w = wallet('100.00');
    const bet = processed(K.Bet, '10.00', w);
    const win = processed(K.Win, '30.00', w, bet);
    expect(win.referenceTransactionId).toBe(bet.id);
    expect(w.balance.amount).toBe('120.00');
    processed(K.Win, '5.00', w);
    expect(w.balance.amount).toBe('125.00');
  });

  test('referencing a BET of another round is rejected', () => {
    const w = wallet('100.00');
    const bet = processed(K.Bet, '10.00', w);
    const win = tx(K.Win, '10.00', { referenceExternalTransactionId: bet.externalTransactionId, roundId: 'other-round' });
    expect(WagerSettlement.settle(win, w, ref(bet), ctx())).toEqual({ type: 'rejected', code: FailureCode.ReferenceMismatch });
  });

  test('WIN on a refunded BET is rejected', () => {
    const w = wallet('100.00');
    const bet = processed(K.Bet, '10.00', w);
    const win = tx(K.Win, '10.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(WagerSettlement.settle(win, w, ref(bet, true), ctx())).toEqual({ type: 'rejected', code: FailureCode.ReferenceAlreadyReversed });
  });
});

describe('LOSS', () => {
  test('is PROCESSED without moving the balance and without ledger entry', () => {
    const w = wallet('100.00');
    const bet = processed(K.Bet, '10.00', w);
    const loss = tx(K.Loss, '0.00', { referenceExternalTransactionId: bet.externalTransactionId });
    const outcome = WagerSettlement.settle(loss, w, ref(bet), ctx());
    expect(outcome).toEqual({ type: 'processed', entry: null });
    expect(loss.status).toBe(S.Processed);
    expect(loss.affectsBalance()).toBe(false);
    expect(w.balance.amount).toBe('90.00');
    expect(w.version).toBe(2);
    expect(() => loss.ledgerDirectionFor()).toThrow(InvalidArgumentError);
  });
});

describe('REFUND', () => {
  test('requires a reference', () => {
    expect(() => tx(K.Refund, '10.00')).toThrow(InvalidArgumentError);
  });

  test('credits back a PROCESSED BET, exactly the same amount', () => {
    const w = wallet('100.00');
    const bet = processed(K.Bet, '40.00', w);
    const refund = processed(K.Refund, '40.00', w, bet);
    expect(refund.referenceTransactionId).toBe(bet.id);
    expect(w.balance.amount).toBe('100.00');
  });

  test('amount must be equal to the reference', () => {
    const w = wallet('100.00');
    const bet = processed(K.Bet, '40.00', w);
    const refund = tx(K.Refund, '39.99', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(WagerSettlement.settle(refund, w, ref(bet), ctx())).toEqual({ type: 'rejected', code: FailureCode.ReversalAmountMismatch });
  });

  test('only references BET', () => {
    const w = wallet('100.00');
    const win = processed(K.Win, '40.00', w);
    const refund = tx(K.Refund, '40.00', { referenceExternalTransactionId: win.externalTransactionId });
    expect(WagerSettlement.settle(refund, w, ref(win), ctx())).toEqual({ type: 'rejected', code: FailureCode.InvalidReferenceKind });
  });

  test('a BET is reverted once: second REFUND (or a ROLLBACK after it) is rejected', () => {
    const w = wallet('100.00');
    const bet = processed(K.Bet, '40.00', w);
    processed(K.Refund, '40.00', w, bet);
    const again = tx(K.Refund, '40.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(WagerSettlement.settle(again, w, ref(bet, true), ctx())).toEqual({ type: 'rejected', code: FailureCode.ReferenceAlreadyReversed });
    const rollback = tx(K.Rollback, '40.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(WagerSettlement.settle(rollback, w, ref(bet, true), ctx())).toEqual({ type: 'rejected', code: FailureCode.ReferenceAlreadyReversed });
    expect(w.balance.amount).toBe('100.00');
  });

  test('missing reference parks the transaction as PENDING_REFERENCE', () => {
    const w = wallet('100.00');
    const refund = tx(K.Refund, '40.00', { referenceExternalTransactionId: 'not-yet' });
    expect(WagerSettlement.settle(refund, w, none, ctx())).toEqual({ type: 'pending_reference' });
    expect(refund.status).toBe(S.PendingReference);
    expect(refund.referenceAttempts).toBe(1);
    expect(w.version).toBe(1);
  });

  test('reference not PROCESSED (REJECTED) cannot be reverted', () => {
    const w = wallet('10.00');
    const bet = tx(K.Bet, '40.00');
    WagerSettlement.settle(bet, w, none, ctx()); // rejected: insufficient funds
    const refund = tx(K.Refund, '40.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(WagerSettlement.settle(refund, w, ref(bet), ctx())).toEqual({ type: 'rejected', code: FailureCode.ReferenceNotProcessed });
  });
});

describe('ROLLBACK', () => {
  test('inverts the direction of the reference', () => {
    const w = wallet('100.00');
    const bet = processed(K.Bet, '30.00', w);
    const rb = tx(K.Rollback, '30.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(rb.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
    WagerSettlement.settle(rb, w, ref(bet), ctx());
    expect(w.balance.amount).toBe('100.00');

    const win = processed(K.Win, '50.00', w);
    const rbWin = tx(K.Rollback, '50.00', { referenceExternalTransactionId: win.externalTransactionId });
    expect(rbWin.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
    WagerSettlement.settle(rbWin, w, ref(win), ctx());
    expect(w.balance.amount).toBe('100.00');
  });

  test('rollback that would make the balance negative is rejected with a distinct code', () => {
    const w = wallet('0.00');
    const win = processed(K.Win, '50.00', w);
    processed(K.Bet, '50.00', w);
    const rb = tx(K.Rollback, '50.00', { referenceExternalTransactionId: win.externalTransactionId });
    const outcome = WagerSettlement.settle(rb, w, ref(win), ctx());
    expect(outcome).toEqual({ type: 'rejected', code: FailureCode.ReversalInsufficientFunds });
    expect(FailureCode.ReversalInsufficientFunds).not.toBe(FailureCode.InsufficientFunds);
    expect(w.balance.amount).toBe('0.00');
  });

  test('ROLLBACK of a LOSS is an invalid reference kind', () => {
    const w = wallet('100.00');
    const loss = processed(K.Loss, '0.00', w);
    const rb = tx(K.Rollback, '1.00', { referenceExternalTransactionId: loss.externalTransactionId });
    expect(WagerSettlement.settle(rb, w, ref(loss), ctx())).toEqual({ type: 'rejected', code: FailureCode.InvalidReferenceKind });
  });

  test('reference still PENDING_REFERENCE keeps the rollback pending', () => {
    const w = wallet('100.00');
    const win = tx(K.Win, '10.00', { referenceExternalTransactionId: 'missing-bet' });
    WagerSettlement.settle(win, w, none, ctx());
    const rb = tx(K.Rollback, '10.00', { referenceExternalTransactionId: win.externalTransactionId });
    expect(WagerSettlement.settle(rb, w, ref(win), ctx())).toEqual({ type: 'pending_reference' });
  });
});

describe('WagerTransaction state machine', () => {
  test('terminal states cannot transition', () => {
    const w = wallet('100.00');
    const bet = processed(K.Bet, '1.00', w);
    expect(bet.isTerminal()).toBe(true);
    expect(() => bet.reject(FailureCode.InsufficientFunds, at, w.balance)).toThrow(InvalidTransactionStateError);
    expect(() => bet.markPendingReference(at)).toThrow(InvalidTransactionStateError);
    expect(() => bet.markProcessed(undefined, at, w.balance)).toThrow(InvalidTransactionStateError);
    expect(() => bet.fail(FailureCode.InternalProcessingError, at)).toThrow(InvalidTransactionStateError);
  });

  test('PENDING_REFERENCE can be rescheduled and then finish', () => {
    const w = wallet('100.00');
    const r = tx(K.Refund, '1.00', { referenceExternalTransactionId: 'later' });
    r.markPendingReference(at);
    r.markPendingReference(at);
    expect(r.referenceAttempts).toBe(2);
    WagerSettlement.expireReference(r, w, at);
    expect(r.status).toBe(S.Rejected);
    expect(r.failureCode).toBe(FailureCode.ReferenceNotFound);
  });

  test('OPENING cannot be created through the external factory', () => {
    expect(() => tx(K.Opening, '1.00')).toThrow(InvalidArgumentError);
  });

  test('self reference is refused', () => {
    expect(() => tx(K.Refund, '1.00', { externalTransactionId: 'same', referenceExternalTransactionId: 'same' })).toThrow(InvalidArgumentError);
  });
});

describe('idempotency payload hash', () => {
  const cmd: WagerCommand = {
    providerId: 'provider-a',
    externalTransactionId: 'tx-1',
    idempotencyKey: 'provider-a:tx-1',
    playerId: 'p-1',
    walletId: '0192f291-27dd-7d3f-8071-5f8685deef37',
    roundId: 'r-1',
    gameId: 'g-1',
    kind: K.Bet as WagerCommand['kind'],
    money: { amount: '25.00', currency: 'BRL' },
  };

  test('canonical JSON sorts keys recursively and omits undefined', () => {
    expect(canonicalJson({ b: '1', a: { d: '2', c: undefined, b: '3' } })).toBe('{"a":{"b":"3","d":"2"},"b":"1"}');
    expect(() => canonicalJson({ amount: 1.5 })).toThrow(TypeError);
  });

  test('same business payload => same hash regardless of key order, amount scale and idempotency key', () => {
    const reordered = Object.fromEntries(Object.entries(cmd).reverse()) as unknown as WagerCommand;
    expect(Object.keys(reordered)[0]).toBe('money');
    expect(wagerPayloadHash(reordered)).toBe(wagerPayloadHash(cmd));
    expect(() => wagerPayloadHash({ ...cmd, money: { amount: '25', currency: 'BRL' } })).toThrow(); // scale is part of the contract
    expect(wagerPayloadHash({ ...cmd, idempotencyKey: 'other' })).toBe(wagerPayloadHash(cmd));
  });

  test('divergent payload under the same key => different hash => conflict, not replay', () => {
    const original = tx(K.Bet, '25.00', { payloadHash: wagerPayloadHash(cmd) });
    const divergent = wagerPayloadHash({ ...cmd, money: { amount: '26.00', currency: 'BRL' } });
    expect(original.matchesPayload(wagerPayloadHash(cmd))).toBe(true);
    expect(original.matchesPayload(divergent)).toBe(false);
    expect(wagerPayloadHash({ ...cmd, walletId: '0192f291-27dd-7d3f-8071-5f8685deef38' })).not.toBe(wagerPayloadHash(cmd));
  });
});

describe('Outbox / events', () => {
  test('exponential backoff with cap', () => {
    const policy = { baseDelayMs: 1000, maxDelayMs: 10_000 };
    expect([1, 2, 3, 4, 5, 6].map((a) => backoffDelayMs(a, policy))).toEqual([1000, 2000, 4000, 8000, 10_000, 10_000]);
  });

  test('outbox message lifecycle and JSON-stable payload (Money as strings)', () => {
    const w = wallet('100.00');
    const entry = w.debit(brl('10.00'), { transactionId: 't', ledgerEntryId: 'l', at });
    const event = WalletBalanceChanged.from(w, entry, { eventId: 'e-1', correlationId: 'c-1', occurredAt: at });
    const json = event.toJSON();
    expect(json.eventType).toBe('WalletBalanceChanged');
    expect(json.version).toBe(1);
    expect(json.data.balanceAfter).toEqual({ amount: '90.00', currency: 'BRL' });
    expect(json.occurredAt).toBe(at.toISOString());

    const m = OutboxMessage.enqueue(event);
    expect(m.isDue(at)).toBe(true);
    m.scheduleRetry(at, { baseDelayMs: 1000, maxDelayMs: 60_000 });
    expect(m.attempts).toBe(1);
    expect(m.isDue(at)).toBe(false);
    m.markPublished(at);
    expect(m.isPending()).toBe(false);
    expect(() => m.markPublished(at)).toThrow(InvalidTransactionStateError);
  });
});
