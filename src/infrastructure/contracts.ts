import { z } from 'zod';
import type { WagerCommand } from '../application/wager-command';
import type { WagerTransactionKind } from '../domain/wager-transaction';

/**
 * Input contracts shared by HTTP and SQS. Money is a decimal STRING with a
 * fixed scale of exactly 2 decimal places ("25.00"; "25" and "25.0" are 400);
 * numbers are refused (z.string()), so a JSON number like 25.5 never reaches the domain.
 */
const AMOUNT = /^(0|[1-9]\d{0,17})\.\d{2}$/;
const id = (max: number) =>
  z
    .string()
    .min(1)
    .max(max)
    .regex(/^[\x21-\x7E]+$/, 'printable ASCII without spaces');

export const moneySchema = z
  .object({
    amount: z.string().regex(AMOUNT, 'decimal string, non-negative, exactly 2 decimal places (e.g. "25.00")'),
    currency: z.string().regex(/^[A-Z]{3}$/, 'ISO-4217 alpha code'),
  })
  .strict();

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const wagerFields = {
  providerId: z.string().regex(/^[a-z0-9][a-z0-9_-]{0,63}$/, 'lowercase provider id'),
  externalTransactionId: id(128),
  playerId: id(128),
  walletId: z.string().regex(UUID, 'uuid'),
  roundId: id(128),
  gameId: id(128),
  kind: z.enum(['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK']),
  money: moneySchema,
  referenceExternalTransactionId: id(128).optional(),
};

function crossFieldRules(value: { kind: string; money: { amount: string }; referenceExternalTransactionId?: string; externalTransactionId: string }, ctx: z.RefinementCtx) {
  const needsRef = value.kind === 'REFUND' || value.kind === 'ROLLBACK';
  if (needsRef && !value.referenceExternalTransactionId) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['referenceExternalTransactionId'], message: `${value.kind} requires referenceExternalTransactionId` });
  }
  if (value.kind === 'BET' && value.referenceExternalTransactionId) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['referenceExternalTransactionId'], message: 'BET must not reference another transaction' });
  }
  if (value.referenceExternalTransactionId && value.referenceExternalTransactionId === value.externalTransactionId) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['referenceExternalTransactionId'], message: 'a transaction cannot reference itself' });
  }
  if (value.kind !== 'LOSS' && value.money.amount === '0.00') {
    ctx.addIssue({ code: z.ZodIssueCode.custom, path: ['money', 'amount'], message: `${value.kind} amount must be greater than zero` });
  }
}

export const wagerHttpBodySchema = z.object(wagerFields).strict().superRefine(crossFieldRules);

export const idempotencyKeySchema = z
  .string({ required_error: 'Idempotency-Key header is required' })
  .min(1, 'Idempotency-Key header is required')
  .max(255)
  .regex(/^[\x21-\x7E]+$/, 'Idempotency-Key must be printable ASCII without spaces');

export const wagerMessageSchema = z
  .object({
    messageId: id(255),
    type: z.literal('WagerTransactionRequested'),
    occurredAt: z.string().datetime({ offset: true }),
    correlationId: id(255).optional(),
    data: z
      .object({ ...wagerFields, idempotencyKey: idempotencyKeySchema })
      .strict()
      .superRefine(crossFieldRules),
  })
  .strict();

export type WagerHttpBody = z.infer<typeof wagerHttpBodySchema>;
export type WagerMessage = z.infer<typeof wagerMessageSchema>;

export function toWagerCommand(body: WagerHttpBody, idempotencyKey: string): WagerCommand {
  const cmd: WagerCommand = {
    providerId: body.providerId,
    externalTransactionId: body.externalTransactionId,
    idempotencyKey,
    playerId: body.playerId,
    walletId: body.walletId.toLowerCase(),
    roundId: body.roundId,
    gameId: body.gameId,
    kind: body.kind as WagerCommand['kind'] & WagerTransactionKind,
    money: { amount: body.money.amount, currency: body.money.currency },
  };
  if (body.referenceExternalTransactionId !== undefined) cmd.referenceExternalTransactionId = body.referenceExternalTransactionId;
  return cmd;
}

export const createWalletSchema = z
  .object({
    playerId: id(128),
    initialBalance: moneySchema,
  })
  .strict();

export const ledgerQuerySchema = z.object({
  cursor: z.string().max(512).optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
});

export function formatZodError(error: z.ZodError): { path: string; message: string }[] {
  return error.issues.map((i) => ({ path: i.path.join('.') || '(root)', message: i.message }));
}
