import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post, Query, Req, Res, UseGuards } from '@nestjs/common';
import type { Request, Response } from 'express';
import { CreateWalletUseCase } from '../application/create-wallet.use-case';
import { NotFoundError } from '../application/errors';
import { ProcessWagerTransactionUseCase } from '../application/process-wager-transaction.use-case';
import { WageringQueries } from '../application/queries';
import type { WagerResult } from '../application/wager-command';
import { WagerTransactionStatus } from '../domain/wager-transaction';
import {
  createWalletSchema,
  formatZodError,
  idempotencyKeySchema,
  ledgerQuerySchema,
  toWagerCommand,
  wagerHttpBodySchema,
} from '../infrastructure/contracts';
import { ProviderAuthGuard } from './auth';
import { RequestValidationError } from './errors.filter';

type CorrelatedRequest = Request & { correlationId: string };

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function assertUuid(value: string, what: string): string {
  if (!UUID.test(value)) throw new NotFoundError(`${what} ${value} not found`);
  return value.toLowerCase();
}

@Controller('wallets')
@UseGuards(ProviderAuthGuard)
export class WalletsController {
  constructor(
    @Inject(CreateWalletUseCase) private readonly createWallet: CreateWalletUseCase,
    @Inject(WageringQueries) private readonly queries: WageringQueries,
  ) {}

  @Post()
  @HttpCode(201)
  async create(@Body() body: unknown, @Req() req: CorrelatedRequest) {
    const parsed = createWalletSchema.safeParse(body);
    if (!parsed.success) throw new RequestValidationError('VALIDATION_ERROR', 'Invalid request', formatZodError(parsed.error));
    const wallet = await this.createWallet.execute(parsed.data, req.correlationId);
    return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance, version: wallet.version };
  }

  @Get(':walletId')
  async get(@Param('walletId') walletId: string) {
    return this.queries.wallet(assertUuid(walletId, 'Wallet'));
  }

  @Get(':walletId/ledger')
  async ledger(@Param('walletId') walletId: string, @Query() query: unknown) {
    const parsed = ledgerQuerySchema.safeParse(query);
    if (!parsed.success) throw new RequestValidationError('VALIDATION_ERROR', 'Invalid query', formatZodError(parsed.error));
    return this.queries.ledger(assertUuid(walletId, 'Wallet'), parsed.data.cursor, parsed.data.limit);
  }

  @Post(':walletId/reconciliation')
  @HttpCode(200)
  async reconcile(@Param('walletId') walletId: string) {
    return this.queries.reconcile(assertUuid(walletId, 'Wallet'));
  }
}

/**
 * Submission status mapping (consistent with ErrorsFilter):
 *   201 PROCESSED (first time)        200 PROCESSED (idempotent replay)
 *   202 PENDING_REFERENCE (accepted, processing pending; same on replay)
 *   422 REJECTED by a business rule (persisted, or WALLET_NOT_FOUND/UNKNOWN_PROVIDER not persisted)
 * Replays carry idempotentReplay: true and header Idempotent-Replay: true.
 */
@Controller()
@UseGuards(ProviderAuthGuard)
export class WageringController {
  constructor(
    @Inject(ProcessWagerTransactionUseCase) private readonly wagering: ProcessWagerTransactionUseCase,
    @Inject(WageringQueries) private readonly queries: WageringQueries,
  ) {}

  @Post('wagering/transactions')
  async submit(
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Body() body: unknown,
    @Req() req: CorrelatedRequest,
    @Res({ passthrough: true }) res: Response,
  ) {
    const key = idempotencyKeySchema.safeParse(idempotencyKey ?? '');
    if (!key.success) {
      const code = idempotencyKey ? 'VALIDATION_ERROR' : 'IDEMPOTENCY_KEY_REQUIRED';
      throw new RequestValidationError(code, key.error.issues[0]?.message ?? 'Invalid Idempotency-Key', formatZodError(key.error));
    }
    const parsed = wagerHttpBodySchema.safeParse(body);
    if (!parsed.success) throw new RequestValidationError('VALIDATION_ERROR', 'Invalid request', formatZodError(parsed.error));

    const result = await this.wagering.execute(toWagerCommand(parsed.data, key.data), {
      source: 'http',
      correlationId: req.correlationId,
    });
    return this.present(result, res);
  }

  @Get('wagering/transactions/:transactionId')
  async get(@Param('transactionId') transactionId: string) {
    return this.queries.transaction(assertUuid(transactionId, 'Transaction'));
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async getByExternal(@Param('providerId') providerId: string, @Param('externalTransactionId') externalTransactionId: string) {
    return this.queries.transactionByExternalId(providerId, externalTransactionId);
  }

  private present(result: WagerResult, res: Response) {
    switch (result.type) {
      case 'transaction': {
        const t = result.transaction;
        const status =
          t.status === WagerTransactionStatus.Processed
            ? result.idempotentReplay
              ? 200
              : 201
            : t.status === WagerTransactionStatus.PendingReference || t.status === WagerTransactionStatus.Pending
              ? 202
              : 422;
        res.status(status);
        res.setHeader('Idempotent-Replay', String(result.idempotentReplay));
        return {
          transactionId: t.transactionId,
          status: t.status,
          kind: t.kind,
          balance: t.balance,
          failureCode: t.failureCode,
          referenceTransactionId: t.referenceTransactionId,
          idempotentReplay: result.idempotentReplay,
        };
      }
      case 'unpersisted_rejection':
        res.status(422);
        res.setHeader('Idempotent-Replay', 'false');
        return { transactionId: null, status: 'REJECTED', balance: null, failureCode: result.failureCode, idempotentReplay: false };
      case 'duplicate_message':
        // Not reachable over HTTP (no inbox); kept for exhaustiveness.
        res.status(200);
        return { status: 'DUPLICATE', idempotentReplay: true };
    }
  }
}
