import { type ArgumentsHost, Catch, type ExceptionFilter, HttpException, HttpStatus } from '@nestjs/common';
import type { Request, Response } from 'express';
import { ZodError } from 'zod';
import {
  ExternalTransactionConflictError,
  IdempotencyConflictError,
  NotFoundError,
  TransientInfrastructureError,
  WalletAlreadyExistsError,
} from '../application/errors';
import type { AppLogger } from '../application/observability';
import { DomainError } from '../domain/errors';
import { formatZodError } from '../infrastructure/contracts';
import { isConnectionError } from '../infrastructure/persistence/pg-errors';

export interface ErrorBody {
  error: { code: string; message: string; retryable: boolean; details?: unknown };
}

/** Raised by controllers for header/body validation failures. */
export class RequestValidationError extends Error {
  constructor(
    readonly code: 'VALIDATION_ERROR' | 'IDEMPOTENCY_KEY_REQUIRED',
    message: string,
    readonly details?: unknown,
  ) {
    super(message);
  }
}

/**
 * Single error mapping for every endpoint (consistent semantics):
 *   400 VALIDATION_ERROR / IDEMPOTENCY_KEY_REQUIRED  invalid payload — fix it, do not resend as is
 *   404 NOT_FOUND                                    unknown resource (queries)
 *   409 IDEMPOTENCY_KEY_CONFLICT                     same key, different payload — never a replay
 *   409 EXTERNAL_TRANSACTION_CONFLICT                external id reused under another key
 *   409 WALLET_ALREADY_EXISTS
 *   503 TRANSIENT_INFRASTRUCTURE_ERROR (+Retry-After) safe to retry with the same Idempotency-Key
 *   500 INTERNAL_ERROR
 * Business rejections are not errors: they are 422 responses built by the controller.
 */
@Catch()
export class ErrorsFilter implements ExceptionFilter {
  constructor(private readonly logger: AppLogger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const res = ctx.getResponse<Response>();
    const req = ctx.getRequest<Request & { correlationId?: string }>();
    const [status, body] = this.map(exception);
    if (status >= 500) {
      this.logger.error('request failed', {
        correlationId: req.correlationId,
        path: req.path,
        method: req.method,
        status,
        code: body.error.code,
        error: exception instanceof Error ? exception.message : String(exception),
        stack: status === 500 && exception instanceof Error ? exception.stack?.split('\n').slice(0, 8).join(' | ') : undefined,
      });
    }
    if (status === HttpStatus.SERVICE_UNAVAILABLE) res.setHeader('Retry-After', '1');
    res.status(status).json(body);
  }

  private map(e: unknown): [number, ErrorBody] {
    const body = (code: string, message: string, retryable: boolean, details?: unknown): ErrorBody => ({
      error: details === undefined ? { code, message, retryable } : { code, message, retryable, details },
    });
    if (e instanceof RequestValidationError) return [400, body(e.code, e.message, false, e.details)];
    if (e instanceof ZodError) return [400, body('VALIDATION_ERROR', 'Invalid request', false, formatZodError(e))];
    if (e instanceof DomainError) return [400, body('VALIDATION_ERROR', e.message, false, { domainCode: e.code })];
    if (e instanceof IdempotencyConflictError) {
      return [409, body(e.code, e.message, false, { existingTransactionId: e.existingTransactionId })];
    }
    if (e instanceof ExternalTransactionConflictError) {
      return [409, body(e.code, e.message, false, { existingTransactionId: e.existingTransactionId })];
    }
    if (e instanceof WalletAlreadyExistsError) return [409, body(e.code, e.message, false)];
    if (e instanceof NotFoundError) return [404, body(e.code, e.message, false)];
    if (e instanceof TransientInfrastructureError) return [503, body(e.code, e.message, true, { reason: e.reason })];
    if (isConnectionError(e)) return [503, body('TRANSIENT_INFRASTRUCTURE_ERROR', 'Database unavailable', true, { reason: 'database_unavailable' })];
    if (e instanceof HttpException) {
      const status = e.getStatus();
      const code = status === 404 ? 'NOT_FOUND' : status === 400 ? 'VALIDATION_ERROR' : `HTTP_${status}`;
      return [status, body(code, e.message, status >= 500)];
    }
    return [500, body('INTERNAL_ERROR', 'Internal error', true)];
  }
}
