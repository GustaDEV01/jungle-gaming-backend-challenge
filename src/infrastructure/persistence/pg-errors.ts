/** Extracts SQLSTATE / constraint name from a MikroORM-wrapped pg error. */
export interface PgErrorInfo {
  code?: string;
  constraint?: string;
  message: string;
}

export function pgErrorInfo(error: unknown): PgErrorInfo {
  const seen = new Set<unknown>();
  let current: unknown = error;
  let code: string | undefined;
  let constraint: string | undefined;
  while (current && typeof current === 'object' && !seen.has(current)) {
    seen.add(current);
    const e = current as { code?: unknown; constraint?: unknown; previous?: unknown; cause?: unknown };
    if (!code && typeof e.code === 'string' && /^[0-9A-Z]{5}$/.test(e.code)) code = e.code;
    if (!constraint && typeof e.constraint === 'string') constraint = e.constraint;
    current = e.previous ?? e.cause;
  }
  return { code, constraint, message: error instanceof Error ? error.message : String(error) };
}

export const PG = {
  uniqueViolation: '23505',
  checkViolation: '23514',
  foreignKeyViolation: '23503',
  serializationFailure: '40001',
  deadlockDetected: '40P01',
  lockNotAvailable: '55P03',
  queryCanceled: '57014',
  adminShutdown: '57P01',
  cannotConnectNow: '57P03',
} as const;

const CONNECTION_CODES = new Set(['08000', '08001', '08003', '08004', '08006', '57P01', '57P02', '57P03', '53300']);
const NODE_NET_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN', 'ConnectionRefused']);

/** Database unreachable / connection dropped: transient. */
export function isConnectionError(error: unknown): boolean {
  const { code, message } = pgErrorInfo(error);
  if (code && CONNECTION_CODES.has(code)) return true;
  const errno = (error as { code?: unknown })?.code;
  if (typeof errno === 'string' && NODE_NET_CODES.has(errno)) return true;
  return /ECONNREFUSED|ECONNRESET|Connection terminated|connect ETIMEDOUT|timeout exceeded when trying to connect|Unable to connect|ConnectionRefused/i.test(
    message,
  );
}
