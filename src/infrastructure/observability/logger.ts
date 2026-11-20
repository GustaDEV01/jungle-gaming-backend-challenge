import type { LoggerService } from '@nestjs/common';
import type { AppLogger, LogFields } from '../../application/observability';

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 } as const;
type Level = keyof typeof LEVELS;

/** Field names that must never reach the logs (payloads, credentials). */
const REDACTED = new Set(['payload', 'body', 'authorization', 'password', 'secret', 'token']);

/**
 * JSON-lines structured logger. One object per line on stdout with the
 * correlation fields (correlationId, messageId, transactionId, walletId,
 * providerId) when available. Implements Nest's LoggerService too.
 */
export class JsonLogger implements AppLogger, LoggerService {
  constructor(
    private readonly base: { service: string; instanceId: string },
    private readonly minLevel: Level = 'info',
  ) {}

  debug(msg: string, fields?: LogFields | unknown): void {
    this.write('debug', msg, fields);
  }
  info(msg: string, fields?: LogFields | unknown): void {
    this.write('info', msg, fields);
  }
  warn(msg: string, fields?: LogFields | unknown): void {
    this.write('warn', msg, fields);
  }
  error(msg: string, fields?: LogFields | unknown): void {
    this.write('error', msg, fields);
  }
  // Nest LoggerService
  log(msg: string, context?: unknown): void {
    this.write('info', String(msg), typeof context === 'string' ? { context } : undefined);
  }
  verbose(msg: string, context?: unknown): void {
    this.write('debug', String(msg), typeof context === 'string' ? { context } : undefined);
  }
  fatal(msg: string, context?: unknown): void {
    this.write('error', String(msg), typeof context === 'string' ? { context } : undefined);
  }

  private write(level: Level, msg: string, fields: unknown): void {
    if (LEVELS[level] < LEVELS[this.minLevel]) return;
    const line: Record<string, unknown> = { ts: new Date().toISOString(), level, msg, ...this.base };
    if (fields && typeof fields === 'object' && !Array.isArray(fields)) {
      for (const [key, value] of Object.entries(fields as Record<string, unknown>)) {
        if (value === undefined) continue;
        line[key] = REDACTED.has(key.toLowerCase()) ? '[redacted]' : value;
      }
    } else if (typeof fields === 'string') {
      line.context = fields;
    }
    process.stdout.write(`${JSON.stringify(line)}\n`);
  }
}
