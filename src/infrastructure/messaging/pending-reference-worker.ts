import type { MikroORM } from '@mikro-orm/postgresql';
import type { AppLogger } from '../../application/observability';
import type { ResolvePendingReferenceUseCase } from '../../application/resolve-pending-reference.use-case';
import { DomainError } from '../../domain/errors';
import type { AppConfig } from '../config';

/**
 * Scheduled worker for PENDING_REFERENCE transactions. Claims due rows with
 * FOR UPDATE SKIP LOCKED + lease (same pattern as the outbox, safe with many
 * instances), then resolves each one through the use case, which takes the
 * wallet lock first (same lock order as the main path).
 */
export class PendingReferenceWorker {
  private timer?: ReturnType<typeof setTimeout>;
  private running = false;
  private current?: Promise<number>;

  constructor(
    private readonly orm: MikroORM,
    private readonly resolver: ResolvePendingReferenceUseCase,
    private readonly config: AppConfig['pendingReference'],
    private readonly logger: AppLogger,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    const tick = async () => {
      if (!this.running) return;
      try {
        this.current = this.runOnce();
        await this.current;
      } catch (error) {
        this.logger.error('pending reference cycle failed', { error: (error as Error).message });
      }
      if (this.running) this.timer = setTimeout(tick, this.config.pollIntervalMs);
    };
    this.timer = setTimeout(tick, this.config.pollIntervalMs);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) clearTimeout(this.timer);
    await this.current?.catch(() => undefined);
  }

  async runOnce(): Promise<number> {
    const claimed = await this.orm.em.fork().execute<{ id: string; wallet_id: string }[]>(
      `with due as (
         select id from wager_transactions
          where status = 'PENDING_REFERENCE' and next_reference_check_at <= now()
          order by next_reference_check_at
          limit ?
          for update skip locked)
       update wager_transactions t
          set next_reference_check_at = now() + (? * interval '1 millisecond')
         from due where t.id = due.id
       returning t.id, t.wallet_id`,
      [this.config.batchSize, this.config.leaseMs],
    );
    let resolved = 0;
    for (const row of claimed) {
      try {
        const result = await this.resolver.resolve(row.id, row.wallet_id);
        if (result !== 'still_pending' && result !== 'skipped') resolved++;
      } catch (error) {
        if (error instanceof DomainError) {
          // Permanent: retrying cannot help. Terminal FAILED, auditable.
          await this.resolver.markFailed(row.id, row.wallet_id, error.message).catch(() => undefined);
          continue;
        }
        // Transient: the lease expiry brings it back; nothing is lost.
        this.logger.warn('pending reference resolution failed', { transactionId: row.id, walletId: row.wallet_id, error: (error as Error).message });
      }
    }
    return resolved;
  }
}
