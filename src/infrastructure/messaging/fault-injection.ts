import { TransientInfrastructureError } from '../../application/errors';

export const FAULT_INJECTOR = Symbol('FAULT_INJECTOR');

/**
 * Test-only fault injection, driven by the FAULT_INJECTION env var (comma
 * separated). It is inert when the variable is unset (production default).
 *
 *   crash_after_commit_before_ack        process exits after COMMIT, before SQS delete
 *   crash_after_outbox_claim             outbox publisher exits after claiming, before publishing
 *   crash_after_publish_before_mark      outbox publisher exits after publishing, before marking published
 *   transient_game:<gameId>:<n>          SQS handler throws a transient error while ApproximateReceiveCount <= n
 *                                        (n = "always" never succeeds -> DLQ after maxReceiveCount)
 */
export class FaultInjector {
  private readonly flags: Set<string>;
  private readonly transientGames = new Map<string, number>();

  constructor(spec: string | undefined, private readonly exit: (code: number) => never = (code) => process.exit(code)) {
    this.flags = new Set();
    for (const raw of (spec ?? '').split(',').map((s) => s.trim()).filter(Boolean)) {
      if (raw.startsWith('transient_game:')) {
        const [, game, n] = raw.split(':');
        if (game) this.transientGames.set(game, n === 'always' ? Number.POSITIVE_INFINITY : Number.parseInt(n ?? '1', 10));
      } else {
        this.flags.add(raw);
      }
    }
  }

  get enabled(): boolean {
    return this.flags.size > 0 || this.transientGames.size > 0;
  }

  crashIf(point: 'crash_after_commit_before_ack' | 'crash_after_outbox_claim' | 'crash_after_publish_before_mark'): void {
    if (this.flags.has(point)) {
      process.stdout.write(`${JSON.stringify({ level: 'warn', msg: 'FAULT INJECTION: crashing', point })}\n`);
      this.exit(137);
    }
  }

  beforeWagerMessage(gameId: string, receiveCount: number): void {
    const limit = this.transientGames.get(gameId);
    if (limit !== undefined && receiveCount <= limit) {
      throw new TransientInfrastructureError(`injected transient failure (receive ${receiveCount})`, 'fault_injection');
    }
  }
}
