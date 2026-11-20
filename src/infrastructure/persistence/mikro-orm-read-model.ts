import type { MikroORM } from '@mikro-orm/postgresql';
import type { LedgerPage, ReconciliationData, WageringReadModel } from '../../application/ports';
import { encodeCursor } from '../../application/queries';
import type { Wallet } from '../../domain/wallet';
import type { WagerTransaction } from '../../domain/wager-transaction';
import { ledgerToDomain, transactionToDomain, walletToDomain } from './mappers';
import { LedgerEntryRecord, WagerTransactionRecord, WalletRecord } from './schemas';

export class MikroOrmReadModel implements WageringReadModel {
  constructor(private readonly orm: MikroORM) {}

  private em() {
    return this.orm.em.fork();
  }

  async findWallet(walletId: string): Promise<Wallet | null> {
    const r = await this.em().findOne(WalletRecord, { id: walletId });
    return r ? walletToDomain(r) : null;
  }

  async findTransaction(id: string): Promise<WagerTransaction | null> {
    const r = await this.em().findOne(WagerTransactionRecord, { id });
    return r ? transactionToDomain(r) : null;
  }

  async findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | null> {
    const r = await this.em().findOne(WagerTransactionRecord, { providerId, externalTransactionId });
    return r ? transactionToDomain(r) : null;
  }

  async ledgerPage(walletId: string, afterVersion: number | null, limit: number): Promise<LedgerPage> {
    const rows = await this.em().find(
      LedgerEntryRecord,
      { walletId, walletVersion: { $gt: afterVersion ?? 0 } },
      { orderBy: { walletVersion: 'asc' }, limit: limit + 1 },
    );
    const page = rows.slice(0, limit);
    const last = page[page.length - 1];
    return {
      entries: page.map(ledgerToDomain),
      nextCursor: rows.length > limit && last ? encodeCursor(last.walletVersion) : null,
    };
  }

  /**
   * One SQL statement => one snapshot: the stored balance and the ledger are
   * read consistently even while writers keep appending. The sum is computed
   * in PostgreSQL numeric (exact) and returned as text.
   */
  async reconcile(walletId: string): Promise<ReconciliationData | null> {
    const rows = await this.em().execute<
      {
        stored: string;
        currency: string;
        version: number;
        calculated: string;
        entries: number;
        last_version: number | null;
        chain_breaks: number;
      }[]
    >(
      `with entries as (
         select e.*, lag(e.balance_after) over w as prev_after, lag(e.wallet_version) over w as prev_version
           from wallet_ledger_entries e where e.wallet_id = ?
         window w as (order by e.wallet_version)
       )
       select w.balance_amount::text as stored,
              w.currency,
              w.version,
              coalesce((select sum(case when direction = 'CREDIT' then amount else -amount end) from entries), 0)::text as calculated,
              (select count(*) from entries)::int as entries,
              (select max(wallet_version) from entries) as last_version,
              (select count(*) from entries
                where prev_after is not null and (prev_after <> balance_before or prev_version <> wallet_version - 1))::int as chain_breaks
         from wallets w where w.id = ?`,
      [walletId, walletId],
    );
    const r = rows[0];
    if (!r) return null;
    return {
      storedBalance: r.stored,
      calculatedBalance: normaliseScale(r.calculated),
      currency: r.currency.trim(),
      checkedEntries: r.entries,
      lastEntryVersion: r.last_version === null ? null : Number(r.last_version),
      walletVersion: r.version,
      chainBreaks: r.chain_breaks,
    };
  }
}

/** "0" -> "0.00", "-5" -> "-5.00"; string manipulation only (never via number). */
function normaliseScale(value: string): string {
  const [intPart, frac = ''] = value.split('.');
  return `${intPart}.${(frac + '00').slice(0, 2)}`;
}
