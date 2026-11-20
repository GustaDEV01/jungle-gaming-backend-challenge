import { Migration } from '@mikro-orm/migrations';

/**
 * Audit fixes AUD-01 and AUD-02 (docs/AUDIT_REPORT.md).
 *
 * AUD-01: the balance observed when a transaction was decided is always in the
 * WALLET currency, which can differ from the operation currency (CURRENCY_MISMATCH
 * rejections). It now has its own column, tied to the wallet currency by FK.
 *
 * AUD-02: the ledger must correspond to a valid movement, enforced in the schema:
 *  - currency of every entry == wallet currency (composite FK);
 *  - at COMMIT (deferred, O(1) index lookups): the entry's transaction is PROCESSED,
 *    is not a LOSS, has the same amount and currency, and the direction matches its
 *    kind (BET -> DEBIT; OPENING/WIN/REFUND -> CREDIT; ROLLBACK -> inverse of its reference);
 *  - and the other way round: a PROCESSED transaction that moves balance has its entry.
 */
export class Migration20261120000003_ledger_transaction_integrity extends Migration {
  override async up(): Promise<void> {
    // ---------------------------------------------------------------- AUD-01
    this.addSql(`alter table wager_transactions add column result_balance_currency char(3);`);
    // Backfill (terminal rows are immutable for the application; the migration owner may fix history).
    this.addSql(`alter table wager_transactions disable trigger wager_tx_guard_trg;`);
    this.addSql(`
      update wager_transactions t set result_balance_currency = w.currency
        from wallets w where w.id = t.wallet_id and t.result_balance_amount is not null;`);
    this.addSql(`alter table wager_transactions enable trigger wager_tx_guard_trg;`);
    this.addSql(`
      alter table wager_transactions add constraint wager_tx_result_balance_pair
        check ((result_balance_amount is null) = (result_balance_currency is null));`);
    this.addSql(`alter table wallets add constraint wallets_id_currency_unique unique (id, currency);`);
    this.addSql(`
      alter table wager_transactions add constraint wager_tx_result_balance_wallet_currency_fk
        foreign key (wallet_id, result_balance_currency) references wallets (id, currency);`);

    // ---------------------------------------------------------------- AUD-02
    this.addSql(`
      alter table wallet_ledger_entries add constraint ledger_wallet_currency_fk
        foreign key (wallet_id, currency) references wallets (id, currency);`);

    this.addSql(`
      create function ledger_transaction_check() returns trigger language plpgsql as $$
      declare
        t record;
        reference_kind text;
        expected text;
      begin
        select status, kind, amount, currency, reference_transaction_id into t
          from wager_transactions where id = new.transaction_id;
        if t.status is distinct from 'PROCESSED' then
          raise exception 'ledger entry % references transaction % in status %', new.id, new.transaction_id, t.status
            using errcode = '23514';
        end if;
        if t.kind = 'LOSS' then
          raise exception 'LOSS transaction % cannot have ledger entries', new.transaction_id using errcode = '23514';
        end if;
        if new.amount <> t.amount or new.currency <> t.currency then
          raise exception 'ledger entry % (% %) differs from transaction % (% %)',
            new.id, new.amount, new.currency, new.transaction_id, t.amount, t.currency using errcode = '23514';
        end if;
        if t.kind = 'ROLLBACK' then
          select kind into reference_kind from wager_transactions where id = t.reference_transaction_id;
          expected := case reference_kind when 'BET' then 'CREDIT' when 'WIN' then 'DEBIT' when 'REFUND' then 'DEBIT' end;
        else
          expected := case t.kind when 'BET' then 'DEBIT' when 'WIN' then 'CREDIT' when 'REFUND' then 'CREDIT' when 'OPENING' then 'CREDIT' end;
        end if;
        if expected is null or new.direction <> expected then
          raise exception 'ledger entry % direction % is not valid for % transaction %', new.id, new.direction, t.kind, new.transaction_id
            using errcode = '23514';
        end if;
        return null;
      end $$;`);
    this.addSql(`
      create constraint trigger ledger_transaction_check_trg
        after insert on wallet_ledger_entries
        deferrable initially deferred
        for each row execute function ledger_transaction_check();`);

    this.addSql(`
      create function wager_tx_ledger_presence() returns trigger language plpgsql as $$
      begin
        if new.status = 'PROCESSED' and new.kind <> 'LOSS' and not exists (
             select 1 from wallet_ledger_entries e where e.wallet_id = new.wallet_id and e.transaction_id = new.id) then
          raise exception 'processed % transaction % has no ledger entry', new.kind, new.id using errcode = '23514';
        end if;
        return null;
      end $$;`);
    this.addSql(`
      create constraint trigger wager_tx_ledger_presence_trg
        after insert or update of status on wager_transactions
        deferrable initially deferred
        for each row execute function wager_tx_ledger_presence();`);
  }

  override async down(): Promise<void> {
    this.addSql(`drop trigger if exists wager_tx_ledger_presence_trg on wager_transactions;`);
    this.addSql(`drop function if exists wager_tx_ledger_presence();`);
    this.addSql(`drop trigger if exists ledger_transaction_check_trg on wallet_ledger_entries;`);
    this.addSql(`drop function if exists ledger_transaction_check();`);
    this.addSql(`alter table wallet_ledger_entries drop constraint if exists ledger_wallet_currency_fk;`);
    this.addSql(`alter table wager_transactions drop constraint if exists wager_tx_result_balance_wallet_currency_fk;`);
    this.addSql(`alter table wager_transactions drop constraint if exists wager_tx_result_balance_pair;`);
    this.addSql(`alter table wallets drop constraint if exists wallets_id_currency_unique;`);
    this.addSql(`alter table wager_transactions drop column if exists result_balance_currency;`);
  }
}
