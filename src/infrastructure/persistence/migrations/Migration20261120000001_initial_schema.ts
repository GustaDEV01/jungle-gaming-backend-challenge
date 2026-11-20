import { Migration } from '@mikro-orm/migrations';

/**
 * Initial schema. Every invariant that can be expressed declaratively lives
 * here (README §5.9): uniqueness, non-negativity, ledger arithmetic,
 * ledger immutability, terminal-state immutability and single reversal.
 */
export class Migration20261120000001_initial_schema extends Migration {
  override async up(): Promise<void> {
    // ---------------------------------------------------------------- providers
    this.addSql(`
      create table providers (
        id          text primary key check (id ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
        name        text not null,
        active      boolean not null default true,
        is_internal boolean not null default false,
        created_at  timestamptz not null default now()
      );`);
    this.addSql(`
      insert into providers (id, name, active, is_internal) values
        ('internal',   'Internal (wallet opening)', true, true),
        ('provider-a', 'Provider A', true, false),
        ('provider-b', 'Provider B', true, false),
        ('provider-c', 'Provider C', true, false);`);

    // ------------------------------------------------------------------ wallets
    this.addSql(`
      create table wallets (
        id             uuid primary key,
        player_id      text not null check (length(player_id) between 1 and 128),
        currency       char(3) not null check (currency ~ '^[A-Z]{3}$'),
        balance_amount numeric(20,2) not null,
        version        integer not null check (version >= 1),
        created_at     timestamptz not null,
        updated_at     timestamptz not null,
        constraint wallets_balance_non_negative check (balance_amount >= 0),
        constraint wallets_player_currency_unique unique (player_id, currency)
      );`);

    // ------------------------------------------------------- wager_transactions
    this.addSql(`
      create table wager_transactions (
        id                                uuid primary key,
        provider_id                       text not null references providers(id),
        external_transaction_id           text not null check (length(external_transaction_id) between 1 and 128),
        idempotency_key                   text not null check (length(idempotency_key) between 1 and 255),
        payload_hash                      char(64) not null check (payload_hash ~ '^[0-9a-f]{64}$'),
        wallet_id                         uuid not null references wallets(id),
        player_id                         text not null,
        round_id                          text not null,
        game_id                           text not null,
        kind                              text not null check (kind in ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount                            numeric(20,2) not null check (amount >= 0),
        currency                          char(3) not null check (currency ~ '^[A-Z]{3}$'),
        reference_external_transaction_id text,
        reference_transaction_id          uuid references wager_transactions(id),
        status                            text not null check (status in ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code                      text,
        result_balance_amount             numeric(20,2) check (result_balance_amount >= 0),
        reference_attempts                integer not null default 0 check (reference_attempts >= 0),
        next_reference_check_at           timestamptz,
        created_at                        timestamptz not null,
        processed_at                      timestamptz,
        correlation_id                    text,

        constraint wager_tx_idempotency_key_unique unique (idempotency_key),
        constraint wager_tx_provider_external_unique unique (provider_id, external_transaction_id),
        -- target of the composite FK from the ledger (ledger.wallet_id must match the tx wallet)
        constraint wager_tx_id_wallet_unique unique (id, wallet_id),

        constraint wager_tx_reference_required check (
          kind not in ('REFUND','ROLLBACK') or reference_external_transaction_id is not null),
        constraint wager_tx_no_reference_for_bet check (
          kind not in ('BET','OPENING') or reference_external_transaction_id is null),
        constraint wager_tx_positive_amount check (kind = 'LOSS' or amount > 0),
        constraint wager_tx_opening_internal check ((kind = 'OPENING') = (provider_id = 'internal')),
        constraint wager_tx_failure_code_iff_failed check (
          (status in ('REJECTED','FAILED')) = (failure_code is not null)),
        constraint wager_tx_processed_at_when_terminal check (
          status not in ('PROCESSED','REJECTED','FAILED') or processed_at is not null),
        constraint wager_tx_result_balance_when_processed check (
          status <> 'PROCESSED' or result_balance_amount is not null),
        constraint wager_tx_pending_reference_schedule check (
          status <> 'PENDING_REFERENCE' or next_reference_check_at is not null),
        constraint wager_tx_reversal_has_reference_id check (
          status <> 'PROCESSED' or kind not in ('REFUND','ROLLBACK') or reference_transaction_id is not null)
      );`);

    // A transaction can be reverted at most once (by REFUND or ROLLBACK, whichever comes first).
    this.addSql(`
      create unique index wager_tx_single_reversal_uq
        on wager_transactions (reference_transaction_id)
        where kind in ('REFUND','ROLLBACK') and status = 'PROCESSED';`);
    // Pending-reference worker scan.
    this.addSql(`
      create index wager_tx_pending_reference_idx
        on wager_transactions (next_reference_check_at)
        where status = 'PENDING_REFERENCE';`);
    this.addSql(`create index wager_tx_wallet_created_idx on wager_transactions (wallet_id, created_at);`);

    // Terminal states are immutable and transactions are never deleted.
    this.addSql(`
      create function wager_tx_guard() returns trigger language plpgsql as $$
      begin
        if tg_op = 'DELETE' then
          raise exception 'wager_transactions rows cannot be deleted' using errcode = 'P0001';
        end if;
        if old.status in ('PROCESSED','REJECTED','FAILED') then
          raise exception 'wager transaction % is terminal (%) and cannot be modified', old.id, old.status using errcode = 'P0001';
        end if;
        if new.id <> old.id or new.provider_id <> old.provider_id
           or new.external_transaction_id <> old.external_transaction_id
           or new.idempotency_key <> old.idempotency_key or new.payload_hash <> old.payload_hash
           or new.wallet_id <> old.wallet_id or new.kind <> old.kind
           or new.amount <> old.amount or new.currency <> old.currency then
          raise exception 'immutable columns of wager transaction % cannot change', old.id using errcode = 'P0001';
        end if;
        return new;
      end $$;`);
    this.addSql(`
      create trigger wager_tx_guard_trg before update or delete on wager_transactions
        for each row execute function wager_tx_guard();`);

    // ------------------------------------------------------- wallet_ledger_entries
    this.addSql(`
      create table wallet_ledger_entries (
        id              uuid primary key,
        wallet_id       uuid not null references wallets(id),
        transaction_id  uuid not null,
        direction       text not null check (direction in ('DEBIT','CREDIT')),
        amount          numeric(20,2) not null check (amount > 0),
        currency        char(3) not null check (currency ~ '^[A-Z]{3}$'),
        balance_before  numeric(20,2) not null check (balance_before >= 0),
        balance_after   numeric(20,2) not null check (balance_after >= 0),
        wallet_version  integer not null check (wallet_version >= 1),
        created_at      timestamptz not null,
        -- the entry belongs to the same wallet as its transaction
        constraint ledger_tx_wallet_fk foreign key (transaction_id, wallet_id)
          references wager_transactions (id, wallet_id),
        -- at most one entry per transaction per wallet
        constraint ledger_wallet_tx_unique unique (wallet_id, transaction_id),
        -- one entry per wallet version: two concurrent writers can never both append version N
        constraint ledger_wallet_version_unique unique (wallet_id, wallet_version),
        -- arithmetic of every entry
        constraint ledger_entry_balanced check (
          (direction = 'CREDIT' and balance_after = balance_before + amount) or
          (direction = 'DEBIT'  and balance_after = balance_before - amount))
      );`);
    this.addSql(`create index ledger_wallet_created_idx on wallet_ledger_entries (wallet_id, created_at, id);`);

    // Append-only: UPDATE, DELETE and TRUNCATE are refused.
    this.addSql(`
      create function ledger_append_only() returns trigger language plpgsql as $$
      begin
        raise exception 'wallet_ledger_entries is append-only (% refused)', tg_op using errcode = 'P0001';
      end $$;`);
    this.addSql(`
      create trigger ledger_no_update_delete before update or delete on wallet_ledger_entries
        for each row execute function ledger_append_only();`);
    this.addSql(`
      create trigger ledger_no_truncate before truncate on wallet_ledger_entries
        for each statement execute function ledger_append_only();`);

    // Balance <-> ledger coupling, checked at COMMIT (deferred) with O(1) index lookups:
    //  * every wallet balance change (and the opening balance) must have the ledger
    //    entry for exactly that version, ending exactly at that balance;
    //  * versions move by exactly +1 per change and never change without a balance change;
    //  * every ledger entry chains on the previous one (balance_before = previous balance_after)
    //    and the wallet has reached its version.
    this.addSql(`
      create function wallet_ledger_coupling() returns trigger language plpgsql as $$
      begin
        if tg_op = 'UPDATE' then
          if new.balance_amount = old.balance_amount then
            if new.version <> old.version then
              raise exception 'wallet % version changed without balance change', new.id using errcode = '23514';
            end if;
            return null;
          end if;
          if new.version <> old.version + 1 then
            raise exception 'wallet % version must increment by 1 per balance change', new.id using errcode = '23514';
          end if;
        elsif new.version <> 1 then
          raise exception 'wallet % must be created with version 1', new.id using errcode = '23514';
        elsif new.balance_amount = 0 then
          return null;
        end if;
        if not exists (
          select 1 from wallet_ledger_entries e
           where e.wallet_id = new.id and e.wallet_version = new.version and e.balance_after = new.balance_amount) then
          raise exception 'wallet % balance % (version %) has no matching ledger entry', new.id, new.balance_amount, new.version
            using errcode = '23514';
        end if;
        return null;
      end $$;`);
    this.addSql(`
      create constraint trigger wallet_ledger_coupling_trg
        after insert or update of balance_amount, version on wallets
        deferrable initially deferred
        for each row execute function wallet_ledger_coupling();`);

    this.addSql(`
      create function ledger_chain_check() returns trigger language plpgsql as $$
      declare
        prev_after numeric(20,2);
        wallet_version_now integer;
      begin
        select version into wallet_version_now from wallets where id = new.wallet_id;
        if wallet_version_now is null or wallet_version_now < new.wallet_version then
          raise exception 'ledger entry % references wallet version % not applied to wallet %', new.id, new.wallet_version, new.wallet_id
            using errcode = '23514';
        end if;
        select balance_after into prev_after from wallet_ledger_entries
         where wallet_id = new.wallet_id and wallet_version = new.wallet_version - 1;
        if prev_after is null then
          -- first entry: opening credit (v1) or first movement of a zero-opened wallet (v2)
          if new.balance_before <> 0 or new.wallet_version > 2 then
            raise exception 'ledger entry % breaks the chain of wallet %', new.id, new.wallet_id using errcode = '23514';
          end if;
        elsif prev_after <> new.balance_before then
          raise exception 'ledger entry % balance_before % does not match previous balance_after %', new.id, new.balance_before, prev_after
            using errcode = '23514';
        end if;
        return null;
      end $$;`);
    this.addSql(`
      create constraint trigger ledger_chain_check_trg
        after insert on wallet_ledger_entries
        deferrable initially deferred
        for each row execute function ledger_chain_check();`);

    // ------------------------------------------------------------ inbox_messages
    this.addSql(`
      create table inbox_messages (
        consumer_name  text not null,
        message_id     text not null check (length(message_id) between 1 and 255),
        payload_hash   char(64) not null,
        transaction_id uuid references wager_transactions(id),
        received_at    timestamptz not null,
        processed_at   timestamptz,
        constraint inbox_consumer_message_pk primary key (consumer_name, message_id)
      );`);

    // ----------------------------------------------------------- outbox_messages
    this.addSql(`
      create table outbox_messages (
        id              uuid primary key,
        aggregate_id    text not null,
        event_type      text not null,
        payload         jsonb not null,
        occurred_at     timestamptz not null,
        attempts        integer not null default 0 check (attempts >= 0),
        next_attempt_at timestamptz not null,
        published_at    timestamptz,
        last_error      text
      );`);
    this.addSql(`
      create index outbox_pending_idx on outbox_messages (next_attempt_at)
        where published_at is null;`);
  }

  override async down(): Promise<void> {
    this.addSql(`drop table if exists outbox_messages;`);
    this.addSql(`drop table if exists inbox_messages;`);
    this.addSql(`drop trigger if exists ledger_no_truncate on wallet_ledger_entries;`);
    this.addSql(`drop trigger if exists ledger_no_update_delete on wallet_ledger_entries;`);
    this.addSql(`drop table if exists wallet_ledger_entries;`);
    this.addSql(`drop function if exists ledger_append_only();`);
    this.addSql(`drop trigger if exists wager_tx_guard_trg on wager_transactions;`);
    this.addSql(`drop table if exists wager_transactions;`);
    this.addSql(`drop function if exists wager_tx_guard();`);
    this.addSql(`drop function if exists ledger_chain_check();`);
    this.addSql(`drop table if exists wallets;`);
    this.addSql(`drop function if exists wallet_ledger_coupling();`);
    this.addSql(`drop table if exists providers;`);
  }
}
