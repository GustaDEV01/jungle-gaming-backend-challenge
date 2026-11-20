import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import { runMigrations } from '../../src/infrastructure/persistence/migrate';
import { dropDatabase, freshDatabase, sql } from '../support/infra';

/**
 * The schema itself enforces the invariants (README §5.9): these tests bypass
 * the application and hit PostgreSQL directly.
 */
let url: string;
let db: SQL;

const W = '00000000-0000-7000-8000-000000000001';
const TX_OPEN = '00000000-0000-7000-8000-0000000000a1';
const TX_BET = '00000000-0000-7000-8000-0000000000a2';
const hash = 'a'.repeat(64);

async function expectSqlError(fn: () => Promise<unknown>, pattern: RegExp): Promise<void> {
  let error: unknown;
  try {
    await fn();
  } catch (e) {
    error = e;
  }
  expect(error).toBeDefined();
  expect(String((error as Error).message ?? error)).toMatch(pattern);
}

beforeAll(async () => {
  url = await freshDatabase('schema');
  db = sql(url);
  // A valid wallet with its opening entry, created atomically (the deferred triggers check at COMMIT).
  await db.begin(async (tx) => {
    await tx`insert into wallets (id, player_id, currency, balance_amount, version, created_at, updated_at)
             values (${W}, 'p-1', 'BRL', 100.00, 1, now(), now())`;
    await tx`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id,
               player_id, round_id, game_id, kind, amount, currency, status, result_balance_amount, result_balance_currency, created_at, processed_at)
             values (${TX_OPEN}, 'internal', 'opening:w1', 'internal:opening:w1', ${hash}, ${W}, 'p-1', 'opening', 'internal',
               'OPENING', 100.00, 'BRL', 'PROCESSED', 100.00, 'BRL', now(), now())`;
    await tx`insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
             values (gen_random_uuid(), ${W}, ${TX_OPEN}, 'CREDIT', 100.00, 'BRL', 0.00, 100.00, 1, now())`;
  });
});

afterAll(async () => {
  await db?.close();
  await dropDatabase(url);
});

describe('migrations', () => {
  test('are reversible on a clean database: up -> down -> up', async () => {
    const other = await freshDatabase('migr');
    try {
      const odb = sql(other);
      const tables = async () =>
        (await odb`select count(*)::int as n from information_schema.tables where table_schema = 'public' and table_name <> 'mikro_orm_migrations'`)[0].n;
      expect(await tables()).toBe(6);
      expect(await runMigrations(other, 'down-all')).toEqual([
        'reverted Migration20261120000003_ledger_transaction_integrity',
        'reverted Migration20261120000002_outbox_sequence',
        'reverted Migration20261120000001_initial_schema',
      ]);
      expect(await tables()).toBe(0);
      const [{ n: functions }] = await odb`select count(*)::int as n from pg_proc p join pg_namespace s on s.oid = p.pronamespace where s.nspname = 'public'`;
      expect(functions).toBe(0);
      expect(await runMigrations(other, 'up')).toEqual([
        'applied Migration20261120000001_initial_schema',
        'applied Migration20261120000002_outbox_sequence',
        'applied Migration20261120000003_ledger_transaction_integrity',
      ]);
      const [{ n: seqColumn }] = await odb`select count(*)::int as n from information_schema.columns where table_name = 'outbox_messages' and column_name = 'seq'`;
      expect(seqColumn).toBe(1);
      expect(await tables()).toBe(6);
      await odb.close();
    } finally {
      await dropDatabase(other);
    }
  });
});

describe('wallets', () => {
  test('balance cannot be negative (CHECK)', async () => {
    await expectSqlError(() => db`update wallets set balance_amount = -1 where id = ${W}`, /wallets_balance_non_negative/);
  });

  test('one wallet per (player_id, currency) (UNIQUE)', async () => {
    await expectSqlError(
      () => db`insert into wallets (id, player_id, currency, balance_amount, version, created_at, updated_at)
               values (gen_random_uuid(), 'p-1', 'BRL', 0, 1, now(), now())`,
      /wallets_player_currency_unique/,
    );
  });

  test('balance change without the matching ledger entry is refused at COMMIT', async () => {
    await expectSqlError(() => db`update wallets set balance_amount = 90.00, version = 2 where id = ${W}`, /no matching ledger entry/);
    await expectSqlError(() => db`update wallets set version = 5 where id = ${W}`, /version changed without balance change/);
    await expectSqlError(() => db`update wallets set balance_amount = 90.00, version = 3 where id = ${W}`, /increment by 1/);
  });

  test('money columns are exact numeric(20,2)', async () => {
    const [col] = await db`select numeric_precision, numeric_scale from information_schema.columns where table_name = 'wallets' and column_name = 'balance_amount'`;
    expect(col).toEqual({ numeric_precision: 20, numeric_scale: 2 });
  });
});

describe('wager_transactions', () => {
  const insertTx = (id: string, extId: string, key: string, failureCode: string | null = 'INSUFFICIENT_FUNDS') =>
    db.unsafe(`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id,
               player_id, round_id, game_id, kind, amount, currency, status, failure_code, result_balance_amount, result_balance_currency, created_at, processed_at)
             values ('${id}', 'provider-a', '${extId}', '${key}', '${hash}', '${W}', 'p-1', 'r', 'g', 'BET', 10.00, 'BRL',
               'REJECTED', ${failureCode ? `'${failureCode}'` : 'null'}, 100.00, 'BRL', now(), now())`);

  test('idempotency_key is globally unique and (provider_id, external_transaction_id) is unique', async () => {
    await db.unsafe(`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id,
               player_id, round_id, game_id, kind, amount, currency, status, failure_code, result_balance_amount, result_balance_currency, created_at, processed_at)
             values ('${TX_BET}', 'provider-a', 'ext-1', 'key-1', '${hash}', '${W}', 'p-1', 'r', 'g', 'BET', 10.00, 'BRL',
               'REJECTED', 'INSUFFICIENT_FUNDS', 100.00, 'BRL', now(), now())`);
    await expectSqlError(() => insertTx(crypto.randomUUID(), 'ext-2', 'key-1'), /wager_tx_idempotency_key_unique/);
    await expectSqlError(() => insertTx(crypto.randomUUID(), 'ext-1', 'key-2'), /wager_tx_provider_external_unique/);
  });

  test('REJECTED requires a failure code; REFUND requires a reference; unknown provider refused (FK)', async () => {
    await expectSqlError(() => insertTx(crypto.randomUUID(), 'ext-3', 'key-3', null), /wager_tx_failure_code_iff_failed/);
    await expectSqlError(
      () =>
        db.unsafe(`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id,
               player_id, round_id, game_id, kind, amount, currency, status, created_at, reference_attempts, next_reference_check_at)
             values (gen_random_uuid(), 'provider-a', 'ext-4', 'key-4', '${hash}', '${W}', 'p-1', 'r', 'g', 'REFUND', 10.00, 'BRL',
               'PENDING_REFERENCE', now(), 1, now())`),
      /wager_tx_reference_required/,
    );
    await expectSqlError(
      () =>
        db.unsafe(`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id,
               player_id, round_id, game_id, kind, amount, currency, status, created_at)
             values (gen_random_uuid(), 'nobody', 'ext-5', 'key-5', '${hash}', '${W}', 'p-1', 'r', 'g', 'BET', 10.00, 'BRL', 'PENDING', now())`),
      /foreign key/,
    );
  });

  test('terminal transactions are immutable and transactions are never deleted', async () => {
    await expectSqlError(() => db`update wager_transactions set status = 'PROCESSED' where id = ${TX_BET}`, /is terminal/);
    await expectSqlError(() => db`delete from wager_transactions where id = ${TX_BET}`, /cannot be deleted/);
  });

  test('a transaction can be reverted only once (partial unique index)', async () => {
    const mk = (tx: SQL, kind: string, ext: string) =>
      tx.unsafe(`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id,
               player_id, round_id, game_id, kind, amount, currency, status, result_balance_amount, result_balance_currency, created_at, processed_at,
               reference_external_transaction_id, reference_transaction_id)
             values (gen_random_uuid(), 'provider-a', '${ext}', '${ext}', '${hash}', '${W}', 'p-1', 'r', 'g', '${kind}', 10.00, 'BRL',
               'PROCESSED', 100.00, 'BRL', now(), now(), 'opening:w1', '${TX_OPEN}')`);
    // Both reversals in ONE transaction: the partial unique index fires on the second INSERT,
    // before COMMIT (where the deferred "processed needs a ledger entry" check would also refuse them).
    await expectSqlError(() => db.begin(async (tx) => { await mk(tx, 'ROLLBACK', 'rb-1'); await mk(tx, 'REFUND', 'rf-1'); }), /wager_tx_single_reversal_uq/);
    await expectSqlError(() => db.begin(async (tx) => { await mk(tx, 'ROLLBACK', 'rb-2'); await mk(tx, 'ROLLBACK', 'rb-3'); }), /wager_tx_single_reversal_uq/);
  });
});

describe('wallet_ledger_entries', () => {
  test('append-only: UPDATE, DELETE and TRUNCATE are refused', async () => {
    await expectSqlError(() => db`update wallet_ledger_entries set amount = 1 where wallet_id = ${W}`, /append-only/);
    await expectSqlError(() => db`delete from wallet_ledger_entries where wallet_id = ${W}`, /append-only/);
    await expectSqlError(() => db`truncate wallet_ledger_entries cascade`, /append-only/);
    const [{ n }] = await db`select count(*)::int as n from wallet_ledger_entries where wallet_id = ${W}`;
    expect(n).toBe(1);
  });

  test('entry arithmetic must balance (CHECK)', async () => {
    await expectSqlError(
      () => db`insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
               values (gen_random_uuid(), ${W}, ${TX_BET}, 'DEBIT', 10.00, 'BRL', 100.00, 95.00, 2, now())`,
      /ledger_entry_balanced/,
    );
  });

  test('at most one entry per transaction, one per wallet version, chained, and only for applied versions', async () => {
    await expectSqlError(
      () => db`insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
               values (gen_random_uuid(), ${W}, ${TX_OPEN}, 'CREDIT', 1.00, 'BRL', 100.00, 101.00, 2, now())`,
      /ledger_wallet_tx_unique/,
    );
    await expectSqlError(
      () => db`insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
               values (gen_random_uuid(), ${W}, ${TX_BET}, 'CREDIT', 1.00, 'BRL', 0.00, 1.00, 1, now())`,
      /ledger_wallet_version_unique/,
    );
    // entry for a version the wallet never reached (no wallet update) -> refused at commit
    await expectSqlError(
      () => db`insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
               values (gen_random_uuid(), ${W}, ${TX_BET}, 'DEBIT', 10.00, 'BRL', 100.00, 90.00, 2, now())`,
      /not applied to wallet/,
    );
  });

  test('ledger entry must belong to the wallet of its transaction (composite FK)', async () => {
    await expectSqlError(
      () => db`insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
               values (gen_random_uuid(), gen_random_uuid(), ${TX_BET}, 'DEBIT', 10.00, 'BRL', 100.00, 90.00, 2, now())`,
      /foreign key/,
    );
  });

  test('a correct movement (PROCESSED BET + wallet + ledger in one transaction) is accepted', async () => {
    // TX_BET is REJECTED: booking it would be refused (ledger_transaction_check). A real movement needs a PROCESSED BET.
    const processedBet = '00000000-0000-7000-8000-0000000000b1';
    await db.begin(async (tx) => {
      await tx`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id,
                 player_id, round_id, game_id, kind, amount, currency, status, result_balance_amount, result_balance_currency, created_at, processed_at)
               values (${processedBet}, 'provider-a', 'ext-ok', 'key-ok', ${hash}, ${W}, 'p-1', 'r', 'g', 'BET', 10.00, 'BRL',
                 'PROCESSED', 90.00, 'BRL', now(), now())`;
      await tx`update wallets set balance_amount = 90.00, version = 2 where id = ${W}`;
      await tx`insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
               values (gen_random_uuid(), ${W}, ${processedBet}, 'DEBIT', 10.00, 'BRL', 100.00, 90.00, 2, now())`;
    });
    const [w] = await db`select balance_amount::text as b, version from wallets where id = ${W}`;
    expect(w).toEqual({ b: '90.00', version: 2 });
  });
});

describe('inbox', () => {
  test('(consumer_name, message_id) is unique', async () => {
    await db`insert into inbox_messages (consumer_name, message_id, payload_hash, received_at) values ('c', 'm-1', ${hash}, now())`;
    await expectSqlError(
      () => db`insert into inbox_messages (consumer_name, message_id, payload_hash, received_at) values ('c', 'm-1', ${hash}, now())`,
      /inbox_consumer_message_pk/,
    );
    await db`insert into inbox_messages (consumer_name, message_id, payload_hash, received_at) values ('other', 'm-1', ${hash}, now())`;
  });
});
