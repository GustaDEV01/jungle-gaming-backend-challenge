import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import { ProcessWagerTransactionUseCase } from '../../src/application/process-wager-transaction.use-case';
import { TRANSACTION_RUNNER, type TransactionRunner, type WageringUnitOfWork } from '../../src/application/ports';
import { wagerPayloadHash, type ProcessingMeta, type WagerCommand } from '../../src/application/wager-command';
import type { QueueUrls } from '../../src/infrastructure/messaging/sqs';
import { createWallet, deleteQueues, dropDatabase, ledgerStats, queues, sql, startApp, testEnv, uniqueName, type RunningApp, type TestEnv } from '../support/infra';

/**
 * Atomicity between inbox, wager transaction, wallet, ledger and outbox: the
 * real use case runs inside the real SQL transaction and a failure is forced
 * right before COMMIT. Nothing may survive.
 */
let t: TestEnv;
let app: RunningApp;
let db: SQL;
let q: QueueUrls;

beforeAll(async () => {
  t = await testEnv('atom', { SQS_CONSUMER_ENABLED: 'false', OUTBOX_PUBLISHER_ENABLED: 'false', PENDING_REFERENCE_WORKER_ENABLED: 'false' });
  q = await queues(t.env);
  app = await startApp(t.env);
  db = sql(t.databaseUrl);
});

afterAll(async () => {
  await app?.close();
  await db?.close();
  await deleteQueues(q);
  await dropDatabase(t.databaseUrl);
});

async function counts(walletId: string, messageId: string) {
  const [row] = await db`
    select (select count(*)::int from wager_transactions where wallet_id = ${walletId} and kind <> 'OPENING') as txs,
           (select count(*)::int from wallet_ledger_entries where wallet_id = ${walletId}) as ledger,
           (select count(*)::int from outbox_messages where aggregate_id = ${walletId}) as outbox,
           (select count(*)::int from inbox_messages where message_id = ${messageId}) as inbox,
           (select balance_amount::text from wallets where id = ${walletId}) as balance,
           (select version from wallets where id = ${walletId}) as version`;
  return row as { txs: number; ledger: number; outbox: number; inbox: number; balance: string; version: number };
}

describe('atomicity', () => {
  test('failure before COMMIT rolls back inbox + transaction + wallet + ledger + outbox together', async () => {
    const { walletId, playerId } = await createWallet(app.url, '100.00');
    const before = await counts(walletId, 'none');
    const useCase = app.app.get(ProcessWagerTransactionUseCase);
    const runner = app.app.get<TransactionRunner>(TRANSACTION_RUNNER);
    const messageId = uniqueName('atomic-msg');
    const cmd: WagerCommand = {
      providerId: 'provider-a',
      externalTransactionId: uniqueName('atomic'),
      idempotencyKey: uniqueName('key'),
      playerId,
      walletId,
      roundId: 'r-1',
      gameId: 'g-1',
      kind: 'BET' as WagerCommand['kind'],
      money: { amount: '30.00', currency: 'BRL' },
    };
    const meta: ProcessingMeta = { source: 'sqs', correlationId: 'c-1', inbox: { consumerName: 'atomicity-test', messageId, payloadHash: 'b'.repeat(64) } };
    const process = (useCase as unknown as { process: (u: WageringUnitOfWork, c: WagerCommand, h: string, m: ProcessingMeta) => Promise<unknown> }).process.bind(useCase);

    let seenInside: Awaited<ReturnType<typeof counts>> | undefined;
    await expect(
      runner.run(async (uow) => {
        const result = await process(uow, cmd, wagerPayloadHash(cmd), meta);
        expect((result as { type: string }).type).toBe('transaction');
        // Everything was written inside this transaction... (visible only to it, so read from outside shows nothing)
        seenInside = await counts(walletId, messageId);
        throw new Error('simulated crash before COMMIT');
      }, 'atomicity-test'),
    ).rejects.toThrow('simulated crash before COMMIT');

    // ...but outside / after the rollback nothing survived
    expect(seenInside).toEqual(before); // other sessions never saw uncommitted rows
    const after = await counts(walletId, messageId);
    expect(after).toEqual({ ...before, inbox: 0 });
    expect(after.balance).toBe('100.00');
    expect(after.version).toBe(1);

    // The very same command now commits everything together
    const committed = await useCase.execute(cmd, meta);
    expect(committed.type).toBe('transaction');
    const final = await counts(walletId, messageId);
    expect(final.txs).toBe(1);
    expect(final.ledger).toBe(2);
    expect(final.inbox).toBe(1);
    expect(final.balance).toBe('70.00');
    expect(final.version).toBe(2);
    // OPENING: Processed + BalanceChanged; BET: Processed + BalanceChanged
    expect(final.outbox).toBe(4);
  });

  test('wallet opening: wallet, OPENING transaction, CREDIT entry and events are committed together', async () => {
    const { walletId } = await createWallet(app.url, '250.75');
    const [row] = await db`
      select (select count(*)::int from wager_transactions where wallet_id = ${walletId} and kind = 'OPENING' and status = 'PROCESSED') as opening,
             (select count(*)::int from wallet_ledger_entries where wallet_id = ${walletId} and direction = 'CREDIT' and amount = 250.75) as credit,
             (select count(*)::int from outbox_messages where aggregate_id = ${walletId}) as events`;
    expect(row).toEqual({ opening: 1, credit: 1, events: 2 });
    const zero = await createWallet(app.url, '0.00');
    expect(await ledgerStats(db, zero.walletId)).toMatchObject({ entries: 0, balance: '0.00' });
  });

  test('events exist only for committed transactions (outbox written in the same transaction)', async () => {
    const [row] = await db`
      select count(*)::int as orphan from outbox_messages o
       where o.event_type in ('WagerTransactionProcessed','WagerTransactionRejected','WagerTransactionPendingReference')
         and not exists (select 1 from wager_transactions t where t.id = (o.payload->'data'->>'transactionId')::uuid)`;
    expect(row.orphan).toBe(0);
  });
});
