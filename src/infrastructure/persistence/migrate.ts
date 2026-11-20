import type { MikroORM } from '@mikro-orm/postgresql';
import { createOrm } from './orm';

type Command = 'up' | 'down' | 'down-all' | 'status';

/**
 * Migration CLI: bun src/infrastructure/persistence/migrate.ts <up|down [--all]|status>
 *
 * Several instances may start at once (RUN_MIGRATIONS=true). A transaction-scoped
 * advisory lock held on a dedicated connection serialises them. It is used for
 * migrations only — never on the wallet path.
 */
export async function runMigrations(databaseUrl: string, command: Command): Promise<string[]> {
  const orm = await createOrm(databaseUrl, 3);
  try {
    const knex = orm.em.getConnection().getKnex();
    return await knex.transaction(async (trx) => {
      await trx.raw('select pg_advisory_xact_lock(724001)');
      return execute(orm, command);
    });
  } finally {
    await orm.close(true);
  }
}

async function execute(orm: MikroORM, command: Command): Promise<string[]> {
  const migrator = orm.getMigrator();
  switch (command) {
    case 'up':
      return (await migrator.up()).map((m) => `applied ${m.name}`);
    case 'down':
      return (await migrator.down()).map((m) => `reverted ${m.name}`);
    case 'down-all':
      return (await migrator.down({ to: 0 })).map((m) => `reverted ${m.name}`);
    case 'status': {
      const executed = await migrator.getExecutedMigrations();
      const pending = await migrator.getPendingMigrations();
      return [...executed.map((m) => `executed ${m.name}`), ...pending.map((m) => `pending  ${m.name}`)];
    }
  }
}

if (import.meta.main) {
  const arg = process.argv[2] ?? 'up';
  const command = arg === 'down' && process.argv.includes('--all') ? 'down-all' : arg;
  if (!['up', 'down', 'down-all', 'status'].includes(command)) {
    console.error(`unknown command ${arg}`);
    process.exit(2);
  }
  const url = process.env.DATABASE_URL ?? 'postgresql://postgres:postgres@localhost:5432/wagering';
  runMigrations(url, command as Command)
    .then((lines) => {
      for (const line of lines.length ? lines : ['nothing to do']) console.log(line);
      process.exit(0);
    })
    .catch((error) => {
      console.error(error);
      process.exit(1);
    });
}
