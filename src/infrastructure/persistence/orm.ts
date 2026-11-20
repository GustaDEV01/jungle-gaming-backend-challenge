import { MikroORM, PostgreSqlDriver, defineConfig } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';
import { Migration20261120000001_initial_schema } from './migrations/Migration20261120000001_initial_schema';
import { Migration20261120000002_outbox_sequence } from './migrations/Migration20261120000002_outbox_sequence';
import { ALL_SCHEMAS } from './schemas';

export const MIKRO_ORM = Symbol('MIKRO_ORM');

/** Explicit list: no filesystem globbing, works the same from src (Bun) and dist. */
export const MIGRATIONS = [
  { name: 'Migration20261120000001_initial_schema', class: Migration20261120000001_initial_schema },
  { name: 'Migration20261120000002_outbox_sequence', class: Migration20261120000002_outbox_sequence },
];

export function ormConfig(databaseUrl: string, poolMax = 20) {
  return defineConfig({
    driver: PostgreSqlDriver,
    clientUrl: databaseUrl,
    entities: ALL_SCHEMAS,
    extensions: [Migrator],
    forceUtcTimezone: true,
    allowGlobalContext: false,
    debug: false,
    pool: { min: 0, max: poolMax, idleTimeoutMillis: 10_000 },
    discovery: { warnWhenNoEntities: false },
    migrations: {
      migrationsList: MIGRATIONS,
      tableName: 'mikro_orm_migrations',
      transactional: true,
      allOrNothing: true,
      snapshot: false,
    },
  });
}

export async function createOrm(databaseUrl: string, poolMax = 20): Promise<MikroORM> {
  return MikroORM.init(ormConfig(databaseUrl, poolMax));
}
