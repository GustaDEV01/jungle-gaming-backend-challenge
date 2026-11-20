import { createApp } from './bootstrap';
import { loadConfig } from './infrastructure/config';
import { runMigrations } from './infrastructure/persistence/migrate';

async function main(): Promise<void> {
  const config = loadConfig();
  if (process.env.RUN_MIGRATIONS === 'true') {
    // Concurrent instances are safe: MikroORM's migrator runs each migration in a
    // transaction and the DDL takes locks; a loser fails fast on the duplicate and retries.
    for (let attempt = 1; ; attempt++) {
      try {
        await runMigrations(config.database.url, 'up');
        break;
      } catch (error) {
        if (attempt >= 5) throw error;
        await Bun.sleep(1_000 * attempt);
      }
    }
  }
  const app = await createApp(config);
  await app.listen(config.port, '0.0.0.0');
  process.stdout.write(
    `${JSON.stringify({ ts: new Date().toISOString(), level: 'info', msg: 'listening', port: config.port, instanceId: config.instanceId })}\n`,
  );
}

main().catch((error) => {
  process.stderr.write(`${JSON.stringify({ level: 'error', msg: 'fatal startup error', error: String(error?.stack ?? error) })}\n`);
  process.exit(1);
});
