import { mkdirSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { Subprocess } from 'bun';
import { http, uniqueName } from './infra';

const ROOT = resolve(import.meta.dir, '..', '..');
const LOG_DIR = process.env.TEST_PROCESS_LOG_DIR ?? join(tmpdir(), 'wagering-test-logs');
mkdirSync(LOG_DIR, { recursive: true });

export async function freePort(): Promise<number> {
  return new Promise((ok, fail) => {
    const server = createServer();
    server.once('error', fail);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as { port: number };
      server.close(() => ok(port));
    });
  });
}

export interface Instance {
  name: string;
  url: string;
  logFile: string;
  proc: Subprocess;
  exitCode(): Promise<number>;
  kill(): Promise<void>;
  logs(): Promise<string>;
}

/**
 * Starts a REAL application process (`bun src/main.ts`) — separate OS process,
 * own connection pool, own SQS consumer and workers. Used to prove the
 * multi-instance guarantees without any shared memory.
 */
export async function spawnInstance(env: Record<string, string>, name: string, extra: Record<string, string> = {}): Promise<Instance> {
  const port = await freePort();
  const logFile = join(LOG_DIR, `${uniqueName(name)}.log`);
  const proc = Bun.spawn([process.execPath, 'src/main.ts'], {
    cwd: ROOT,
    env: { ...process.env, ...env, PORT: String(port), INSTANCE_ID: name, LOG_LEVEL: 'info', ...extra },
    stdout: Bun.file(logFile),
    stderr: Bun.file(`${logFile}.err`),
  });
  const instance: Instance = {
    name,
    url: `http://127.0.0.1:${port}`,
    logFile,
    proc,
    exitCode: () => proc.exited,
    kill: async () => {
      if (proc.exitCode === null && !proc.killed) proc.kill('SIGKILL');
      await proc.exited;
    },
    logs: async () => `${await Bun.file(logFile).text().catch(() => '')}\n${await Bun.file(`${logFile}.err`).text().catch(() => '')}`,
  };
  const started = Date.now();
  while (Date.now() - started < 60_000) {
    if (proc.exitCode !== null) throw new Error(`${name} exited early (${proc.exitCode}):\n${(await instance.logs()).slice(-3000)}`);
    try {
      const res = await http('GET', `${instance.url}/health/ready`);
      if (res.status === 200) return instance;
    } catch {
      /* not up yet */
    }
    await Bun.sleep(200);
  }
  await instance.kill();
  throw new Error(`${name} did not become ready:\n${(await instance.logs()).slice(-3000)}`);
}

export async function metric(instance: Instance, pattern: RegExp): Promise<number> {
  const text = await fetch(`${instance.url}/metrics`).then((r) => r.text());
  let total = 0;
  for (const line of text.split('\n')) {
    if (!line.startsWith('#') && pattern.test(line)) total += Number.parseFloat(line.slice(line.lastIndexOf(' ') + 1));
  }
  return total;
}
