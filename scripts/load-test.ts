import { DeleteMessageBatchCommand, GetQueueUrlCommand, ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";

/**
 * Load test (optional differential) — `bun run test:load`.
 *
 * Targets running instances (default: the 3 docker compose instances).
 *   LOAD_TARGETS=http://localhost:3001,http://localhost:3002,http://localhost:3003
 *   LOAD_DURATION_S=30  LOAD_CONCURRENCY=48  LOAD_WALLETS=200  LOAD_HOT_SHARE=0.2  LOAD_DUP_SHARE=0.1
 *
 * Workload per request (round-robin over targets):
 *   - 80% BET 1.00..5.00, 20% WIN 1.00..10.00 on a random wallet;
 *   - LOAD_HOT_SHARE of requests hit ONE hot wallet (lock contention);
 *   - LOAD_DUP_SHARE of requests re-send a previous request (idempotent replay).
 * At the end: every wallet is reconciled and the metrics of every instance are
 * scraped (lock conflicts, outbox lag). Amounts are generated as strings in cents.
 */
const targets = (process.env.LOAD_TARGETS ?? 'http://localhost:3001,http://localhost:3002,http://localhost:3003').split(',');
const durationS = Number.parseInt(process.env.LOAD_DURATION_S ?? '30', 10);
const concurrency = Number.parseInt(process.env.LOAD_CONCURRENCY ?? '48', 10);
const walletCount = Number.parseInt(process.env.LOAD_WALLETS ?? '200', 10);
const hotShare = Number.parseFloat(process.env.LOAD_HOT_SHARE ?? '0.2');
const dupShare = Number.parseFloat(process.env.LOAD_DUP_SHARE ?? '0.1');

type Sent = { url: string; key: string; body: string };
const latencies: number[] = [];
const statuses = new Map<string, number>();
let networkErrors = 0;
const sent: Sent[] = [];

const cents = (min: number, max: number) => {
  const c = min * 100 + Math.floor(Math.random() * (max - min) * 100);
  return `${Math.floor(c / 100)}.${String(c % 100).padStart(2, '0')}`;
};

async function scrape(target: string): Promise<Map<string, number>> {
  const text = await fetch(`${target}/metrics`).then((r) => r.text());
  const out = new Map<string, number>();
  for (const line of text.split('\n')) {
    if (!line || line.startsWith('#')) continue;
    const idx = line.lastIndexOf(' ');
    const name = line.slice(0, idx);
    out.set(name, (out.get(name) ?? 0) + Number.parseFloat(line.slice(idx + 1)));
  }
  return out;
}

const sum = (m: Map<string, number>, prefix: string) => [...m].filter(([k]) => k.startsWith(prefix)).reduce((a, [, v]) => a + v, 0);
const pct = (sorted: number[], p: number) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? 0;

// Downstream consumer of the integration events (dedups by eventId, like a real consumer must).
const sqs = new SQSClient({
  region: 'us-east-1',
  endpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});
const eventIds = new Set<string>();
let received = 0;
let stopDrain = false;
let drainers: Promise<void>[] = [];

async function drainEvents(queueUrl: string): Promise<void> {
  while (!stopDrain) {
    try {
      const res = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: ['All'] }));
      const msgs = res.Messages ?? [];
      if (msgs.length === 0) continue;
      for (const m of msgs) {
        received++;
        eventIds.add(m.MessageAttributes?.eventId?.StringValue ?? m.MessageId!);
      }
      await sqs.send(new DeleteMessageBatchCommand({ QueueUrl: queueUrl, Entries: msgs.map((m, i) => ({ Id: String(i), ReceiptHandle: m.ReceiptHandle! })) }));
    } catch {
      await Bun.sleep(200);
    }
  }
}

async function main() {
  console.log(`targets=${targets.join(' ')} duration=${durationS}s concurrency=${concurrency} wallets=${walletCount} hot=${hotShare} dup=${dupShare}`);
  const eventsUrl = (await sqs.send(new GetQueueUrlCommand({ QueueName: process.env.SQS_EVENTS_QUEUE ?? 'wagering-events.fifo' }))).QueueUrl!;
  drainers = Array.from({ length: 4 }, () => drainEvents(eventsUrl));
  const before = await Promise.all(targets.map(scrape));
  const wallets: { id: string; playerId: string }[] = [];
  for (let i = 0; i < walletCount + 1; i++) {
    const playerId = `load-${Date.now().toString(36)}-${i}`;
    const res = await fetch(`${targets[i % targets.length]}/wallets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ playerId, initialBalance: { amount: i === 0 ? '1000000.00' : '500.00', currency: 'BRL' } }),
    });
    wallets.push({ id: ((await res.json()) as { id: string }).id, playerId });
  }
  const hot = wallets[0]!;
  let seq = 0;
  const deadline = performance.now() + durationS * 1000;
  const started = performance.now();

  const worker = async () => {
    while (performance.now() < deadline) {
      let req: Sent;
      const n = seq++;
      if (sent.length > 50 && Math.random() < dupShare) {
        req = sent[Math.floor(Math.random() * sent.length)]!;
      } else {
        const w = Math.random() < hotShare ? hot : wallets[1 + Math.floor(Math.random() * walletCount)]!;
        const kind = Math.random() < 0.8 ? 'BET' : 'WIN';
        const ext = `ld-${started.toFixed(0)}-${n}`;
        req = {
          url: targets[n % targets.length]!,
          key: `provider-a:${ext}`,
          body: JSON.stringify({
            providerId: 'provider-a',
            externalTransactionId: ext,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `r-${n % 1000}`,
            gameId: 'load',
            kind,
            money: { amount: kind === 'BET' ? cents(1, 5) : cents(1, 10), currency: 'BRL' },
          }),
        };
        sent.push(req);
      }
      const t0 = performance.now();
      try {
        const res = await fetch(`${req.url}/wagering/transactions`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'Idempotency-Key': req.key },
          body: req.body,
        });
        await res.arrayBuffer();
        latencies.push(performance.now() - t0);
        statuses.set(String(res.status), (statuses.get(String(res.status)) ?? 0) + 1);
      } catch {
        networkErrors++;
      }
    }
  };
  await Promise.all(Array.from({ length: concurrency }, worker));
  const elapsed = (performance.now() - started) / 1000;

  // let the outbox drain and sample its lag
  const endOfLoad = await Promise.all(targets.map(scrape));
  const backlogAtEnd = Math.max(...endOfLoad.map((x) => sum(x, 'wagering_outbox_pending_messages')));
  let maxOldest = 0;
  const drainStarted = performance.now();
  let drained = false;
  while (performance.now() - drainStarted < 180_000) {
    const m = await Promise.all(targets.map(scrape));
    const oldest = Math.max(...m.map((x) => sum(x, 'wagering_outbox_oldest_pending_age_seconds')));
    const pending = Math.max(...m.map((x) => sum(x, 'wagering_outbox_pending_messages')));
    maxOldest = Math.max(maxOldest, oldest);
    if (pending === 0) {
      drained = true;
      break;
    }
    await Bun.sleep(500);
  }
  const drainSeconds = (performance.now() - drainStarted) / 1000;
  await Bun.sleep(3_000);
  stopDrain = true;
  await Promise.all(drainers);
  const after = await Promise.all(targets.map(scrape));
  const delta = (prefix: string) => after.reduce((a, m, i) => a + sum(m, prefix) - sum(before[i]!, prefix), 0);

  let inconsistent = 0;
  for (const w of wallets) {
    const rec = (await (await fetch(`${targets[0]}/wallets/${w.id}/reconciliation`, { method: 'POST' })).json()) as { consistent: boolean };
    if (!rec.consistent) inconsistent++;
  }

  const sorted = [...latencies].sort((a, b) => a - b);
  const total = latencies.length + networkErrors;
  const errors5xx = [...statuses].filter(([s]) => s.startsWith('5')).reduce((a, [, v]) => a + v, 0);
  const lagCount = delta('wagering_outbox_lag_seconds_count');
  const lagSum = delta('wagering_outbox_lag_seconds_sum');
  const report = {
    requests: total,
    durationSeconds: Number(elapsed.toFixed(1)),
    throughputRps: Number((latencies.length / elapsed).toFixed(1)),
    latencyMs: { p50: Number(pct(sorted, 50).toFixed(1)), p95: Number(pct(sorted, 95).toFixed(1)), p99: Number(pct(sorted, 99).toFixed(1)), max: Number((sorted.at(-1) ?? 0).toFixed(1)) },
    statusCodes: Object.fromEntries([...statuses].sort()),
    errorRate: Number(((errors5xx + networkErrors) / Math.max(total, 1)).toFixed(4)),
    networkErrors,
    lockConflicts: {
      contended: delta('wagering_lock_conflicts_total{kind="contended"}'),
      lockTimeout: delta('wagering_lock_conflicts_total{kind="lock_timeout"}'),
      deadlock: delta('wagering_lock_conflicts_total{kind="deadlock"}'),
      uniqueRace: delta('wagering_lock_conflicts_total{kind="unique_race"}'),
    },
    idempotentReplays: delta('wagering_duplicates_total{kind="idempotent_replay"'),
    outbox: {
      published: delta('wagering_outbox_published_total'),
      backlogAtEndOfLoad: backlogAtEnd,
      drained,
      drainSecondsAfterLoad: Number(drainSeconds.toFixed(1)),
      avgLagSeconds: lagCount ? Number((lagSum / lagCount).toFixed(3)) : 0,
      maxOldestPendingSeconds: Number(maxOldest.toFixed(2)),
    },
    downstreamConsumer: { messagesReceived: received, uniqueEventIds: eventIds.size, duplicates: received - eventIds.size },
    reconciliation: { wallets: wallets.length, inconsistent },
  };
  console.log(JSON.stringify(report, null, 2));
  if (inconsistent > 0) process.exit(1);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
