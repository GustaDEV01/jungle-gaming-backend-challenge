import { loadConfig } from '../src/infrastructure/config';
import { createSqsClient, ensureQueues } from '../src/infrastructure/messaging/sqs';

/** Idempotent provisioning of the FIFO queues + DLQ (redrive policy). The app also does it on startup. */
const config = loadConfig();
const urls = await ensureQueues(createSqsClient(config.sqs), config.sqs);
console.log(JSON.stringify(urls, null, 2));
