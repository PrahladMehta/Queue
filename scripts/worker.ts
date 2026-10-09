import { loadConfig } from '../src/config.ts';
import { createPool } from '../src/db.ts';
import { createJsonLogger } from '../src/logger.ts';
import type { JsonValue } from '../src/types.ts';
import { runWorker, type Handler } from '../src/worker.ts';

function field(payload: JsonValue, key: string): JsonValue | undefined {
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) return undefined;
  return payload[key];
}

const handlers = new Map<string, Handler>([
  ['log', async (payload, job) => {
    console.log(`[job ${job.id}] ${String(field(payload, 'message') ?? '(no message)')}`);
  }],
  ['sleep', async (payload) => {
    const ms = field(payload, 'ms');
    if (typeof ms !== 'number' || ms < 0 || ms > 600_000) {
      throw new Error(`sleep: payload.ms must be a number between 0 and 600000, got ${JSON.stringify(ms)}`);
    }
    await new Promise((resolve) => setTimeout(resolve, ms));
  }],
  ['fail', async (payload) => {
    throw new Error(String(field(payload, 'message') ?? 'intentional failure'));
  }],
]);

const config = loadConfig();
const log = createJsonLogger({ worker: process.env.WORKER_NAME ?? `worker-${process.pid}` });
const pool = createPool(config.databaseUrl, 2);
const controller = new AbortController();

let stopping = false;
const onSignal = (): void => {
  if (stopping) {
    log.error('worker.forced_exit');
    process.exit(130);
  }
  stopping = true;
  log.info('worker.stopping');
  controller.abort();
};
process.on('SIGINT', onSignal);
process.on('SIGTERM', onSignal);

try {
  await runWorker({ db: pool, handlers, log, pollIntervalMs: config.pollIntervalMs, signal: controller.signal });
} finally {
  await pool.end();
}
