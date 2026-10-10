import { loadConfig } from '../src/config.ts';
import { createPool } from '../src/db.ts';
import { PermanentError } from '../src/errors.ts';
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
      throw new PermanentError(`sleep: payload.ms must be a number between 0 and 600000, got ${JSON.stringify(ms)}`);
    }
    await new Promise((resolve) => setTimeout(resolve, ms));
  }],
  ['fail', async (payload) => {
    const message = String(field(payload, 'message') ?? 'intentional failure');
    throw field(payload, 'permanent') === 1 ? new PermanentError(message) : new Error(message);
  }],
  // fails on the first `failTimes` attempts, then succeeds
  ['flaky', async (payload, job) => {
    const failTimes = field(payload, 'failTimes') ?? 2;
    if (typeof failTimes !== 'number') throw new PermanentError('flaky: payload.failTimes must be a number');
    if (job.attempts <= failTimes) throw new Error(`flaky: failing attempt ${job.attempts} of ${failTimes}`);
    console.log(`[job ${job.id}] flaky succeeded on attempt ${job.attempts}`);
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
  await runWorker({
    db: pool,
    handlers,
    log,
    pollIntervalMs: config.pollIntervalMs,
    retry: config.retry,
    signal: controller.signal,
  });
} finally {
  await pool.end();
}
