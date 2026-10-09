import { setTimeout as sleep } from 'node:timers/promises';
import { errorMessage } from './errors.ts';
import type { Logger } from './logger.ts';
import { claimNext, markFailed, markSucceeded, type Db } from './queue.ts';
import type { Job, JsonValue } from './types.ts';

export type Handler = (payload: JsonValue, job: Job) => Promise<void>;
export type Handlers = ReadonlyMap<string, Handler>;

export type TickResult =
  | { kind: 'idle' }
  | { kind: 'succeeded'; job: Job; durationMs: number }
  | { kind: 'failed'; job: Job; error: string; durationMs: number };

export async function processNextJob(db: Db, handlers: Handlers, log: Logger): Promise<TickResult> {
  const claimed = await claimNext(db);
  if (claimed === null) return { kind: 'idle' };

  const startedAt = performance.now();
  const elapsed = (): number => Math.round(performance.now() - startedAt);
  log.info('job.claimed', { jobId: claimed.id, type: claimed.type, attempts: claimed.attempts });

  const handler = handlers.get(claimed.type);
  if (handler === undefined) {
    return fail(db, log, claimed, `no handler registered for job type '${claimed.type}'`, elapsed());
  }

  try {
    await handler(claimed.payload, claimed);
  } catch (err) {
    return fail(db, log, claimed, errorMessage(err), elapsed());
  }

  // outside the try: a DB error here isn't a handler failure
  const job = await markSucceeded(db, claimed.id);
  const durationMs = elapsed();
  log.info('job.succeeded', { jobId: job.id, type: job.type, durationMs });
  return { kind: 'succeeded', job, durationMs };
}

async function fail(db: Db, log: Logger, claimed: Job, error: string, durationMs: number): Promise<TickResult> {
  const job = await markFailed(db, claimed.id, error);
  log.error('job.failed', { jobId: job.id, type: job.type, error, durationMs });
  return { kind: 'failed', job, error, durationMs };
}

export interface WorkerOptions {
  db: Db;
  handlers: Handlers;
  log: Logger;
  pollIntervalMs: number;
  signal: AbortSignal;
}

export async function runWorker(opts: WorkerOptions): Promise<void> {
  const { db, handlers, log, pollIntervalMs, signal } = opts;
  log.info('worker.started', { pollIntervalMs });

  while (!signal.aborted) {
    let result: TickResult;
    try {
      result = await processNextJob(db, handlers, log);
    } catch (err) {
      log.error('worker.error', { error: errorMessage(err) });
      await pause(pollIntervalMs, signal);
      continue;
    }
    if (result.kind === 'idle') await pause(pollIntervalMs, signal);
  }

  log.info('worker.stopped');
}

async function pause(ms: number, signal: AbortSignal): Promise<void> {
  try {
    await sleep(ms, undefined, { signal });
  } catch (err) {
    if (!signal.aborted) throw err;
  }
}
