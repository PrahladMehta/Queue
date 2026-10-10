import { setTimeout as sleep } from 'node:timers/promises';
import { DEFAULT_RETRY, retryDelayMs, type BackoffOptions } from './backoff.ts';
import { errorMessage, PermanentError } from './errors.ts';
import type { Logger } from './logger.ts';
import { claimNext, markFailed, markSucceeded, retryLater, type Db } from './queue.ts';
import type { Job, JsonValue } from './types.ts';

export type Handler = (payload: JsonValue, job: Job) => Promise<void>;
export type Handlers = ReadonlyMap<string, Handler>;

export type TickResult =
  | { kind: 'idle' }
  | { kind: 'succeeded'; job: Job; durationMs: number }
  | { kind: 'retrying'; job: Job; error: string; delayMs: number; durationMs: number }
  | { kind: 'failed'; job: Job; error: string; durationMs: number };

export async function processNextJob(
  db: Db,
  handlers: Handlers,
  log: Logger,
  retry: BackoffOptions = DEFAULT_RETRY,
): Promise<TickResult> {
  const claimed = await claimNext(db);
  if (claimed === null) return { kind: 'idle' };

  const startedAt = performance.now();
  const elapsed = (): number => Math.round(performance.now() - startedAt);
  log.info('job.claimed', {
    jobId: claimed.id, type: claimed.type, attempt: claimed.attempts, maxAttempts: claimed.maxAttempts,
  });

  const handler = handlers.get(claimed.type);
  if (handler === undefined) {
    return fail(db, log, claimed, `no handler registered for job type '${claimed.type}'`, elapsed());
  }

  try {
    await handler(claimed.payload, claimed);
  } catch (err) {
    const error = errorMessage(err);
    if (err instanceof PermanentError || claimed.attempts >= claimed.maxAttempts) {
      return fail(db, log, claimed, error, elapsed());
    }
    return scheduleRetry(db, log, claimed, error, retryDelayMs(claimed.attempts, retry), elapsed());
  }

  // outside the try: a DB error here isn't a handler failure
  const job = await markSucceeded(db, claimed.id);
  const durationMs = elapsed();
  log.info('job.succeeded', { jobId: job.id, type: job.type, attempt: job.attempts, durationMs });
  return { kind: 'succeeded', job, durationMs };
}

async function scheduleRetry(
  db: Db, log: Logger, claimed: Job, error: string, delayMs: number, durationMs: number,
): Promise<TickResult> {
  const job = await retryLater(db, claimed.id, error, delayMs);
  log.error('job.retry_scheduled', {
    jobId: job.id, type: job.type, attempt: job.attempts, maxAttempts: job.maxAttempts, delayMs, error, durationMs,
  });
  return { kind: 'retrying', job, error, delayMs, durationMs };
}

async function fail(db: Db, log: Logger, claimed: Job, error: string, durationMs: number): Promise<TickResult> {
  const job = await markFailed(db, claimed.id, error);
  log.error('job.failed', { jobId: job.id, type: job.type, attempt: job.attempts, error, durationMs });
  return { kind: 'failed', job, error, durationMs };
}

export interface WorkerOptions {
  db: Db;
  handlers: Handlers;
  log: Logger;
  pollIntervalMs: number;
  retry?: BackoffOptions;
  signal: AbortSignal;
}

export async function runWorker(opts: WorkerOptions): Promise<void> {
  const { db, handlers, log, pollIntervalMs, retry = DEFAULT_RETRY, signal } = opts;
  log.info('worker.started', { pollIntervalMs, retryBaseMs: retry.baseMs, retryMaxMs: retry.maxMs });

  while (!signal.aborted) {
    let result: TickResult;
    try {
      result = await processNextJob(db, handlers, log, retry);
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
