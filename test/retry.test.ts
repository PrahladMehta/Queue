import assert from 'node:assert/strict';
import { after, before, beforeEach, describe, test } from 'node:test';
import type { Pool } from 'pg';
import { PermanentError } from '../src/errors.ts';
import { claimNext, enqueue, getJob, InvalidTransitionError, retryLater } from '../src/queue.ts';
import { processNextJob, type Handler, type TickResult } from '../src/worker.ts';
import { createMemoryLogger, makeDue, resetJobs, setupTestDb } from './helpers.ts';

const RETRY = { baseMs: 1000, maxMs: 60_000 };

let pool: Pool;
before(async () => {
  pool = await setupTestDb();
});
after(async () => {
  await pool.end();
});
beforeEach(async () => {
  await resetJobs(pool);
});

// run one job to a final state, skipping the backoff waits
async function runToEnd(handlers: Map<string, Handler>): Promise<TickResult[]> {
  const results: TickResult[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await processNextJob(pool, handlers, createMemoryLogger(), RETRY);
    if (result.kind === 'idle') return results;
    results.push(result);
    await makeDue(pool);
  }
  throw new Error('job never reached a final state');
}

describe('retryLater', () => {
  test('puts a running job back in the queue with a future run_at', async () => {
    const { id } = await enqueue(pool, 't');
    await claimNext(pool);
    const job = await retryLater(pool, id, 'timeout', 60_000);

    assert.equal(job.status, 'queued');
    assert.equal(job.attempts, 1);
    assert.equal(job.lastError, 'timeout');
    assert.equal(job.finishedAt, null);
    const { rows } = await pool.query<{ secs: number }>(
      'SELECT extract(epoch FROM run_at - now())::float AS secs FROM jobs WHERE id = $1',
      [id],
    );
    assert.ok((rows[0]?.secs ?? 0) > 55, 'run_at should be ~60s ahead');
  });

  test('job is not claimable until run_at', async () => {
    const { id } = await enqueue(pool, 't');
    await claimNext(pool);
    await retryLater(pool, id, 'timeout', 60_000);

    assert.equal(await claimNext(pool), null);
    await makeDue(pool);
    const again = await claimNext(pool);
    assert.equal(again?.id, id);
    assert.equal(again?.attempts, 2);
  });

  test('due retries are claimed in run_at order', async () => {
    const a = await enqueue(pool, 'a');
    const b = await enqueue(pool, 'b');
    await claimNext(pool);
    await claimNext(pool);
    await retryLater(pool, a.id, 'x', 2000);
    await retryLater(pool, b.id, 'x', 1000);
    await pool.query(`UPDATE jobs SET run_at = run_at - interval '1 hour'`);

    assert.equal((await claimNext(pool))?.id, b.id);
    assert.equal((await claimNext(pool))?.id, a.id);
  });

  test('rejects a job that is not running', async () => {
    const { id } = await enqueue(pool, 't');
    await assert.rejects(retryLater(pool, id, 'x', 0), InvalidTransitionError);
  });

  test('rejects a job with no attempts left', async () => {
    const { id } = await enqueue(pool, 't', {}, { maxAttempts: 1 });
    await claimNext(pool);
    await assert.rejects(retryLater(pool, id, 'x', 0), /no attempts left/);
    assert.equal((await getJob(pool, id))?.status, 'running');
  });

  test('rejects a negative delay', async () => {
    const { id } = await enqueue(pool, 't');
    await claimNext(pool);
    await assert.rejects(retryLater(pool, id, 'x', -1), /non-negative/);
  });
});

describe('worker retries', () => {
  test('failure schedules a retry', async () => {
    const log = createMemoryLogger();
    const handlers = new Map<string, Handler>([['t', async () => { throw new Error('503'); }]]);
    const { id } = await enqueue(pool, 't');

    const result = await processNextJob(pool, handlers, log, RETRY);

    assert.equal(result.kind, 'retrying');
    assert.ok(result.kind === 'retrying' && result.delayMs >= 0 && result.delayMs < RETRY.baseMs);
    const job = await getJob(pool, id);
    assert.equal(job?.status, 'queued');
    assert.equal(job?.attempts, 1);
    assert.equal(job?.lastError, '503');
    const entry = log.entries.find((e) => e.event === 'job.retry_scheduled');
    assert.equal(entry?.fields.attempt, 1);
    assert.equal(entry?.fields.maxAttempts, 5);
  });

  test('succeeds on a later attempt', async () => {
    const handlers = new Map<string, Handler>([['flaky', async (_payload, job) => {
      if (job.attempts < 3) throw new Error(`attempt ${job.attempts} failed`);
    }]]);
    const { id } = await enqueue(pool, 'flaky');

    const results = await runToEnd(handlers);

    assert.deepEqual(results.map((r) => r.kind), ['retrying', 'retrying', 'succeeded']);
    const job = await getJob(pool, id);
    assert.equal(job?.status, 'succeeded');
    assert.equal(job?.attempts, 3);
  });

  test('gives up after max_attempts', async () => {
    let calls = 0;
    const handlers = new Map<string, Handler>([['t', async (_payload, job) => {
      calls++;
      throw new Error(`boom ${job.attempts}`);
    }]]);
    const { id } = await enqueue(pool, 't', {}, { maxAttempts: 3 });

    const results = await runToEnd(handlers);

    assert.deepEqual(results.map((r) => r.kind), ['retrying', 'retrying', 'failed']);
    assert.equal(calls, 3);
    const job = await getJob(pool, id);
    assert.equal(job?.status, 'failed');
    assert.equal(job?.attempts, 3);
    assert.equal(job?.lastError, 'boom 3');
    assert.ok(job?.finishedAt instanceof Date);
  });

  test('PermanentError fails on the first attempt', async () => {
    const handlers = new Map<string, Handler>([['t', async () => { throw new PermanentError('bad payload'); }]]);
    const { id } = await enqueue(pool, 't');

    const results = await runToEnd(handlers);

    assert.deepEqual(results.map((r) => r.kind), ['failed']);
    const job = await getJob(pool, id);
    assert.equal(job?.attempts, 1);
    assert.equal(job?.lastError, 'bad payload');
  });

  test('unknown type fails on the first attempt', async () => {
    const { id } = await enqueue(pool, 'nobody_handles_this');
    const results = await runToEnd(new Map());
    assert.deepEqual(results.map((r) => r.kind), ['failed']);
    assert.equal((await getJob(pool, id))?.attempts, 1);
  });

  test('maxAttempts 1 means no retry', async () => {
    const handlers = new Map<string, Handler>([['t', async () => { throw new Error('nope'); }]]);
    await enqueue(pool, 't', {}, { maxAttempts: 1 });
    const results = await runToEnd(handlers);
    assert.deepEqual(results.map((r) => r.kind), ['failed']);
  });
});

describe('constraints', () => {
  test('enqueue rejects invalid maxAttempts', async () => {
    await assert.rejects(enqueue(pool, 't', {}, { maxAttempts: 0 }), /positive integer/);
    await assert.rejects(enqueue(pool, 't', {}, { maxAttempts: 1.5 }), /positive integer/);
  });

  test('db rejects max_attempts < 1', async () => {
    await assert.rejects(
      pool.query(`INSERT INTO jobs (type, max_attempts) VALUES ('t', 0)`),
      { code: '23514' },
    );
  });

  test('db rejects attempts > max_attempts', async () => {
    const { id } = await enqueue(pool, 't', {}, { maxAttempts: 2 });
    await assert.rejects(
      pool.query('UPDATE jobs SET attempts = 3 WHERE id = $1', [id]),
      { code: '23514' },
    );
  });
});
