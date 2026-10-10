import assert from 'node:assert/strict';
import { after, before, beforeEach, describe, test } from 'node:test';
import type { Pool } from 'pg';
import { enqueue, getJob } from '../src/queue.ts';
import type { JsonValue } from '../src/types.ts';
import { processNextJob, runWorker, type Handler } from '../src/worker.ts';
import { createMemoryLogger, resetJobs, setupTestDb, statusCounts, waitFor } from './helpers.ts';

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

const noop: Handler = async () => {};

describe('processNextJob', () => {
  test('idle when empty', async () => {
    const result = await processNextJob(pool, new Map([['t', noop]]), createMemoryLogger());
    assert.deepEqual(result, { kind: 'idle' });
  });

  test('runs the handler and marks success', async () => {
    const seen: JsonValue[] = [];
    const handlers = new Map<string, Handler>([['email', async (payload) => void seen.push(payload)]]);
    const log = createMemoryLogger();
    const { id } = await enqueue(pool, 'email', { to: 'a@example.com' });

    const result = await processNextJob(pool, handlers, log);

    assert.equal(result.kind, 'succeeded');
    assert.deepEqual(seen, [{ to: 'a@example.com' }]);
    assert.equal((await getJob(pool, id))?.status, 'succeeded');
    assert.deepEqual(log.entries.map((e) => e.event), ['job.claimed', 'job.succeeded']);
    assert.equal(log.entries[1]?.fields.jobId, id);
  });

  test('handler throws on last attempt -> job failed', async () => {
    const handlers = new Map<string, Handler>([['email', async () => { throw new Error('SMTP 550'); }]]);
    const log = createMemoryLogger();
    const { id } = await enqueue(pool, 'email', {}, { maxAttempts: 1 });

    const result = await processNextJob(pool, handlers, log);

    assert.equal(result.kind, 'failed');
    const job = await getJob(pool, id);
    assert.equal(job?.status, 'failed');
    assert.equal(job?.lastError, 'SMTP 550');
    assert.equal(job?.attempts, 1);
    assert.ok(log.entries.some((e) => e.level === 'error' && e.event === 'job.failed'));
  });

  test('handler throws a non-Error', async () => {
    const handlers = new Map<string, Handler>([['t', async () => { throw 'plain string'; }]]);
    const { id } = await enqueue(pool, 't');
    await processNextJob(pool, handlers, createMemoryLogger());
    assert.equal((await getJob(pool, id))?.lastError, 'plain string');
  });

  test('unknown type -> job failed', async () => {
    const { id } = await enqueue(pool, 'nobody_handles_this');
    const result = await processNextJob(pool, new Map([['t', noop]]), createMemoryLogger());
    assert.equal(result.kind, 'failed');
    const job = await getJob(pool, id);
    assert.equal(job?.status, 'failed');
    assert.match(job?.lastError ?? '', /no handler registered/);
  });

  test("type 'toString' is not resolved from Object.prototype", async () => {
    const { id } = await enqueue(pool, 'toString');
    await processNextJob(pool, new Map([['t', noop]]), createMemoryLogger());
    assert.equal((await getJob(pool, id))?.status, 'failed');
  });
});

describe('runWorker', () => {
  test('keeps going after a failed job', async () => {
    const handlers = new Map<string, Handler>([
      ['ok', noop],
      ['bad', async () => { throw new Error('nope'); }],
    ]);
    await enqueue(pool, 'ok');
    await enqueue(pool, 'bad', {}, { maxAttempts: 1 });
    await enqueue(pool, 'ok');

    const controller = new AbortController();
    const running = runWorker({
      db: pool, handlers, log: createMemoryLogger(), pollIntervalMs: 10, signal: controller.signal,
    });
    await waitFor(async () => {
      const counts = await statusCounts(pool);
      return counts.queued === undefined && counts.running === undefined;
    }, 5_000, 'all jobs to finish');
    controller.abort();
    await running;

    assert.deepEqual(await statusCounts(pool), { succeeded: 2, failed: 1 });
  });

  test('abort interrupts the idle wait', async () => {
    const controller = new AbortController();
    const log = createMemoryLogger();
    const startedAt = Date.now();
    const running = runWorker({
      db: pool, handlers: new Map(), log, pollIntervalMs: 60_000, signal: controller.signal,
    });
    setTimeout(() => controller.abort(), 50);
    await running;

    assert.ok(Date.now() - startedAt < 2_000);
    assert.equal(log.entries.at(-1)?.event, 'worker.stopped');
  });
});
