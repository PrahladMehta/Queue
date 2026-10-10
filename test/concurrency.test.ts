import assert from 'node:assert/strict';
import { after, before, beforeEach, test } from 'node:test';
import type { Pool } from 'pg';
import { claimNext, enqueue } from '../src/queue.ts';
import { processNextJob, type Handler } from '../src/worker.ts';
import { createMemoryLogger, resetJobs, setupTestDb, statusCounts } from './helpers.ts';

const WORKERS = 10;
const JOBS = 500;

let pool: Pool;
before(async () => {
  pool = await setupTestDb(WORKERS + 5);
});
after(async () => {
  await pool.end();
});
beforeEach(async () => {
  await resetJobs(pool);
});

test(`${WORKERS} workers drain ${JOBS} jobs, each claimed once`, async () => {
  await pool.query(
    `INSERT INTO jobs (type, payload) SELECT 'bulk', jsonb_build_object('n', g) FROM generate_series(1, $1::int) g`,
    [JOBS],
  );

  const claimedByWorker = await Promise.all(
    Array.from({ length: WORKERS }, async () => {
      const ids: string[] = [];
      for (let job = await claimNext(pool); job !== null; job = await claimNext(pool)) {
        ids.push(job.id);
      }
      return ids;
    }),
  );

  const all = claimedByWorker.flat();
  assert.equal(all.length, JOBS);
  assert.equal(new Set(all).size, JOBS);

  const { rows } = await pool.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM jobs WHERE status = 'running' AND attempts = 1`,
  );
  assert.equal(rows[0]?.n, JOBS);

  const busy = claimedByWorker.filter((ids) => ids.length > 0).length;
  assert.ok(busy >= 2, `only ${busy} worker(s) got jobs`);
  console.log(`  jobs per worker: ${claimedByWorker.map((ids) => ids.length).join(', ')}`);
});

test('random failures + retries: no overlapping runs, every attempt counted', async () => {
  const COUNT = 200;
  const MAX = 4;
  await pool.query(
    `INSERT INTO jobs (type, max_attempts) SELECT 'flaky', $2 FROM generate_series(1, $1::int)`,
    [COUNT, MAX],
  );

  const calls = new Map<string, number>();
  const inFlight = new Set<string>();
  const overlaps: string[] = [];
  const handlers = new Map<string, Handler>([['flaky', async (_payload, job) => {
    if (inFlight.has(job.id)) overlaps.push(job.id);
    inFlight.add(job.id);
    calls.set(job.id, (calls.get(job.id) ?? 0) + 1);
    try {
      await new Promise((resolve) => setTimeout(resolve, Math.random() * 3));
      if (Math.random() < 0.5) throw new Error('random failure');
    } finally {
      inFlight.delete(job.id);
    }
  }]]);
  const noWait = { baseMs: 0, maxMs: 0 };
  const log = createMemoryLogger();

  await Promise.all(
    Array.from({ length: WORKERS }, async () => {
      for (;;) {
        const result = await processNextJob(pool, handlers, log, noWait);
        if (result.kind !== 'idle') continue;
        const counts = await statusCounts(pool);
        if (counts.queued === undefined && counts.running === undefined) return;
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    }),
  );

  assert.deepEqual(overlaps, []);
  const { rows } = await pool.query<{ id: string; status: string; attempts: number }>(
    'SELECT id, status, attempts FROM jobs',
  );
  assert.equal(rows.length, COUNT);
  for (const row of rows) {
    assert.ok(row.status === 'succeeded' || row.status === 'failed', `job ${row.id} is ${row.status}`);
    assert.equal(calls.get(row.id), row.attempts, `job ${row.id}: handler calls vs attempts`);
    if (row.status === 'failed') assert.equal(row.attempts, MAX, `failed job ${row.id} used all attempts`);
  }
  const counts = await statusCounts(pool);
  const totalCalls = [...calls.values()].reduce((a, b) => a + b, 0);
  console.log(`  succeeded=${counts.succeeded ?? 0} failed=${counts.failed ?? 0} handler calls=${totalCalls}`);
});

test('two workers, one job: exactly one wins (50 rounds)', async () => {
  for (let round = 0; round < 50; round++) {
    await enqueue(pool, 'contested');
    const [a, b] = await Promise.all([claimNext(pool), claimNext(pool)]);
    const winners = [a, b].filter((job) => job !== null).length;
    assert.equal(winners, 1, `round ${round}: ${winners} winners`);
  }
});
