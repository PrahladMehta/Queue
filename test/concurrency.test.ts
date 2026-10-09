import assert from 'node:assert/strict';
import { after, before, beforeEach, test } from 'node:test';
import type { Pool } from 'pg';
import { claimNext, enqueue } from '../src/queue.ts';
import { resetJobs, setupTestDb } from './helpers.ts';

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

test('two workers, one job: exactly one wins (50 rounds)', async () => {
  for (let round = 0; round < 50; round++) {
    await enqueue(pool, 'contested');
    const [a, b] = await Promise.all([claimNext(pool), claimNext(pool)]);
    const winners = [a, b].filter((job) => job !== null).length;
    assert.equal(winners, 1, `round ${round}: ${winners} winners`);
  }
});
