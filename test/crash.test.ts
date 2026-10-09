import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { after, before, test } from 'node:test';
import type { Pool } from 'pg';
import { claimNext, enqueue, getJob } from '../src/queue.ts';
import { resetJobs, setupTestDb, testDatabaseUrl, waitFor } from './helpers.ts';

const WORKER_SCRIPT = path.join(import.meta.dirname, '..', 'scripts', 'worker.ts');

let pool: Pool;
before(async () => {
  pool = await setupTestDb();
  await resetJobs(pool);
});
after(async () => {
  await pool.end();
});

// Documents current behaviour: no leases yet, so a dead worker's job is stuck.
test('killed worker leaves its job running', async () => {
  const { id } = await enqueue(pool, 'sleep', { ms: 30_000 });

  const child = spawn(process.execPath, [WORKER_SCRIPT], {
    env: { ...process.env, DATABASE_URL: testDatabaseUrl(), POLL_INTERVAL_MS: '50' },
    stdio: 'ignore',
  });
  const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));

  try {
    await waitFor(async () => (await getJob(pool, id))?.status === 'running', 10_000, 'worker to claim the job');
  } finally {
    child.kill('SIGKILL');
    await exited;
  }

  await new Promise((resolve) => setTimeout(resolve, 500));
  const job = await getJob(pool, id);
  assert.equal(job?.status, 'running');
  assert.equal(job?.finishedAt, null);
  assert.equal(await claimNext(pool), null);
});
