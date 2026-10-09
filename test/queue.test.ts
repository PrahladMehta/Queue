import assert from 'node:assert/strict';
import { after, before, beforeEach, describe, test } from 'node:test';
import type { Pool } from 'pg';
import { withTransaction } from '../src/db.ts';
import { claimNext, enqueue, getJob, InvalidTransitionError, markFailed, markSucceeded } from '../src/queue.ts';
import { resetJobs, setupTestDb } from './helpers.ts';

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

describe('enqueue', () => {
  test('creates a queued job', async () => {
    const job = await enqueue(pool, 'send_email', { userId: 42 });
    assert.equal(typeof job.id, 'string');
    assert.equal(job.type, 'send_email');
    assert.deepEqual(job.payload, { userId: 42 });
    assert.equal(job.status, 'queued');
    assert.equal(job.attempts, 0);
    assert.equal(job.lastError, null);
    assert.ok(job.createdAt instanceof Date);
    assert.equal(job.startedAt, null);
    assert.equal(job.finishedAt, null);
  });

  test('stores arrays as JSON', async () => {
    const payload = { ids: [1, 2, 3], nested: { ok: true, note: null } };
    assert.deepEqual((await enqueue(pool, 'batch', payload)).payload, payload);
    assert.deepEqual((await enqueue(pool, 'batch', [1, 2, 3])).payload, [1, 2, 3]);
  });

  test('rejects a blank type', async () => {
    await assert.rejects(enqueue(pool, '   '), /non-empty/);
  });

  test('db rejects an unknown status', async () => {
    await assert.rejects(
      pool.query(`INSERT INTO jobs (type, status) VALUES ('x', 'sucess')`),
      { code: '23514' },
    );
  });

  test('rolled back with the surrounding transaction', async () => {
    await assert.rejects(
      withTransaction(pool, async (client) => {
        await enqueue(client, 'send_welcome_email', { userId: 7 });
        throw new Error('signup failed');
      }),
      /signup failed/,
    );
    const { rows } = await pool.query<{ n: number }>('SELECT count(*)::int AS n FROM jobs');
    assert.equal(rows[0]?.n, 0);
  });
});

describe('claimNext', () => {
  test('returns null when empty', async () => {
    assert.equal(await claimNext(pool), null);
  });

  test('claims the oldest job', async () => {
    const first = await enqueue(pool, 'a');
    await enqueue(pool, 'b');

    const claimed = await claimNext(pool);
    assert.ok(claimed);
    assert.equal(claimed.id, first.id);
    assert.equal(claimed.status, 'running');
    assert.equal(claimed.attempts, 1);
    assert.ok(claimed.startedAt instanceof Date);
  });

  test('does not hand out the same job twice', async () => {
    await enqueue(pool, 'only');
    assert.ok(await claimNext(pool));
    assert.equal(await claimNext(pool), null);
  });

  test('ignores finished jobs', async () => {
    const done = await enqueue(pool, 'done');
    await claimNext(pool);
    await markSucceeded(pool, done.id);
    const waiting = await enqueue(pool, 'waiting');

    assert.equal((await claimNext(pool))?.id, waiting.id);
  });

  test('skips rows locked by another transaction', async () => {
    const first = await enqueue(pool, 'a');
    const second = await enqueue(pool, 'b');

    const other = await pool.connect();
    try {
      await other.query('BEGIN');
      assert.equal((await claimNext(other))?.id, first.id);

      assert.equal((await claimNext(pool))?.id, second.id);
      assert.equal(await claimNext(pool), null);

      await other.query('COMMIT');
    } finally {
      other.release(true);
    }
    assert.equal((await getJob(pool, first.id))?.status, 'running');
  });
});

describe('transitions', () => {
  test('running -> succeeded', async () => {
    const { id } = await enqueue(pool, 't');
    await claimNext(pool);
    const job = await markSucceeded(pool, id);
    assert.equal(job.status, 'succeeded');
    assert.equal(job.lastError, null);
    assert.ok(job.finishedAt instanceof Date);
    assert.ok(job.startedAt !== null && job.finishedAt >= job.startedAt);
  });

  test('running -> failed', async () => {
    const { id } = await enqueue(pool, 't');
    await claimNext(pool);
    const job = await markFailed(pool, id, 'smtp timeout');
    assert.equal(job.status, 'failed');
    assert.equal(job.lastError, 'smtp timeout');
    assert.ok(job.finishedAt instanceof Date);
  });

  test('queued -> succeeded is rejected', async () => {
    const { id } = await enqueue(pool, 't');
    await assert.rejects(markSucceeded(pool, id), (err: unknown) => {
      assert.ok(err instanceof InvalidTransitionError);
      assert.equal(err.actual, 'queued');
      return true;
    });
    const job = await getJob(pool, id);
    assert.equal(job?.status, 'queued');
    assert.equal(job?.finishedAt, null);
  });

  test('succeeded is final', async () => {
    const { id } = await enqueue(pool, 't');
    await claimNext(pool);
    await markSucceeded(pool, id);
    await assert.rejects(markSucceeded(pool, id), InvalidTransitionError);
    await assert.rejects(markFailed(pool, id, 'late'), InvalidTransitionError);
    const job = await getJob(pool, id);
    assert.equal(job?.status, 'succeeded');
    assert.equal(job?.lastError, null);
  });

  test('failed is final', async () => {
    const { id } = await enqueue(pool, 't');
    await claimNext(pool);
    await markFailed(pool, id, 'boom');
    await assert.rejects(markSucceeded(pool, id), InvalidTransitionError);
  });

  test('missing job', async () => {
    await assert.rejects(markSucceeded(pool, '999999'), (err: unknown) => {
      assert.ok(err instanceof InvalidTransitionError);
      assert.equal(err.actual, null);
      return true;
    });
  });
});
