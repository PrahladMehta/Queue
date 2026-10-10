// Races workers using a two-query claim (SELECT, then UPDATE) against claimNext
// and counts jobs that got claimed more than once.
import type { Pool } from 'pg';
import { loadConfig } from '../../src/config.ts';
import { createPool } from '../../src/db.ts';
import { claimNext } from '../../src/queue.ts';

const JOBS = 300;
const WORKERS = 10;
const TYPE = 'experiment.naive_claim';

async function naiveClaim(pool: Pool): Promise<string | null> {
  const found = await pool.query<{ id: string }>(
    `SELECT id FROM jobs WHERE type = $1 AND status = 'queued' ORDER BY id LIMIT 1`,
    [TYPE],
  );
  const id = found.rows[0]?.id;
  if (id === undefined) return null;
  await pool.query(`UPDATE jobs SET status = 'running', attempts = attempts + 1 WHERE id = $1`, [id]);
  return id;
}

async function race(pool: Pool, label: string, claim: () => Promise<string | null>): Promise<void> {
  await pool.query('DELETE FROM jobs WHERE type = $1', [TYPE]);
  // high max_attempts so double claims show up in the count instead of tripping the CHECK
  await pool.query(
    `INSERT INTO jobs (type, max_attempts) SELECT $1, 1000 FROM generate_series(1, $2::int)`,
    [TYPE, JOBS],
  );

  let claims = 0;
  await Promise.all(
    Array.from({ length: WORKERS }, async () => {
      while ((await claim()) !== null) claims++;
    }),
  );

  const stats = await pool.query<{ doubled: string; max_attempts: number }>(
    `SELECT count(*) FILTER (WHERE attempts > 1) AS doubled, max(attempts) AS max_attempts
     FROM jobs WHERE type = $1`,
    [TYPE],
  );
  const s = stats.rows[0];
  console.log(
    `${label.padEnd(10)} jobs=${JOBS} claims=${claims} ` +
      `claimed more than once=${s?.doubled} max attempts=${s?.max_attempts}`,
  );

  const leftover = await pool.query(
    `SELECT id, status, attempts, run_at, clock_timestamp() AS db_now
     FROM jobs WHERE type = $1 AND status = 'queued' ORDER BY id`,
    [TYPE],
  );
  if (leftover.rowCount !== 0) {
    console.log(`  ${leftover.rowCount} job(s) never claimed:`);
    for (const row of leftover.rows.slice(0, 10)) console.log(`  ${JSON.stringify(row)}`);
  }
}

const pool = createPool(loadConfig().databaseUrl, WORKERS + 2);
try {
  await race(pool, 'naive', () => naiveClaim(pool));

  // claimNext isn't filtered by type, so it would also grab any real queued jobs
  const others = await pool.query<{ n: string }>(
    `SELECT count(*) AS n FROM jobs WHERE status = 'queued' AND type <> $1`,
    [TYPE],
  );
  if (others.rows[0]?.n !== '0') {
    console.log('skipping claimNext run: other queued jobs exist');
  } else {
    await race(pool, 'claimNext', async () => (await claimNext(pool))?.id ?? null);
  }
} finally {
  await pool.query('DELETE FROM jobs WHERE type = $1', [TYPE]);
  await pool.end();
}
