// npm run enqueue -- <type> [key=value ...] [--count N]
import { parseArgs } from 'node:util';
import { loadConfig } from '../src/config.ts';
import { createPool } from '../src/db.ts';
import { enqueue } from '../src/queue.ts';
import type { JsonValue } from '../src/types.ts';

const USAGE = 'usage: npm run enqueue -- <type> [key=value ...] [--count N]';

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: { count: { type: 'string', default: '1' } },
});

const [type, ...pairs] = positionals;
if (type === undefined) {
  console.error(USAGE);
  process.exit(2);
}

const count = Number(values.count);
if (!Number.isInteger(count) || count < 1) {
  console.error(`--count must be a positive integer, got '${values.count}'`);
  process.exit(2);
}

const payload: Record<string, JsonValue> = {};
for (const pair of pairs) {
  const eq = pair.indexOf('=');
  if (eq <= 0) {
    console.error(`expected key=value, got '${pair}'\n${USAGE}`);
    process.exit(2);
  }
  const raw = pair.slice(eq + 1);
  payload[pair.slice(0, eq)] = raw !== '' && Number.isFinite(Number(raw)) ? Number(raw) : raw;
}

const pool = createPool(loadConfig().databaseUrl, 1);
try {
  for (let i = 0; i < count; i++) {
    const job = await enqueue(pool, type, payload);
    console.log(`enqueued job ${job.id} type=${job.type} payload=${JSON.stringify(job.payload)}`);
  }
} finally {
  await pool.end();
}
